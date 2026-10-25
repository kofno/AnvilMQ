use crate::{
    leases,
    queue::v1::{AddJobBulkRequest, AddJobBulkResponse, AddJobRequest, AddJobResponse},
    telemetry::Metrics,
    MyQueueService,
};
use prost::Message;
use rusqlite::{OptionalExtension, TransactionBehavior};
use tonic::Status;

/// Maximum number of jobs a single `AddJobBulk` call may submit. The whole batch is one
/// atomic IMMEDIATE transaction, so this bounds both memory and how long the single SQLite
/// write lock is held.
pub const MAX_BULK_BATCH_SIZE: usize = 5000;

fn internal(error: rusqlite::Error) -> Status {
    Status::internal(error.to_string())
}

fn db_fail(error: rusqlite::Error) -> EnqueueFailure {
    EnqueueFailure::Status(internal(error))
}

/// The forensic facts for one admission-time enqueue rejection. The in-tx core builds this
/// instead of persisting it directly, because recording needs a separate transaction that
/// cannot run while the (possibly batched) enqueue transaction holds the single SQLite write
/// lock. The caller persists it via `record_rejection` after rolling the enqueue tx back.
struct RejectionRecord {
    kind: &'static str,
    trace_id: Option<String>,
    parent_id: Option<String>,
    execution_depth: Option<i64>,
    rate_limit_facet: Option<String>,
    detail: String,
}

/// Why a single job could not be enqueued inside the shared transaction.
///
/// `Status` carries a plain gRPC error (validation inside the tx, an internal DB error, or an
/// idempotency conflict). `Rejected` is a business rejection that must additionally be
/// durably recorded (see `RejectionRecord`) and counted on the rejection metrics; `name` is
/// the queue name for the rejection record.
enum EnqueueFailure {
    Status(Status),
    Rejected {
        status: Status,
        record: RejectionRecord,
        name: String,
    },
}

/// A deferred success-metric intention. Success counters MUST NOT fire until the enqueue
/// transaction commits, otherwise a rolled-back atomic batch would inflate them. The core
/// returns one of these per job and the caller flushes them only after `tx.commit()`.
enum SuccessMetric {
    Inserted {
        job_id: String,
        state: &'static str,
        name: String,
        receipt_created: bool,
    },
    Replayed {
        job_id: String,
    },
}

impl SuccessMetric {
    fn flush(&self, metrics: &Metrics) {
        match self {
            SuccessMetric::Inserted {
                job_id,
                state,
                name,
                receipt_created,
            } => {
                if *receipt_created {
                    metrics.enqueue_receipt_created();
                }
                metrics.transition(None, state, "enqueued");
                metrics.named.event(name, "enqueued");
                tracing::info!(job_id = %job_id, attempt = 0, to = state, "job transition");
            }
            SuccessMetric::Replayed { job_id } => {
                metrics.enqueue_replay();
                tracing::info!(job_id = %job_id, "enqueue receipt replayed");
            }
        }
    }
}

/// The result of enqueuing one job inside the shared transaction: the response returned to
/// the client plus the success metric to flush after commit.
struct EnqueueOutcome {
    response: AddJobResponse,
    success: SuccessMetric,
}

/// Fire the rejection metric(s) for a persisted rejection record. Mirrors the per-kind
/// metric pairing from the original single-enqueue path.
fn fire_rejection_metric(metrics: &Metrics, record: &RejectionRecord) {
    match record.kind {
        "ingress_velocity" => metrics.ingress_rejection(),
        "chain_quarantine" => metrics.chain_quarantine(),
        // parent_not_found, inconsistent_depth, circuit_breaker
        _ => metrics.ancestry_rejection(),
    }
    metrics.rejection_recorded(record.kind);
}

/// Persist an admission-time enqueue rejection for post-hoc forensics.
///
/// Runs in its OWN fresh IMMEDIATE transaction on `conn`. The caller must have already
/// dropped the main enqueue transaction (rolling it back) so the rejected job consumes no
/// ingress quota and leaves no job row; recording the rejection separately keeps that
/// rollback intact while still durably capturing why the enqueue was refused. `conn` is a
/// MutexGuard at the call site; deref coercion passes it as `&mut Connection`. A failure to
/// persist is logged and swallowed so it never masks the original rejection returned to the
/// caller.
fn record_rejection(
    conn: &mut rusqlite::Connection,
    now: i64,
    name: &str,
    record: &RejectionRecord,
) {
    let outcome = (|| -> rusqlite::Result<()> {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO enqueue_rejections (rejected_at, kind, name, trace_id, parent_id, execution_depth, rate_limit_facet, detail) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            rusqlite::params![
                now,
                record.kind,
                name,
                record.trace_id,
                record.parent_id,
                record.execution_depth,
                record.rate_limit_facet,
                record.detail
            ],
        )?;
        tx.commit()
    })();
    if let Err(error) = outcome {
        tracing::warn!(%error, kind = record.kind, name, "failed to persist enqueue rejection record");
    }
}

/// Validate and normalize one `AddJobRequest` BEFORE any transaction opens, returning the
/// serialized idempotency receipt request (`None` when deduplication is disabled). Failing
/// here is cheap and, for an atomic batch, means nothing is written and no tx is opened.
fn validate_and_prepare(
    req: &mut AddJobRequest,
    max_execution_depth: u32,
) -> Result<Option<Vec<u8>>, Status> {
    if !req.idempotency_key.is_empty()
        && (req.idempotency_key.trim().is_empty() || req.idempotency_key.len() > 256)
    {
        return Err(Status::invalid_argument(
            "idempotency_key must be nonblank and at most 256 UTF-8 bytes",
        ));
    }
    if req.delay_ms < 0 || req.retry_backoff_ms < 0 || req.retry_backoff_max_ms < 0 {
        return Err(Status::invalid_argument("delays must not be negative"));
    }
    if req.retry_backoff_max_ms == 0 {
        req.retry_backoff_max_ms = 60_000.max(req.retry_backoff_ms);
    }
    if req.retry_backoff_max_ms < req.retry_backoff_ms {
        return Err(Status::invalid_argument(
            "backoff cap must be at least the base",
        ));
    }
    if req.retry_jitter.is_nan() || !(0.0..=1.0).contains(&req.retry_jitter) {
        return Err(Status::invalid_argument(
            "retry_jitter must be a fraction in [0, 1]",
        ));
    }
    if req.max_attempts == 0 {
        req.max_attempts = 3;
    }
    let metadata = req.metadata.get_or_insert_default();
    // Fast-path reject on the supplied depth. This is only a cheap pre-check; the
    // authoritative circuit-breaker guard runs post-derivation inside the transaction,
    // because children now send 0 and let the server derive the real depth.
    if metadata.execution_depth > max_execution_depth {
        return Err(Status::resource_exhausted(format!(
            "Circuit breaker tripped: execution depth {} exceeds max allowed {}",
            metadata.execution_depth, max_execution_depth
        )));
    }
    // Compare normalized input, before generating IDs/timestamps. Exact bytes avoid
    // hash collisions and make changes to payload OR scheduling/ownership options conflicts.
    Ok(if req.idempotency_key.is_empty() {
        None
    } else {
        Some(req.encode_to_vec())
    })
}

/// Enqueue one pre-validated job inside the shared `tx`. This is the single source of truth
/// for admission (idempotency replay, ingress velocity, ancestry/depth, circuit breaker, and
/// chain quarantine) shared by both single and bulk enqueues.
///
/// It performs no side effects outside the transaction: it never commits, never rolls back,
/// never calls `record_rejection`, and never fires success metrics. A business rejection is
/// returned as `EnqueueFailure::Rejected` carrying the forensic record so the caller can roll
/// the tx back first and then record/count it. `now` is read once per batch so every job
/// shares a consistent timestamp.
#[allow(clippy::too_many_arguments)]
fn enqueue_one(
    tx: &rusqlite::Transaction,
    req: AddJobRequest,
    now: i64,
    max_execution_depth: u32,
    max_chain_size: u64,
    receipt_request: Option<Vec<u8>>,
    metrics: &Metrics,
) -> Result<EnqueueOutcome, EnqueueFailure> {
    if let Some(expected) = &receipt_request {
        let existing = tx
            .query_row(
                "SELECT request, job_id, initial_state FROM enqueue_receipts WHERE queue_name = ?1 AND idempotency_key = ?2",
                rusqlite::params![req.name, req.idempotency_key],
                |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
            )
            .optional()
            .map_err(db_fail)?;
        if let Some((original, id, state)) = existing {
            if &original != expected {
                // A conflict always aborts (single or batch); count it at detection. It
                // records no forensic row, so it is a plain Status, not a Rejected.
                metrics.enqueue_conflict();
                tracing::warn!(job_id = %id, "enqueue idempotency conflict");
                return Err(EnqueueFailure::Status(Status::already_exists(
                    "idempotency key was already used for a different enqueue request",
                )));
            }
            return Ok(EnqueueOutcome {
                response: AddJobResponse {
                    id: id.clone(),
                    state,
                    replayed: true,
                },
                success: SuccessMetric::Replayed { job_id: id },
            });
        }
    }
    // Sliding-window ingress admission control. Runs after the idempotency replay
    // early-return (so replays never consume quota) and before any job insert. Only facets
    // with a rule are throttled.
    if !req.rate_limit_facet.is_empty()
        && !crate::ingress_limit::admit(tx, &req.rate_limit_facet, now).map_err(db_fail)?
    {
        tracing::warn!(name = %req.name, facet = %req.rate_limit_facet, "enqueue rejected: ingress velocity limit");
        let message = format!(
            "ingress velocity limit exceeded for facet {}",
            req.rate_limit_facet
        );
        return Err(EnqueueFailure::Rejected {
            status: Status::resource_exhausted(message.clone()),
            record: RejectionRecord {
                kind: "ingress_velocity",
                trace_id: None,
                parent_id: None,
                execution_depth: None,
                rate_limit_facet: Some(req.rate_limit_facet.clone()),
                detail: message,
            },
            name: req.name.clone(),
        });
    }
    let available_at = now.checked_add(req.delay_ms).ok_or_else(|| {
        EnqueueFailure::Status(Status::invalid_argument("delay timestamp overflows"))
    })?;
    let id = uuid::Uuid::new_v4().to_string();
    let state = if req.delay_ms > 0 {
        "Delayed"
    } else {
        "Waiting"
    };
    let metadata = req.metadata.unwrap_or_default();
    let parent_id = if metadata.parent_id.is_empty() {
        None
    } else {
        Some(metadata.parent_id.clone())
    };
    // Ancestry validation only kicks in when a parent is claimed; parentless enqueues
    // keep their historical behavior (backward compatible). When a parent is present the
    // server OWNS the lineage facts: the child's execution_depth is derived from the
    // parent (supplied 0 => derive), and the trace_id is inherited authoritatively.
    let (effective_depth, trace_id) = if let Some(parent) = parent_id.as_deref() {
        let parent_row = tx
            .query_row(
                "SELECT execution_depth, trace_id FROM jobs WHERE id = ?1
                 UNION ALL SELECT execution_depth, trace_id FROM job_history WHERE id = ?1 LIMIT 1",
                [parent],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(db_fail)?;
        let Some((parent_depth, parent_trace)) = parent_row else {
            tracing::warn!(name = %req.name, parent_id = %parent, "enqueue rejected: parent not found");
            let message = format!("ancestry: parent {parent} not found");
            return Err(EnqueueFailure::Rejected {
                status: Status::failed_precondition(message.clone()),
                record: RejectionRecord {
                    kind: "parent_not_found",
                    trace_id: None,
                    parent_id: Some(parent.to_string()),
                    execution_depth: None,
                    rate_limit_facet: None,
                    detail: message,
                },
                name: req.name.clone(),
            });
        };
        let derived_depth = parent_depth + 1;
        // Derive-when-unset, validate-when-provided: a supplied 0 is a safe sentinel
        // because any job WITH a parent has depth >= 1, so the server fills it in.
        let effective_depth = if metadata.execution_depth == 0 {
            derived_depth
        } else if i64::from(metadata.execution_depth) == derived_depth {
            i64::from(metadata.execution_depth)
        } else {
            tracing::warn!(name = %req.name, parent_id = %parent, execution_depth = metadata.execution_depth, parent_depth, "enqueue rejected: inconsistent execution depth");
            let message = format!(
                "ancestry: execution_depth {} must equal parent depth + 1 ({})",
                metadata.execution_depth, derived_depth
            );
            return Err(EnqueueFailure::Rejected {
                status: Status::invalid_argument(message.clone()),
                record: RejectionRecord {
                    kind: "inconsistent_depth",
                    trace_id: Some(parent_trace),
                    parent_id: Some(parent.to_string()),
                    execution_depth: Some(i64::from(metadata.execution_depth)),
                    rate_limit_facet: None,
                    detail: message,
                },
                name: req.name.clone(),
            });
        };
        // Inherit the parent's lineage id authoritatively so a spoofed/absent trace_id
        // cannot evade the per-lineage quarantine below.
        (effective_depth, parent_trace)
    } else {
        let trace_id = if metadata.trace_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            metadata.trace_id.clone()
        };
        (i64::from(metadata.execution_depth), trace_id)
    };
    // Authoritative circuit-breaker guard on the DERIVED depth. Children send 0 and
    // let the server derive, so this post-derivation check (not the cheap pre-tx one)
    // is the real recursion breaker.
    if effective_depth > i64::from(max_execution_depth) {
        tracing::warn!(name = %req.name, trace_id = %trace_id, parent_id = parent_id.as_deref().unwrap_or(""), execution_depth = effective_depth, max_execution_depth, "enqueue rejected: circuit breaker tripped");
        let message = format!(
            "Circuit breaker tripped: execution depth {effective_depth} exceeds max allowed {max_execution_depth}"
        );
        return Err(EnqueueFailure::Rejected {
            status: Status::resource_exhausted(message.clone()),
            record: RejectionRecord {
                kind: "circuit_breaker",
                trace_id: Some(trace_id),
                parent_id,
                execution_depth: Some(effective_depth),
                rate_limit_facet: None,
                detail: message,
            },
            name: req.name.clone(),
        });
    }
    // Runaway-chain quarantine: bound total jobs per lineage. Disabled (and zero-cost)
    // when the cap is 0. Read + upsert are PK operations on chain_counters.
    if max_chain_size > 0 {
        let current = tx
            .query_row(
                "SELECT job_count FROM chain_counters WHERE trace_id = ?1",
                [&trace_id],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .map_err(db_fail)?
            .unwrap_or(0);
        if current as u64 >= max_chain_size {
            tracing::warn!(name = %req.name, trace_id = %trace_id, parent_id = parent_id.as_deref().unwrap_or(""), chain_size = current, max_chain_size, "enqueue rejected: chain quarantined");
            let message = format!(
                "chain quarantined: lineage {trace_id} exceeded max chain size {max_chain_size}"
            );
            return Err(EnqueueFailure::Rejected {
                status: Status::resource_exhausted(message.clone()),
                record: RejectionRecord {
                    kind: "chain_quarantine",
                    trace_id: Some(trace_id),
                    parent_id,
                    execution_depth: Some(effective_depth),
                    rate_limit_facet: None,
                    detail: message,
                },
                name: req.name.clone(),
            });
        }
        tx.execute(
            "INSERT INTO chain_counters(trace_id, job_count, updated_at) VALUES (?1, 1, ?2)
                ON CONFLICT(trace_id) DO UPDATE SET job_count = job_count + 1, updated_at = ?2",
            rusqlite::params![trace_id, now],
        )
        .map_err(db_fail)?;
    }
    tx.execute(
        "INSERT INTO jobs (id, name, state, priority, payload, parent_id, trace_id, execution_depth, max_attempts, created_at, updated_at, rate_limit_facet, available_at, retry_backoff_ms, retry_backoff_max_ms, retry_jitter)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11, ?12, ?13, ?14, ?15)",
        rusqlite::params![
            id,
            req.name,
            state,
            req.priority,
            req.payload,
            parent_id,
            trace_id,
            effective_depth,
            req.max_attempts,
            now,
            if req.rate_limit_facet.is_empty() {
                None
            } else {
                Some(&req.rate_limit_facet)
            },
            available_at,
            req.retry_backoff_ms,
            req.retry_backoff_max_ms,
            req.retry_jitter
        ],
    )
    .map_err(db_fail)?;
    let receipt_created = receipt_request.is_some();
    if let Some(original) = receipt_request.as_ref() {
        tx.execute(
            "INSERT INTO enqueue_receipts(queue_name, idempotency_key, request, job_id, initial_state, created_at) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![req.name, req.idempotency_key, original, id, state, now],
        )
        .map_err(db_fail)?;
    }
    Ok(EnqueueOutcome {
        response: AddJobResponse {
            id: id.clone(),
            state: state.into(),
            replayed: false,
        },
        success: SuccessMetric::Inserted {
            job_id: id,
            state,
            name: req.name,
            receipt_created,
        },
    })
}

/// Run a pre-validated batch of jobs in a single IMMEDIATE transaction, all-or-nothing.
///
/// On the first per-job failure the whole transaction is rolled back; for a business
/// rejection the forensic record is persisted and the matching rejection metric fired, once.
/// `index_prefix` tags a rejection's message with its batch position (bulk only) so single
/// enqueues keep byte-identical error messages. On full success, success metrics are flushed
/// only after commit so a rolled-back batch never inflates counters.
fn execute_batch(
    conn: &mut rusqlite::Connection,
    metrics: &Metrics,
    jobs: Vec<(AddJobRequest, Option<Vec<u8>>)>,
    max_execution_depth: u32,
    max_chain_size: u64,
    index_prefix: bool,
) -> Result<Vec<AddJobResponse>, Status> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(internal)?;
    let now = leases::now_ms(&tx).map_err(internal)?;
    let mut outcomes = Vec::with_capacity(jobs.len());
    for (index, (req, receipt_request)) in jobs.into_iter().enumerate() {
        match enqueue_one(
            &tx,
            req,
            now,
            max_execution_depth,
            max_chain_size,
            receipt_request,
            metrics,
        ) {
            Ok(outcome) => outcomes.push(outcome),
            Err(EnqueueFailure::Status(status)) => {
                drop(tx);
                return Err(status);
            }
            Err(EnqueueFailure::Rejected {
                status,
                record,
                name,
            }) => {
                // Roll the whole (possibly batched) tx back first, then durably record the
                // single offending rejection in its own fresh tx and count it once.
                drop(tx);
                record_rejection(conn, now, &name, &record);
                fire_rejection_metric(metrics, &record);
                return Err(if index_prefix {
                    Status::new(
                        status.code(),
                        format!("bulk job {index}: {}", status.message()),
                    )
                } else {
                    status
                });
            }
        }
    }
    tx.commit().map_err(internal)?;
    // Flush success metrics only after the commit succeeds.
    let responses = outcomes
        .into_iter()
        .map(|outcome| {
            outcome.success.flush(metrics);
            outcome.response
        })
        .collect();
    Ok(responses)
}

pub async fn enqueue(
    service: &MyQueueService,
    mut req: AddJobRequest,
) -> Result<AddJobResponse, Status> {
    let receipt_request = validate_and_prepare(&mut req, service.max_execution_depth)?;
    let metrics = service.db_manager.metrics.clone();
    let max_execution_depth = service.max_execution_depth;
    let max_chain_size = service.max_chain_size;
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let mut conn = conn.blocking_lock();
        let mut responses = execute_batch(
            &mut conn,
            &metrics,
            vec![(req, receipt_request)],
            max_execution_depth,
            max_chain_size,
            false,
        )?;
        Ok(responses.pop().expect("single enqueue yields one response"))
    })
    .await
    .map_err(|error| Status::internal(error.to_string()))?
}

pub async fn enqueue_bulk(
    service: &MyQueueService,
    req: AddJobBulkRequest,
) -> Result<AddJobBulkResponse, Status> {
    if req.requests.is_empty() {
        return Err(Status::invalid_argument(
            "bulk enqueue requires at least one job",
        ));
    }
    if req.requests.len() > MAX_BULK_BATCH_SIZE {
        return Err(Status::invalid_argument(format!(
            "bulk enqueue exceeds max batch size {MAX_BULK_BATCH_SIZE}"
        )));
    }
    // Pre-validate and prepare every request up front; a validation error aborts the whole
    // batch before any transaction opens, so nothing is written.
    let mut jobs = Vec::with_capacity(req.requests.len());
    for mut job in req.requests {
        let receipt_request = validate_and_prepare(&mut job, service.max_execution_depth)?;
        jobs.push((job, receipt_request));
    }
    let metrics = service.db_manager.metrics.clone();
    let max_execution_depth = service.max_execution_depth;
    let max_chain_size = service.max_chain_size;
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let mut conn = conn.blocking_lock();
        let responses = execute_batch(
            &mut conn,
            &metrics,
            jobs,
            max_execution_depth,
            max_chain_size,
            true,
        )?;
        Ok(AddJobBulkResponse { responses })
    })
    .await
    .map_err(|error| Status::internal(error.to_string()))?
}
