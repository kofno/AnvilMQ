use crate::{
    leases,
    queue::v1::{AddJobRequest, AddJobResponse},
    MyQueueService,
};
use prost::Message;
use rusqlite::{OptionalExtension, TransactionBehavior};
use tonic::Status;

pub async fn enqueue(
    service: &MyQueueService,
    mut req: AddJobRequest,
) -> Result<AddJobResponse, Status> {
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
    if req.max_attempts == 0 {
        req.max_attempts = 3;
    }
    let metadata = req.metadata.get_or_insert_default();
    // Fast-path reject on the supplied depth. This is only a cheap pre-check; the
    // authoritative circuit-breaker guard runs post-derivation inside the transaction,
    // because children now send 0 and let the server derive the real depth.
    if metadata.execution_depth > service.max_execution_depth {
        return Err(Status::resource_exhausted(format!(
            "Circuit breaker tripped: execution depth {} exceeds max allowed {}",
            metadata.execution_depth, service.max_execution_depth
        )));
    }
    let max_execution_depth = service.max_execution_depth;
    // Compare normalized input, before generating IDs/timestamps. Exact bytes avoid
    // hash collisions and make changes to payload OR scheduling/ownership options conflicts.
    let receipt_request = if req.idempotency_key.is_empty() {
        None
    } else {
        Some(req.encode_to_vec())
    };
    let metrics = service.db_manager.metrics.clone();
    let max_chain_size = service.max_chain_size;
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let internal = |error: rusqlite::Error| Status::internal(error.to_string());
        let mut conn = conn.blocking_lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(internal)?;
        if let Some(expected) = &receipt_request {
            let existing = tx.query_row("SELECT request, job_id, initial_state FROM enqueue_receipts WHERE queue_name = ?1 AND idempotency_key = ?2",
                rusqlite::params![req.name, req.idempotency_key], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))).optional().map_err(internal)?;
            if let Some((original, id, state)) = existing {
                if &original != expected {
                    metrics.enqueue_conflict();
                    tracing::warn!(job_id = %id, "enqueue idempotency conflict");
                    return Err(Status::already_exists("idempotency key was already used for a different enqueue request"));
                }
                tx.commit().map_err(internal)?;
                metrics.enqueue_replay();
                tracing::info!(job_id = %id, "enqueue receipt replayed");
                return Ok(AddJobResponse { id, state, replayed: true });
            }
        }
        let now = leases::now_ms(&tx).map_err(internal)?;
        // Sliding-window ingress admission control. Runs after the idempotency replay
        // early-return (so replays never consume quota) and before any job insert. A
        // rejection returns Err, which drops the uncommitted tx and rolls back, so nothing
        // is written and no quota is consumed. Only facets with a rule are throttled.
        if !req.rate_limit_facet.is_empty()
            && !crate::ingress_limit::admit(&tx, &req.rate_limit_facet, now).map_err(internal)?
        {
            metrics.ingress_rejection();
            tracing::warn!(name = %req.name, facet = %req.rate_limit_facet, "enqueue rejected: ingress velocity limit");
            return Err(Status::resource_exhausted(format!(
                "ingress velocity limit exceeded for facet {}",
                req.rate_limit_facet
            )));
        }
        let available_at = now.checked_add(req.delay_ms).ok_or_else(|| Status::invalid_argument("delay timestamp overflows"))?;
        let id = uuid::Uuid::new_v4().to_string();
        let state = if req.delay_ms > 0 { "Delayed" } else { "Waiting" };
        let metadata = req.metadata.unwrap_or_default();
        let parent_id = if metadata.parent_id.is_empty() { None } else { Some(metadata.parent_id.clone()) };
        // Ancestry validation only kicks in when a parent is claimed; parentless enqueues
        // keep their historical behavior (backward compatible). Parent lookups are PK point
        // reads across the live and history tables, so they stay cheap on the hot path.
        // When a parent is present the server OWNS the lineage facts: the child's
        // execution_depth is derived from the parent (supplied 0 => derive), and the
        // trace_id is inherited authoritatively.
        let (effective_depth, trace_id) = if let Some(parent) = parent_id.as_deref() {
            let parent_row = tx.query_row(
                "SELECT execution_depth, trace_id FROM jobs WHERE id = ?1
                 UNION ALL SELECT execution_depth, trace_id FROM job_history WHERE id = ?1 LIMIT 1",
                [parent], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))).optional().map_err(internal)?;
            let Some((parent_depth, parent_trace)) = parent_row else {
                metrics.ancestry_rejection();
                tracing::warn!(name = %req.name, parent_id = %parent, "enqueue rejected: parent not found");
                return Err(Status::failed_precondition(format!("ancestry: parent {parent} not found")));
            };
            let derived_depth = parent_depth + 1;
            // Derive-when-unset, validate-when-provided: a supplied 0 is a safe sentinel
            // because any job WITH a parent has depth >= 1, so the server fills it in.
            let effective_depth = if metadata.execution_depth == 0 {
                derived_depth
            } else if i64::from(metadata.execution_depth) == derived_depth {
                i64::from(metadata.execution_depth)
            } else {
                metrics.ancestry_rejection();
                tracing::warn!(name = %req.name, parent_id = %parent, execution_depth = metadata.execution_depth, parent_depth, "enqueue rejected: inconsistent execution depth");
                return Err(Status::invalid_argument(format!(
                    "ancestry: execution_depth {} must equal parent depth + 1 ({})",
                    metadata.execution_depth, derived_depth)));
            };
            // Inherit the parent's lineage id authoritatively so a spoofed/absent trace_id
            // cannot evade the per-lineage quarantine below.
            (effective_depth, parent_trace)
        } else {
            let trace_id = if metadata.trace_id.is_empty() { uuid::Uuid::new_v4().to_string() } else { metadata.trace_id.clone() };
            (i64::from(metadata.execution_depth), trace_id)
        };
        // Authoritative circuit-breaker guard on the DERIVED depth. Children send 0 and
        // let the server derive, so this post-derivation check (not the cheap pre-tx one)
        // is the real recursion breaker.
        if effective_depth > i64::from(max_execution_depth) {
            metrics.ancestry_rejection();
            tracing::warn!(name = %req.name, trace_id = %trace_id, parent_id = ?parent_id, execution_depth = effective_depth, max_execution_depth, "enqueue rejected: circuit breaker tripped");
            return Err(Status::resource_exhausted(format!(
                "Circuit breaker tripped: execution depth {effective_depth} exceeds max allowed {max_execution_depth}"
            )));
        }
        // Runaway-chain quarantine: bound total jobs per lineage. Disabled (and zero-cost)
        // when the cap is 0. Read + upsert are PK operations on chain_counters.
        if max_chain_size > 0 {
            let current = tx.query_row("SELECT job_count FROM chain_counters WHERE trace_id = ?1", [&trace_id], |r| r.get::<_, i64>(0)).optional().map_err(internal)?.unwrap_or(0);
            if current as u64 >= max_chain_size {
                metrics.chain_quarantine();
                tracing::warn!(name = %req.name, trace_id = %trace_id, parent_id = ?parent_id, chain_size = current, max_chain_size, "enqueue rejected: chain quarantined");
                return Err(Status::resource_exhausted(format!(
                    "chain quarantined: lineage {trace_id} exceeded max chain size {max_chain_size}")));
            }
            tx.execute("INSERT INTO chain_counters(trace_id, job_count, updated_at) VALUES (?1, 1, ?2)
                ON CONFLICT(trace_id) DO UPDATE SET job_count = job_count + 1, updated_at = ?2",
                rusqlite::params![trace_id, now]).map_err(internal)?;
        }
        tx.execute("INSERT INTO jobs (id, name, state, priority, payload, parent_id, trace_id, execution_depth, max_attempts, created_at, updated_at, rate_limit_facet, available_at, retry_backoff_ms, retry_backoff_max_ms)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11, ?12, ?13, ?14)",
            rusqlite::params![id, req.name, state, req.priority, req.payload,
                parent_id, trace_id, effective_depth,
                req.max_attempts, now, if req.rate_limit_facet.is_empty() { None } else { Some(req.rate_limit_facet) }, available_at, req.retry_backoff_ms, req.retry_backoff_max_ms]).map_err(internal)?;
        if let Some(original) = receipt_request.as_ref() {
            tx.execute("INSERT INTO enqueue_receipts(queue_name, idempotency_key, request, job_id, initial_state, created_at) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![req.name, req.idempotency_key, original, id, state, now]).map_err(internal)?;
        }
        tx.commit().map_err(internal)?;
        if receipt_request.is_some() { metrics.enqueue_receipt_created(); }
        metrics.transition(None, state, "enqueued");
        metrics.named.event(&req.name, "enqueued");
        tracing::info!(job_id = %id, attempt = 0, to = state, "job transition");
        Ok(AddJobResponse { id, state: state.into(), replayed: false })
    }).await.map_err(|error| Status::internal(error.to_string()))?
}
