use rusqlite::{OptionalExtension, TransactionBehavior};
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};

#[cfg(test)]
mod ancestry_tests;
mod backup;
mod db;
mod enqueue;
#[cfg(test)]
mod enqueue_tests;
mod fts;
mod ingress_limit;
mod leases;
#[cfg(test)]
mod lifecycle_tests;
mod pressure;
mod rate_limit;
mod reader;
mod retention;
mod scheduling;
mod telemetry;
use db::DatabaseManager;

pub mod queue {
    pub mod v1 {
        tonic::include_proto!("queue.v1");
    }
}

use queue::v1::{
    queue_service_server::{QueueService, QueueServiceServer},
    AddJobRequest, AddJobResponse, CompleteJobRequest, CompleteJobResponse, FailJobRequest,
    FailJobResponse, GetNextJobRequest, GetNextJobResponse,
};

pub struct MyQueueService {
    db_manager: Arc<DatabaseManager>,
    max_execution_depth: u32,
    max_chain_size: u64,
    fairness_enabled: bool,
    lease_duration_ms: i64,
}

#[tonic::async_trait]
impl QueueService for MyQueueService {
    async fn add_job(
        &self,
        request: Request<AddJobRequest>,
    ) -> Result<Response<AddJobResponse>, Status> {
        let _timer = self.db_manager.metrics.timer("AddJob");
        enqueue::enqueue(self, request.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_next_job(
        &self,
        request: Request<GetNextJobRequest>,
    ) -> Result<Response<GetNextJobResponse>, Status> {
        let _timer = self.db_manager.metrics.timer("GetNextJob");
        let req = request.into_inner();
        if req.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id must not be blank"));
        }
        if req.queue_names.iter().any(|name| name.trim().is_empty()) {
            return Err(Status::invalid_argument(
                "queue_names must not contain blank names",
            ));
        }
        let metrics = self.db_manager.metrics.clone();
        let fairness_enabled = self.fairness_enabled;
        let lease_duration_ms = self.lease_duration_ms;
        let conn_arc = self.db_manager.get_shared_connection();
        let job = tokio::task::spawn_blocking(move || -> rusqlite::Result<GetNextJobResponse> {
            let mut conn = conn_arc.blocking_lock();
            // Acquire the database write lock before selecting, including across connections.
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now = leases::now_ms(&tx)?;
            let mut sql = String::from(
                "SELECT id, name, payload, parent_id, trace_id, execution_depth, attempts, state
                 FROM jobs WHERE state IN ('Waiting', 'Delayed') AND available_at <= ?1",
            );
            if !req.queue_names.is_empty() {
                sql.push_str(" AND name IN (");
                sql.push_str(&vec!["?"; req.queue_names.len()].join(","));
                sql.push(')');
            }
            let parameters: Vec<rusqlite::types::Value> = std::iter::once(rusqlite::types::Value::Integer(now)).chain(req.queue_names.iter().cloned().map(rusqlite::types::Value::Text)).collect();
            let blocked = "EXISTS (SELECT 1 FROM rate_limit_rules r LEFT JOIN rate_limit_counters c ON c.facet_key = r.facet_pattern WHERE r.facet_pattern = jobs.rate_limit_facet AND (r.max_jobs = 0 OR (c.window_expires_at > ?1 AND c.current_count >= r.max_jobs)))";
            // CASE is lazy: with no rules, avoid scanning the backlog solely for telemetry.
            // Read this inside the claim transaction so concurrent rule changes stay serialized.
            let throttled: bool = tx.query_row(&rate_limit::throttled_poll_sql(&sql, blocked), rusqlite::params_from_iter(parameters.iter()), |r| r.get(0))?;
            // Fairness rotates among equal-priority due jobs by least-recently-served facet; when
            // disabled the query is byte-for-byte the legacy priority+FIFO claim (zero overhead).
            let claim_sql = if fairness_enabled {
                let mut fair = String::from(
                    "SELECT jobs.id, jobs.name, jobs.payload, jobs.parent_id, jobs.trace_id,
                            jobs.execution_depth, jobs.attempts, jobs.state
                     FROM jobs
                     LEFT JOIN facet_dispatch d ON d.facet_key = COALESCE(jobs.rate_limit_facet, '')
                     WHERE jobs.state IN ('Waiting', 'Delayed') AND jobs.available_at <= ?1",
                );
                if !req.queue_names.is_empty() {
                    fair.push_str(" AND jobs.name IN (");
                    fair.push_str(&vec!["?"; req.queue_names.len()].join(","));
                    fair.push(')');
                }
                fair.push_str(&format!(" AND NOT {blocked}"));
                fair.push_str(" ORDER BY jobs.priority ASC, COALESCE(d.last_served_seq, 0) ASC, jobs.created_at ASC, jobs.id ASC LIMIT 1");
                fair
            } else {
                sql.push_str(&format!(" AND NOT {blocked}"));
                sql.push_str(" ORDER BY priority ASC, created_at ASC, id ASC LIMIT 1");
                sql
            };
            let job = tx
                .query_row(
                    &claim_sql,
                    rusqlite::params_from_iter(
                        std::iter::once(rusqlite::types::Value::Integer(now)).chain(
                            req.queue_names
                                .iter()
                                .cloned()
                                .map(rusqlite::types::Value::Text),
                        ),
                    ),
                    |row| {
                        Ok(GetNextJobResponse {
                            found: true,
                            id: row.get(0)?,
                            name: row.get(1)?,
                            payload: row.get(2)?,
                            metadata: Some(queue::v1::JobMetadata {
                                parent_id: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                                trace_id: row.get(4)?,
                                execution_depth: row.get(5)?,
                            }),
                            attempts: row.get(6)?,
                            lease_expires_at_ms: 0,
                        })
                    },
                )
                .optional()?;
            let Some(mut job) = job else {
                tx.commit()?;
                if throttled { metrics.throttled(); }
                return Ok(GetNextJobResponse::default());
            };
            rate_limit::consume(&tx, &job.id, now)?;
            let (prior_state, created_at, available_at): (String, i64, i64) = tx.query_row("SELECT state, created_at, available_at FROM jobs WHERE id = ?1", [&job.id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            job.attempts = job
                .attempts
                .checked_add(1)
                .ok_or(rusqlite::Error::InvalidQuery)?;
            let now = leases::now_ms(&tx)?;
            job.lease_expires_at_ms = now + lease_duration_ms;
            tx.execute(
                "UPDATE jobs SET state = 'Active', worker_id = ?1, attempts = ?2,
                 updated_at = ?4, lease_expires_at_ms = ?5
                 WHERE id = ?3 AND state IN ('Waiting', 'Delayed')",
                rusqlite::params![
                    req.worker_id,
                    job.attempts,
                    job.id,
                    now,
                    job.lease_expires_at_ms
                ],
            )?;
            if fairness_enabled {
                // Record this facet as the most-recently-served so the next claim rotates past
                // it. Commits atomically with the claim; a no-hit poll or rollback records nothing.
                let facet: String = tx
                    .query_row(
                        "SELECT COALESCE(rate_limit_facet, '') FROM jobs WHERE id = ?1",
                        [&job.id],
                        |r| r.get(0),
                    )?;
                let next_seq: i64 = tx.query_row(
                    "SELECT COALESCE(MAX(last_served_seq), 0) + 1 FROM facet_dispatch",
                    [],
                    |r| r.get(0),
                )?;
                tx.execute(
                    "INSERT INTO facet_dispatch(facet_key, last_served_seq, last_served_at)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT(facet_key) DO UPDATE SET
                       last_served_seq = excluded.last_served_seq,
                       last_served_at = excluded.last_served_at",
                    rusqlite::params![facet, next_seq, now],
                )?;
            }
            tx.commit()?;
            if throttled { metrics.throttled(); }
            metrics.transition(Some(&prior_state), "Active", "claimed");
            metrics.named.event(&job.name, "claimed");
            metrics.pressure.claimed(created_at, available_at, now, job.attempts);
            tracing::info!(job_id = %job.id, worker_id = %req.worker_id, attempt = job.attempts, from = %prior_state, to = "Active", "job transition");
            Ok(job)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(job))
    }

    async fn complete_job(
        &self,
        request: Request<CompleteJobRequest>,
    ) -> Result<Response<CompleteJobResponse>, Status> {
        let _timer = self.db_manager.metrics.timer("CompleteJob");
        let req = request.into_inner();
        self.finish_job(req.id, req.worker_id, req.attempt, None)
            .await?;
        Ok(Response::new(CompleteJobResponse { success: true }))
    }

    async fn upsert_rate_limit_rule(
        &self,
        request: Request<queue::v1::UpsertRateLimitRuleRequest>,
    ) -> Result<Response<queue::v1::UpsertRateLimitRuleResponse>, Status> {
        let _timer = self.db_manager.metrics.timer("UpsertRateLimitRule");
        rate_limit::upsert(self.db_manager.clone(), request.into_inner())
            .await
            .map(Response::new)
    }

    async fn delete_rate_limit_rule(
        &self,
        request: Request<queue::v1::DeleteRateLimitRuleRequest>,
    ) -> Result<Response<queue::v1::DeleteRateLimitRuleResponse>, Status> {
        let _timer = self.db_manager.metrics.timer("DeleteRateLimitRule");
        rate_limit::delete(self.db_manager.clone(), request.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_rate_limit_status(
        &self,
        request: Request<queue::v1::GetRateLimitStatusRequest>,
    ) -> Result<Response<queue::v1::GetRateLimitStatusResponse>, Status> {
        let _timer = self.db_manager.metrics.timer("GetRateLimitStatus");
        rate_limit::status(self.db_manager.clone(), request.into_inner())
            .await
            .map(Response::new)
    }

    async fn upsert_ingress_limit_rule(
        &self,
        request: Request<queue::v1::UpsertIngressLimitRuleRequest>,
    ) -> Result<Response<queue::v1::UpsertIngressLimitRuleResponse>, Status> {
        let _timer = self.db_manager.metrics.timer("UpsertIngressLimitRule");
        ingress_limit::upsert(self.db_manager.clone(), request.into_inner())
            .await
            .map(Response::new)
    }

    async fn delete_ingress_limit_rule(
        &self,
        request: Request<queue::v1::DeleteIngressLimitRuleRequest>,
    ) -> Result<Response<queue::v1::DeleteIngressLimitRuleResponse>, Status> {
        let _timer = self.db_manager.metrics.timer("DeleteIngressLimitRule");
        ingress_limit::delete(self.db_manager.clone(), request.into_inner())
            .await
            .map(Response::new)
    }

    async fn get_ingress_limit_status(
        &self,
        request: Request<queue::v1::GetIngressLimitStatusRequest>,
    ) -> Result<Response<queue::v1::GetIngressLimitStatusResponse>, Status> {
        let _timer = self.db_manager.metrics.timer("GetIngressLimitStatus");
        ingress_limit::status(self.db_manager.clone(), request.into_inner())
            .await
            .map(Response::new)
    }

    async fn heartbeat(
        &self,
        request: Request<queue::v1::HeartbeatRequest>,
    ) -> Result<Response<queue::v1::HeartbeatResponse>, Status> {
        let _timer = self.db_manager.metrics.timer("Heartbeat");
        self.renew_lease(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn fail_job(
        &self,
        request: Request<FailJobRequest>,
    ) -> Result<Response<FailJobResponse>, Status> {
        let _timer = self.db_manager.metrics.timer("FailJob");
        let req = request.into_inner();
        let moved_to_failed_state = self
            .finish_job(req.id, req.worker_id, req.attempt, Some(req.error_message))
            .await?;
        Ok(Response::new(FailJobResponse {
            success: true,
            moved_to_failed_state,
        }))
    }
}

impl MyQueueService {
    // Returns whether failure exhausted the attempt limit. No acknowledgment is sent before commit.
    async fn finish_job(
        &self,
        id: String,
        worker_id: String,
        attempt: u32,
        error: Option<String>,
    ) -> Result<bool, Status> {
        if id.trim().is_empty() || worker_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "id and worker_id must not be blank",
            ));
        }
        let metrics = self.db_manager.metrics.clone();
        let conn = self.db_manager.get_shared_connection();
        tokio::task::spawn_blocking(move || {
            let internal = |e: rusqlite::Error| Status::internal(e.to_string());
            let mut conn = conn.blocking_lock();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(internal)?;
            let job = tx.query_row(
                "SELECT state, worker_id, attempts, max_attempts, name, created_at FROM jobs WHERE id = ?1", [&id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?, row.get::<_, u32>(2)?, row.get::<_, u32>(3)?, row.get::<_, String>(4)?, row.get::<_, i64>(5)?)),
            ).optional().map_err(internal)?;
            let Some((state, owner, attempts, max_attempts, name, created_at)) = job else {
                let archived = tx.query_row("SELECT state, worker_id, attempts FROM job_history WHERE id = ?1", [&id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?, row.get::<_, u32>(2)?))).optional().map_err(internal)?;
                if let Some((state, owner, attempts)) = archived {
                    // Replay only the exact successful completion. History is the durable receipt;
                    // do not repeat transitions, increment metrics, or accept a FailJob replay.
                    if error.is_none() && state == "Completed" && owner.as_deref() == Some(worker_id.as_str()) && attempts == attempt.max(1) {
                        tx.commit().map_err(internal)?;
                        return Ok(false);
                    }
                    return Err(Status::failed_precondition("job already finished with a different outcome or claim"));
                }
                return Err(Status::not_found("job not found"));
            };
            if state != "Active" {
                return Err(Status::failed_precondition("job is not Active"));
            }
            if owner.as_deref() != Some(worker_id.as_str()) {
                return Err(Status::permission_denied("worker does not own this job"));
            }
            let acknowledged_attempt = if attempt == 0 { 1 } else { attempt };
            if acknowledged_attempt != attempts {
                return Err(Status::failed_precondition("attempt does not match the active claim"));
            }
            let now = leases::now_ms(&tx).map_err(internal)?;
            let expiry: Option<i64> = tx.query_row("SELECT lease_expires_at_ms FROM jobs WHERE id = ?1", [&id], |r| r.get(0)).map_err(internal)?;
            if expiry.is_none_or(|expiry| expiry <= now) {
                return Err(Status::failed_precondition("claim lease has expired"));
            }
            let failed = error.is_some();
            let mut next_state = if failed { "Failed" } else { "Completed" };
            if failed && attempts < max_attempts {
                let (base, cap): (i64, i64) = tx.query_row("SELECT retry_backoff_ms, retry_backoff_max_ms FROM jobs WHERE id = ?1", [&id], |r| Ok((r.get(0)?, r.get(1)?))).map_err(internal)?;
                let delay = scheduling::retry_delay(base, cap, attempts);
                let available_at = now.saturating_add(delay);
                next_state = if delay == 0 { "Waiting" } else { "Delayed" };
                tx.execute(
                    "UPDATE jobs SET state = ?3, available_at = ?4, worker_id = NULL, lease_expires_at_ms = NULL, last_error = ?1,
                     updated_at = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) WHERE id = ?2",
                    rusqlite::params![error, id, if delay == 0 { "Waiting" } else { "Delayed" }, available_at],
                ).map_err(internal)?;
            } else {
                tx.execute(
                    "INSERT INTO job_history (id, name, state, priority, payload, parent_id, trace_id,
                     execution_depth, attempts, max_attempts, created_at, finished_at, worker_id, last_error, rate_limit_facet)
                     SELECT id, name, ?1, priority, payload, parent_id, trace_id, execution_depth, attempts,
                     max_attempts, created_at, CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER),
                     worker_id, CASE WHEN ?2 IS NULL THEN last_error ELSE ?2 END, rate_limit_facet
                     FROM jobs WHERE id = ?3",
                    rusqlite::params![if failed { "Failed" } else { "Completed" }, error, id],
                ).map_err(internal)?;
                tx.execute("DELETE FROM jobs WHERE id = ?1", [&id]).map_err(internal)?;
            }
            tx.commit().map_err(internal)?;
            metrics.transition(Some("Active"), next_state, if failed { "failed" } else { "completed" });
            if failed && attempts < max_attempts {
                metrics.event("retried");
                metrics.named.event(&name, "retried");
            } else {
                metrics.named.event(&name, if failed { "failed" } else { "completed" });
                metrics.named.observe_duration(&name, now.saturating_sub(created_at));
            }
            tracing::info!(job_id = %id, worker_id = %worker_id, attempt = attempts, from = "Active", to = next_state, "job transition");
            Ok(failed && attempts >= max_attempts)
        }).await.map_err(|e| Status::internal(e.to_string()))?
    }
}

/// Default worker lease recovery sweep cadence, overridable via `ANVILMQ_RECOVERY_INTERVAL_MS`.
const DEFAULT_RECOVERY_INTERVAL_MS: u64 = 5_000;

/// Sub-second leases churn recovery, so the configured lease must be at least this long.
const MIN_LEASE_DURATION_MS: i64 = 1_000;

/// Parse `ANVILMQ_LEASE_DURATION_MS`. Absent -> default; present must be an integer >= 1000.
fn parse_lease_duration_ms(raw: Option<&str>) -> Result<i64, String> {
    match raw {
        None => Ok(leases::LEASE_DURATION_MS),
        Some(value) => {
            let parsed = value.trim().parse::<i64>().map_err(|_| {
                format!("ANVILMQ_LEASE_DURATION_MS must be an integer, got {value:?}")
            })?;
            if parsed < MIN_LEASE_DURATION_MS {
                return Err(format!(
                    "ANVILMQ_LEASE_DURATION_MS={parsed} must be >= {MIN_LEASE_DURATION_MS} (sub-second leases churn recovery)"
                ));
            }
            Ok(parsed)
        }
    }
}

/// Parse `ANVILMQ_RECOVERY_INTERVAL_MS`. Absent -> default; present must be a positive integer.
fn parse_recovery_interval_ms(raw: Option<&str>) -> Result<u64, String> {
    match raw {
        None => Ok(DEFAULT_RECOVERY_INTERVAL_MS),
        Some(value) => {
            let parsed = value.trim().parse::<i64>().map_err(|_| {
                format!("ANVILMQ_RECOVERY_INTERVAL_MS must be an integer, got {value:?}")
            })?;
            if parsed <= 0 {
                return Err(format!(
                    "ANVILMQ_RECOVERY_INTERVAL_MS={parsed} must be positive"
                ));
            }
            Ok(parsed as u64)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (log_writer, _log_guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .json()
        .with_writer(log_writer)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    // Initialize embedded libSQL/SQLite database file
    let database_path = std::env::var("ANVILMQ_DB_PATH").unwrap_or_else(|_| "anvil.db".into());
    let durability = match std::env::var("ANVILMQ_DURABILITY") {
        Ok(value) => db::Durability::parse(&value)?,
        Err(std::env::VarError::NotPresent) => db::Durability::Normal,
        Err(error) => return Err(error.into()),
    };
    let pressure = pressure::Pressure::configured(
        &std::env::var("ANVILMQ_METRICS_QUEUES").unwrap_or_default(),
    )?;
    let metrics_mode = match std::env::var("ANVILMQ_METRICS_MODE") {
        Ok(value) => telemetry::MetricsMode::parse(&value)?,
        Err(std::env::VarError::NotPresent) => telemetry::MetricsMode::Allowlist,
        Err(error) => return Err(error.into()),
    };
    let metrics_max_names = match std::env::var("ANVILMQ_METRICS_MAX_NAMES") {
        Ok(value) => telemetry::parse_max_names(&value)?,
        Err(std::env::VarError::NotPresent) => telemetry::DEFAULT_METRICS_MAX_NAMES,
        Err(error) => return Err(error.into()),
    };
    let mut manager = DatabaseManager::with_durability(&database_path, durability).await?;
    let named = match metrics_mode {
        telemetry::MetricsMode::Allowlist => {
            telemetry::NamedLifecycle::configured(&pressure.queue_names())
        }
        telemetry::MetricsMode::All => {
            telemetry::NamedLifecycle::all(metrics_max_names, &pressure.queue_names())
        }
    };
    {
        let metrics = Arc::get_mut(&mut manager.metrics).expect("metrics not yet shared");
        metrics.pressure = pressure;
        metrics.named = named;
    }
    let db_manager = Arc::new(manager);
    // Opt-in full-text search: create the FTS5 index + triggers and backfill existing history
    // rows once at startup, off the hot path. Disabled by default (zero write cost when off).
    let fts_enabled = fts::enabled_from_env();
    if fts_enabled {
        let backfilled = db_manager.enable_fts(fts::DEFAULT_BACKFILL_BATCH).await?;
        tracing::info!(backfilled, "full-text search enabled");
    }
    // Isolated read-only WAL replica connection for observability reads (e.g. /v1/failures).
    // Built before database_path is moved into pressure::spawn below.
    let reader = Arc::new(reader::Reader::from_env(database_path.clone()));
    // Captured before `database_path` is moved into pressure::spawn; the backup task opens its own
    // dedicated connection to this same file.
    let backup_db_path = database_path.clone();
    let sampler = pressure::spawn(database_path, db_manager.metrics.clone());
    // A single shutdown event fans out to both servers so they stop accepting new connections and
    // drain in-flight work together. Capacity 1 is enough: we only ever send one notification.
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    {
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            tracing::info!("shutdown signal received; draining in-flight work");
            let _ = shutdown_tx.send(());
        });
    }
    let http_addr = std::env::var("ANVILMQ_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9090".into())
        .parse()?;
    let http = {
        let mut rx = shutdown_tx.subscribe();
        axum::Server::try_bind(&http_addr)?
            .serve(
                telemetry::router(db_manager.clone(), reader.clone(), fts_enabled)
                    .into_make_service(),
            )
            .with_graceful_shutdown(async move {
                let _ = rx.recv().await;
            })
    };
    let recovery_db = db_manager.clone();
    let lease_duration_ms =
        parse_lease_duration_ms(std::env::var("ANVILMQ_LEASE_DURATION_MS").ok().as_deref())
            .map_err(std::io::Error::other)?;
    let recovery_interval_ms = parse_recovery_interval_ms(
        std::env::var("ANVILMQ_RECOVERY_INTERVAL_MS")
            .ok()
            .as_deref(),
    )
    .map_err(std::io::Error::other)?;
    tracing::info!(
        lease_duration_ms,
        recovery_interval_ms,
        "lease and recovery configured"
    );
    let recovery = tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_millis(recovery_interval_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            match leases::recover_expired(recovery_db.clone()).await {
                Ok(count) if count > 0 => tracing::info!(count, "expired claims recovered"),
                Ok(_) => {}
                Err(error) => {
                    recovery_db
                        .metrics
                        .recovery_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::error!(%error, "lease recovery failed; retrying next tick");
                }
            }
        }
    });

    let retention_cfg =
        retention::RetentionConfig::from_env(lease_duration_ms).map_err(std::io::Error::other)?;
    let retention = if retention_cfg.enabled() {
        tracing::info!(?retention_cfg, "retention sweeper enabled");
        let retention_db = db_manager.clone();
        Some(tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(retention_cfg.interval_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                match retention::sweep(retention_db.clone(), retention_cfg.clone()).await {
                    Ok(outcome) if outcome.total() > 0 => {
                        tracing::info!(pruned = outcome.total(), ?outcome, "retention sweep")
                    }
                    Ok(_) => {}
                    Err(error) => {
                        retention_db.metrics.retention_error();
                        tracing::error!(%error, "retention sweep failed; retrying next tick");
                    }
                }
            }
        }))
    } else {
        tracing::info!("retention sweeper disabled");
        None
    };

    let backup_cfg = backup::BackupConfig::from_env().map_err(std::io::Error::other)?;
    let backup = if backup_cfg.enabled() {
        tracing::info!(?backup_cfg, "snapshot writer enabled");
        let backup_metrics = db_manager.metrics.clone();
        Some(tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(backup_cfg.interval_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                match backup::run_once(backup_db_path.clone(), backup_cfg.clone()).await {
                    Ok(Some(outcome)) => {
                        backup_metrics.backup_success(
                            outcome.timestamp_ms,
                            outcome.duration_ms,
                            outcome.size_bytes,
                        );
                        tracing::info!(
                            path = %outcome.path.display(),
                            size_bytes = outcome.size_bytes,
                            duration_ms = outcome.duration_ms,
                            pruned = outcome.pruned,
                            "snapshot written"
                        );
                    }
                    Ok(None) => {}
                    Err(error) => {
                        backup_metrics.backup_failure();
                        tracing::error!(%error, "snapshot failed; retrying next tick");
                    }
                }
            }
        }))
    } else {
        tracing::info!("snapshot writer disabled");
        None
    };

    let addr = std::env::var("ANVILMQ_ADDR")
        .unwrap_or_else(|_| "[::1]:50051".into())
        .parse()?;
    let max_chain_size = match std::env::var("ANVILMQ_MAX_CHAIN_SIZE") {
        Ok(value) => value.trim().parse::<u64>().map_err(|_| {
            std::io::Error::other("ANVILMQ_MAX_CHAIN_SIZE must be a non-negative integer")
        })?,
        Err(std::env::VarError::NotPresent) => 0,
        Err(error) => return Err(error.into()),
    };
    // Opt-in equal round-robin fairness across facets; default off keeps the claim path identical.
    let fairness_enabled = std::env::var("ANVILMQ_FAIRNESS_ENABLED")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(false);
    let shutdown_db = db_manager.clone();
    let service = MyQueueService {
        db_manager,
        max_execution_depth: 10, // Max recursion depth guardrail
        max_chain_size,          // Runaway-chain quarantine cap; 0 disables
        fairness_enabled,        // Equal round-robin tenant fairness; off = identical to legacy
        lease_duration_ms,       // Worker lease TTL; ANVILMQ_LEASE_DURATION_MS (default 30s)
    };

    tracing::info!(%addr, %http_addr, "AnvilMQ daemon listening");

    let grpc = {
        let mut rx = shutdown_tx.subscribe();
        Server::builder()
            .add_service(QueueServiceServer::new(service))
            .serve_with_shutdown(addr, async move {
                let _ = rx.recv().await;
            })
    };

    // Run both servers until one drains (via the shutdown signal) or errors on its own. If either
    // future returns first, notify the other so it also stops, then await it so we never hang.
    tokio::pin!(grpc);
    tokio::pin!(http);
    let mut grpc_result: Option<Result<(), Box<dyn std::error::Error>>> = None;
    let mut http_result: Option<Result<(), Box<dyn std::error::Error>>> = None;
    tokio::select! {
        result = &mut grpc => {
            grpc_result = Some(result.map_err(Into::into));
            let _ = shutdown_tx.send(());
        }
        result = &mut http => {
            http_result = Some(result.map_err(Into::into));
            let _ = shutdown_tx.send(());
        }
    }
    if grpc_result.is_none() {
        grpc_result = Some((&mut grpc).await.map_err(Into::into));
    }
    if http_result.is_none() {
        http_result = Some((&mut http).await.map_err(Into::into));
    }

    // Both servers have drained. Stop the background tasks so nothing else writes, then checkpoint
    // the WAL. A checkpoint failure is logged but never blocks a clean exit.
    recovery.abort();
    sampler.abort();
    if let Some(retention) = retention {
        retention.abort();
    }
    if let Some(backup) = backup {
        backup.abort();
    }
    if let Err(error) = shutdown_db.checkpoint().await {
        tracing::warn!(%error, "wal checkpoint failed during shutdown; continuing");
    }
    tracing::info!("graceful shutdown complete");

    // Surface any server error (after best-effort cleanup + checkpoint) so failures still exit non-zero.
    grpc_result.transpose()?;
    http_result.transpose()?;

    Ok(())
}

/// Completes when the process receives SIGTERM (unix) or Ctrl-C (all platforms), the signals
/// Kubernetes and interactive sessions use to request termination. Windows has no SIGTERM, so the
/// terminate branch is gated to unix and stubbed elsewhere.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn service() -> MyQueueService {
        service_with_lease(leases::LEASE_DURATION_MS).await
    }

    async fn service_with_lease(lease_duration_ms: i64) -> MyQueueService {
        let service = MyQueueService {
            db_manager: Arc::new(DatabaseManager::new(":memory:").await.unwrap()),
            max_execution_depth: 10,
            max_chain_size: 0,
            fairness_enabled: false,
            lease_duration_ms,
        };
        // Seed the ancestry parent referenced by the enqueue helper so that supplied
        // parent_id/execution_depth pass ancestry validation. Parked in job_history so it
        // is never claimable and does not perturb scheduling/count assertions.
        let conn = service.db_manager.get_shared_connection();
        tokio::task::spawn_blocking(move || {
            conn.blocking_lock().execute(
                "INSERT INTO job_history (id, name, state, priority, payload, trace_id, execution_depth, attempts, max_attempts, created_at, finished_at)
                 VALUES ('parent', 'email', 'Completed', 0, X'', 'trace', 1, 1, 1, 1, 1)",
                [],
            ).unwrap();
        })
        .await
        .unwrap();
        service
    }

    async fn enqueue(service: &MyQueueService, name: &str, priority: i32, delay_ms: i64) -> String {
        service
            .add_job(Request::new(AddJobRequest {
                name: name.into(),
                priority,
                delay_ms,
                payload: vec![0, 1, 255],
                metadata: Some(queue::v1::JobMetadata {
                    parent_id: "parent".into(),
                    trace_id: "trace".into(),
                    execution_depth: 2,
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .id
    }

    async fn claim(service: &MyQueueService, queues: &[&str]) -> GetNextJobResponse {
        service
            .get_next_job(Request::new(GetNextJobRequest {
                worker_id: "worker".into(),
                queue_names: queues.iter().map(|s| s.to_string()).collect(),
            }))
            .await
            .unwrap()
            .into_inner()
    }

    #[test]
    fn lease_duration_parses_defaults_and_rejects_invalid() {
        assert_eq!(
            parse_lease_duration_ms(None).unwrap(),
            leases::LEASE_DURATION_MS
        );
        assert_eq!(parse_lease_duration_ms(Some("45000")).unwrap(), 45_000);
        assert_eq!(parse_lease_duration_ms(Some(" 1000 ")).unwrap(), 1_000);
        assert!(parse_lease_duration_ms(Some("0")).is_err());
        assert!(parse_lease_duration_ms(Some("-5")).is_err());
        assert!(parse_lease_duration_ms(Some("abc")).is_err());
        // Sub-second leases are rejected by the floor.
        assert!(parse_lease_duration_ms(Some("999")).is_err());
    }

    #[test]
    fn recovery_interval_parses_defaults_and_rejects_invalid() {
        assert_eq!(
            parse_recovery_interval_ms(None).unwrap(),
            DEFAULT_RECOVERY_INTERVAL_MS
        );
        assert_eq!(parse_recovery_interval_ms(Some("2500")).unwrap(), 2_500);
        assert!(parse_recovery_interval_ms(Some("0")).is_err());
        assert!(parse_recovery_interval_ms(Some("-1")).is_err());
        assert!(parse_recovery_interval_ms(Some("nope")).is_err());
    }

    #[tokio::test]
    async fn custom_lease_duration_applies_to_claim() {
        let service = service_with_lease(120_000).await;
        enqueue(&service, "email", 0, 0).await;
        let job = claim(&service, &["email"]).await;
        assert!(job.found);
        let conn = service.db_manager.get_shared_connection();
        let now =
            tokio::task::spawn_blocking(move || leases::now_ms(&conn.blocking_lock()).unwrap())
                .await
                .unwrap();
        let remaining = job.lease_expires_at_ms - now;
        // Reflects the configured 120s lease, not the 30s default.
        assert!(
            (110_000..=120_000).contains(&remaining),
            "remaining={remaining}"
        );
    }

    #[tokio::test]
    async fn dequeue_orders_filters_and_returns_persisted_metadata() {
        let service = service().await;
        let older = enqueue(&service, "email", 5, 0).await;
        let newer = enqueue(&service, "email", 5, 0).await;
        let urgent = enqueue(&service, "email", 1, 0).await;
        let other = enqueue(&service, "other", 0, 0).await;
        enqueue(&service, "email", -1, 60000).await;
        let conn = service.db_manager.get_shared_connection();
        let older_copy = older.clone();
        let newer_copy = newer.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            conn.execute("UPDATE jobs SET created_at = 1 WHERE id = ?1", [older_copy])
                .unwrap();
            conn.execute("UPDATE jobs SET created_at = 2 WHERE id = ?1", [newer_copy])
                .unwrap();
        })
        .await
        .unwrap();
        let job = claim(&service, &["email", "missing"]).await;
        assert_eq!(job.id, urgent);
        assert_eq!(job.payload, vec![0, 1, 255]);
        assert_eq!(job.attempts, 1);
        let metadata = job.metadata.unwrap();
        assert_eq!(metadata.parent_id, "parent");
        assert_eq!(metadata.trace_id, "trace");
        assert_eq!(metadata.execution_depth, 2);
        assert_eq!(claim(&service, &["email"]).await.id, older);
        assert_eq!(claim(&service, &["email"]).await.id, newer);
        assert!(!claim(&service, &["email"]).await.found);
        assert_eq!(claim(&service, &[]).await.id, other);
        assert!(!claim(&service, &[]).await.found);
    }

    #[tokio::test]
    async fn concurrent_workers_claim_a_job_only_once() {
        let service = Arc::new(service().await);
        let id = enqueue(&service, "email", 0, 0).await;
        let mut workers = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let service = service.clone();
            workers.spawn(async move { claim(&service, &[]).await });
        }
        let mut claimed = Vec::new();
        while let Some(result) = workers.join_next().await {
            let job = result.unwrap();
            if job.found {
                claimed.push(job.id);
            }
        }
        assert_eq!(claimed, vec![id.clone()]);
        let conn = service.db_manager.get_shared_connection();
        let stored = tokio::task::spawn_blocking(move || {
            conn.blocking_lock()
                .query_row(
                    "SELECT state, worker_id, attempts FROM jobs WHERE id = ?1",
                    [id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, u32>(2)?,
                        ))
                    },
                )
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(stored, ("Active".into(), "worker".into(), 1));
    }

    #[tokio::test]
    async fn failed_claim_rolls_back_and_can_be_retried() {
        let service = service().await;
        let id = enqueue(&service, "email", 0, 0).await;
        let conn = service.db_manager.get_shared_connection();
        tokio::task::spawn_blocking(move || {
            conn.blocking_lock()
                .execute_batch(
                    "CREATE TRIGGER reject_claim AFTER UPDATE ON jobs
                 BEGIN SELECT RAISE(ABORT, 'injected failure'); END;",
                )
                .unwrap();
        })
        .await
        .unwrap();
        let error = service
            .get_next_job(Request::new(GetNextJobRequest {
                worker_id: "worker".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(service
            .db_manager
            .metrics
            .render()
            .contains("anvilmq_due_wait_seconds_count 0\n"));
        let conn = service.db_manager.get_shared_connection();
        tokio::task::spawn_blocking(move || {
            conn.blocking_lock()
                .execute_batch("DROP TRIGGER reject_claim")
                .unwrap();
        })
        .await
        .unwrap();
        let job = claim(&service, &[]).await;
        assert_eq!(job.id, id);
        assert_eq!(job.attempts, 1);
        assert!(service
            .db_manager
            .metrics
            .render()
            .contains("anvilmq_due_wait_seconds_count 1\n"));
    }

    #[tokio::test]
    async fn invalid_poll_does_not_claim_work() {
        let service = service().await;
        let id = enqueue(&service, "email", 0, 0).await;
        for request in [
            GetNextJobRequest::default(),
            GetNextJobRequest {
                worker_id: "worker".into(),
                queue_names: vec![" ".into()],
            },
        ] {
            assert_eq!(
                service
                    .get_next_job(Request::new(request))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
        assert_eq!(claim(&service, &[]).await.id, id);
    }
}
