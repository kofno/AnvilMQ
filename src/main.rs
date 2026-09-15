use rusqlite::{OptionalExtension, TransactionBehavior};
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};

mod db;
mod leases;
#[cfg(test)]
mod lifecycle_tests;
mod scheduling;
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
}

#[tonic::async_trait]
impl QueueService for MyQueueService {
    async fn add_job(
        &self,
        request: Request<AddJobRequest>,
    ) -> Result<Response<AddJobResponse>, Status> {
        let req = request.into_inner();
        if req.delay_ms < 0 || req.retry_backoff_ms < 0 || req.retry_backoff_max_ms < 0 {
            return Err(Status::invalid_argument("delays must not be negative"));
        }
        let backoff = req.retry_backoff_ms;
        let cap = if req.retry_backoff_max_ms == 0 {
            60_000.max(backoff)
        } else {
            req.retry_backoff_max_ms
        };
        if cap < backoff {
            return Err(Status::invalid_argument(
                "backoff cap must be at least the base",
            ));
        }
        let metadata = req.metadata.unwrap_or_default();

        // 1. Enforce Circuit Breaker Check on Ancestry Depth
        if metadata.execution_depth > self.max_execution_depth {
            return Err(Status::resource_exhausted(format!(
                "Circuit breaker tripped: execution depth {} exceeds max allowed {}",
                metadata.execution_depth, self.max_execution_depth
            )));
        }

        let job_id = uuid::Uuid::new_v4().to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let _available_at = now
            .checked_add(req.delay_ms)
            .ok_or_else(|| Status::invalid_argument("delay timestamp overflows"))?;
        let state = if req.delay_ms > 0 {
            "Delayed"
        } else {
            "Waiting"
        };
        let conn_arc = self.db_manager.get_shared_connection();
        let id_clone = job_id.clone();
        let name_clone = req.name.clone();
        let state_clone = state.to_string();
        let priority = req.priority;
        let payload = req.payload;
        let parent_id = if metadata.parent_id.is_empty() {
            None
        } else {
            Some(metadata.parent_id)
        };
        let trace_id = if metadata.trace_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            metadata.trace_id
        };
        let exec_depth = metadata.execution_depth;
        let max_attempts = if req.max_attempts == 0 {
            3
        } else {
            req.max_attempts
        };
        let facet = if req.rate_limit_facet.is_empty() {
            None
        } else {
            Some(req.rate_limit_facet)
        };

        // 2. Perform blocking SQLite write inside spawn_blocking to keep Tokio async loop clean
        tokio::task::spawn_blocking(move || {
            let conn = conn_arc.blocking_lock();
            let now = leases::now_ms(&conn)?;
            let available_at = now.saturating_add(req.delay_ms);
            conn.execute(
                "INSERT INTO jobs (id, name, state, priority, payload, parent_id, trace_id, execution_depth, max_attempts, created_at, updated_at, rate_limit_facet, available_at, retry_backoff_ms, retry_backoff_max_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                rusqlite::params![
                    id_clone,
                    name_clone,
                    state_clone,
                    priority,
                    payload,
                    parent_id,
                    trace_id,
                    exec_depth,
                    max_attempts,
                    now,
                    now,
                    facet, available_at, backoff, cap,
                ],
            )
        }).await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(|e| Status::internal(e.to_string()))?;

        println!(
            "Successfully enqueued job ID: {} (State: {})",
            job_id, state
        );

        Ok(Response::new(AddJobResponse {
            id: job_id,
            state: state.to_string(),
        }))
    }

    async fn get_next_job(
        &self,
        request: Request<GetNextJobRequest>,
    ) -> Result<Response<GetNextJobResponse>, Status> {
        let req = request.into_inner();
        if req.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id must not be blank"));
        }
        if req.queue_names.iter().any(|name| name.trim().is_empty()) {
            return Err(Status::invalid_argument(
                "queue_names must not contain blank names",
            ));
        }
        let conn_arc = self.db_manager.get_shared_connection();
        let job = tokio::task::spawn_blocking(move || -> rusqlite::Result<GetNextJobResponse> {
            let mut conn = conn_arc.blocking_lock();
            // Acquire the database write lock before selecting, including across connections.
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now = leases::now_ms(&tx)?;
            let mut sql = String::from(
                "SELECT id, name, payload, parent_id, trace_id, execution_depth, attempts
                 FROM jobs WHERE state IN ('Waiting', 'Delayed') AND available_at <= ?",
            );
            if !req.queue_names.is_empty() {
                sql.push_str(" AND name IN (");
                sql.push_str(&vec!["?"; req.queue_names.len()].join(","));
                sql.push(')');
            }
            sql.push_str(" ORDER BY priority ASC, created_at ASC, id ASC LIMIT 1");
            let job = tx
                .query_row(
                    &sql,
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
                return Ok(GetNextJobResponse::default());
            };
            job.attempts = job
                .attempts
                .checked_add(1)
                .ok_or(rusqlite::Error::InvalidQuery)?;
            let now = leases::now_ms(&tx)?;
            job.lease_expires_at_ms = now + leases::LEASE_DURATION_MS;
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
            tx.commit()?;
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
        let req = request.into_inner();
        self.finish_job(req.id, req.worker_id, req.attempt, None)
            .await?;
        Ok(Response::new(CompleteJobResponse { success: true }))
    }

    async fn heartbeat(
        &self,
        request: Request<queue::v1::HeartbeatRequest>,
    ) -> Result<Response<queue::v1::HeartbeatResponse>, Status> {
        self.renew_lease(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn fail_job(
        &self,
        request: Request<FailJobRequest>,
    ) -> Result<Response<FailJobResponse>, Status> {
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
        let conn = self.db_manager.get_shared_connection();
        tokio::task::spawn_blocking(move || {
            let internal = |e: rusqlite::Error| Status::internal(e.to_string());
            let mut conn = conn.blocking_lock();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(internal)?;
            let job = tx.query_row(
                "SELECT state, worker_id, attempts, max_attempts FROM jobs WHERE id = ?1", [&id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?, row.get::<_, u32>(2)?, row.get::<_, u32>(3)?)),
            ).optional().map_err(internal)?;
            let Some((state, owner, attempts, max_attempts)) = job else {
                let archived: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM job_history WHERE id = ?1)", [&id], |row| row.get(0)).map_err(internal)?;
                return Err(if archived { Status::failed_precondition("job already finished") } else { Status::not_found("job not found") });
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
            if failed && attempts < max_attempts {
                let (base, cap): (i64, i64) = tx.query_row("SELECT retry_backoff_ms, retry_backoff_max_ms FROM jobs WHERE id = ?1", [&id], |r| Ok((r.get(0)?, r.get(1)?))).map_err(internal)?;
                let delay = scheduling::retry_delay(base, cap, attempts);
                let available_at = now.saturating_add(delay);
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
            Ok(failed && attempts >= max_attempts)
        }).await.map_err(|e| Status::internal(e.to_string()))?
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize embedded libSQL/SQLite database file
    let db_manager = Arc::new(DatabaseManager::new("anvil.db").await?);
    let recovery_db = db_manager.clone();
    let recovery = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            match leases::recover_expired(recovery_db.clone()).await {
                Ok(count) if count > 0 => println!("Recovered {count} expired job claims"),
                Ok(_) => {}
                Err(error) => eprintln!("Lease recovery failed; retrying next tick: {error}"),
            }
        }
    });

    let addr = "[::1]:50051".parse()?;
    let service = MyQueueService {
        db_manager,
        max_execution_depth: 10, // Max recursion depth guardrail
    };

    println!("AnvilMQ daemon listening on {}", addr);

    let result = Server::builder()
        .add_service(QueueServiceServer::new(service))
        .serve(addr)
        .await;
    recovery.abort();
    result?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn service() -> MyQueueService {
        MyQueueService {
            db_manager: Arc::new(DatabaseManager::new(":memory:").await.unwrap()),
            max_execution_depth: 10,
        }
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
