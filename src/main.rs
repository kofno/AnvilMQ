use rusqlite::{OptionalExtension, TransactionBehavior};
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};

mod db;
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
            conn.execute(
                "INSERT INTO jobs (id, name, state, priority, payload, parent_id, trace_id, execution_depth, max_attempts, created_at, updated_at, rate_limit_facet) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
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
                    facet,
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
            let mut sql = String::from(
                "SELECT id, name, payload, parent_id, trace_id, execution_depth, attempts
                 FROM jobs WHERE state = 'Waiting'",
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
                    rusqlite::params_from_iter(req.queue_names.iter()),
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
            tx.execute(
                "UPDATE jobs SET state = 'Active', worker_id = ?1, attempts = ?2,
                 updated_at = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
                 WHERE id = ?3 AND state = 'Waiting'",
                rusqlite::params![req.worker_id, job.attempts, job.id],
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
        _request: Request<CompleteJobRequest>,
    ) -> Result<Response<CompleteJobResponse>, Status> {
        Ok(Response::new(CompleteJobResponse { success: true }))
    }

    async fn fail_job(
        &self,
        _request: Request<FailJobRequest>,
    ) -> Result<Response<FailJobResponse>, Status> {
        Ok(Response::new(FailJobResponse {
            success: true,
            moved_to_failed_state: true,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize embedded libSQL/SQLite database file
    let db_manager = Arc::new(DatabaseManager::new("anvil.db").await?);

    let addr = "[::1]:50051".parse()?;
    let service = MyQueueService {
        db_manager,
        max_execution_depth: 10, // Max recursion depth guardrail
    };

    println!("AnvilMQ daemon listening on {}", addr);

    Server::builder()
        .add_service(QueueServiceServer::new(service))
        .serve(addr)
        .await?;

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
        enqueue(&service, "email", -1, 100).await;
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
