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
    if metadata.execution_depth > service.max_execution_depth {
        return Err(Status::resource_exhausted(format!(
            "Circuit breaker tripped: execution depth {} exceeds max allowed {}",
            metadata.execution_depth, service.max_execution_depth
        )));
    }
    // Compare normalized input, before generating IDs/timestamps. Exact bytes avoid
    // hash collisions and make changes to payload OR scheduling/ownership options conflicts.
    let receipt_request = if req.idempotency_key.is_empty() {
        None
    } else {
        Some(req.encode_to_vec())
    };
    let metrics = service.db_manager.metrics.clone();
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
        let available_at = now.checked_add(req.delay_ms).ok_or_else(|| Status::invalid_argument("delay timestamp overflows"))?;
        let id = uuid::Uuid::new_v4().to_string();
        let state = if req.delay_ms > 0 { "Delayed" } else { "Waiting" };
        let metadata = req.metadata.unwrap_or_default();
        let trace_id = if metadata.trace_id.is_empty() { uuid::Uuid::new_v4().to_string() } else { metadata.trace_id };
        tx.execute("INSERT INTO jobs (id, name, state, priority, payload, parent_id, trace_id, execution_depth, max_attempts, created_at, updated_at, rate_limit_facet, available_at, retry_backoff_ms, retry_backoff_max_ms)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11, ?12, ?13, ?14)",
            rusqlite::params![id, req.name, state, req.priority, req.payload,
                if metadata.parent_id.is_empty() { None } else { Some(metadata.parent_id) }, trace_id, metadata.execution_depth,
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
