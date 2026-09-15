use crate::{
    db::DatabaseManager,
    queue::v1::{HeartbeatRequest, HeartbeatResponse},
    MyQueueService,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use std::sync::Arc;
use tonic::Status;

pub const LEASE_DURATION_MS: i64 = 30_000;

// Read wall time after acquiring the write lock, never before waiting for it.
pub fn now_ms(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row(
        "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)",
        [],
        |r| r.get(0),
    )
}

impl MyQueueService {
    pub async fn renew_lease(&self, req: HeartbeatRequest) -> Result<HeartbeatResponse, Status> {
        if req.id.trim().is_empty() || req.worker_id.trim().is_empty() || req.attempt == 0 {
            return Err(Status::invalid_argument(
                "id, worker_id, and a positive attempt are required",
            ));
        }
        let conn = self.db_manager.get_shared_connection();
        tokio::task::spawn_blocking(move || {
            let internal = |e: rusqlite::Error| Status::internal(e.to_string());
            let mut conn = conn.blocking_lock();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(internal)?;
            let job = tx.query_row("SELECT state, worker_id, attempts, lease_expires_at_ms FROM jobs WHERE id = ?1", [&req.id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, u32>(2)?, r.get::<_, Option<i64>>(3)?))).optional().map_err(internal)?;
            let Some((state, owner, attempt, expiry)) = job else {
                let archived: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM job_history WHERE id = ?1)", [&req.id], |r| r.get(0)).map_err(internal)?;
                return Err(if archived { Status::failed_precondition("job already finished") } else { Status::not_found("job not found") });
            };
            if state != "Active" { return Err(Status::failed_precondition("job is not Active")); }
            if owner.as_deref() != Some(req.worker_id.as_str()) { return Err(Status::permission_denied("worker does not own this job")); }
            let now = now_ms(&tx).map_err(internal)?;
            if attempt != req.attempt || expiry.is_none_or(|expiry| expiry <= now) {
                return Err(Status::failed_precondition("claim is stale or expired"));
            }
            // Never shorten a lease if the system clock moves backwards.
            let expiry = expiry.unwrap().max(now + LEASE_DURATION_MS);
            tx.execute("UPDATE jobs SET lease_expires_at_ms = ?1, updated_at = ?2 WHERE id = ?3", rusqlite::params![expiry, now, req.id]).map_err(internal)?;
            tx.commit().map_err(internal)?;
            Ok(HeartbeatResponse { lease_expires_at_ms: expiry })
        }).await.map_err(|e| Status::internal(e.to_string()))?
    }
}

pub async fn recover_expired(db: Arc<DatabaseManager>) -> Result<usize, Status> {
    let conn = db.get_shared_connection();
    tokio::task::spawn_blocking(move || -> rusqlite::Result<usize> {
        let mut conn = conn.blocking_lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_ms(&tx)?;
        // Bound each transaction so a large expired backlog cannot monopolize the writer.
        let jobs = {
            let mut statement = tx.prepare("SELECT id, attempts, max_attempts, retry_backoff_ms, retry_backoff_max_ms FROM jobs WHERE state = 'Active' AND lease_expires_at_ms <= ?1 ORDER BY lease_expires_at_ms, id LIMIT 100")?;
            let rows = statement.query_map([now], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u32>(1)?, r.get::<_, u32>(2)?, r.get::<_, i64>(3)?, r.get::<_, i64>(4)?)))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (id, attempts, max_attempts, base, cap) in &jobs {
            if attempts < max_attempts {
                let delay = crate::scheduling::retry_delay(*base, *cap, *attempts);
                tx.execute("UPDATE jobs SET state = ?3, available_at = ?4, worker_id = NULL, lease_expires_at_ms = NULL, last_error = 'Worker lease expired', updated_at = ?1 WHERE id = ?2", rusqlite::params![now, id, if delay == 0 { "Waiting" } else { "Delayed" }, now.saturating_add(delay)])?;
            } else {
                tx.execute("INSERT INTO job_history (id, name, state, priority, payload, parent_id, trace_id, execution_depth, attempts, max_attempts, created_at, finished_at, worker_id, last_error, rate_limit_facet)
                    SELECT id, name, 'Failed', priority, payload, parent_id, trace_id, execution_depth, attempts, max_attempts, created_at, ?1, worker_id, 'Worker lease expired', rate_limit_facet FROM jobs WHERE id = ?2", rusqlite::params![now, id])?;
                tx.execute("DELETE FROM jobs WHERE id = ?1", [id])?;
            }
        }
        tx.commit()?;
        Ok(jobs.len())
    }).await.map_err(|e| Status::internal(e.to_string()))?.map_err(|e| Status::internal(e.to_string()))
}
