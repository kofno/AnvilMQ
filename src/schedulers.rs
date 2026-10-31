//! Job scheduler CRUD RPC handlers (A5a — declarative cron/tz-aware schedulers).
//!
//! This covers persistence, proto CRUD, and tz-aware next-run computation only. The background
//! loop that produces jobs from due schedulers is A5b; nothing here spawns a task or enqueues.

use crate::db::{self, DatabaseManager, JobSchedulerRow, SchedulerSpec};
use crate::leases::now_ms;
use crate::queue::v1::*;
use crate::scheduling::{next_cron_after, validate_cron, SchedulerError};
use rusqlite::TransactionBehavior;
use std::sync::Arc;
use tonic::Status;

/// Upper bound on a template payload. Generous for control-plane records while bounding the WAL
/// cost of a single scheduler row.
pub const MAX_SCHEDULER_PAYLOAD_BYTES: usize = 1024 * 1024;

fn internal(e: rusqlite::Error) -> Status {
    Status::internal(e.to_string())
}

fn map_scheduler_error(e: SchedulerError) -> Status {
    match e {
        SchedulerError::InvalidCron(m) => Status::invalid_argument(format!("invalid cron: {m}")),
        SchedulerError::InvalidTimezone(m) => {
            Status::invalid_argument(format!("invalid timezone: {m}"))
        }
        SchedulerError::Compute(m) => Status::internal(format!("next-run computation failed: {m}")),
    }
}

/// Convert a persisted row into its proto representation.
fn row_to_proto(row: JobSchedulerRow) -> JobScheduler {
    JobScheduler {
        id: row.id,
        job_name: row.job_name,
        payload: row.payload,
        cron: row.cron,
        timezone: row.timezone.unwrap_or_default(),
        next_run_at: row.next_run_at,
        produced_count: row.produced_count,
        last_job_id: row.last_job_id,
        priority: row.priority.map(|v| v as i32),
        max_attempts: row.max_attempts.map(|v| v as u32),
        retry_backoff_ms: row.retry_backoff_ms,
        retry_backoff_max_ms: row.retry_backoff_max_ms,
        retry_jitter: row.retry_jitter,
        rate_limit_facet: row.rate_limit_facet,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

pub async fn upsert(
    db: Arc<DatabaseManager>,
    req: UpsertJobSchedulerRequest,
) -> Result<UpsertJobSchedulerResponse, Status> {
    let id = req.id.trim().to_string();
    if id.is_empty() {
        return Err(Status::invalid_argument("id must not be blank"));
    }
    let template = req
        .template
        .ok_or_else(|| Status::invalid_argument("template is required"))?;
    let repeat = req
        .repeat
        .ok_or_else(|| Status::invalid_argument("repeat is required"))?;

    let job_name = template.name.trim().to_string();
    if job_name.is_empty() {
        return Err(Status::invalid_argument("template.name must not be blank"));
    }
    if template.payload.len() > MAX_SCHEDULER_PAYLOAD_BYTES {
        return Err(Status::invalid_argument(format!(
            "payload exceeds {MAX_SCHEDULER_PAYLOAD_BYTES} bytes"
        )));
    }
    let cron = repeat.cron.trim().to_string();
    if cron.is_empty() {
        return Err(Status::invalid_argument("repeat.cron must not be blank"));
    }
    let timezone = repeat.timezone.trim().to_string();
    // Reject bad cron/timezone before touching the database.
    validate_cron(&cron, &timezone).map_err(map_scheduler_error)?;

    if let Some(jitter) = template.retry_jitter {
        if jitter.is_nan() || !(0.0..=1.0).contains(&jitter) {
            return Err(Status::invalid_argument(
                "retry_jitter must be a fraction in [0, 1]",
            ));
        }
    }
    if template.retry_backoff_ms.is_some_and(|v| v < 0)
        || template.retry_backoff_max_ms.is_some_and(|v| v < 0)
    {
        return Err(Status::invalid_argument(
            "backoff delays must not be negative",
        ));
    }
    if let (Some(base), Some(cap)) = (template.retry_backoff_ms, template.retry_backoff_max_ms) {
        if cap < base {
            return Err(Status::invalid_argument(
                "retry_backoff_max_ms must be at least retry_backoff_ms",
            ));
        }
    }

    let spec_base = SchedulerSpec {
        id: id.clone(),
        job_name,
        payload: template.payload,
        cron: cron.clone(),
        // Persist NULL for the UTC default so the stored form is canonical.
        timezone: if timezone.is_empty() {
            None
        } else {
            Some(timezone.clone())
        },
        next_run_at: None,
        priority: template.priority.map(|v| v as i64),
        max_attempts: template.max_attempts.map(|v| v as i64),
        retry_backoff_ms: template.retry_backoff_ms,
        retry_backoff_max_ms: template.retry_backoff_max_ms,
        retry_jitter: template.retry_jitter,
        rate_limit_facet: template.rate_limit_facet,
    };

    let conn = db.get_shared_connection();
    let metrics = db.metrics.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = conn.blocking_lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(internal)?;
        let now = now_ms(&tx).map_err(internal)?;
        let next = next_cron_after(now, &cron, &timezone).map_err(map_scheduler_error)?;
        // Detect insert-vs-update so the active-schedulers gauge only increments when a new
        // scheduler row is created. A5 schedulers are always active (next_run_at non-NULL), so an
        // update leaves the active count unchanged.
        let existed = db::get_scheduler(&tx, &id).map_err(internal)?.is_some();
        let spec = SchedulerSpec {
            next_run_at: Some(next),
            ..spec_base
        };
        db::upsert_scheduler(&tx, &spec, now).map_err(internal)?;
        tx.commit().map_err(internal)?;
        if !existed {
            metrics.scheduler_activated();
        }
        tracing::info!(id = %id, cron = %cron, next_run_at = next, "job scheduler upserted");
        Ok(UpsertJobSchedulerResponse {
            id,
            next_run_at: next,
        })
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
}

pub async fn delete(
    db: Arc<DatabaseManager>,
    req: DeleteJobSchedulerRequest,
) -> Result<DeleteJobSchedulerResponse, Status> {
    let id = req.id.trim().to_string();
    if id.is_empty() {
        return Err(Status::invalid_argument("id must not be blank"));
    }
    let conn = db.get_shared_connection();
    let metrics = db.metrics.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = conn.blocking_lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(internal)?;
        let deleted = db::delete_scheduler(&tx, &id).map_err(internal)?;
        tx.commit().map_err(internal)?;
        if deleted {
            metrics.scheduler_deactivated();
        }
        Ok(DeleteJobSchedulerResponse { deleted })
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
}

pub async fn get(
    db: Arc<DatabaseManager>,
    req: GetJobSchedulerRequest,
) -> Result<JobScheduler, Status> {
    let id = req.id.trim().to_string();
    if id.is_empty() {
        return Err(Status::invalid_argument("id must not be blank"));
    }
    let conn = db.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let conn = conn.blocking_lock();
        let row = db::get_scheduler(&conn, &id).map_err(internal)?;
        row.map(row_to_proto)
            .ok_or_else(|| Status::not_found(format!("no scheduler with id {id}")))
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
}

pub async fn list(
    db: Arc<DatabaseManager>,
    _req: ListJobSchedulersRequest,
) -> Result<ListJobSchedulersResponse, Status> {
    let conn = db.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let conn = conn.blocking_lock();
        let rows = db::list_schedulers(&conn).map_err(internal)?;
        Ok(ListJobSchedulersResponse {
            schedulers: rows.into_iter().map(row_to_proto).collect(),
        })
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
}
