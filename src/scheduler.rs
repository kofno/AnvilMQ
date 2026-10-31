//! A5b background scheduler loop: periodically produce jobs from due declarative schedulers.
//!
//! A5a persists `job_schedulers` records and computes their first `next_run_at`. This loop scans
//! due schedulers, produces one job per due scheduler through the A3 enqueue core (so a scheduled
//! job is indistinguishable from a client enqueue), and atomically advances `next_run_at` to the
//! next future cron occurrence in the SAME transaction as the job insert. Each scheduler fires in
//! its own IMMEDIATE transaction so one failing scheduler never blocks the rest of the batch, and a
//! crash mid-fire rolls back both the job and the advance (exactly-once per fire, retried next
//! tick). Only `next_run_at` is persisted, so the model is restart-safe: a persisted due run fires
//! on the first tick after startup, never a backfill stampede.

use crate::db::{self, DatabaseManager, JobSchedulerRow};
use crate::leases::now_ms;
use crate::queue::v1::AddJobRequest;
use crate::scheduling::next_cron_after;
use crate::telemetry::Metrics;
use rusqlite::TransactionBehavior;
use std::sync::Arc;

/// Default scheduler scan cadence, overridable via `ANVILMQ_SCHEDULER_INTERVAL_MS`.
pub const DEFAULT_SCHEDULER_INTERVAL_MS: u64 = 1_000;

/// Default number of due schedulers fired per tick, overridable via
/// `ANVILMQ_SCHEDULER_BATCH_SIZE`. Bounds how long the single writer is held per tick.
pub const DEFAULT_SCHEDULER_BATCH_SIZE: usize = 256;

/// Static configuration for the scheduler loop. `max_execution_depth` / `max_chain_size` mirror the
/// gRPC service config so produced jobs get identical admission limits.
#[derive(Clone, Copy)]
pub struct SchedulerConfig {
    pub interval_ms: u64,
    pub batch_size: usize,
    pub max_execution_depth: u32,
    pub max_chain_size: u64,
}

/// Why a single scheduler fire failed. Kept internal: the loop counts it on `scheduler_errors`,
/// logs it, and moves on so the same fire retries next tick.
#[derive(Debug, thiserror::Error)]
enum FireError {
    #[error("db error: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("enqueue failed: {0}")]
    Enqueue(String),
    #[error("cron advance failed: {0}")]
    Cron(#[from] crate::scheduling::SchedulerError),
}

/// Build the produced job's request from a scheduler's template columns. A scheduled job is a ROOT
/// job: no parent metadata (depth 0), no delay, and no idempotency key (scheduled jobs are never
/// deduplicated — tx atomicity provides exactly-once). Unset template options fall through to the
/// same server defaults a client enqueue would get.
fn build_request(row: &JobSchedulerRow) -> AddJobRequest {
    AddJobRequest {
        name: row.job_name.clone(),
        payload: row.payload.clone(),
        priority: row.priority.unwrap_or(0) as i32,
        metadata: None,
        max_attempts: row.max_attempts.unwrap_or(0) as u32,
        delay_ms: 0,
        rate_limit_facet: row.rate_limit_facet.clone().unwrap_or_default(),
        retry_backoff_ms: row.retry_backoff_ms.unwrap_or(0),
        retry_backoff_max_ms: row.retry_backoff_max_ms.unwrap_or(0),
        idempotency_key: String::new(),
        retry_jitter: row.retry_jitter.unwrap_or(0.0),
        release_on_finish: false,
    }
}

/// Fire one due scheduler in a single IMMEDIATE transaction: produce its job through the enqueue
/// core, compute the next strictly-after cron occurrence, and advance the scheduler's runtime
/// bookkeeping. The job insert and the advance commit atomically; on ANY failure the whole
/// transaction rolls back (neither the job nor the advance persists) so the fire retries next tick.
/// Success metrics are flushed only AFTER the commit.
fn fire_one(
    conn: &mut rusqlite::Connection,
    row: &JobSchedulerRow,
    config: SchedulerConfig,
    metrics: &Metrics,
) -> Result<(), FireError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let now = now_ms(&tx)?;
    let req = build_request(row);
    let (job_id, success) = crate::enqueue::enqueue_scheduled(
        &tx,
        req,
        now,
        config.max_execution_depth,
        config.max_chain_size,
        metrics,
    )
    .map_err(|status| FireError::Enqueue(status.message().to_string()))?;
    // Strictly-after `now`: a long outage jumps to the next future fire rather than backfilling.
    let tz = row.timezone.as_deref().unwrap_or("");
    let advance = next_cron_after(now, &row.cron, tz)?;
    db::advance_scheduler(&tx, &row.id, advance, &job_id, now)?;
    tx.commit()?;
    success.flush(metrics);
    metrics.scheduler_job_produced();
    Ok(())
}

/// One scheduler tick: select the due batch and fire each due scheduler in isolation. Returns the
/// number of jobs produced. Per-scheduler failures are counted on `scheduler_errors`, logged, and
/// skipped so a single bad scheduler never blocks the rest of the batch.
pub async fn run_once(db: Arc<DatabaseManager>, config: SchedulerConfig) -> usize {
    let metrics = db.metrics.clone();
    let conn = db.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let mut conn = conn.blocking_lock();
        let now = match now_ms(&conn) {
            Ok(now) => now,
            Err(error) => {
                metrics.scheduler_error();
                tracing::error!(%error, "scheduler tick failed to read clock; retrying next tick");
                return 0;
            }
        };
        let due = match db::select_due_schedulers(&conn, now, config.batch_size) {
            Ok(due) => due,
            Err(error) => {
                metrics.scheduler_error();
                tracing::error!(%error, "scheduler tick failed to select due schedulers; retrying next tick");
                return 0;
            }
        };
        let mut produced = 0usize;
        for row in due {
            match fire_one(&mut conn, &row, config, &metrics) {
                Ok(()) => produced += 1,
                Err(error) => {
                    metrics.scheduler_error();
                    tracing::error!(%error, scheduler = %row.id, "scheduler fire failed; retrying next tick");
                }
            }
        }
        produced
    })
    .await
    .unwrap_or_else(|error| {
        tracing::error!(%error, "scheduler tick task panicked");
        0
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{upsert_scheduler, SchedulerSpec};
    use rusqlite::Connection;

    const CONFIG: SchedulerConfig = SchedulerConfig {
        interval_ms: 1_000,
        batch_size: 256,
        max_execution_depth: 10,
        max_chain_size: 0,
    };

    fn conn() -> Connection {
        let mut c = Connection::open_in_memory().unwrap();
        DatabaseManager::run_migrations(&mut c).unwrap();
        c
    }

    fn now_now(conn: &Connection) -> i64 {
        now_ms(conn).unwrap()
    }

    // A fully-specified scheduler due at `next_run_at`, with distinctive template options so the
    // produced job can be asserted to carry them.
    fn due_spec(id: &str, cron: &str, next_run_at: i64) -> SchedulerSpec {
        SchedulerSpec {
            id: id.to_string(),
            job_name: format!("{id}-job"),
            payload: b"scheduled-payload".to_vec(),
            cron: cron.to_string(),
            timezone: Some("UTC".to_string()),
            next_run_at: Some(next_run_at),
            priority: Some(7),
            max_attempts: Some(4),
            retry_backoff_ms: Some(1_500),
            retry_backoff_max_ms: Some(90_000),
            retry_jitter: Some(0.25),
            rate_limit_facet: Some("tenant:a".to_string()),
        }
    }

    fn metrics() -> Metrics {
        Metrics::default()
    }

    #[test]
    fn fires_one_job_per_due_scheduler_carrying_template_opts_as_root() {
        let mut c = conn();
        let m = metrics();
        upsert_scheduler(&c, &due_spec("sched", "*/5 * * * *", 1_000), 1_000).unwrap();
        let due = db::select_due_schedulers(&c, now_now(&c), 10).unwrap();
        assert_eq!(due.len(), 1);
        fire_one(&mut c, &due[0], CONFIG, &m).unwrap();

        // Exactly one job, carrying the template options, as a ROOT (no parent, depth 0, trace set).
        let (count, name, priority, max_attempts, backoff, backoff_max, jitter, facet, parent, depth, trace): (
            i64,
            String,
            i64,
            i64,
            i64,
            i64,
            f64,
            Option<String>,
            Option<String>,
            i64,
            String,
        ) = c
            .query_row(
                "SELECT COUNT(*), name, priority, max_attempts, retry_backoff_ms, retry_backoff_max_ms, retry_jitter, rate_limit_facet, parent_id, execution_depth, trace_id FROM jobs",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                        r.get(9)?,
                        r.get(10)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(name, "sched-job");
        assert_eq!(priority, 7);
        assert_eq!(max_attempts, 4);
        assert_eq!(backoff, 1_500);
        assert_eq!(backoff_max, 90_000);
        assert!((jitter - 0.25).abs() < f64::EPSILON);
        assert_eq!(facet.as_deref(), Some("tenant:a"));
        assert_eq!(parent, None, "scheduled job is a ROOT with no parent");
        assert_eq!(depth, 0, "ROOT job has execution_depth 0");
        assert!(!trace.is_empty(), "ROOT job gets a fresh trace id");

        // The job is immediately claimable: Waiting with available_at == now.
        let (state, available_at): (String, i64) = c
            .query_row("SELECT state, available_at FROM jobs", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(state, "Waiting");
        let now = now_now(&c);
        assert!(available_at <= now);

        // Runtime bookkeeping advanced: produced_count == 1, last_job_id set, next_run_at future.
        let row = db::get_scheduler(&c, "sched").unwrap().unwrap();
        assert_eq!(row.produced_count, 1);
        assert!(row.last_job_id.is_some());
        assert!(
            row.next_run_at.unwrap() > now,
            "next_run_at advanced into the future"
        );
    }

    #[test]
    fn advance_jumps_to_next_future_occurrence_without_backfill() {
        let mut c = conn();
        let m = metrics();
        // next_run_at far in the past; cron fires every 5 minutes.
        upsert_scheduler(&c, &due_spec("sched", "*/5 * * * *", 1), 1).unwrap();
        let due = db::select_due_schedulers(&c, now_now(&c), 10).unwrap();
        assert_eq!(due.len(), 1);
        fire_one(&mut c, &due[0], CONFIG, &m).unwrap();

        let row = db::get_scheduler(&c, "sched").unwrap().unwrap();
        let now = now_now(&c);
        let advance = row.next_run_at.unwrap();
        assert!(advance > now, "no backfill: next_run_at is in the future");
        // Next future 5-minute boundary is within 5 minutes of now.
        assert!(advance <= now + 5 * 60_000);
        assert_eq!(row.produced_count, 1, "exactly one job produced per fire");
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );

        // A second fire advances again and increments produced_count by exactly 1.
        let due = db::select_due_schedulers(&c, now_now(&c), 10).unwrap();
        if let Some(row) = due.first() {
            fire_one(&mut c, row, CONFIG, &m).unwrap();
            let row = db::get_scheduler(&c, "sched").unwrap().unwrap();
            assert_eq!(row.produced_count, 2);
        }
    }

    #[test]
    fn not_yet_due_schedulers_are_skipped() {
        let c = conn();
        let now = now_now(&c);
        upsert_scheduler(&c, &due_spec("due", "*/5 * * * *", now - 1_000), now).unwrap();
        upsert_scheduler(&c, &due_spec("future", "*/5 * * * *", now + 3_600_000), now).unwrap();
        let due = db::select_due_schedulers(&c, now, 10).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, "due");
    }

    #[test]
    fn fire_is_atomic_across_insert_and_advance() {
        // Simulate a failure (crash) between the job insert and the scheduler advance by running
        // the enqueue inside an IMMEDIATE tx and then dropping the tx WITHOUT committing. Neither
        // the job row nor the advance may persist, so the fire is safely retried next tick.
        let mut c = conn();
        let m = metrics();
        upsert_scheduler(&c, &due_spec("sched", "*/5 * * * *", 1_000), 1_000).unwrap();
        let row = db::get_scheduler(&c, "sched").unwrap().unwrap();
        {
            let tx = c
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let now = now_ms(&tx).unwrap();
            let (job_id, _success) = crate::enqueue::enqueue_scheduled(
                &tx,
                build_request(&row),
                now,
                CONFIG.max_execution_depth,
                CONFIG.max_chain_size,
                &m,
            )
            .unwrap();
            db::advance_scheduler(&tx, &row.id, now + 300_000, &job_id, now).unwrap();
            // Drop the tx without committing -> full rollback.
            drop(tx);
        }
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0,
            "job insert rolled back"
        );
        let row = db::get_scheduler(&c, "sched").unwrap().unwrap();
        assert_eq!(row.produced_count, 0, "advance rolled back");
        assert_eq!(row.next_run_at, Some(1_000), "next_run_at unchanged");
        assert_eq!(row.last_job_id, None);
    }

    #[test]
    fn one_erroring_scheduler_does_not_block_others() {
        let mut c = conn();
        let m = metrics();
        // A scheduler with a deliberately invalid cron fails at the advance step AFTER its job is
        // inserted, forcing a rollback of that fire; a healthy sibling in the same batch still
        // fires. (In production cron is validated at upsert; here we persist a bad one directly.)
        upsert_scheduler(&c, &due_spec("good", "*/5 * * * *", 1_000), 1_000).unwrap();
        c.execute(
            "INSERT INTO job_schedulers (id, job_name, payload, cron, timezone, produced_count, next_run_at, priority, created_at, updated_at) VALUES ('bad', 'bad-job', X'', 'not-a-cron', NULL, 0, 1000, NULL, 1000, 1000)",
            [],
        )
        .unwrap();

        let due = db::select_due_schedulers(&c, now_now(&c), 10).unwrap();
        assert_eq!(due.len(), 2);
        let mut produced = 0;
        let mut errored = 0;
        for row in &due {
            match fire_one(&mut c, row, CONFIG, &m) {
                Ok(()) => produced += 1,
                Err(_) => errored += 1,
            }
        }
        assert_eq!(produced, 1, "the healthy scheduler still fired");
        assert_eq!(errored, 1, "the bad scheduler errored in isolation");

        // The good scheduler advanced; the bad one did not, and produced no job.
        assert_eq!(
            db::get_scheduler(&c, "good")
                .unwrap()
                .unwrap()
                .produced_count,
            1
        );
        let bad = db::get_scheduler(&c, "bad").unwrap().unwrap();
        assert_eq!(bad.produced_count, 0);
        assert_eq!(bad.next_run_at, Some(1_000));
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1,
            "only the healthy scheduler's job persisted"
        );
    }

    #[tokio::test]
    async fn run_once_fires_persisted_due_scheduler_on_first_tick() {
        // Restart resume: a scheduler with a persisted due next_run_at fires on the first tick.
        let db = Arc::new(DatabaseManager::new(":memory:").await.unwrap());
        {
            let conn = db.get_shared_connection();
            let conn = conn.lock().await;
            let now = now_ms(&conn).unwrap();
            upsert_scheduler(&conn, &due_spec("sched", "*/5 * * * *", now - 10_000), now).unwrap();
        }
        let produced = run_once(db.clone(), CONFIG).await;
        assert_eq!(produced, 1);
        let conn = db.get_shared_connection();
        let conn = conn.lock().await;
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        let row = db::get_scheduler(&conn, "sched").unwrap().unwrap();
        assert_eq!(row.produced_count, 1);
    }

    #[test]
    fn schedulers_active_gauge_reflects_upsert_and_delete() {
        // Seed two active schedulers, then verify the gauge initializes from persisted state and
        // tracks activation/deactivation deltas lock-free.
        let c = conn();
        upsert_scheduler(&c, &due_spec("s1", "*/5 * * * *", 1_000), 1_000).unwrap();
        upsert_scheduler(&c, &due_spec("s2", "*/5 * * * *", 1_000), 1_000).unwrap();
        let m = Metrics::initialize(&c).unwrap();
        assert!(
            m.render().contains("anvilmq_schedulers_active 2"),
            "gauge seeded from COUNT(*) of active schedulers"
        );
        m.scheduler_activated();
        assert!(m.render().contains("anvilmq_schedulers_active 3"));
        m.scheduler_deactivated();
        m.scheduler_deactivated();
        assert!(m.render().contains("anvilmq_schedulers_active 1"));
    }
}
