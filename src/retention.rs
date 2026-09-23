//! Background retention sweeper.
//!
//! AnvilMQ retains terminal jobs in `job_history` and dedup keys in `enqueue_receipts`
//! forever unless pruned. This mirrors the completed/failed retention bounds (by age and
//! count) of the system AnvilMQ replaces so disk usage reaches steady state instead of growing without
//! limit. Deletes run in bounded IMMEDIATE transactions against the shared writer, and rely
//! on SQLite page reuse rather than VACUUM to avoid locking the writer.

use crate::{db::DatabaseManager, leases::now_ms, leases::LEASE_DURATION_MS, telemetry::Metrics};
use rusqlite::{Connection, TransactionBehavior};
use std::sync::Arc;
use tonic::Status;

/// A prune below this age could delete a terminal job while a duplicate lifecycle RPC is
/// still being retried against `job_history` (completion replay) — reject such configs.
const MIN_AGE_MS: i64 = LEASE_DURATION_MS * 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionConfig {
    /// Delete completed jobs finished more than this long ago. `0` disables.
    pub completed_age_ms: i64,
    /// Keep at most this many completed jobs per name. `0` disables.
    pub completed_count: i64,
    /// Delete failed jobs finished more than this long ago. `0` disables.
    pub failed_age_ms: i64,
    /// Keep at most this many failed jobs per name. `0` disables.
    pub failed_count: i64,
    /// Sweep cadence.
    pub interval_ms: u64,
    /// Max rows deleted per statement per sweep; bounds writer hold time.
    pub batch: i64,
    /// Delete per-lineage `chain_counters` rows idle longer than this. `0` disables.
    pub chain_counter_ttl_ms: i64,
    /// Delete idle `facet_dispatch` fairness rows (no live jobs, idle beyond this). `0` disables.
    pub facet_dispatch_ttl_ms: i64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        // Defaults mirror the retention policy of the system AnvilMQ replaces:
        // removeOnComplete { age: 24h, count: 1000 }, removeOnFail { age: 7d }.
        Self {
            completed_age_ms: 86_400_000,
            completed_count: 1_000,
            failed_age_ms: 604_800_000,
            failed_count: 0,
            interval_ms: 60_000,
            batch: 1_000,
            // A lineage idle beyond a week is treated as finished; a much later straggler
            // reusing the trace starts a fresh chain.
            chain_counter_ttl_ms: 604_800_000,
            // A facet idle beyond a week with no live jobs has left rotation; a returning
            // tenant is served promptly (never-served sorts first) and rejoins rotation.
            facet_dispatch_ttl_ms: 604_800_000,
        }
    }
}

fn env_i64(key: &str, default: i64) -> Result<i64, String> {
    match std::env::var(key) {
        Ok(value) => value
            .trim()
            .parse::<i64>()
            .map_err(|_| format!("{key} must be an integer, got {value:?}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("{key}: {error}")),
    }
}

impl RetentionConfig {
    pub fn from_env() -> Result<Self, String> {
        let d = Self::default();
        let cfg = Self {
            completed_age_ms: env_i64("ANVILMQ_RETENTION_COMPLETED_AGE_MS", d.completed_age_ms)?,
            completed_count: env_i64("ANVILMQ_RETENTION_COMPLETED_COUNT", d.completed_count)?,
            failed_age_ms: env_i64("ANVILMQ_RETENTION_FAILED_AGE_MS", d.failed_age_ms)?,
            failed_count: env_i64("ANVILMQ_RETENTION_FAILED_COUNT", d.failed_count)?,
            interval_ms: env_i64("ANVILMQ_RETENTION_INTERVAL_MS", d.interval_ms as i64)?
                .try_into()
                .map_err(|_| "ANVILMQ_RETENTION_INTERVAL_MS must be non-negative".to_string())?,
            batch: env_i64("ANVILMQ_RETENTION_BATCH", d.batch)?,
            chain_counter_ttl_ms: env_i64(
                "ANVILMQ_MAX_CHAIN_COUNTER_TTL_MS",
                d.chain_counter_ttl_ms,
            )?,
            facet_dispatch_ttl_ms: env_i64(
                "ANVILMQ_FACET_DISPATCH_TTL_MS",
                d.facet_dispatch_ttl_ms,
            )?,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), String> {
        for (key, age) in [
            ("ANVILMQ_RETENTION_COMPLETED_AGE_MS", self.completed_age_ms),
            ("ANVILMQ_RETENTION_FAILED_AGE_MS", self.failed_age_ms),
        ] {
            if age != 0 && age < MIN_AGE_MS {
                return Err(format!(
                    "{key}={age} is below the {MIN_AGE_MS}ms safety floor (use 0 to disable)"
                ));
            }
        }
        for (key, value) in [
            ("ANVILMQ_RETENTION_COMPLETED_COUNT", self.completed_count),
            ("ANVILMQ_RETENTION_FAILED_COUNT", self.failed_count),
        ] {
            if value < 0 {
                return Err(format!("{key}={value} must be >= 0"));
            }
        }
        if self.batch <= 0 {
            return Err(format!(
                "ANVILMQ_RETENTION_BATCH={} must be positive",
                self.batch
            ));
        }
        if self.interval_ms == 0 {
            return Err("ANVILMQ_RETENTION_INTERVAL_MS must be positive".to_string());
        }
        if self.chain_counter_ttl_ms < 0 {
            return Err(format!(
                "ANVILMQ_MAX_CHAIN_COUNTER_TTL_MS={} must be >= 0",
                self.chain_counter_ttl_ms
            ));
        }
        if self.facet_dispatch_ttl_ms < 0 {
            return Err(format!(
                "ANVILMQ_FACET_DISPATCH_TTL_MS={} must be >= 0",
                self.facet_dispatch_ttl_ms
            ));
        }
        Ok(())
    }

    pub fn enabled(&self) -> bool {
        self.completed_age_ms > 0
            || self.completed_count > 0
            || self.failed_age_ms > 0
            || self.failed_count > 0
            || self.chain_counter_ttl_ms > 0
            || self.facet_dispatch_ttl_ms > 0
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct SweepOutcome {
    pub completed_age: usize,
    pub completed_count: usize,
    pub failed_age: usize,
    pub failed_count: usize,
    pub receipts: usize,
    pub chain_counters: usize,
    pub facet_dispatch: usize,
}

impl SweepOutcome {
    pub fn total(&self) -> usize {
        self.completed_age
            + self.completed_count
            + self.failed_age
            + self.failed_count
            + self.receipts
            + self.chain_counters
            + self.facet_dispatch
    }
}

fn prune_age(tx: &Connection, state: &str, cutoff: i64, batch: i64) -> rusqlite::Result<usize> {
    tx.execute(
        "DELETE FROM job_history WHERE id IN (
             SELECT id FROM job_history
             WHERE state = ?1 AND finished_at < ?2
             ORDER BY finished_at ASC LIMIT ?3
         )",
        rusqlite::params![state, cutoff, batch],
    )
}

fn prune_count(tx: &Connection, state: &str, keep: i64, batch: i64) -> rusqlite::Result<usize> {
    // Keep the newest `keep` rows per name; delete the overflow, bounded per statement.
    tx.execute(
        "DELETE FROM job_history WHERE id IN (
             SELECT id FROM (
                 SELECT id, ROW_NUMBER() OVER (
                     PARTITION BY name ORDER BY finished_at DESC, id DESC
                 ) AS rn
                 FROM job_history WHERE state = ?1
             ) WHERE rn > ?2 LIMIT ?3
         )",
        rusqlite::params![state, keep, batch],
    )
}

fn prune_receipts(tx: &Connection, batch: i64) -> rusqlite::Result<usize> {
    // A receipt's dedup window ends when its job leaves both live and history tables, so the
    // idempotency horizon tracks job retention exactly (matching the dedup-by-key semantics of the system it replaces).
    tx.execute(
        "DELETE FROM enqueue_receipts WHERE rowid IN (
             SELECT er.rowid FROM enqueue_receipts er
             WHERE NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = er.job_id)
               AND NOT EXISTS (SELECT 1 FROM job_history h WHERE h.id = er.job_id)
             LIMIT ?1
         )",
        rusqlite::params![batch],
    )
}

fn prune_chain_counters(tx: &Connection, cutoff: i64, batch: i64) -> rusqlite::Result<usize> {
    // Reclaim lineage counters that have been idle past the TTL. Indexed on updated_at so
    // the scan is a bounded range read, never a full-table sweep.
    tx.execute(
        "DELETE FROM chain_counters WHERE trace_id IN (
             SELECT trace_id FROM chain_counters
             WHERE updated_at < ?1
             ORDER BY updated_at ASC LIMIT ?2
         )",
        rusqlite::params![cutoff, batch],
    )
}

fn prune_facet_dispatch(tx: &Connection, cutoff: i64, batch: i64) -> rusqlite::Result<usize> {
    // Reclaim fairness rotation rows for facets idle past the TTL that have no live jobs, so the
    // table stays bounded. Dropping an idle facet is safe: a returning tenant is never-served and
    // sorts first. Indexed on last_served_at so the scan is a bounded range read.
    tx.execute(
        "DELETE FROM facet_dispatch WHERE facet_key IN (
             SELECT facet_key FROM facet_dispatch d
             WHERE d.last_served_at < ?1
               AND NOT EXISTS (
                   SELECT 1 FROM jobs j WHERE COALESCE(j.rate_limit_facet, '') = d.facet_key
               )
             ORDER BY d.last_served_at ASC LIMIT ?2
         )",
        rusqlite::params![cutoff, batch],
    )
}

/// Run one bounded sweep pass. Each dimension deletes at most `batch` rows so the writer is
/// never held for long; the caller's interval re-runs until the backlog is drained. Metrics
/// are updated after commit so gauges never reflect uncommitted deletes.
pub fn sweep_blocking(
    conn: &mut Connection,
    cfg: &RetentionConfig,
    metrics: &Metrics,
) -> rusqlite::Result<SweepOutcome> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let now = now_ms(&tx)?;
    let mut out = SweepOutcome::default();
    if cfg.completed_age_ms > 0 {
        out.completed_age = prune_age(
            &tx,
            "Completed",
            now.saturating_sub(cfg.completed_age_ms),
            cfg.batch,
        )?;
    }
    if cfg.failed_age_ms > 0 {
        out.failed_age = prune_age(
            &tx,
            "Failed",
            now.saturating_sub(cfg.failed_age_ms),
            cfg.batch,
        )?;
    }
    if cfg.completed_count > 0 {
        out.completed_count = prune_count(&tx, "Completed", cfg.completed_count, cfg.batch)?;
    }
    if cfg.failed_count > 0 {
        out.failed_count = prune_count(&tx, "Failed", cfg.failed_count, cfg.batch)?;
    }
    out.receipts = prune_receipts(&tx, cfg.batch)?;
    if cfg.chain_counter_ttl_ms > 0 {
        out.chain_counters =
            prune_chain_counters(&tx, now.saturating_sub(cfg.chain_counter_ttl_ms), cfg.batch)?;
    }
    if cfg.facet_dispatch_ttl_ms > 0 {
        out.facet_dispatch = prune_facet_dispatch(
            &tx,
            now.saturating_sub(cfg.facet_dispatch_ttl_ms),
            cfg.batch,
        )?;
    }
    tx.commit()?;

    metrics.retention_deleted("completed", "age", out.completed_age as u64);
    metrics.retention_deleted("completed", "count", out.completed_count as u64);
    metrics.retention_deleted("failed", "age", out.failed_age as u64);
    metrics.retention_deleted("failed", "count", out.failed_count as u64);
    metrics.retention_deleted("receipt", "orphan", out.receipts as u64);
    metrics.chain_counters_pruned(out.chain_counters as u64);
    metrics.facet_dispatch_pruned(out.facet_dispatch as u64);
    Ok(out)
}

pub async fn sweep(db: Arc<DatabaseManager>, cfg: RetentionConfig) -> Result<SweepOutcome, Status> {
    let metrics = db.metrics.clone();
    let conn = db.get_shared_connection();
    tokio::task::spawn_blocking(move || -> rusqlite::Result<SweepOutcome> {
        let mut conn = conn.blocking_lock();
        sweep_blocking(&mut conn, &cfg, &metrics)
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
    .map_err(|e| Status::internal(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RetentionConfig {
        RetentionConfig {
            completed_age_ms: 0,
            completed_count: 0,
            failed_age_ms: 0,
            failed_count: 0,
            interval_ms: 60_000,
            batch: 1_000,
            chain_counter_ttl_ms: 0,
            facet_dispatch_ttl_ms: 0,
        }
    }

    async fn manager() -> Arc<DatabaseManager> {
        Arc::new(DatabaseManager::new(":memory:").await.unwrap())
    }

    async fn now(db: &DatabaseManager) -> i64 {
        let conn = db.get_shared_connection();
        let guard = conn.lock().await;
        now_ms(&guard).unwrap()
    }

    async fn insert_history(
        db: &DatabaseManager,
        id: &str,
        name: &str,
        state: &str,
        finished: i64,
    ) {
        let conn = db.get_shared_connection();
        let guard = conn.lock().await;
        guard
            .execute(
                "INSERT INTO job_history (id, name, state, priority, payload, trace_id, execution_depth, attempts, max_attempts, created_at, finished_at)
                 VALUES (?1, ?2, ?3, 0, X'', 'trace', 0, 1, 1, ?4, ?4)",
                rusqlite::params![id, name, state, finished],
            )
            .unwrap();
    }

    async fn insert_receipt(db: &DatabaseManager, key: &str, job_id: &str) {
        let conn = db.get_shared_connection();
        let guard = conn.lock().await;
        guard
            .execute(
                "INSERT INTO enqueue_receipts (queue_name, idempotency_key, request, job_id, initial_state, created_at)
                 VALUES ('q', ?1, X'', ?2, 'Waiting', 0)",
                rusqlite::params![key, job_id],
            )
            .unwrap();
    }

    async fn insert_live_job(db: &DatabaseManager, id: &str) {
        let conn = db.get_shared_connection();
        let guard = conn.lock().await;
        guard
            .execute(
                "INSERT INTO jobs (id, name, state, priority, payload, trace_id, execution_depth, created_at, updated_at)
                 VALUES (?1, 'live', 'Waiting', 0, X'', 'trace', 0, 0, 0)",
                rusqlite::params![id],
            )
            .unwrap();
    }

    async fn count(db: &DatabaseManager, sql: &str) -> i64 {
        let conn = db.get_shared_connection();
        let guard = conn.lock().await;
        guard.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    #[tokio::test]
    async fn age_prune_deletes_only_rows_older_than_cutoff() {
        let db = manager().await;
        let t = now(&db).await;
        insert_history(&db, "old", "A", "Completed", t - 7_200_000).await;
        insert_history(&db, "fresh", "A", "Completed", t - 60_000).await;
        let mut c = cfg();
        c.completed_age_ms = 3_600_000;
        let out = sweep(db.clone(), c).await.unwrap();
        assert_eq!(out.completed_age, 1);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM job_history").await, 1);
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM job_history WHERE id = 'fresh'").await,
            1
        );
    }

    #[tokio::test]
    async fn count_prune_keeps_newest_n_per_name() {
        let db = manager().await;
        let t = now(&db).await;
        for (i, name) in [(0, "A"), (1, "A"), (2, "A"), (3, "B")] {
            insert_history(&db, &format!("{name}-{i}"), name, "Completed", t - i * 1000).await;
        }
        let mut c = cfg();
        c.completed_count = 1;
        let out = sweep(db.clone(), c).await.unwrap();
        // A had 3 rows -> keep newest 1 (delete 2); B had 1 -> keep.
        assert_eq!(out.completed_count, 2);
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM job_history WHERE name = 'A'").await,
            1
        );
        // Newest A row (smallest offset, id A-0) survives.
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM job_history WHERE id = 'A-0'").await,
            1
        );
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM job_history WHERE name = 'B'").await,
            1
        );
    }

    #[tokio::test]
    async fn disabled_dimensions_prune_nothing() {
        let db = manager().await;
        let t = now(&db).await;
        insert_history(&db, "old", "A", "Completed", t - 7_200_000).await;
        let out = sweep(db.clone(), cfg()).await.unwrap();
        assert_eq!(out.total(), 0);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM job_history").await, 1);
    }

    #[tokio::test]
    async fn failed_and_completed_are_pruned_independently() {
        let db = manager().await;
        let t = now(&db).await;
        insert_history(&db, "c-old", "A", "Completed", t - 7_200_000).await;
        insert_history(&db, "f-old", "A", "Failed", t - 7_200_000).await;
        let mut c = cfg();
        c.completed_age_ms = 3_600_000;
        let out = sweep(db.clone(), c).await.unwrap();
        assert_eq!(out.completed_age, 1);
        assert_eq!(out.failed_age, 0);
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM job_history WHERE state = 'Failed'"
            )
            .await,
            1
        );
    }

    #[tokio::test]
    async fn orphan_receipts_pruned_but_referenced_ones_kept() {
        let db = manager().await;
        insert_live_job(&db, "live-1").await;
        insert_history(&db, "hist-1", "A", "Completed", 0).await;
        insert_receipt(&db, "k-live", "live-1").await;
        insert_receipt(&db, "k-hist", "hist-1").await;
        insert_receipt(&db, "k-orphan", "gone").await;
        let out = sweep(db.clone(), cfg()).await.unwrap();
        assert_eq!(out.receipts, 1);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM enqueue_receipts").await, 2);
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM enqueue_receipts WHERE idempotency_key = 'k-orphan'"
            )
            .await,
            0
        );
    }

    #[tokio::test]
    async fn live_jobs_are_never_touched() {
        let db = manager().await;
        let t = now(&db).await;
        insert_live_job(&db, "live-1").await;
        insert_history(&db, "old", "A", "Completed", t - 7_200_000).await;
        let mut c = cfg();
        c.completed_age_ms = 3_600_000;
        c.completed_count = 1;
        sweep(db.clone(), c).await.unwrap();
        assert_eq!(count(&db, "SELECT COUNT(*) FROM jobs").await, 1);
    }

    #[tokio::test]
    async fn batches_bound_deletes_and_drain_across_sweeps() {
        let db = manager().await;
        let t = now(&db).await;
        for i in 0..5 {
            insert_history(&db, &format!("old-{i}"), "A", "Completed", t - 7_200_000).await;
        }
        let mut c = cfg();
        c.completed_age_ms = 3_600_000;
        c.batch = 2;
        let first = sweep(db.clone(), c.clone()).await.unwrap();
        assert_eq!(first.completed_age, 2);
        let second = sweep(db.clone(), c.clone()).await.unwrap();
        assert_eq!(second.completed_age, 2);
        let third = sweep(db.clone(), c).await.unwrap();
        assert_eq!(third.completed_age, 1);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM job_history").await, 0);
    }

    #[test]
    fn safety_floor_rejects_short_ages() {
        let mut c = cfg();
        c.completed_age_ms = 1_000;
        assert!(c.validate().is_err());
        c.completed_age_ms = MIN_AGE_MS;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn zero_ages_are_allowed_and_disable_dimensions() {
        assert!(cfg().validate().is_ok());
        assert!(!cfg().enabled());
    }

    #[test]
    fn defaults_match_replaced_retention() {
        let d = RetentionConfig::default();
        assert_eq!(d.completed_age_ms, 86_400_000);
        assert_eq!(d.completed_count, 1_000);
        assert_eq!(d.failed_age_ms, 604_800_000);
        assert!(d.enabled());
        assert!(d.validate().is_ok());
    }

    async fn insert_facet_dispatch(db: &DatabaseManager, key: &str, seq: i64, served_at: i64) {
        let conn = db.get_shared_connection();
        let guard = conn.lock().await;
        guard
            .execute(
                "INSERT INTO facet_dispatch (facet_key, last_served_seq, last_served_at)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![key, seq, served_at],
            )
            .unwrap();
    }

    #[tokio::test]
    async fn facet_dispatch_prune_removes_idle_rows_without_live_jobs() {
        let db = manager().await;
        let t = now(&db).await;
        // Idle beyond TTL, no live job -> pruned.
        insert_facet_dispatch(&db, "tenant:idle", 1, t - 7_200_000).await;
        // Idle beyond TTL but still has a live job -> kept.
        insert_facet_dispatch(&db, "tenant:live", 2, t - 7_200_000).await;
        insert_live_job(&db, "live-job").await;
        sql_exec(
            &db,
            "UPDATE jobs SET rate_limit_facet = 'tenant:live' WHERE id = 'live-job'",
        )
        .await;
        // Recently served -> kept.
        insert_facet_dispatch(&db, "tenant:fresh", 3, t - 60_000).await;
        let mut c = cfg();
        c.facet_dispatch_ttl_ms = 3_600_000;
        let out = sweep(db.clone(), c).await.unwrap();
        assert_eq!(out.facet_dispatch, 1);
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM facet_dispatch").await,
            2,
            "only the idle unreferenced facet is pruned"
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM facet_dispatch WHERE facet_key = 'tenant:idle'"
            )
            .await,
            0
        );
    }

    async fn sql_exec(db: &DatabaseManager, sql: &'static str) {
        let conn = db.get_shared_connection();
        let guard = conn.lock().await;
        guard.execute_batch(sql).unwrap();
    }
}
