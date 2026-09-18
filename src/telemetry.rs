use crate::db::DatabaseManager;
use axum::{extract::State, http::StatusCode, routing::get, Router};
use std::{
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering::Relaxed},
        Arc,
    },
    time::Instant,
};

const STATES: [&str; 5] = ["Waiting", "Delayed", "Active", "Completed", "Failed"];
const EVENTS: [&str; 6] = [
    "enqueued",
    "claimed",
    "completed",
    "failed",
    "retried",
    "lease_expired",
];
const METHODS: [&str; 8] = [
    "AddJob",
    "GetNextJob",
    "CompleteJob",
    "FailJob",
    "Heartbeat",
    "UpsertRateLimitRule",
    "DeleteRateLimitRule",
    "GetRateLimitStatus",
];
const BOUNDS: [u64; 7] = [1000, 5000, 10000, 50000, 100000, 1000000, 5000000];
#[derive(Default)]
struct Latency {
    count: AtomicU64,
    micros: AtomicU64,
    buckets: [AtomicU64; 7],
}
#[derive(Default)]
pub struct Metrics {
    enqueue_replays: AtomicU64,
    enqueue_conflicts: AtomicU64,
    enqueue_receipts: AtomicI64,
    throttled_polls: AtomicU64,
    states: [AtomicI64; 5],
    events: [AtomicU64; 6],
    latency: [Latency; 8],
    pub recovery_errors: AtomicU64,
}
impl Metrics {
    pub fn initialize(conn: &rusqlite::Connection) -> rusqlite::Result<Self> {
        let metrics = Self::default();
        metrics.enqueue_receipts.store(
            conn.query_row("SELECT COUNT(*) FROM enqueue_receipts", [], |r| r.get(0))?,
            Relaxed,
        );
        let mut statement = conn.prepare("SELECT state, COUNT(*) FROM (SELECT state FROM jobs UNION ALL SELECT state FROM job_history) GROUP BY state")?;
        let rows =
            statement.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        for row in rows {
            let (state, count) = row?;
            if let Some(i) = STATES.iter().position(|s| *s == state) {
                metrics.states[i].store(count, Relaxed);
            }
        }
        Ok(metrics)
    }
    pub fn transition(&self, from: Option<&str>, to: &str, event: &str) {
        if let Some(i) = from.and_then(|s| STATES.iter().position(|v| *v == s)) {
            self.states[i].fetch_sub(1, Relaxed);
        }
        if let Some(i) = STATES.iter().position(|v| *v == to) {
            self.states[i].fetch_add(1, Relaxed);
        }
        self.event(event);
    }
    pub fn event(&self, event: &str) {
        if let Some(i) = EVENTS.iter().position(|v| *v == event) {
            self.events[i].fetch_add(1, Relaxed);
        }
    }
    pub fn timer(self: &Arc<Self>, method: &'static str) -> Timer {
        Timer {
            metrics: self.clone(),
            method,
            start: Instant::now(),
        }
    }
    pub fn throttled(&self) {
        self.throttled_polls.fetch_add(1, Relaxed);
    }
    pub fn enqueue_replay(&self) {
        self.enqueue_replays.fetch_add(1, Relaxed);
    }
    pub fn enqueue_conflict(&self) {
        self.enqueue_conflicts.fetch_add(1, Relaxed);
    }
    pub fn enqueue_receipt_created(&self) {
        self.enqueue_receipts.fetch_add(1, Relaxed);
    }
    pub fn render(&self) -> String {
        let mut out = String::from("# HELP anvilmq_jobs Persisted jobs by state including retained history.\n# TYPE anvilmq_jobs gauge\n");
        for (i, state) in STATES.iter().enumerate() {
            out += &format!(
                "anvilmq_jobs{{state=\"{state}\"}} {}\n",
                self.states[i].load(Relaxed)
            );
        }
        out += "# HELP anvilmq_transitions_total Committed lifecycle events since process start.\n# TYPE anvilmq_transitions_total counter\n";
        for (i, event) in EVENTS.iter().enumerate() {
            out += &format!(
                "anvilmq_transitions_total{{event=\"{event}\"}} {}\n",
                self.events[i].load(Relaxed)
            );
        }
        out += "# HELP anvilmq_rpc_duration_seconds Handler duration including validation failures and database waits.\n# TYPE anvilmq_rpc_duration_seconds histogram\n";
        for (i, method) in METHODS.iter().enumerate() {
            let l = &self.latency[i];
            for (b, bound) in BOUNDS.iter().enumerate() {
                out += &format!(
                    "anvilmq_rpc_duration_seconds_bucket{{method=\"{method}\",le=\"{}\"}} {}\n",
                    *bound as f64 / 1e6,
                    l.buckets[b].load(Relaxed)
                );
            }
            out += &format!("anvilmq_rpc_duration_seconds_bucket{{method=\"{method}\",le=\"+Inf\"}} {}\nanvilmq_rpc_duration_seconds_count{{method=\"{method}\"}} {}\nanvilmq_rpc_duration_seconds_sum{{method=\"{method}\"}} {}\n", l.count.load(Relaxed), l.count.load(Relaxed), l.micros.load(Relaxed) as f64 / 1e6);
        }
        out += "# HELP anvilmq_recovery_errors_total Failed recovery batches.\n# TYPE anvilmq_recovery_errors_total counter\n";
        out += &format!(
            "anvilmq_recovery_errors_total {}\n",
            self.recovery_errors.load(Relaxed)
        );
        out += &format!("# HELP anvilmq_throttled_polls_total Polls encountering at least one due throttled job.\n# TYPE anvilmq_throttled_polls_total counter\nanvilmq_throttled_polls_total {}\n", self.throttled_polls.load(Relaxed));
        out += &format!("# HELP anvilmq_enqueue_replays_total Matching enqueue retries since process start.\n# TYPE anvilmq_enqueue_replays_total counter\nanvilmq_enqueue_replays_total {}\n# HELP anvilmq_enqueue_conflicts_total Conflicting enqueue keys since process start.\n# TYPE anvilmq_enqueue_conflicts_total counter\nanvilmq_enqueue_conflicts_total {}\n# HELP anvilmq_enqueue_receipts Retained enqueue idempotency receipts.\n# TYPE anvilmq_enqueue_receipts gauge\nanvilmq_enqueue_receipts {}\n", self.enqueue_replays.load(Relaxed), self.enqueue_conflicts.load(Relaxed), self.enqueue_receipts.load(Relaxed));
        out
    }
}
pub struct Timer {
    metrics: Arc<Metrics>,
    method: &'static str,
    start: Instant,
}
impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(i) = METHODS.iter().position(|v| *v == self.method) {
            let us = self.start.elapsed().as_micros().min(u64::MAX as u128) as u64;
            let l = &self.metrics.latency[i];
            for (i, bound) in BOUNDS.iter().enumerate() {
                if us <= *bound {
                    l.buckets[i].fetch_add(1, Relaxed);
                }
            }
            l.micros.fetch_add(us, Relaxed);
            l.count.fetch_add(1, Relaxed);
        }
    }
}
pub fn router(db: Arc<DatabaseManager>) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(ready))
        .with_state(db)
}
async fn metrics(
    State(db): State<Arc<DatabaseManager>>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        db.metrics.render(),
    )
}
async fn ready(State(db): State<Arc<DatabaseManager>>) -> (StatusCode, &'static str) {
    // Fail fast under contention; never queue unbounded blocking probes behind a busy writer.
    let Ok(mut conn) = db.get_shared_connection().try_lock_owned() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "database busy\n");
    };
    let probe = tokio::task::spawn_blocking(move || -> rusqlite::Result<()> {
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.query_row(
            "SELECT COUNT(*) FROM (SELECT id FROM jobs LIMIT 1)",
            [],
            |r| r.get::<_, i64>(0),
        )?;
        tx.rollback()
    });
    match tokio::time::timeout(std::time::Duration::from_secs(1), probe).await {
        Ok(Ok(Ok(()))) => (StatusCode::OK, "ready\n"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "database unavailable\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn readiness_fails_fast_under_contention_and_metrics_remain_available() {
        let db = Arc::new(DatabaseManager::new(":memory:").await.unwrap());
        assert_eq!(ready(State(db.clone())).await.0, StatusCode::OK);
        let conn = db.get_shared_connection();
        let _guard = conn.lock().await;
        assert_eq!(
            ready(State(db.clone())).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(metrics(State(db))
            .await
            .1
            .contains("# TYPE anvilmq_jobs gauge"));
    }
    #[test]
    fn initializes_state_counts_but_not_event_counters() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE jobs(state TEXT); CREATE TABLE job_history(state TEXT); CREATE TABLE enqueue_receipts(id TEXT); INSERT INTO jobs VALUES ('Waiting'),('Active'),('Delayed'); INSERT INTO job_history VALUES ('Completed'),('Completed'),('Failed');").unwrap();
        let output = Metrics::initialize(&conn).unwrap().render();
        assert!(output.contains("anvilmq_jobs{state=\"Completed\"} 2"));
        assert!(output.contains("anvilmq_jobs{state=\"Active\"} 1"));
        assert!(output.contains("anvilmq_transitions_total{event=\"enqueued\"} 0"));
    }
}
