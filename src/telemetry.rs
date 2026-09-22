use crate::db::DatabaseManager;
use crate::reader::{Reader, ReaderError};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
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
// Per-name lifecycle labels for the bounded-cardinality named series. These mirror the
// BullMQ dashboards' {queue,name,result} breakdowns without unbounded producer labels.
const NAMED_EVENTS: [&str; 5] = ["enqueued", "claimed", "completed", "failed", "retried"];
const DURATION_BOUNDS_MS: [i64; 10] = [1, 10, 100, 500, 1000, 5000, 15000, 60000, 300000, 3600000];
// Retention sweeper deletion counters. Bounded cardinality: 3 targets x 3 reasons.
const RETENTION_TARGETS: [&str; 3] = ["completed", "failed", "receipt"];
const RETENTION_REASONS: [&str; 3] = ["age", "count", "orphan"];

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

#[derive(Default)]
struct Latency {
    count: AtomicU64,
    micros: AtomicU64,
    buckets: [AtomicU64; 7],
}
// Enqueue-to-terminal latency histogram rendered in seconds, per allowlisted job name.
#[derive(Default)]
struct DurationHistogram {
    buckets: [AtomicU64; 10],
    count: AtomicU64,
    millis: AtomicU64,
}
impl DurationHistogram {
    fn observe(&self, ms: i64) {
        let ms = ms.max(0);
        for (i, bound) in DURATION_BOUNDS_MS.iter().enumerate() {
            if ms <= *bound {
                self.buckets[i].fetch_add(1, Relaxed);
            }
        }
        self.millis.fetch_add(ms as u64, Relaxed);
        self.count.fetch_add(1, Relaxed);
    }
    fn render(&self, metric: &str, labels: &str, out: &mut String) {
        for (i, bound) in DURATION_BOUNDS_MS.iter().enumerate() {
            *out += &format!(
                "{metric}_bucket{{{labels},le=\"{}\"}} {}\n",
                *bound as f64 / 1000.,
                self.buckets[i].load(Relaxed)
            );
        }
        *out += &format!(
            "{metric}_bucket{{{labels},le=\"+Inf\"}} {}\n{metric}_count{{{labels}}} {}\n{metric}_sum{{{labels}}} {}\n",
            self.count.load(Relaxed),
            self.count.load(Relaxed),
            self.millis.load(Relaxed) as f64 / 1000.
        );
    }
}
#[derive(Default)]
struct NamedSeries {
    events: [AtomicU64; 5],
    duration: DurationHistogram,
}
/// Bounded per-job-name lifecycle counters and duration histogram. Only names in the
/// configured allowlist (shared with pressure sampling via `ANVILMQ_METRICS_QUEUES`)
/// produce series, so metric cardinality never grows with arbitrary producer names.
#[derive(Default)]
pub struct NamedLifecycle {
    names: Vec<(String, NamedSeries)>,
}
impl NamedLifecycle {
    pub fn configured(names: &[String]) -> Self {
        Self {
            names: names
                .iter()
                .map(|n| (n.clone(), NamedSeries::default()))
                .collect(),
        }
    }
    fn series(&self, name: &str) -> Option<&NamedSeries> {
        self.names.iter().find(|(n, _)| n == name).map(|(_, s)| s)
    }
    pub fn event(&self, name: &str, event: &str) {
        if let (Some(series), Some(i)) = (
            self.series(name),
            NAMED_EVENTS.iter().position(|e| *e == event),
        ) {
            series.events[i].fetch_add(1, Relaxed);
        }
    }
    pub fn observe_duration(&self, name: &str, ms: i64) {
        if let Some(series) = self.series(name) {
            series.duration.observe(ms);
        }
    }
    fn render(&self, out: &mut String) {
        if self.names.is_empty() {
            return;
        }
        *out += "# HELP anvilmq_jobs_by_name_total Committed lifecycle events since process start, per allowlisted job name.\n# TYPE anvilmq_jobs_by_name_total counter\n";
        for (name, series) in &self.names {
            let name = escape_label(name);
            for (i, event) in NAMED_EVENTS.iter().enumerate() {
                *out += &format!(
                    "anvilmq_jobs_by_name_total{{name=\"{name}\",event=\"{event}\"}} {}\n",
                    series.events[i].load(Relaxed)
                );
            }
        }
        *out += "# HELP anvilmq_job_duration_seconds Enqueue to terminal (completed or exhausted-failed) latency by name; includes queue wait, retries, and backoff.\n# TYPE anvilmq_job_duration_seconds histogram\n";
        for (name, series) in &self.names {
            series.duration.render(
                "anvilmq_job_duration_seconds",
                &format!("name=\"{}\"", escape_label(name)),
                out,
            );
        }
    }
}
#[derive(Default)]
pub struct Metrics {
    pub pressure: crate::pressure::Pressure,
    pub named: NamedLifecycle,
    enqueue_replays: AtomicU64,
    enqueue_conflicts: AtomicU64,
    enqueue_receipts: AtomicI64,
    throttled_polls: AtomicU64,
    states: [AtomicI64; 5],
    events: [AtomicU64; 6],
    latency: [Latency; 8],
    pub recovery_errors: AtomicU64,
    retention_deleted: [[AtomicU64; 3]; 3],
    retention_errors: AtomicU64,
    ancestry_rejections: AtomicU64,
    chain_quarantines: AtomicU64,
    chain_counters_pruned: AtomicU64,
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
    /// Records `n` retention deletions and decrements the matching level gauge so
    /// `anvilmq_jobs{state}` / `anvilmq_enqueue_receipts` flatten as history is pruned.
    pub fn retention_deleted(&self, target: &str, reason: &str, n: u64) {
        if n == 0 {
            return;
        }
        if let (Some(t), Some(r)) = (
            RETENTION_TARGETS.iter().position(|v| *v == target),
            RETENTION_REASONS.iter().position(|v| *v == reason),
        ) {
            self.retention_deleted[t][r].fetch_add(n, Relaxed);
        }
        match target {
            "completed" => {
                if let Some(i) = STATES.iter().position(|s| *s == "Completed") {
                    self.states[i].fetch_sub(n as i64, Relaxed);
                }
            }
            "failed" => {
                if let Some(i) = STATES.iter().position(|s| *s == "Failed") {
                    self.states[i].fetch_sub(n as i64, Relaxed);
                }
            }
            "receipt" => {
                self.enqueue_receipts.fetch_sub(n as i64, Relaxed);
            }
            _ => {}
        }
    }
    pub fn retention_error(&self) {
        self.retention_errors.fetch_add(1, Relaxed);
    }
    /// Enqueue rejected because a supplied `parent_id` did not resolve or the child's
    /// `execution_depth` was not exactly `parent + 1`.
    pub fn ancestry_rejection(&self) {
        self.ancestry_rejections.fetch_add(1, Relaxed);
    }
    /// Enqueue rejected because its lineage (`trace_id`) exceeded the configured
    /// runaway-chain cap.
    pub fn chain_quarantine(&self) {
        self.chain_quarantines.fetch_add(1, Relaxed);
    }
    /// Idle per-lineage counter rows reclaimed by the retention sweeper's TTL prune.
    pub fn chain_counters_pruned(&self, n: u64) {
        if n > 0 {
            self.chain_counters_pruned.fetch_add(n, Relaxed);
        }
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
        out += "# HELP anvilmq_retention_deleted_total History and receipt rows pruned by the retention sweeper.\n# TYPE anvilmq_retention_deleted_total counter\n";
        for (t, target) in RETENTION_TARGETS.iter().enumerate() {
            for (r, reason) in RETENTION_REASONS.iter().enumerate() {
                out += &format!(
                    "anvilmq_retention_deleted_total{{target=\"{target}\",reason=\"{reason}\"}} {}\n",
                    self.retention_deleted[t][r].load(Relaxed)
                );
            }
        }
        out += "# HELP anvilmq_retention_errors_total Failed retention sweep batches.\n# TYPE anvilmq_retention_errors_total counter\n";
        out += &format!(
            "anvilmq_retention_errors_total {}\n",
            self.retention_errors.load(Relaxed)
        );
        out += &format!("# HELP anvilmq_throttled_polls_total Polls encountering at least one due throttled job.\n# TYPE anvilmq_throttled_polls_total counter\nanvilmq_throttled_polls_total {}\n", self.throttled_polls.load(Relaxed));
        out += &format!("# HELP anvilmq_enqueue_replays_total Matching enqueue retries since process start.\n# TYPE anvilmq_enqueue_replays_total counter\nanvilmq_enqueue_replays_total {}\n# HELP anvilmq_enqueue_conflicts_total Conflicting enqueue keys since process start.\n# TYPE anvilmq_enqueue_conflicts_total counter\nanvilmq_enqueue_conflicts_total {}\n# HELP anvilmq_enqueue_receipts Retained enqueue idempotency receipts.\n# TYPE anvilmq_enqueue_receipts gauge\nanvilmq_enqueue_receipts {}\n", self.enqueue_replays.load(Relaxed), self.enqueue_conflicts.load(Relaxed), self.enqueue_receipts.load(Relaxed));
        out += &format!("# HELP anvilmq_ancestry_rejections_total Enqueues rejected for missing parent or inconsistent execution depth.\n# TYPE anvilmq_ancestry_rejections_total counter\nanvilmq_ancestry_rejections_total {}\n# HELP anvilmq_chain_quarantines_total Enqueues rejected because their lineage exceeded the runaway-chain cap.\n# TYPE anvilmq_chain_quarantines_total counter\nanvilmq_chain_quarantines_total {}\n# HELP anvilmq_chain_counters_pruned_total Idle per-lineage counter rows reclaimed by the retention sweeper.\n# TYPE anvilmq_chain_counters_pruned_total counter\nanvilmq_chain_counters_pruned_total {}\n", self.ancestry_rejections.load(Relaxed), self.chain_quarantines.load(Relaxed), self.chain_counters_pruned.load(Relaxed));
        self.named.render(&mut out);
        self.pressure.render(&mut out);
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
/// Shared HTTP handler state. Bundles the single-writer `DatabaseManager` (used by the
/// metrics/readiness probes) with the isolated read-only [`Reader`] (used by observability read
/// endpoints such as `/v1/failures`). Cheap to clone: both fields are `Arc`s.
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<DatabaseManager>,
    pub reader: Arc<Reader>,
}

pub fn router(db: Arc<DatabaseManager>, reader: Arc<Reader>) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(ready))
        .route("/console", get(console))
        .route("/v1/failures", get(failures))
        .route("/v1/search", get(search))
        .with_state(AppState { db, reader })
}

/// Self-contained, read-only search console UI. The entire page (inline CSS + vanilla JS) is
/// embedded in the binary via `include_str!`, so it needs no build step and pulls no external
/// assets — it works air-gapped inside a cluster. It is served same-origin and consumes only the
/// public `/v1/search` JSON endpoint; it holds no state and never touches the database directly.
const CONSOLE_HTML: &str = include_str!("console.html");

async fn console() -> Html<&'static str> {
    Html(CONSOLE_HTML)
}
async fn metrics(
    State(app): State<AppState>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        app.db.metrics.render(),
    )
}
async fn ready(State(app): State<AppState>) -> (StatusCode, &'static str) {
    // Fail fast under contention; never queue unbounded blocking probes behind a busy writer.
    let Ok(mut conn) = app.db.get_shared_connection().try_lock_owned() else {
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

/// Default and hard-capped page size for the recent-failures feed.
const FAILURES_DEFAULT_LIMIT: usize = 100;
const FAILURES_MAX_LIMIT: usize = 1000;

/// Query parameters for `GET /v1/failures`.
#[derive(Deserialize)]
pub struct FailuresParams {
    /// Optional exact job-name filter.
    name: Option<String>,
    /// Optional lower bound on `finished_at` (inclusive); only rows finished at or after this.
    since_ms: Option<i64>,
    /// Page size; defaults to 100 and is hard-capped at 1000.
    limit: Option<usize>,
}

/// One terminal failure row from `job_history`.
#[derive(Serialize)]
pub struct FailureRow {
    id: String,
    name: String,
    attempts: i64,
    finished_at: i64,
    last_error: Option<String>,
    trace_id: Option<String>,
    execution_depth: i64,
}

/// Recent terminal failures feed, served read-only off the WAL replica connection.
///
/// `job_history` rows are TERMINAL failures only — a job that fails but still has retries left
/// stays in `jobs` and never reaches history — so this feed lists exhausted failures with their
/// `last_error`. That record-level drill-down (which job, which error) is exactly what the
/// aggregate Prometheus counters cannot provide. The read runs on the isolated `Reader`, so it
/// never contends on the single-writer mutex.
async fn failures(
    State(app): State<AppState>,
    Query(params): Query<FailuresParams>,
) -> Result<Json<Vec<FailureRow>>, StatusCode> {
    let limit = params
        .limit
        .unwrap_or(FAILURES_DEFAULT_LIMIT)
        .min(FAILURES_MAX_LIMIT) as i64;
    let name = params.name;
    let since_ms = params.since_ms;
    let rows = app
        .reader
        .query(move |conn| {
            // Fully parameterized; user input never interpolated into SQL. Backed by
            // idx_history_name_state_finished / idx_history_state_finished.
            let mut sql = String::from(
                "SELECT id, name, attempts, finished_at, last_error, trace_id, execution_depth \
                 FROM job_history WHERE state = 'Failed'",
            );
            let mut args: Vec<rusqlite::types::Value> = Vec::new();
            if let Some(name) = name {
                sql.push_str(" AND name = ?");
                args.push(rusqlite::types::Value::Text(name));
            }
            if let Some(since) = since_ms {
                sql.push_str(" AND finished_at >= ?");
                args.push(rusqlite::types::Value::Integer(since));
            }
            sql.push_str(" ORDER BY finished_at DESC LIMIT ?");
            args.push(rusqlite::types::Value::Integer(limit));
            let mut statement = conn.prepare(&sql)?;
            let rows = statement.query_map(rusqlite::params_from_iter(args), |r| {
                Ok(FailureRow {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    attempts: r.get(2)?,
                    finished_at: r.get(3)?,
                    last_error: r.get(4)?,
                    trace_id: r.get(5)?,
                    execution_depth: r.get(6)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        })
        .await
        .map_err(|error| match error {
            ReaderError::Busy | ReaderError::Timeout | ReaderError::Unavailable => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            ReaderError::Db(_) => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
    Ok(Json(rows))
}

/// Default and hard-capped page size for the search API.
const SEARCH_DEFAULT_LIMIT: usize = 100;
const SEARCH_MAX_LIMIT: usize = 1000;
/// Escape character for LIKE patterns. User input has `\`, `%`, and `_` escaped against this so
/// wildcards in a query are matched literally rather than expanding the search.
const LIKE_ESCAPE: char = '\\';

/// Escapes LIKE metacharacters in user input so `%` and `_` match literally. The escape character
/// itself is escaped first to avoid double-expansion. The result is wrapped in `%…%` by the caller
/// and bound as a parameter (never interpolated), with `ESCAPE '\'` on every LIKE clause.
fn escape_like(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch == LIKE_ESCAPE || ch == '%' || ch == '_' {
            out.push(LIKE_ESCAPE);
        }
        out.push(ch);
    }
    out
}

/// Query parameters for `GET /v1/search`.
#[derive(Deserialize)]
pub struct SearchParams {
    /// Free-text query; escaped LIKE match across `id`, `name`, `trace_id`, and `last_error`.
    /// Empty/whitespace is treated as absent. The payload BLOB is intentionally not searched.
    q: Option<String>,
    /// Optional exact job-name filter.
    name: Option<String>,
    /// Optional exact lifecycle-state filter (e.g. `Failed`, `Completed`).
    state: Option<String>,
    /// Optional exact trace-id filter.
    trace_id: Option<String>,
    /// Optional lower bound on `finished_at` (inclusive); only rows finished at or after this.
    since_ms: Option<i64>,
    /// Page size; defaults to 100 and is hard-capped at 1000.
    limit: Option<usize>,
}

/// One `job_history` row returned by the search API.
#[derive(Serialize, Debug)]
pub struct SearchRow {
    id: String,
    name: String,
    state: String,
    attempts: i64,
    created_at: i64,
    finished_at: i64,
    last_error: Option<String>,
    trace_id: Option<String>,
    execution_depth: i64,
}

/// Read-only job/history search served off the WAL replica connection.
///
/// Searches `job_history` — the retention-bounded record of terminal runs — with structured
/// filters plus an escaped LIKE free-text match. Live in-flight `jobs` are not searched here (a
/// follow-up), and neither is the payload BLOB (heavy; a future opt-in). At least one predicate is
/// required so the query is never an unbounded full scan; results are ordered `finished_at DESC`
/// and backed by the `idx_history_*_finished` indexes where the filters allow. Like `/v1/failures`,
/// the read runs on the isolated `Reader` and never contends on the single-writer mutex.
async fn search(
    State(app): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<Vec<SearchRow>>, StatusCode> {
    let limit = params
        .limit
        .unwrap_or(SEARCH_DEFAULT_LIMIT)
        .min(SEARCH_MAX_LIMIT) as i64;
    // Treat an empty/whitespace query as absent so it never counts as a predicate.
    let q = params
        .q
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let name = params.name;
    let state = params.state;
    let trace_id = params.trace_id;
    let since_ms = params.since_ms;
    // Reject a predicate-free request rather than scanning the whole history table.
    if q.is_none() && name.is_none() && state.is_none() && trace_id.is_none() && since_ms.is_none()
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let rows = app
        .reader
        .query(move |conn| {
            // Fully parameterized; user input never interpolated into SQL.
            let mut sql = String::from(
                "SELECT id, name, state, attempts, created_at, finished_at, last_error, trace_id, execution_depth \
                 FROM job_history WHERE 1=1",
            );
            let mut args: Vec<rusqlite::types::Value> = Vec::new();
            if let Some(q) = q {
                let pattern = format!("%{}%", escape_like(&q));
                let idx = args.len() + 1;
                sql.push_str(&format!(
                    " AND (id LIKE ?{idx} ESCAPE '\\' OR name LIKE ?{idx} ESCAPE '\\' \
                       OR trace_id LIKE ?{idx} ESCAPE '\\' OR last_error LIKE ?{idx} ESCAPE '\\')"
                ));
                args.push(rusqlite::types::Value::Text(pattern));
            }
            if let Some(name) = name {
                args.push(rusqlite::types::Value::Text(name));
                sql.push_str(&format!(" AND name = ?{}", args.len()));
            }
            if let Some(state) = state {
                args.push(rusqlite::types::Value::Text(state));
                sql.push_str(&format!(" AND state = ?{}", args.len()));
            }
            if let Some(trace_id) = trace_id {
                args.push(rusqlite::types::Value::Text(trace_id));
                sql.push_str(&format!(" AND trace_id = ?{}", args.len()));
            }
            if let Some(since) = since_ms {
                args.push(rusqlite::types::Value::Integer(since));
                sql.push_str(&format!(" AND finished_at >= ?{}", args.len()));
            }
            args.push(rusqlite::types::Value::Integer(limit));
            sql.push_str(&format!(" ORDER BY finished_at DESC LIMIT ?{}", args.len()));
            let mut statement = conn.prepare(&sql)?;
            let rows = statement.query_map(rusqlite::params_from_iter(args), |r| {
                Ok(SearchRow {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    state: r.get(2)?,
                    attempts: r.get(3)?,
                    created_at: r.get(4)?,
                    finished_at: r.get(5)?,
                    last_error: r.get(6)?,
                    trace_id: r.get(7)?,
                    execution_depth: r.get(8)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
        })
        .await
        .map_err(|error| match error {
            ReaderError::Busy | ReaderError::Timeout | ReaderError::Unavailable => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            ReaderError::Db(_) => StatusCode::INTERNAL_SERVER_ERROR,
        })?;
    Ok(Json(rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Durability;
    use crate::reader::Reader;

    fn state_for(db: Arc<DatabaseManager>, path: &str) -> AppState {
        AppState {
            db,
            reader: Arc::new(Reader::from_env(path.to_string())),
        }
    }

    #[tokio::test]
    async fn console_serves_self_contained_html_page() {
        use axum::response::IntoResponse;
        let page = console().await;
        assert!(page.0.contains("AnvilMQ Search Console"));
        let response = page.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "text/html; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn readiness_fails_fast_under_contention_and_metrics_remain_available() {
        let db = Arc::new(DatabaseManager::new(":memory:").await.unwrap());
        let app = state_for(db.clone(), ":memory:");
        assert_eq!(ready(State(app.clone())).await.0, StatusCode::OK);
        let conn = db.get_shared_connection();
        let _guard = conn.lock().await;
        assert_eq!(
            ready(State(app.clone())).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(metrics(State(app))
            .await
            .1
            .contains("# TYPE anvilmq_jobs gauge"));
    }

    #[tokio::test]
    async fn failures_feed_filters_orders_and_caps() {
        let path = std::env::temp_dir().join(format!("anvil-failfeed-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(
            DatabaseManager::with_durability(path.to_str().unwrap(), Durability::Normal)
                .await
                .unwrap(),
        );
        // Seed a mix of Failed and Completed rows across names and finished_at values.
        let seed: Vec<(String, String, String, i64, i64)> = vec![
            ("f1".into(), "email".into(), "Failed".into(), 3, 100),
            ("f2".into(), "email".into(), "Failed".into(), 5, 300),
            ("f3".into(), "sms".into(), "Failed".into(), 2, 200),
            ("c1".into(), "email".into(), "Completed".into(), 1, 400),
        ];
        {
            let conn = db.get_shared_connection();
            tokio::task::spawn_blocking(move || {
                let conn = conn.blocking_lock();
                for (id, name, state, attempts, finished) in seed {
                    conn.execute(
                        "INSERT INTO job_history (id, name, state, priority, payload, parent_id, trace_id, execution_depth, attempts, max_attempts, created_at, finished_at, last_error) \
                         VALUES (?1, ?2, ?3, 0, X'', NULL, 'trace', 0, ?4, 3, 0, ?5, 'boom')",
                        rusqlite::params![id, name, state, attempts, finished],
                    )
                    .unwrap();
                }
            })
            .await
            .unwrap();
        }
        let app = state_for(db.clone(), path.to_str().unwrap());

        // No filters: only Failed rows, newest finished_at first.
        let all = failures(
            State(app.clone()),
            Query(FailuresParams {
                name: None,
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            all.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
            vec!["f2", "f3", "f1"]
        );
        assert!(all.iter().all(|r| r.name != "email" || r.id != "c1"));

        // name filter.
        let email = failures(
            State(app.clone()),
            Query(FailuresParams {
                name: Some("email".into()),
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            email.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
            vec!["f2", "f1"]
        );

        // since_ms lower bound (inclusive).
        let recent = failures(
            State(app.clone()),
            Query(FailuresParams {
                name: None,
                since_ms: Some(200),
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            recent.iter().map(|r| r.id.clone()).collect::<Vec<_>>(),
            vec!["f2", "f3"]
        );

        // limit honored.
        let limited = failures(
            State(app.clone()),
            Query(FailuresParams {
                name: None,
                since_ms: None,
                limit: Some(1),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].id, "f2");

        // Oversized limit clamps to the hard cap rather than erroring.
        let capped = failures(
            State(app.clone()),
            Query(FailuresParams {
                name: None,
                since_ms: None,
                limit: Some(50_000),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(capped.len(), 3);

        // last_error is surfaced for drill-down.
        assert_eq!(all[0].last_error.as_deref(), Some("boom"));

        drop(app);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    /// Seeds `job_history` with full rows for the search tests.
    /// Columns per tuple: (id, name, state, trace_id, last_error, created_at, finished_at).
    async fn seed_search_history(
        db: &DatabaseManager,
        rows: Vec<(&str, &str, &str, &str, Option<&str>, i64, i64)>,
    ) {
        let owned: Vec<(String, String, String, String, Option<String>, i64, i64)> = rows
            .into_iter()
            .map(|(id, name, state, trace, err, created, finished)| {
                (
                    id.to_string(),
                    name.to_string(),
                    state.to_string(),
                    trace.to_string(),
                    err.map(|e| e.to_string()),
                    created,
                    finished,
                )
            })
            .collect();
        let conn = db.get_shared_connection();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            for (id, name, state, trace, err, created, finished) in owned {
                conn.execute(
                    "INSERT INTO job_history (id, name, state, priority, payload, parent_id, trace_id, execution_depth, attempts, max_attempts, created_at, finished_at, last_error) \
                     VALUES (?1, ?2, ?3, 0, X'', NULL, ?4, 0, 3, 3, ?5, ?6, ?7)",
                    rusqlite::params![id, name, state, trace, created, finished, err],
                )
                .unwrap();
            }
        })
        .await
        .unwrap();
    }

    fn ids(rows: &[SearchRow]) -> Vec<String> {
        rows.iter().map(|r| r.id.clone()).collect()
    }

    #[tokio::test]
    async fn search_like_matches_across_columns_and_filters_narrow() {
        let path = std::env::temp_dir().join(format!("anvil-search-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(
            DatabaseManager::with_durability(path.to_str().unwrap(), Durability::Normal)
                .await
                .unwrap(),
        );
        seed_search_history(
            &db,
            vec![
                (
                    "job-alpha",
                    "email",
                    "Failed",
                    "trace-1",
                    Some("timeout talking to smtp"),
                    10,
                    100,
                ),
                ("job-beta", "sms", "Completed", "trace-2", None, 20, 200),
                (
                    "gamma-id",
                    "email",
                    "Failed",
                    "alpha-trace",
                    Some("boom"),
                    30,
                    300,
                ),
            ],
        )
        .await;
        let app = state_for(db.clone(), path.to_str().unwrap());

        // `q` matches id (job-alpha) and trace_id (alpha-trace).
        let by_alpha = search(
            State(app.clone()),
            Query(SearchParams {
                q: Some("alpha".into()),
                name: None,
                state: None,
                trace_id: None,
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(ids(&by_alpha), vec!["gamma-id", "job-alpha"]);

        // `q` matches last_error text.
        let by_error = search(
            State(app.clone()),
            Query(SearchParams {
                q: Some("smtp".into()),
                name: None,
                state: None,
                trace_id: None,
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(ids(&by_error), vec!["job-alpha"]);

        // `q` matches name.
        let by_name_q = search(
            State(app.clone()),
            Query(SearchParams {
                q: Some("sms".into()),
                name: None,
                state: None,
                trace_id: None,
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(ids(&by_name_q), vec!["job-beta"]);

        // Structured filters narrow: name + state.
        let email_failed = search(
            State(app.clone()),
            Query(SearchParams {
                q: None,
                name: Some("email".into()),
                state: Some("Failed".into()),
                trace_id: None,
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(ids(&email_failed), vec!["gamma-id", "job-alpha"]);

        // trace_id exact filter.
        let by_trace = search(
            State(app.clone()),
            Query(SearchParams {
                q: None,
                name: None,
                state: None,
                trace_id: Some("trace-2".into()),
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(ids(&by_trace), vec!["job-beta"]);

        // since_ms lower bound (inclusive) on finished_at.
        let recent = search(
            State(app.clone()),
            Query(SearchParams {
                q: None,
                name: None,
                state: None,
                trace_id: None,
                since_ms: Some(200),
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(ids(&recent), vec!["gamma-id", "job-beta"]);

        // Nullable fields round-trip.
        assert_eq!(by_trace[0].last_error, None);

        drop(app);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn search_escapes_like_metacharacters() {
        let path =
            std::env::temp_dir().join(format!("anvil-search-esc-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(
            DatabaseManager::with_durability(path.to_str().unwrap(), Durability::Normal)
                .await
                .unwrap(),
        );
        seed_search_history(
            &db,
            vec![
                (
                    "id-100pct",
                    "email",
                    "Failed",
                    "t1",
                    Some("100% failure"),
                    10,
                    100,
                ),
                (
                    "id-plain",
                    "email",
                    "Failed",
                    "t2",
                    Some("50 percent"),
                    20,
                    200,
                ),
                (
                    "id_under",
                    "email",
                    "Failed",
                    "t3",
                    Some("has_underscore"),
                    30,
                    300,
                ),
                (
                    "idXunder",
                    "email",
                    "Failed",
                    "t4",
                    Some("noUnderscore"),
                    40,
                    400,
                ),
            ],
        )
        .await;
        let app = state_for(db.clone(), path.to_str().unwrap());

        // `%` is literal: matches only the "100%" row, not every row.
        let pct = search(
            State(app.clone()),
            Query(SearchParams {
                q: Some("100%".into()),
                name: None,
                state: None,
                trace_id: None,
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(ids(&pct), vec!["id-100pct"]);

        // `_` is literal: matches "id_under" but NOT "idXunder".
        let under = search(
            State(app.clone()),
            Query(SearchParams {
                q: Some("id_under".into()),
                name: None,
                state: None,
                trace_id: None,
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(ids(&under), vec!["id_under"]);

        drop(app);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn search_orders_caps_and_requires_predicate() {
        let path =
            std::env::temp_dir().join(format!("anvil-search-cap-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(
            DatabaseManager::with_durability(path.to_str().unwrap(), Durability::Normal)
                .await
                .unwrap(),
        );
        seed_search_history(
            &db,
            vec![
                ("s1", "email", "Failed", "t", Some("e"), 10, 100),
                ("s2", "email", "Failed", "t", Some("e"), 20, 300),
                ("s3", "email", "Failed", "t", Some("e"), 30, 200),
            ],
        )
        .await;
        let app = state_for(db.clone(), path.to_str().unwrap());

        // Ordered finished_at DESC.
        let ordered = search(
            State(app.clone()),
            Query(SearchParams {
                q: None,
                name: Some("email".into()),
                state: None,
                trace_id: None,
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(ids(&ordered), vec!["s2", "s3", "s1"]);

        // limit honored.
        let limited = search(
            State(app.clone()),
            Query(SearchParams {
                q: None,
                name: Some("email".into()),
                state: None,
                trace_id: None,
                since_ms: None,
                limit: Some(1),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(ids(&limited), vec!["s2"]);

        // Oversized limit clamps to the hard cap rather than erroring.
        let capped = search(
            State(app.clone()),
            Query(SearchParams {
                q: None,
                name: Some("email".into()),
                state: None,
                trace_id: None,
                since_ms: None,
                limit: Some(50_000),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(capped.len(), 3);

        // No predicate at all → 400, never an unbounded scan. Whitespace-only q counts as absent.
        let err = search(
            State(app.clone()),
            Query(SearchParams {
                q: Some("   ".into()),
                name: None,
                state: None,
                trace_id: None,
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err, StatusCode::BAD_REQUEST);

        drop(app);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn search_returns_while_writer_mutex_held() {
        let path =
            std::env::temp_dir().join(format!("anvil-search-iso-{}.db", uuid::Uuid::new_v4()));
        let db = Arc::new(
            DatabaseManager::with_durability(path.to_str().unwrap(), Durability::Normal)
                .await
                .unwrap(),
        );
        seed_search_history(
            &db,
            vec![("iso", "email", "Failed", "t", Some("boom"), 10, 100)],
        )
        .await;
        let app = state_for(db.clone(), path.to_str().unwrap());
        // Hold the writer mutex for the whole read to prove the reader is isolated from it.
        let conn = db.get_shared_connection();
        let guard = conn.lock().await;
        let rows = search(
            State(app.clone()),
            Query(SearchParams {
                q: Some("boom".into()),
                name: None,
                state: None,
                trace_id: None,
                since_ms: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(ids(&rows), vec!["iso"]);
        drop(guard);
        drop(conn);
        drop(app);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn named_lifecycle_is_bounded_to_allowlist_and_renders_series() {
        let metrics = Metrics {
            named: NamedLifecycle::configured(&["ProcessActivity".into(), "Stress".into()]),
            ..Default::default()
        };
        metrics.named.event("ProcessActivity", "enqueued");
        metrics.named.event("ProcessActivity", "completed");
        metrics.named.observe_duration("ProcessActivity", 42);
        metrics.named.event("Stress", "failed");
        // Names outside the allowlist must never create a series.
        metrics.named.event("Unlisted", "completed");
        metrics.named.observe_duration("Unlisted", 5);
        let output = metrics.render();
        assert!(output.contains(
            "anvilmq_jobs_by_name_total{name=\"ProcessActivity\",event=\"completed\"} 1"
        ));
        assert!(output
            .contains("anvilmq_jobs_by_name_total{name=\"ProcessActivity\",event=\"enqueued\"} 1"));
        assert!(output.contains("anvilmq_jobs_by_name_total{name=\"Stress\",event=\"failed\"} 1"));
        assert!(output.contains("anvilmq_job_duration_seconds_count{name=\"ProcessActivity\"} 1"));
        assert!(output.contains(
            "anvilmq_job_duration_seconds_bucket{name=\"ProcessActivity\",le=\"0.1\"} 1"
        ));
        assert!(!output.contains("Unlisted"));
    }

    #[test]
    fn named_lifecycle_renders_nothing_without_allowlist() {
        assert!(!Metrics::default()
            .render()
            .contains("anvilmq_jobs_by_name_total"));
    }
}
