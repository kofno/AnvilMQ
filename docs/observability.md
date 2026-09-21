# Queue pressure

`/metrics` now exposes cached backlog depth/age and committed claim timing in addition to the existing lifecycle counters, RPC latency, throttling, recovery, retention, and idempotency metrics.

## Enable named queues

Global pressure metrics include every queue. Named metrics are opt-in to keep producer-controlled names from creating unlimited time series:

```powershell
$env:ANVILMQ_METRICS_QUEUES = 'invoices,email'
cargo run
```

For Helm, set `metricsQueues: [invoices, email]`. Restart to change the allowlist. Up to 32 unique exact names, each at most 256 UTF-8 bytes; commas and surrounding whitespace are not supported. Default is an empty allowlist. Never use a tenant/job ID as a queue-name metric label. Escaping handles quotes, backslashes, and newlines. Invalid configuration fails startup.

## Signals and semantics

| Metric | Meaning |
| --- | --- |
| `anvilmq_backlog_jobs{scope,queue,kind}` | Sampled live jobs. `waiting`, `delayed`, `active` are stored states. `due` is Waiting/Delayed with a deadline at or before sample time, including throttled work. `unscheduled` has no deadline. `expired_leases` is Active with an expired/missing lease. These kinds overlap: do not sum all kinds. |
| `anvilmq_oldest_due_age_seconds{scope,queue}` | Time since the oldest due Waiting/Delayed job became eligible by time; zero when none. Uses max(created_at, available_at), so intentional delay/backoff is excluded. Throttling and lack of workers contribute to age. |
| `anvilmq_first_claim_delay_seconds` | Histogram of enqueue to first committed claim, including initial scheduled delay. Retries do not add samples. |
| `anvilmq_due_wait_seconds` | Histogram of time past due at each committed claim, including retries; excludes scheduled delay/backoff. |
| `anvilmq_pressure_sample_timestamp_seconds` | Last successful sample Unix timestamp. Zero until first success. |
| `anvilmq_pressure_sample_errors_total` | Failed/interrupted sample attempts since startup. |
| `anvilmq_pressure_sample_duration_seconds` | Duration of the last sampling attempt. |

`scope="all",queue=""` is the aggregate; `scope="queue"` identifies allowlisted queues. Do not sum both scopes. Named queues remain present at zero after they drain. Histograms and lifecycle event rates are broker-wide in this increment, not per queue. Counters/histograms reset on restart; use `rate`/`increase`, not subtraction across restarts.

Arrival rate uses `rate(anvilmq_transitions_total{event="enqueued"}[5m])`, completion rate uses `event="completed"`. Retries do not count as new arrivals. Their difference is not exact backlog growth: terminal failures also remove jobs and deliberate scheduling changes when work becomes due. Use the due-depth trend alongside the rates. `anvilmq_throttled_polls_total` counts polls encountering blocked work, not blocked jobs or time spent throttled; its rate depends on polling frequency. `event="lease_expired"` counts recovered claims, whereas `expired_leases` reports claims still awaiting recovery.

Claim histograms only describe claimed work. A stopped worker fleet produces no new latency samples: oldest-due age and depth remain the leading signals. These are not handler duration or end-to-end completion histograms.

## Sampling cost and freshness

Scrapes never query SQLite or acquire the writer mutex. Every five seconds, a short-lived read-only connection samples live jobs through a covering index (no payload/history scan). SQLite's progress hook interrupts queries after an approximate 100ms budget, checked every 1,000 VM instructions; lock waits are limited to 25ms. These are query safeguards, not a hard wall-clock limit on OS I/O or connection opening. Sampling attempts never overlap. File-backed storage is required; `:memory:` cannot be observed from the separate connection.

Sampling still costs O(live jobs) read/aggregation work and briefly holds a WAL read snapshot. The covering index also adds write/storage cost. A timed-out sample preserves the last complete result and increments the error counter. At large backlogs, samples may remain stale; alert on freshness rather than interpreting frozen/initial zero values as health. No DB work is added to polling for these gauges. Validate throughput overhead on Azure with representative payloads and backlog sizes.

Cached ages advance at sample time, not scrape time. Independent atomic reads/writes may briefly mix adjacent samples, as with existing lifecycle gauges; this endpoint is not a transactional snapshot. Deadlines use wall time, so clock adjustments affect ages (negative values clamp to zero).

## Recent failures feed

`GET /v1/failures` returns recent terminal failures from `job_history` as a JSON array. Only exhausted failures reach `job_history`: a job that fails but still has retries left stays live in `jobs`, so this feed never shows in-flight retries — only failures that gave up, each with its `last_error`. This is the record-level drill-down (which job, which error) that the aggregate Prometheus counters cannot provide.

The read runs on the isolated read-only WAL replica connection ([see the reader primitive](#read-only-replica-connection)); it opens a separate `SQLITE_OPEN_READ_ONLY` connection and never acquires the single-writer mutex, so the feed cannot stall enqueue/claim/complete. Concurrency and per-query time are bounded by `ANVILMQ_READER_MAX_CONCURRENCY` and `ANVILMQ_READER_TIMEOUT_MS`; a busy or timed-out read returns HTTP 503.

Query parameters (all optional):

| Parameter | Type | Meaning |
| --- | --- | --- |
| `name` | string | Restrict to one job name (exact match). |
| `since_ms` | integer | Only rows with `finished_at >= since_ms` (inclusive). |
| `limit` | integer | Page size; defaults to 100 and is hard-capped at 1000. |

Rows are ordered by `finished_at` descending (newest first), backed by the `idx_history_name_state_finished` / `idx_history_state_finished` indexes. Each element has the shape:

```json
{
  "id": "0f9c…",
  "name": "email",
  "attempts": 3,
  "finished_at": 1727040000000,
  "last_error": "connection refused",
  "trace_id": "abc123",
  "execution_depth": 0
}
```

`last_error` and `trace_id` may be `null`. Example:

```powershell
(Invoke-WebRequest "http://127.0.0.1:9090/v1/failures?name=email&limit=50").Content
```

A Grafana table over this endpoint (via a JSON/Infinity datasource) is a follow-up; this ships the endpoint only.

## Read-only replica connection

Observability read endpoints (currently the recent-failures feed) run on a dedicated read-only connection rather than the broker's single writer. SQLite in WAL mode allows one writer plus many concurrent readers, so each read opens a fresh `SQLITE_OPEN_READ_ONLY` connection (with `query_only=true` and a tight busy timeout), runs on the blocking thread pool, and is interrupted by a progress handler once the per-query budget is exhausted — the same safeguards the pressure sampler uses. Because these connections never touch the writer mutex, heavy or slow reads are isolated from enqueue/claim/complete. Two environment variables bound the primitive: `ANVILMQ_READER_MAX_CONCURRENCY` (default 4, minimum 1) caps concurrent reader connections, and `ANVILMQ_READER_TIMEOUT_MS` (default 500) caps per-query wall-clock time. File-backed storage is required; `:memory:` databases are per-connection and invisible to the separate reader.

## Dashboard and alerts

1. Configure a Prometheus scrape target using [the example](../observability/prometheus.yaml). In Kubernetes, use the actual Service address and allow monitoring traffic through the chart's NetworkPolicy. This does not install a monitoring stack or a ServiceMonitor.
2. Load [alerts.yaml](../observability/alerts.yaml) through Prometheus `rule_files` (or adapt the group into your existing PrometheusRule deployment).
3. Import [anvilmq-dashboard.json](../charts/anvilmq/dashboards/anvilmq-dashboard.json) into Grafana and select a Prometheus data source and broker instance.
4. Import [anvilmq-functions.json](../charts/anvilmq/dashboards/anvilmq-functions.json) into Grafana and select a Prometheus data source. This per-function view (uid `anvilmq-functions`) covers only allowlisted job names (`ANVILMQ_METRICS_QUEUES`): a per-function throughput/failure-rate/p95-latency overview table, a recently-failed-functions table (terminal failures over the selected range, retries excluded), and a failures-over-time-by-function bars panel. Per-failure error messages and payloads are not shown; that drill-down depends on the read-only history query API tracked as a Phase 6 roadmap item.

Both dashboards live in `charts/anvilmq/dashboards/`. The Helm chart can auto-provision them through the Grafana dashboard sidecar (kube-prometheus-stack convention): set `dashboards.enabled=true` to emit a labelled ConfigMap that the sidecar imports. See `charts/anvilmq/values.yaml` for the label, folder, and namespace overrides.

The dashboard separates backlog, per-queue due age, arrivals/completions/retries, p95/p99 timing, throttled polls, lease recovery, RPC latency, and sample health. Empty histogram windows can show no data; do not interpret this as zero latency. Alert thresholds are initial examples, not a workload SLO: tune the 60-second age threshold and one-job/second growth threshold against real jobs. Keep your existing scrape-target-down, PVC free-space, and node/storage alerts; stale-metric rules cannot detect a target that disappears entirely.

Validate artifacts with:

```text
promtool check config observability/prometheus.yaml
promtool check rules observability/alerts.yaml
promtool test rules observability/alerts.test.yaml
```

Use the dashboard during load tests and node recovery experiments. Start with oldest-due age and growth, then determine whether the constraint is worker capacity, dispatch limits, RPC/storage latency, or recovery trouble.

Artifact formats follow the [Prometheus rule documentation](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) and [Grafana dashboard JSON model](https://grafana.com/docs/grafana-cloud/learn-and-build/visualizations/dashboards/build-dashboards/view-dashboard-json-model/).
