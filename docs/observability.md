# Queue pressure

`/metrics` now exposes cached backlog depth/age and committed claim timing in addition to the existing lifecycle counters, RPC latency, throttling, recovery, retention, and idempotency metrics.

## Enable named queues

Global pressure metrics include every queue. Named metrics are opt-in to keep producer-controlled names from creating unlimited time series:

```powershell
$env:ANVILMQ_METRICS_QUEUES = 'invoices,email'
cargo run
```

For Helm, set `metricsQueues: [invoices, email]`. Restart to change the allowlist. Up to 32 unique exact names, each at most 256 UTF-8 bytes; commas and surrounding whitespace are not supported. Default is an empty allowlist. Never use a tenant/job ID as a queue-name metric label. Escaping handles quotes, backslashes, and newlines. Invalid configuration fails startup.

## All-names metric mode

Per-name lifecycle series (`anvilmq_jobs_by_name_total`, `anvilmq_job_duration_seconds`) are allowlist-bound by default: only `ANVILMQ_METRICS_QUEUES` names produce series. Set `ANVILMQ_METRICS_MODE=all` (case-insensitive; unset or `allowlist` keeps today's behavior) to auto-register every job name on first sighting, so every function appears without maintaining an allowlist.

```powershell
$env:ANVILMQ_METRICS_MODE = 'all'
$env:ANVILMQ_METRICS_MAX_NAMES = '200'   # optional; default 100, max 1000
cargo run
```

Cardinality stays bounded: `ANVILMQ_METRICS_MAX_NAMES` caps the number of distinct registered names (default 100, hard ceiling 1000). Total per-name series is roughly cap x (5 counters + histogram buckets), so worst case is deterministic. Once the cap is reached, further unseen names create **no** series; instead a single overflow counter `anvilmq_jobs_by_name_dropped_total` increments (a nonzero value tells operators names were dropped — no per-dropped-name cardinality). The gauge `anvilmq_named_series` reports the current registered count so you can alert on proximity to the cap. Non-numeric, zero, or over-ceiling caps, and unknown mode values, fail startup.

Allowlisted names (`ANVILMQ_METRICS_QUEUES`) are pre-seeded up front in `all` mode too — they always appear and count toward the cap, so pressure and named series agree. Render order is deterministic (names sorted). This mode only changes per-name lifecycle cardinality: **per-queue pressure series stay allowlist-bound** and are unaffected by the mode or cap.

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
| `anvilmq_enqueue_rejections_total{kind}` | Admission-time enqueue rejections by kind (`ingress_velocity`, `parent_not_found`, `inconsistent_depth`, `circuit_breaker`, `chain_quarantine`). Each corresponds to a durable row in the [enqueue-rejections feed](#enqueue-rejections-feed); rejected enqueues never become jobs. |
| `anvilmq_rejections_pruned_total` | Aged-out forensic `enqueue_rejections` rows reclaimed by the retention sweeper (`ANVILMQ_RETENTION_REJECTIONS_AGE_MS`). |

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

A Grafana table over this endpoint ships as the `anvilmq-failures` dashboard, backed by the Infinity datasource; see [Dashboard and alerts](#dashboard-and-alerts).

## Enqueue rejections feed

`GET /v1/rejections` returns durably-recorded admission-time enqueue rejections from `enqueue_rejections` as a JSON array. It captures the five rejection kinds that are refused at enqueue time — `ingress_velocity`, `parent_not_found`, `inconsistent_depth`, `circuit_breaker`, and `chain_quarantine` — each of which creates **no job row**. Because the rejected enqueue never became a job, these events appear nowhere in the recent-failures feed or the search API; this feed is the only record-level trail for them.

It exists for durable forensics. A rejected enqueue used to leave only an aggregate counter increment and a transient log line, so once the log rotated there was no way to reconstruct which lineage tripped a circuit breaker or chain quarantine. Each row here carries `trace_id` and `parent_id`, so an operator can trace a rejection back to the offending lineage even after that lineage's jobs have been pruned from job history.

The read runs on the isolated read-only WAL replica connection ([see the reader primitive](#read-only-replica-connection)); it opens a separate `SQLITE_OPEN_READ_ONLY` connection and never acquires the single-writer mutex, so the feed cannot stall enqueue/claim/complete. Concurrency and per-query time are bounded by `ANVILMQ_READER_MAX_CONCURRENCY` and `ANVILMQ_READER_TIMEOUT_MS`; a busy or timed-out read returns HTTP 503.

Query parameters (all optional):

| Parameter | Type | Meaning |
| --- | --- | --- |
| `kind` | string | Restrict to one rejection kind (exact match), e.g. `circuit_breaker`. |
| `name` | string | Restrict to one job name (exact match). |
| `trace_id` | string | Restrict to one lineage (exact `trace_id` match). |
| `since_ms` | integer | Only rows with `rejected_at >= since_ms` (inclusive). |
| `limit` | integer | Page size; defaults to 100 and is hard-capped at 1000. |

Rows are ordered by `rejected_at` descending (newest first), then `id` descending, backed by the `idx_rejections_name_rejected` / `idx_rejections_rejected_at` indexes. Each element has the shape:

```json
{
  "id": 42,
  "rejected_at": 1727040000000,
  "kind": "circuit_breaker",
  "name": "fanout",
  "trace_id": "abc123",
  "parent_id": "0f9c…",
  "execution_depth": 32,
  "rate_limit_facet": "tenant-7",
  "detail": "execution depth 32 exceeds maximum"
}
```

`trace_id`, `parent_id`, `execution_depth`, and `rate_limit_facet` may be `null`. Rows are retention-bounded: records older than `ANVILMQ_RETENTION_REJECTIONS_AGE_MS` (default `604800000`, 7d; `0` disables) are deleted by the retention sweeper, and the reclaimed-row count surfaces as `anvilmq_rejections_pruned_total`. Example:

```powershell
(Invoke-WebRequest "http://127.0.0.1:9090/v1/rejections?kind=circuit_breaker&limit=50").Content
```

## Search

`GET /v1/search` runs a read-only search over `job_history` — the retention-bounded record of terminal runs — and returns a JSON array. It answers "find the past runs that match X" for on-call drill-down when the aggregate counters and the recent-failures feed are not specific enough. Live in-flight `jobs` are not searched (a follow-up), and neither is the payload BLOB (heavy; a future opt-in). The read runs on the isolated read-only WAL replica connection ([see the reader primitive](#read-only-replica-connection)); it opens a separate `SQLITE_OPEN_READ_ONLY` connection and never acquires the single-writer mutex, so search cannot stall enqueue/claim/complete. Concurrency and per-query time are bounded by `ANVILMQ_READER_MAX_CONCURRENCY` and `ANVILMQ_READER_TIMEOUT_MS`; a busy or timed-out read returns HTTP 503.

Query parameters (all optional individually, but at least one is required):

| Parameter | Type | Meaning |
| --- | --- | --- |
| `q` | string | Free-text match across `id`, `name`, `trace_id`, and `last_error` (a bounded set of low-cost text columns). Whitespace-only is treated as absent. Uses escaped LIKE by default, or tokenized FTS5 `MATCH` when `ANVILMQ_FTS_ENABLED` is set (on the FTS path a term with a trailing `*` runs a whole-token prefix query — see [Opt-in full-text search](#opt-in-full-text-search-fts5)). |
| `name` | string | Restrict to one job name (exact match). |
| `state` | string | Restrict to one lifecycle state, e.g. `Failed` or `Completed` (exact match). |
| `trace_id` | string | Restrict to one trace id (exact match). |
| `since_ms` | integer | Only rows with `finished_at >= since_ms` (inclusive). `finished_at` is used (not `created_at`) because history is retention-bounded by it and it is index-backed. |
| `limit` | integer | Page size; defaults to 100 and is hard-capped at 1000. |

At least one of `q`, `name`, `state`, `trace_id`, or `since_ms` must be present; a request with no predicate returns HTTP 400 rather than scanning the whole table. The `limit` cap is always enforced.

`q` is matched with SQL `LIKE ? ESCAPE '\'` and is always bound as a parameter — never interpolated. The `\`, `%`, and `_` characters in the query are escaped, so a `q` of `100%` or `id_1` matches those characters literally instead of acting as wildcards.

Rows are ordered by `finished_at` descending (newest first), backed by the `idx_history_name_state_finished` / `idx_history_state_finished` indexes for the common `name`/`state`/`since_ms` filters. Each element has the shape:

```json
{
  "id": "0f9c…",
  "name": "email",
  "state": "Failed",
  "attempts": 3,
  "created_at": 1727039990000,
  "finished_at": 1727040000000,
  "last_error": "connection refused",
  "trace_id": "abc123",
  "execution_depth": 0
}
```

`last_error` and `trace_id` may be `null`. Example:

```powershell
(Invoke-WebRequest "http://127.0.0.1:9090/v1/search?q=timeout&state=Failed&limit=50").Content
```

### Opt-in full-text search (FTS5)

By default `q` is the escaped-LIKE substring scan described above. Setting `ANVILMQ_FTS_ENABLED` to a truthy value (`1`, `true`, or `yes`, ASCII case-insensitive) switches the free-text path to a SQLite FTS5 full-text index. It is **off by default**; when disabled there is no index, no triggers, and no added write cost, and `/v1/search` behaves exactly as documented above.

- **What is indexed.** A standalone FTS5 table (`job_history_fts`) over `job_history` metadata only: `name`, `trace_id`, and `last_error` are tokenized and searchable, and `id` is stored `UNINDEXED` for retrieval. The payload BLOB is intentionally not indexed (a future opt-in).
- **Query semantics.** When enabled and `q` is present, `q` is resolved with `MATCH` (joined back to `job_history` so every returned field and the structured `name`/`state`/`trace_id`/`since_ms`/`limit` filters still apply and the response shape is unchanged). Matching is token-based rather than substring: `q=smtp` finds a row whose `last_error` is `timeout talking to smtp`, but a partial token like `mtp` does not match. Each whitespace-separated term is quoted and escaped before it reaches FTS5, so query text is treated as literal terms (implicit AND) — FTS5 operators are neutralized and a pathological `q` returns an empty/best-effort result, never an HTTP 5xx. Requests with no `q` (structured filters only) are unaffected by the flag.
- **Prefix wildcard (`word*`).** A term that ends in an explicit trailing `*` (e.g. `upstrea*`) is run as a **whole-token prefix** query: it matches every indexed token that *begins with* the stem, so `upstrea*` matches `upstream` and `upstream-svc`. This is a prefix, not a substring — the stem must be a leading fragment of a token, so `pstream` (no leading `up`, no trailing `*`) still matches nothing. Only a trailing `*` triggers this; a `*` anywhere else (`foo*bar`, `*word`) is treated as a literal character, and a term that is only stars (`*`, `**`) contributes nothing. A term without a trailing `*` keeps its exact-token behavior. This applies only on the FTS path; the default escaped-LIKE path already does substring matching. A future opt-in could add trigram indexing (or an FTS5 `prefix=` table option to precompute prefix tokens) for infix/faster-prefix search; neither is implemented today.
- **Maintenance and retention.** The index is kept in sync by SQLite triggers, not application code: an `AFTER INSERT` trigger mirrors each new terminal row in and an `AFTER DELETE` trigger removes it, so the index stays correct across every write site and is automatically bounded by the retention sweeper (pruned history rows drop out of the index in the same transaction). Existing history is backfilled into the index once at startup, in bounded batches and idempotently, off the hot path.

## Console

`GET /console` serves a built-in, read-only search console: a single HTML page that drives the
`/v1/search` and `/v1/jobs/{id}` APIs from the browser. It is served same-origin off the broker's
existing HTTP server, so no CORS is involved, and the JSON API remains the source of truth — the
console is purely a consumer of the public `/v1/search` and `/v1/jobs/{id}` endpoints and adds no
private or side-channel API.

The page is fully self-contained: its inline CSS and vanilla JavaScript are embedded in the binary
(`include_str!`) with no build step and no external CDN or network dependency, so it works
air-gapped inside a cluster.

Features:

- A filter bar mapping to `/v1/search` params: free-text `q`, exact `name`, a `state` select
  (any / Waiting / Delayed / Active / Completed / Failed), exact `trace_id`, a relative-time picker
  (5m / 15m / 1h / 6h / 24h / all) that computes `since_ms` client-side (`all` omits it), and a
  `limit` (default 100, capped at 1000). It requires at least one predicate, matching the API.
- A results table (name, state, attempts, execution depth, created, finished, last error, trace id,
  id) with humanized timestamps and a result count.
- **Lineage drill-down**: clicking a `trace_id` pins it as the filter, clears the free-text query,
  re-runs the search, and re-sorts the chain by `execution_depth` ascending so the fan-out lineage
  reads top-down. A "clear lineage / back to search" control returns to normal search.
- **Job-detail click-through**: clicking a row's `id` opens the full single-job record inline in a
  modal overlay, fetched same-origin from `GET /v1/jobs/{id}`. It renders every field the endpoint
  returns (state, source, priority, attempts/max, timestamps, ancestry, worker/lease info, and the
  payload) with a failed job's `last_error` surfaced prominently at the top for one-click triage.
  The payload is pretty-printed when JSON and shown verbatim (noted as base64-encoded binary) when
  the bytes are not UTF-8 JSON. The overlay shows a loading state, maps non-200s to clear in-panel
  messages (400 invalid id, 404 unknown / aged-out, 503 reader busy), and closes via a Close button,
  the ESC key, or a backdrop click. The open job is reflected in the URL hash (`#job/<id>`) so a
  refresh or shared link reopens it; the trace-id lineage drill-down remains a separate action.
- Graceful response handling: HTTP 400 shows an "add at least one filter" hint (not an error), 503
  shows a "reader busy, try again" notice, and other non-200 responses show a generic error.

All values from the API (job `name`, `last_error`, `trace_id`, etc.) are user-controlled strings and
are inserted via `textContent` / `createElement` only, never via HTML string interpolation, so the
console is safe against stored XSS. Like the endpoints it consumes, the console is unauthenticated;
expose the HTTP listener only on a trusted network.

## Read-only replica connection

Observability read endpoints (currently the recent-failures feed, the enqueue-rejections feed, and the search API) run on a dedicated read-only connection rather than the broker's single writer. SQLite in WAL mode allows one writer plus many concurrent readers, so each read opens a fresh `SQLITE_OPEN_READ_ONLY` connection (with `query_only=true` and a tight busy timeout), runs on the blocking thread pool, and is interrupted by a progress handler once the per-query budget is exhausted — the same safeguards the pressure sampler uses. Because these connections never touch the writer mutex, heavy or slow reads are isolated from enqueue/claim/complete. Two environment variables bound the primitive: `ANVILMQ_READER_MAX_CONCURRENCY` (default 4, minimum 1) caps concurrent reader connections, and `ANVILMQ_READER_TIMEOUT_MS` (default 500) caps per-query wall-clock time. File-backed storage is required; `:memory:` databases are per-connection and invisible to the separate reader.

## Dashboard and alerts

1. Configure a Prometheus scrape target using [the example](../observability/prometheus.yaml). In Kubernetes, use the actual Service address and allow monitoring traffic through the chart's NetworkPolicy. This does not install a monitoring stack or a ServiceMonitor.
2. Load [alerts.yaml](../observability/alerts.yaml) through Prometheus `rule_files` (or adapt the group into your existing PrometheusRule deployment).
3. Import [anvilmq-dashboard.json](../charts/anvilmq/dashboards/anvilmq-dashboard.json) into Grafana and select a Prometheus data source and broker instance.
4. Import [anvilmq-functions.json](../charts/anvilmq/dashboards/anvilmq-functions.json) into Grafana and select a Prometheus data source. This per-function view (uid `anvilmq-functions`) covers only allowlisted job names (`ANVILMQ_METRICS_QUEUES`): a per-function throughput/failure-rate/p95-latency overview table, a recently-failed-functions table (terminal failures over the selected range, retries excluded), and a failures-over-time-by-function bars panel. Per-failure error messages and payloads are not shown; that drill-down depends on the read-only history query API tracked as a Phase 6 roadmap item.
5. Import [anvilmq-failures.json](../charts/anvilmq/dashboards/anvilmq-failures.json) into Grafana and select an **Infinity** data source. This single-table view (uid `anvilmq-failures`) reads the recent-failures feed (`GET /v1/failures`) as JSON through the [`yesoreyeram-infinity-datasource`](https://grafana.com/grafana/plugins/yesoreyeram-infinity-datasource/) plugin — the record-level drill-down (which job, which error) that the aggregate Prometheus counters cannot provide. Columns, in order: `finished_at` (rendered as a timestamp from the feed's epoch-ms value), `name`, `last_error` (wide, wrapped — the key troubleshooting signal), `attempts`, `execution_depth`, `trace_id`, and `id`. The panel query uses a relative URL (`/v1/failures?limit=1000&since_ms=${__from}`) so the base URL comes from the provisioned datasource, and it honors the dashboard time picker: Grafana's `${__from}` (epoch ms range start) is passed as the feed's `since_ms` lower bound. Unlike the other dashboards this one needs the Infinity plugin installed in Grafana (e.g. via `GF_INSTALL_PLUGINS=yesoreyeram-infinity-datasource` or `grafana.plugins`).

Both Prometheus dashboards live in `charts/anvilmq/dashboards/`. The Helm chart can auto-provision every dashboard in that directory through the Grafana dashboard sidecar (kube-prometheus-stack convention): set `dashboards.enabled=true` to emit a labelled ConfigMap that the sidecar imports. See `charts/anvilmq/values.yaml` for the label, folder, and namespace overrides.

The `anvilmq-failures` dashboard additionally needs an Infinity datasource pointed at the broker's HTTP port. The chart can optionally provision one: set `infinityDatasource.enabled=true` to emit a datasource-provisioning ConfigMap (labelled for the Grafana datasource sidecar) for a `yesoreyeram-infinity-datasource` whose base URL defaults to the in-cluster broker service DNS and HTTP port. It is **off by default** and safe to leave off on clusters without the Infinity plugin; the chart cannot install the plugin, so Grafana must already have it (`GF_INSTALL_PLUGINS` / `grafana.plugins`). Override the URL, name, sidecar label/value, and namespace via the `infinityDatasource` block in `charts/anvilmq/values.yaml`.

The dashboard separates backlog, per-queue due age, arrivals/completions/retries, p95/p99 timing, throttled polls, lease recovery, RPC latency, and sample health. Empty histogram windows can show no data; do not interpret this as zero latency. Alert thresholds are initial examples, not a workload SLO: tune the 60-second age threshold and one-job/second growth threshold against real jobs. Keep your existing scrape-target-down, PVC free-space, and node/storage alerts; stale-metric rules cannot detect a target that disappears entirely.

Validate artifacts with:

```text
promtool check config observability/prometheus.yaml
promtool check rules observability/alerts.yaml
promtool test rules observability/alerts.test.yaml
```

Use the dashboard during load tests and node recovery experiments. Start with oldest-due age and growth, then determine whether the constraint is worker capacity, dispatch limits, RPC/storage latency, or recovery trouble.

Artifact formats follow the [Prometheus rule documentation](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) and [Grafana dashboard JSON model](https://grafana.com/docs/grafana-cloud/learn-and-build/visualizations/dashboards/build-dashboards/view-dashboard-json-model/).
