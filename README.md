# AnvilMQ

AnvilMQ is an early-stage Rust queue engine intended for autonomous regional Kubernetes deployments. The current implementation is a single-process gRPC server with embedded SQLite persistence, atomic enqueue/dequeue, and a caller-supplied execution-depth guard. Sub-millisecond latency, high availability, and tenant rate limiting are goals, not verified capabilities.

## Current architecture

For a versioned container, Helm chart, and packaged Bun client, see [evaluation releases and Azure deployment](docs/release-and-deploy.md). The chart deploys one broker with FULL durability and a dedicated PVC; it does not provide HA yet.

- Rust Edition 2021, Tokio, and Tonic/Protocol Buffers.
- Bundled SQLite through `rusqlite`. The target architecture in AGENTS.md specifies libSQL; that migration remains open.
- Database work runs in `spawn_blocking`, with one shared connection protected by a mutex.
- WAL with configurable `ANVILMQ_DURABILITY=NORMAL|FULL` (default NORMAL). NORMAL permits loss of recent acknowledged writes after OS/power failure; FULL requests commit synchronization on retained storage. Startup logs the applied settings. See the [durability contract](docs/durability.md) for storage assumptions and operational guidance.
- Initial schema creation and additive ownership/error-history migrations run in a transaction. Existing jobs are preserved.

## Implemented behavior

### Enqueue

`AddJob` persists a UUID, payload, priority, ancestry metadata, attempts limit, timestamps, and optional rate-limit facet. Missing trace IDs are generated. Execution depth greater than 10 is rejected before persistence; ancestry is not verified against a parent record. An immediate transaction atomically inserts the job and, when requested, its idempotency receipt.

Optional `idempotency_key` (protobuf tag 10) deduplicates within the exact job/queue `name`. Empty/omitted means ordinary enqueue; nonempty keys must be nonblank and at most 256 UTF-8 bytes. Matching retries return the original ID and initial enqueue state with `replayed=true` (response tag 3), even after completion or failure. This is an enqueue receipt, not a current-state query. Replays do not reset delays, create another job, consume attempts, or increment the enqueued counter.

The same key with different payload bytes, metadata, priority, delay, retry settings, or rate-limit facet returns AlreadyExists. Default attempts/backoff caps and omitted metadata are normalized before comparison; generated trace IDs and timestamps are excluded. JSON key ordering is not normalized: producers must preserve the original serialized request. Keys are opaque and case-sensitive, scoped to queue name rather than facet; include tenant/business identity when appropriate.

Receipts survive restart and terminal job transitions and are retained indefinitely in this first version, independently of job history. They contain the normalized request including payload, so keyed jobs add storage overhead. There is no TTL or cleanup endpoint yet; deleting receipts removes the corresponding deduplication guarantee. Monitor receipt count and PVC usage. Use a new key for intentionally new work and retain the same key/request across producer retries/restarts. Handler side effects remain at least once. See [client usage](client/README.md).

### Delays and retry backoff

`delay_ms` sets the initial persisted `available_at` timestamp. Zero is immediately eligible; positive delays return state Delayed. Dequeue directly claims due jobs (`available_at <= server time`) without a separate promotion loop.

New per-job protobuf fields `retry_backoff_ms` (tag 8) and `retry_backoff_max_ms` (tag 9) configure retries. Base zero preserves immediate retries. A zero cap selects `max(60000, base)` milliseconds; an explicit cap must be at least the base. Negative delays/backoff values and overflowing initial timestamps return InvalidArgument.

After attempt N fails, delay is `min(base * 2^(N-1), cap)`. For base 1000 and cap 10000, delays are 1s, 2s, 4s, 8s, then 10s. The same policy applies to expired leases, measured from recovery time. Positive retries enter Delayed; zero-delay retries enter Waiting. Exhausted jobs go straight to Failed history. Arithmetic saturates to avoid overflow; no jitter is applied.

Due times and policies survive restart. Existing jobs default to zero backoff; legacy Waiting jobs remain eligible. These timestamps use the server wall clock, so clock adjustments can change scheduling timing. Workers must regenerate protobuf bindings to set the new fields.

### Dequeue

`GetNextJob` atomically claims at most one `Waiting` job:

1. Require a nonblank `worker_id`; reject blank entries in `queue_names`.
2. Filter by exact job `name` matches. An empty list polls all names; the current contract has no separate queue identifier.
3. Select by ascending numeric priority (lower numbers first), then oldest `created_at`, then ID for deterministic ties.
4. Set the job to `Active`, persist `worker_id` and a 30-second lease expiration, increment `attempts`, and update its timestamp in one immediate transaction.
5. Commit before returning the payload, metadata, and incremented attempt count. The first claim returns `attempts=1`.

When no matching work exists, the response has `found=false`. Active jobs and jobs with future or unknown due times are excluded. Due Waiting and Delayed jobs are eligible. Concurrent polls cannot claim the same Waiting job; database write contention waits up to five seconds before returning an error.

The response includes `lease_expires_at_ms` (Unix epoch milliseconds). Workers must finish or renew before this deadline. A lost dequeue response consumes an attempt; recovery makes the job eligible again if attempts remain.

### Worker leases and recovery

Call `Heartbeat` with `id`, `worker_id`, and the positive `attempt` from dequeue. A successful heartbeat returns the renewed `lease_expires_at_ms`, at least 30 seconds from server time when the transaction obtains its write lock. Send heartbeats approximately every 10 seconds while executing; clients must regenerate protobuf bindings to use this RPC.

For live jobs, heartbeat, completion, and failure require a matching, unexpired Active claim. Expiration is inclusive (`deadline <= server time`). Expired claims return FailedPrecondition even before recovery runs; a heartbeat cannot resurrect them. A stale attempt cannot acknowledge or renew a newer claim, including when the same worker ID is reused. Already committed completions can be replayed as described below. Stop processing when ownership is lost; the broker cannot cancel external side effects already in progress.

The daemon runs recovery immediately on startup and every five seconds thereafter. Each immediate transaction handles up to 100 expired claims. Jobs with attempts remaining are rescheduled using their backoff policy with ownership and lease cleared; exhausted jobs move atomically to Failed history. Recovery records `Worker lease expired`, preserves the attempt count, and logs recovered counts or errors. Failed batches roll back and retry on the next tick. Large backlogs may take multiple ticks to drain.

Lease duration is currently a fixed `LEASE_DURATION_MS = 30_000` in `src/leases.rs`; the recovery interval is five seconds in `src/main.rs`. Runtime configuration is not implemented. Expirations persist across restarts and use the server's wall clock; keep the host clock synchronized. Forward clock jumps can expire work early, and backward jumps can delay recovery.

Delivery follows at-least-once processing semantics within the configured attempt limit and existing storage durability constraints. Recovery can cause duplicate execution, so job handlers must make side effects idempotent. This does not provide exactly-once external execution.

On upgrade, stop old workers before starting this version. The additive migration preserves existing jobs and treats legacy Active jobs without a lease as expired for immediate recovery. Existing unexpired leases are preserved on restart. Older clients that cannot heartbeat must finish within 30 seconds.

### Completion, failure, and retries

`CompleteJob` verifies that the job is Active and owned by the requesting worker, inserts a Completed history record, and deletes the live job in one immediate transaction. History preserves payload, ancestry, priority, attempt counts, creation time, finish time, worker, facet, and the most recent failure message (if any).

`FailJob` performs the same ownership checks and records `error_message`. If `attempts < max_attempts`, it reschedules the job and clears worker ownership; `moved_to_failed_state=false`. Otherwise it atomically moves the job to history as Failed and returns `moved_to_failed_state=true`. Attempts increment only on dequeue. `max_attempts=0` at enqueue defaults to three total attempts. Retries retain original priority/creation time and follow the per-job backoff policy below. Only the latest error is retained, not a per-attempt log.

Workers must send the dequeue response's `attempts` as `attempt` in completion/failure requests. This prevents acknowledgments from an older claim affecting a later claim by the same worker. The new protobuf fields use previously unused tags. For older callers, omitted/zero `attempt` is accepted only on the first attempt; retry-aware clients must regenerate their bindings and send the attempt number.

Acknowledgments return success only after commit. If a completion response is lost, repeating `CompleteJob` with the same job ID, worker ID, and successful attempt returns success from the Completed history record, including after restart. Zero remains an alias for attempt one. Replays do not change history or increment transition/state metrics; RPC latency metrics still count each request. The original lease need not remain valid once completion has committed.

Blank IDs return InvalidArgument, unknown jobs return NotFound, and an incorrect Active-job owner returns PermissionDenied. Non-Active live jobs, stale attempts, and archived records with a different owner, attempt, or outcome return FailedPrecondition without mutation. `FailJob` replays remain rejected. Completion receipts last as long as the history record is retained; legacy records without worker identity cannot prove a match. These ownership checks compare caller-provided identifiers; they are not authentication, and completion idempotency does not deduplicate external handler side effects.

## Known limitations

- Legacy Delayed jobs created before persisted scheduling have unknown due times and remain unscheduled. Inspect and explicitly reschedule them; the migration does not guess their original delay.
- Rate limits support exact facets and fixed windows; wildcard matching and weighted tenant fairness are not implemented.
- The server defaults to loopback. A non-loopback bind requires an explicit `ANVILMQ_ADDR`; transport authentication/TLS are not implemented.
- No Raft replication or global telemetry exists yet.

## Roadmap

### Phase 1: Core plumbing

- [x] Rust project and core protobuf contract.
- [x] Tonic server bootstrap.
- [x] Embedded SQLite connection, WAL, and initial schema.
- [ ] Resolve the libSQL target versus current rusqlite implementation.

### Phase 2: Transactional lifecycle (current focus)

- [x] Atomic enqueue and priority index.
- [x] Atomic Waiting -> Active dequeue with queue filtering and worker ownership.
- [x] Transactional completion/failure, ownership checks, and job history.
- [x] Immediate retries up to the attempt limit and stale-acknowledgment protection.
- [x] Persisted worker leases, heartbeat renewal, and abandoned-job recovery.
- [x] Retry backoff and persisted delayed scheduling.
- [x] Background retention sweep bounding job history and idempotency receipts (age + per-name count), matching the replaced BullMQ deployment's `removeOnComplete`/`removeOnFail` policy.
- [x] Initial TypeScript/Bun client with JSON enqueue, worker heartbeats, graceful draining, and a real gRPC demo/test. Not BullMQ-compatible.

### Phase 3: Safety controls

- [x] Persist metadata and reject supplied execution depth above the limit.
- [ ] Validate ancestry and quarantine runaway chains.
- [ ] Sliding-window ingress velocity controls.

### Phase 4: Faceted rate limiting

- [x] Rule/counter tables and stored job facet.
- [x] Exact-match rule management/usage RPCs and atomic fixed-window enforcement during dequeue.
- [ ] Fairness across tenants/departments.

### Phase 5: Regional high availability

- [ ] Raft consensus and replicated state transitions.
- [ ] Kubernetes configuration, persistent storage, and failure testing.

### Phase 6: Observability

- [x] Atomic lifecycle metrics, RPC latency histograms, and an axum Prometheus endpoint.
- [x] Structured transition logs and health/readiness probes.
- [x] Cached queue-pressure depth/age, claim-wait histograms, and Grafana dashboard/Prometheus alert examples. See [queue-pressure observability](docs/observability.md).
- [ ] Asynchronous regional telemetry aggregation.
- [ ] Latency and throughput benchmarks with documented durability settings.

## Faceted rate limits

`UpsertRateLimitRule(facet_pattern, max_jobs, window_duration_ms)` creates or replaces an exact-match rule. Wildcards (`*`, `?`), blank keys, surrounding whitespace, and nonpositive/overflowing durations are rejected. `max_jobs=0` pauses claims for that facet. **Every upsert resets usage**, including an identical update; do not repeatedly upsert rules as a reconciliation heartbeat.

`DeleteRateLimitRule(facet_key)` removes the rule and counter; repeated deletion returns `deleted=false`. `GetRateLimitStatus(facet_key)` reports rule existence, effective count, limit, duration, window expiration, and throttling. An expired or unstarted window reports count/deadline zero without writing to the database. Missing rules mean unrestricted claims.

Windows start on the first claim and remain fixed until expiry (`expires <= now` resets on the next claim). Quota is shared across all queue names with the same `rate_limit_facet`. Each claim consumes one unit, including retries and recovered jobs. Completion/failure does not refund usage. Counter changes and the claim commit together; failed claims roll back quota consumption. Rules and counters persist across restarts. These are dispatch-rate limits, not concurrency limits or enqueue admission controls.

Dequeue skips throttled candidates and preserves priority/age ordering among eligible jobs; a blocked tenant cannot prevent an eligible different facet from progressing. This is not a round-robin fairness guarantee. If all matching jobs are throttled, polling returns `found=false`; the client continues normal polling. Jobs without a matching rule remain unrestricted. Limits depend on caller-supplied facets and trusted administrative access; they are not an authorization boundary.

The TypeScript client exports `RateLimits`:

```typescript
const limits = new RateLimits();
await limits.upsert("practice:123", 100, 60000);
console.log(await limits.status("practice:123"));
await queue.add(data, { rateLimitFacet: "practice:123" });
// await limits.delete("practice:123");
limits.close();
```

`anvilmq_throttled_polls_total` counts committed polls that encounter at least one due, queue-matching throttled job, even if another job is dispatched. It has no tenant/facet labels and does not count rejected jobs individually. The three administrative RPCs also have bounded latency labels.

## Retention

Terminal jobs are copied into `job_history` and keyed enqueues leave dedup records in `enqueue_receipts`. Both are pruned by a background sweeper so on-disk state reaches a steady size instead of growing without bound. Defaults mirror the BullMQ deployment this replaces (`removeOnComplete { age: 24h, count: 1000 }`, `removeOnFail { age: 7d }`).

- Completed jobs older than `ANVILMQ_RETENTION_COMPLETED_AGE_MS` (default `86400000`, 24h) are deleted; `0` disables age pruning.
- At most `ANVILMQ_RETENTION_COMPLETED_COUNT` completed jobs are kept per job name, newest first (default `1000`); `0` disables count pruning.
- Failed jobs older than `ANVILMQ_RETENTION_FAILED_AGE_MS` (default `604800000`, 7d) are deleted; `ANVILMQ_RETENTION_FAILED_COUNT` (default `0`, disabled) bounds retained failures per name.
- The sweep runs every `ANVILMQ_RETENTION_INTERVAL_MS` (default `60000`) and deletes at most `ANVILMQ_RETENTION_BATCH` rows per statement (default `1000`), draining backlogs across ticks so the shared writer is never held for long.
- An idempotency receipt is removed once its job is gone from both the live and history tables, so the dedup window tracks job retention exactly.
- Age values below a safety floor (`2 x` the 30s lease) are rejected at startup so a terminal job cannot be pruned while a duplicate lifecycle RPC is still replaying against history. Set every dimension to `0` to disable the sweeper entirely.

Space is reclaimed by SQLite page reuse at steady state; the sweeper does not run `VACUUM`, which would lock the writer. The `anvilmq_jobs{state}` gauges for terminal states flatten once retention keeps pace with completion throughput.

### Tuning batch and interval

The sweeper shares the single writer connection with every enqueue, claim, and complete, so a sweep tick briefly serializes against the hot path. Two knobs govern the trade-off:

- **Drain capacity** is roughly `ANVILMQ_RETENTION_BATCH / (ANVILMQ_RETENTION_INTERVAL_MS / 1000)` rows per second. If sustained completion throughput exceeds this, terminal history keeps growing even with the sweeper running — the bounded batch protects the writer but caps how fast the backlog drains. Size capacity to comfortably exceed peak completion rate. For example, the defaults (`1000` per `60000` ms) sustain ~16 completions/sec; a broker retiring 100 jobs/sec needs a larger batch or shorter interval.
- **Latency profile** is set by batch size. Each tick deletes up to `ANVILMQ_RETENTION_BATCH` rows inside one transaction while holding the writer, so a large batch drains faster but produces deeper, spikier stalls on concurrent enqueue/claim latency. Smaller batches run more often on a shorter interval give the same drain capacity with smoother latency. Prefer a batch sized just above peak completion throughput over an oversized one.

The `idx_history_state_finished` and `idx_history_name_state_finished` indexes keep each delete cheap (shorter holds), at the cost of minor index maintenance on the completion write path. Deletes run on the blocking thread pool, so they never starve the async runtime — the writer lock is the only contention point.

## Observability

The HTTP listener defaults to `127.0.0.1:9090`; override with `ANVILMQ_HTTP_ADDR`. A bind failure stops startup. These endpoints are unauthenticated; expose them only on a trusted network.

- `GET /metrics`: Prometheus text exposition from in-memory atomics; no database access or storage locks during scrapes.
- `GET /healthz`: HTTP 200 while the HTTP server is responsive; no database dependency.
- `GET /readyz`: HTTP 200 after opening a database write transaction, reading the jobs table, and rolling back. Returns 503 for contention, database errors, or a one-second timeout. It deliberately fails fast if the shared connection is busy; use a failure threshold for deployment probes. A timed-out SQLite call can continue on its blocking thread, with subsequent probes failing fast until it releases the connection. This is an access check, not a disk durability or capacity test.

```powershell
Invoke-WebRequest http://127.0.0.1:9090/healthz
Invoke-WebRequest http://127.0.0.1:9090/readyz
(Invoke-WebRequest http://127.0.0.1:9090/metrics).Content
```

Metrics have only fixed state, event, method, and histogram-bound labels:

| Metric | Meaning |
| --- | --- |
| `anvilmq_jobs{state}` | Current Waiting, Delayed, Active, Completed, Failed counts; terminal states count retained history |
| `anvilmq_transitions_total{event}` | Committed enqueued, claimed, completed, failed, retried, and lease_expired events since startup |
| `anvilmq_rpc_duration_seconds{method}` | Histogram with `_bucket`, `_sum`, `_count`; handler latency includes validation failures and database waits, excludes network transport |
| `anvilmq_recovery_errors_total` | Recovery batches that failed and will be retried |
| `anvilmq_retention_deleted_total{target,reason}` | History/receipt rows pruned by the retention sweeper; `target` is completed, failed, or receipt and `reason` is age, count, or orphan |
| `anvilmq_retention_errors_total` | Retention sweep batches that failed and will be retried |
| `anvilmq_enqueue_replays_total` | Matching keyed enqueue retries since startup; rising rates can indicate lost responses or producer retry pressure |
| `anvilmq_enqueue_conflicts_total` | Key reuse with different request contents since startup; investigate producer identity/serialization mistakes |
| `anvilmq_enqueue_receipts` | Retained idempotency records, initialized from storage; monitor alongside PVC usage |

The `failed` event counts accepted FailJob calls, including those scheduled for retry. Lease expiry has its own event and increments `retried` when attempts remain. State gauges initialize from jobs/history at startup; cumulative counters reset on restart. Lifecycle/receipt counts update after successful commits while the writer lock is held; conflict counters count rejected requests without mutations. Scrapes may see brief intermediate values across independent atomics and are not a transactional snapshot. Counts assume this daemon owns database writes; external SQL changes or a second process writing the same database are not reflected automatically. Idempotency metrics have no queue, key, or job-ID labels; replay/conflict logs identify the original job ID without logging keys or payloads.

JSON logs include committed transitions with job ID, attempt, source/destination state, and worker ID for claims and acknowledgments. Payloads and arbitrary error messages are not logged in transition events. Set `RUST_LOG` (default `info`) to control verbosity. The background log queue is bounded and may drop logs under sustained overload; logs are diagnostic, not an audit record.

Example Prometheus scrape configuration:

```yaml
scrape_configs:
  - job_name: anvilmq
    scrape_interval: 15s
    static_configs:
      - targets: ["127.0.0.1:9090"]
```

The client integration test checks health/readiness and confirms five completed jobs, zero Active jobs, two retries, and one expired lease after its success/backoff/crash-recovery flow.

## Local development

Prerequisites: Rust/Cargo with Edition 2021 support, `protoc` on PATH (or `PROTOC` set to its executable), and native build tools for bundled SQLite. Windows MSVC builds require the Visual Studio C++ build tools and Windows SDK.

```powershell
rustc --version
cargo --version
protoc --version
cargo build
cargo test
cargo run
```

On Windows, the protobuf compiler can be installed with:

```powershell
winget install --id Google.Protobuf --exact --source winget
```

Restart the terminal after installation to refresh PATH. `cargo fmt --check` can be used when the Rust formatting component is installed.

The daemon defaults to `[::1]:50051` and `anvil.db` in the working directory. Override these with `ANVILMQ_ADDR` and `ANVILMQ_DB_PATH`. SQLite also creates WAL/SHM sidecar files. Tests use isolated databases, not the local daemon database.

### TypeScript client and demo

See [client/README.md](client/README.md) for the API, shutdown behavior, and crash-recovery demo. After building the daemon:

```powershell
cd client
bun install --frozen-lockfile
bun run check
bun run test
```

The integration test launches its own daemon and worker processes and takes roughly 40 seconds. For an interactive demo, run `cargo run` from the repository root, then `bun run demo worker` and `bun run demo seed` in separate terminals in `client`.

The package/binary name is currently `rusty-queue`. Source files are `src/main.rs` (RPC handlers and server), `src/db.rs` (connection/schema), and `proto/queue.proto` (wire contract).

### Container reliability harness

Run `./harness/smoke.ps1` to build the Linux images and verify lifecycle behavior plus broker process-crash persistence in an isolated Compose deployment. See [harness/README.md](harness/README.md) for prerequisites, ports, JSON reports, and cleanup. This first slice is a smoke/restart check, not a performance or power-loss benchmark.

Run `./harness/load.ps1 -Producers 4 -Workers 4 -DurationSeconds 30` for a configurable container load scenario. Timestamped reports and `harness/artifacts/latest.md` show throughput, p50/p95/p99 latency, errors, and drain time. See the harness documentation for pacing, payload options, and measurement limits.

Run `./harness/crash-active.ps1` to kill the broker with active claims and concurrent producers, restart on the same volume, and verify acknowledged-job recovery plus stale-attempt fencing. Reports separate expected redeliveries from ambiguous enqueue outcomes; see the harness documentation for scope and timing details.

See [the initial local performance baseline](harness/BASELINE.md) for the first measured throughput and latency results.

Raft is evaluated against pod/node failure tolerance and failover time, independently of single-node throughput. See [availability requirements](docs/availability.md).
