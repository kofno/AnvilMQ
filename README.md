# AnvilMQ

AnvilMQ is an early-stage Rust queue engine intended for autonomous regional Kubernetes deployments. The current implementation is a single-process gRPC server with embedded SQLite persistence, atomic enqueue/dequeue, and a caller-supplied execution-depth guard. Sub-millisecond latency, high availability, and tenant rate limiting are goals, not verified capabilities.

## Current architecture

- Rust Edition 2021, Tokio, and Tonic/Protocol Buffers.
- Bundled SQLite through `rusqlite`. The target architecture in AGENTS.md specifies libSQL; that migration remains open.
- Database work runs in `spawn_blocking`, with one shared connection protected by a mutex.
- WAL mode with `synchronous=NORMAL`: application-crash recovery is supported by SQLite, but acknowledged writes can be lost after an OS crash or power failure. Stronger durability requires a deliberate configuration change and performance validation.
- Initial schema creation and additive ownership/error-history migrations run in a transaction. Existing jobs are preserved.

## Implemented behavior

### Enqueue

`AddJob` persists a UUID, payload, priority, ancestry metadata, attempts limit, timestamps, and optional rate-limit facet. Missing trace IDs are generated. Execution depth greater than 10 is rejected before persistence; ancestry is not verified against a parent record. A single INSERT provides atomic enqueue.

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

Heartbeat, completion, and failure require a matching, unexpired Active claim. Expiration is inclusive (`deadline <= server time`). Expired claims return FailedPrecondition even before recovery runs; a heartbeat cannot resurrect them. A stale attempt cannot acknowledge or renew a newer claim, including when the same worker ID is reused. Stop processing when ownership is lost; the broker cannot cancel external side effects already in progress.

The daemon runs recovery immediately on startup and every five seconds thereafter. Each immediate transaction handles up to 100 expired claims. Jobs with attempts remaining are rescheduled using their backoff policy with ownership and lease cleared; exhausted jobs move atomically to Failed history. Recovery records `Worker lease expired`, preserves the attempt count, and logs recovered counts or errors. Failed batches roll back and retry on the next tick. Large backlogs may take multiple ticks to drain.

Lease duration is currently a fixed `LEASE_DURATION_MS = 30_000` in `src/leases.rs`; the recovery interval is five seconds in `src/main.rs`. Runtime configuration is not implemented. Expirations persist across restarts and use the server's wall clock; keep the host clock synchronized. Forward clock jumps can expire work early, and backward jumps can delay recovery.

Delivery follows at-least-once processing semantics within the configured attempt limit and existing storage durability constraints. Recovery can cause duplicate execution, so job handlers must make side effects idempotent. This does not provide exactly-once external execution.

On upgrade, stop old workers before starting this version. The additive migration preserves existing jobs and treats legacy Active jobs without a lease as expired for immediate recovery. Existing unexpired leases are preserved on restart. Older clients that cannot heartbeat must finish within 30 seconds.

### Completion, failure, and retries

`CompleteJob` verifies that the job is Active and owned by the requesting worker, inserts a Completed history record, and deletes the live job in one immediate transaction. History preserves payload, ancestry, priority, attempt counts, creation time, finish time, worker, facet, and the most recent failure message (if any).

`FailJob` performs the same ownership checks and records `error_message`. If `attempts < max_attempts`, it reschedules the job and clears worker ownership; `moved_to_failed_state=false`. Otherwise it atomically moves the job to history as Failed and returns `moved_to_failed_state=true`. Attempts increment only on dequeue. `max_attempts=0` at enqueue defaults to three total attempts. Retries retain original priority/creation time and follow the per-job backoff policy below. Only the latest error is retained, not a per-attempt log.

Workers must send the dequeue response's `attempts` as `attempt` in completion/failure requests. This prevents acknowledgments from an older claim affecting a later claim by the same worker. The new protobuf fields use previously unused tags. For older callers, omitted/zero `attempt` is accepted only on the first attempt; retry-aware clients must regenerate their bindings and send the attempt number.

Acknowledgments return success only after commit. Blank IDs return InvalidArgument, unknown jobs return NotFound, and an incorrect Active-job owner returns PermissionDenied. Non-Active jobs, stale attempts, and already archived jobs return FailedPrecondition without mutation. Duplicate terminal acknowledgments are rejected rather than replayed as success. These ownership checks compare caller-provided identifiers; they are not authentication.

## Known limitations

- Legacy Delayed jobs created before persisted scheduling have unknown due times and remain unscheduled. Inspect and explicitly reschedule them; the migration does not guess their original delay.
- Rate-limit tables and facets exist, but enforcement and management RPCs do not.
- The server binds to loopback, so it is not yet reachable through a normal Kubernetes Service.
- No Raft replication, Prometheus endpoint, global telemetry, or TypeScript/Bun client exists yet.

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
- [ ] TypeScript/Bun client with a BullMQ-style API.

### Phase 3: Safety controls

- [x] Persist metadata and reject supplied execution depth above the limit.
- [ ] Validate ancestry and quarantine runaway chains.
- [ ] Sliding-window ingress velocity controls.

### Phase 4: Faceted rate limiting

- [x] Rule/counter tables and stored job facet.
- [ ] Management RPCs and atomic enforcement during dequeue.
- [ ] Fairness across tenants/departments.

### Phase 5: Regional high availability

- [ ] Raft consensus and replicated state transitions.
- [ ] Kubernetes configuration, persistent storage, and failure testing.

### Phase 6: Observability

- [ ] Atomic counters and an axum Prometheus endpoint.
- [ ] Asynchronous regional telemetry aggregation.
- [ ] Latency and throughput benchmarks with documented durability settings.

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

The daemon listens on `[::1]:50051` and opens `anvil.db` in the working directory. SQLite also creates WAL/SHM sidecar files. Tests use isolated databases, not the local daemon database.

The package/binary name is currently `rusty-queue`. Source files are `src/main.rs` (RPC handlers and server), `src/db.rs` (connection/schema), and `proto/queue.proto` (wire contract).
