# AnvilMQ

AnvilMQ is an early-stage Rust queue engine intended for autonomous regional Kubernetes deployments. The current implementation is a single-process gRPC server with embedded SQLite persistence, atomic enqueue/dequeue, and a caller-supplied execution-depth guard. Sub-millisecond latency, high availability, and tenant rate limiting are goals, not verified capabilities.

## Current architecture

- Rust Edition 2021, Tokio, and Tonic/Protocol Buffers.
- Bundled SQLite through `rusqlite`. The target architecture in AGENTS.md specifies libSQL; that migration remains open.
- Database work runs in `spawn_blocking`, with one shared connection protected by a mutex.
- WAL mode with `synchronous=NORMAL`: application-crash recovery is supported by SQLite, but acknowledged writes can be lost after an OS crash or power failure. Stronger durability requires a deliberate configuration change and performance validation.
- Initial schema creation and additive worker-ownership migration run in a transaction. Existing jobs are preserved.

## Implemented behavior

### Enqueue

`AddJob` persists a UUID, payload, priority, ancestry metadata, attempts limit, timestamps, and optional rate-limit facet. Missing trace IDs are generated. Execution depth greater than 10 is rejected before persistence; ancestry is not verified against a parent record. A single INSERT provides atomic enqueue.

### Dequeue

`GetNextJob` atomically claims at most one `Waiting` job:

1. Require a nonblank `worker_id`; reject blank entries in `queue_names`.
2. Filter by exact job `name` matches. An empty list polls all names; the current contract has no separate queue identifier.
3. Select by ascending numeric priority (lower numbers first), then oldest `created_at`, then ID for deterministic ties.
4. Set the job to `Active`, persist `worker_id`, increment `attempts`, and update its timestamp in one immediate transaction.
5. Commit before returning the payload, metadata, and incremented attempt count. The first claim returns `attempts=1`.

When no matching work exists, the response has `found=false`. Active and Delayed jobs are excluded. Concurrent polls cannot claim the same Waiting job; database write contention waits up to five seconds before returning an error.

This is a claim operation, not a complete delivery guarantee. There are no leases, heartbeats, or abandoned-job recovery yet. If a worker crashes or the response is lost after commit, the job remains Active.

## Known limitations

- `CompleteJob` and `FailJob` are stubs: they currently return success without changing state or writing history. Do not use those responses as evidence that a job finished.
- Positive `delay_ms` creates a Delayed job, but its due time is not persisted and there is no scheduler. Such jobs are never dequeued. Use `delay_ms=0` for the implemented flow.
- Retry limits are stored but retries and exhausted-attempt handling are not implemented.
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
- [ ] Transactional completion/failure and job history.
- [ ] Worker leases, abandoned-job recovery, retries, and delayed scheduling.
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
