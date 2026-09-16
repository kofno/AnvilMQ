# AnvilMQ TypeScript client (Bun)

This is a repo-local, private client using the canonical `../proto/queue.proto` at runtime. It is a BullMQ-style starting point, not a compatible replacement or a published package. Bun is required for the demo/test scripts. Transport is plaintext gRPC for local development; authentication and TLS configuration are not implemented.

## Setup and validation

From the repository root:

```powershell
cargo build
cd client
bun install --frozen-lockfile
bun run check
bun run test
```

The integration test starts an isolated daemon on a temporary port/database. It takes roughly 40 seconds because it exercises real lease expiry, kills a worker process, and keeps another handler alive beyond the 30-second lease while draining. It does not touch `anvil.db`.

## API

```typescript
import { Queue, Worker } from "./src/index";

const queue = new Queue<{ recipient: string }>("email");
await queue.add({ recipient: "user@example.com" }, {
  delayMs: 1000,
  maxAttempts: 3,
  retryBackoffMs: 1000,
  retryBackoffMaxMs: 10000,
});
queue.close();

const worker = new Worker<{ recipient: string }>("email", async (job, signal) => {
  // Use job.id as an idempotency key and pass signal to cancellable operations.
  // Throw to fail this attempt. Return normally to complete it.
  console.log(job.data.recipient, job.id, job.attempts);
}, { onError: error => console.error(error) });

process.once("SIGINT", () => { void worker.close(); });
```

`Queue(name).add(data, options)` serializes JSON; the queue name maps to the protocol's `name`. There is no separate job-type field. Put a `kind` in your payload when needed. Options also include `priority`, `metadata`, and `rateLimitFacet`. Generic types provide compile-time help, not runtime payload validation.

Both constructors accept `address` (default `[::1]:50051`) and `rpcTimeoutMs` (default 5000). Workers accept `workerId` (default a fresh UUID), `pollIntervalMs` (250), `heartbeatIntervalMs` (10000, maximum 10000), and `onError`. Each worker processes one job at a time; create additional workers for concurrency.

Workers poll immediately, send heartbeats during async processing, and acknowledge with the claimed attempt number. Handler errors, including JSON decoding errors, invoke FailJob. If heartbeat fails or times out, the handler's signal aborts and the client sends no acknowledgment. Handlers must honor cancellation; the client cannot undo external side effects. Avoid blocking the event loop, which prevents heartbeats.

`await worker.close()` stops polling and drains an in-flight claim, including a claim returned while shutdown begins. Heartbeats continue until the handler settles. A handler that never settles will prevent graceful shutdown; force-killing the process leaves recovery to the server. No automatic RPC retries are performed for enqueue/acknowledgment because a timeout may follow a successful commit. Errors are reported through `onError`; polling errors retry after the poll interval. A completion RPC error is never converted into a failure acknowledgment.

Delivery is at least once within the server's attempt and durability limits. Side effects must tolerate duplicates. The `leaseExpiresAtMs` on the job is the initial claim deadline; the worker renews it internally.

## Interactive demo

Use three terminals:

```powershell
# Terminal 1, repository root
cargo run
```

```powershell
# Terminal 2, client directory
bun run demo worker
```

```powershell
# Terminal 3, client directory
bun run demo seed
```

The seed adds success, retry (fails twice), delayed, and long-running jobs. Watch attempt numbers and handler output. Press Ctrl+C in the worker for graceful draining. To demonstrate crash recovery, force-kill the worker process after `START ... long`, then launch `bun run demo worker` again. After the lease expires and recovery runs, the same job ID is delivered on a later attempt.

`HANDLER DONE` means the handler returned; it is printed before the completion RPC and is not a durable acknowledgment. RPC failures are logged separately.

For a separate daemon, set `ANVILMQ_ADDR` in both terminals and optionally `ANVILMQ_DB_PATH` in the server terminal. Defaults remain unchanged.

## Exact-facet rate limits

```typescript
import { RateLimits } from "./src/index";
const limits = new RateLimits();
await limits.upsert("practice:123", 10, 60000); // ten claims per fixed window
console.log(await limits.status("practice:123"));
await queue.add(data, { rateLimitFacet: "practice:123" });
await limits.delete("practice:123");
limits.close();
```

Each upsert resets the facet's usage window; zero quota pauses dispatch. Limits are shared across queue names and count retries as new claims. Wildcards are not supported. Management RPCs assume trusted-network access. Workers continue polling when all eligible work is throttled.

`WorkerOptions.onCompleted(id)` is called only after a successful completion acknowledgment. The load harness uses it to measure end-to-end latency; handler return alone is not counted as completion. Keep this callback synchronous and lightweight.
