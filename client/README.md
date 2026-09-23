# AnvilMQ TypeScript client (Bun)

This is a repo-local, private client using the canonical `../proto/queue.proto` at runtime. It is a minimal starting point, not a compatible replacement for any existing client or a published package. Bun is required for the demo/test scripts. Transport is plaintext gRPC for local development; authentication and TLS configuration are not implemented.

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

It also runs a test-only gRPC proxy that forwards completion to the real broker, then discards the committed response. Both injected `Unavailable` and a silent response loss causing `DeadlineExceeded` must recover with the same completion token and one handler invocation. Further cases verify three-call retry exhaustion, no retry on permission errors, no FailJob fallback, no redelivery of completed jobs, and exactly one completion transition despite repeated successful RPCs. The broker has no production fault-injection switch.

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

`await worker.close()` stops polling and drains an in-flight claim, including a claim returned while shutdown begins. Heartbeats continue until the handler settles. A handler that never settles will prevent graceful shutdown; force-killing the process leaves recovery to the server.

Completion retries use the exact same job/worker/attempt token: up to three total RPC calls for `Unavailable` or `DeadlineExceeded`, with 100ms then 200ms waits. Each call has `rpcTimeoutMs`; shutdown waits for this bounded completion sequence (about 15.3 seconds at the default timeout). Other status codes are not retried. The handler is not rerun, and `onCompleted` fires once only after a successful acknowledgment. Intermediate retryable errors are suppressed; terminal/exhausted errors reach `onError`. Exhaustion still leaves the completion outcome uncertain. Heartbeats stop when the handler settles; an uncommitted completion that outlives its lease is rejected by the broker.

Enqueue without a key and FailJob are not automatically retried. Keyed enqueue uses the bounded retry policy below. Polling errors retry after the poll interval. A completion RPC error is never converted into a failure acknowledgment. Deploy the broker's idempotent completion support before this client: an older broker may reject an otherwise successful replay.

## Long-running jobs

A job keeps its lease for as long as it needs — minutes, if necessary — as long as heartbeats keep reaching the broker before each 30-second lease lapses. The `Worker` sends those heartbeats automatically on a background task; you never call `heartbeat` yourself. Your processor only has to cooperate in two ways.

**Rule 1 — Do not block the event loop.** Heartbeats share your process's event loop. A synchronous, CPU-bound stretch (rendering a large PDF, a tight loop with no `await`) starves the heartbeat task; the lease lapses at 30 seconds; server recovery requeues the job and you get a duplicate run. Break work into chunks with `await` between them, or offload heavy compute to a `worker_threads` Worker or subprocess and `await` the result.

**Rule 2 — Honor the `AbortSignal`.** If a heartbeat fails (broker unreachable, or the job was already recovered and handed to another worker), the `Worker` aborts the `signal` passed to your processor. Check `signal.aborted` at chunk boundaries and thread `signal` into abortable I/O so you stop promptly; otherwise you keep doing orphaned work that races the requeued copy. The client cannot undo external side effects, so make handlers idempotent — delivery is at least once.

The helpers below (`fetchReportRows`, `renderReportOffThread`, `sendEmailIdempotent`) are illustrative placeholders for your own code.

```ts
import { Worker } from "./src/index";

// Email report generation: gather data in pages, render, then send. May take minutes.
const worker = new Worker<{ reportId: string; recipient: string }>(
  "email-report",
  async (job, signal) => {
    const { reportId, recipient } = job.data;

    // Bail out immediately if we've lost the lease — don't do orphaned work.
    const ensureLeased = () => {
      if (signal.aborted) throw new Error("lease lost; abandoning to avoid duplicate work");
    };

    // 1) Page through source data. Each `await` yields the event loop, so the
    //    background heartbeat keeps renewing the 30-second lease while we work.
    const rows: ReportRow[] = [];
    for (let page = 0; ; page++) {
      ensureLeased();
      const batch = await fetchReportRows(reportId, page, { signal }); // pass signal to abortable I/O
      if (batch.length === 0) break;
      rows.push(...batch);
    }

    // 2) CPU-heavy render. Offload to a worker thread so we never block the
    //    event loop (which would starve heartbeats). Chunk it if you render inline.
    ensureLeased();
    const pdf = await renderReportOffThread(rows, { signal });

    // 3) Deliver. An idempotent send keyed by reportId tolerates at-least-once retries.
    ensureLeased();
    await sendEmailIdempotent(recipient, pdf, { idempotencyKey: reportId, signal });
  },
  { heartbeatIntervalMs: 10_000 }, // default; the client caps this at 10s, safely under the 30s lease
);

process.once("SIGINT", () => void worker.close()); // graceful drain; heartbeats continue until the handler settles
```

Avoid this anti-pattern:

```ts
// Starves heartbeats: a synchronous loop that never yields.
async (job, signal) => {
  for (const row of hugeArray) {
    renderRowSync(row); // CPU-bound, no await -> event loop blocked
  }
  // ~30 seconds in, the lease is already gone and the job is being retried elsewhere.
};
```

On success the client acknowledges with `CompleteJob` (idempotent, same attempt token); on a thrown error it sends `FailJob` (retried per `maxAttempts` and backoff); if the handler was aborted, it sends nothing and lets server recovery requeue the job.

## Built-in parentage

Ancestry is automatic: to enqueue a child of the job you are processing, call
`job.enqueueChild(...)`. You declare only the child's queue name, payload, and ordinary
options — the server owns the lineage facts. It forces the child's `parentId` to the
current job's id, derives `executionDepth` (parent depth + 1), and inherits the lineage
`traceId`. Those lineage fields cannot be set through `enqueueChild`; a caller-supplied
`parentId` is ignored. The call reuses the worker's existing gRPC channel, so a fan-out
does not open a connection per child.

```typescript
const worker = new Worker<{ orderId: string }>("orders", async job => {
  // Kick off a child job on any queue; parentage is filled in server-side.
  await job.enqueueChild("shipments", { orderId: job.data.orderId });

  // Ordinary options (priority, delayMs, maxAttempts, backoff, rateLimitFacet,
  // idempotencyKey) are accepted; lineage fields are not.
  await job.enqueueChild(
    "receipts",
    { orderId: job.data.orderId },
    { idempotencyKey: `receipt:${job.data.orderId}` },
  );
});
```

The recursion circuit breaker still applies to the derived depth: a child whose lineage
would exceed the broker's maximum execution depth is rejected with `RESOURCE_EXHAUSTED`.

## Safe enqueue retries

```typescript
// Create/store this key once per intended operation, outside the retry loop.
const result = await queue.add(
  { invoiceId: "123" },
  { idempotencyKey: "tenant-a:generate-invoice:123:v1" }
);
console.log(result.id, result.replayed);
```

Keys are scoped to the queue name. Matching requests return one job ID; conflicting payload/options return `AlreadyExists` without retry. The client accepts nonblank keys up to 256 UTF-8 bytes. Omit the option to retain ordinary enqueue behavior. The reply is an enqueue receipt, not a status query: a matching replay returns the job's ORIGINAL enqueue state (`Waiting`, or `Delayed` if a delay was set) even after the job has run. Treat `result.replayed === true` as "nothing new was created" and use `result.id` to look up live progress separately. See [Idempotent enqueue](../README.md#idempotent-enqueue) for the full model.

Only keyed enqueue automatically retries `Unavailable` and `DeadlineExceeded`: three calls maximum, 100ms then 200ms waits, each with `rpcTimeoutMs`. Request data is serialized and options copied once before retrying. After exhaustion, the outcome is still uncertain; retain the same key and original request for a later retry. Changing the key could create duplicate work. This is not a durable producer buffer: use an outbox if submissions must survive producer-process loss before acknowledgment.

Receipts have no standalone TTL and are reclaimed only once the job is gone from both the live and history tables, so the dedup window tracks job retention. The producer should reuse a stable business-operation ID or persist a generated UUID before its first request. Preserve payload serialization and options across restarts; JSON property ordering is significant. New intended work needs a new key. This prevents duplicate insertion, not repeated handler side effects.

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
