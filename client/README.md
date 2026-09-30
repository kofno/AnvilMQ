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

`Queue(name).add(data, options)` serializes JSON; the queue name maps to the protocol's `name`. There is no separate job-type field. Put a `kind` in your payload when needed. Options also include `priority`, `metadata`, `rateLimitFacet`, and the durability knobs `idempotencyKey`, `enqueueRetryMaxMs`, `enqueueRetryBaseMs`, and `noIdempotencyKey` (see [Producer durability](#producer-durability)). `Queue.addBulk(items, bulkOpts?)` fans out to one `add` per item with bounded concurrency. Generic types provide compile-time help, not runtime payload validation.

Both constructors accept `address` (default `[::1]:50051`) and `rpcTimeoutMs` (default 5000). They also accept `keepalive` to tune HTTP/2 transport liveness (see [Transport resilience](#transport-resilience)). Workers accept `workerId` (default a fresh UUID), `pollIntervalMs` (250), `heartbeatIntervalMs` (10000, maximum 10000), `onError`, the resilience options `pollBackoffMaxMs`, `connectionLostThreshold`, `onConnectionLost`, and `onConnectionRestored`, plus the health options `livenessStaleMs` and `readinessStaleMs` (see [Kubernetes health](#kubernetes-health)). Each worker processes one job at a time; create additional workers for concurrency.

Workers poll immediately, send heartbeats during async processing, and acknowledge with the claimed attempt number. Handler errors, including JSON decoding errors, invoke FailJob. If heartbeat fails or times out, the handler's signal aborts and the client sends no acknowledgment. Handlers must honor cancellation; the client cannot undo external side effects. Avoid blocking the event loop, which prevents heartbeats.

`await worker.close()` stops polling and drains an in-flight claim, including a claim returned while shutdown begins. Heartbeats continue until the handler settles. A handler that never settles will prevent graceful shutdown; force-killing the process leaves recovery to the server.

Completion retries use the exact same job/worker/attempt token: up to three total RPC calls for `Unavailable` or `DeadlineExceeded`, with 100ms then 200ms waits. Each call has `rpcTimeoutMs`; shutdown waits for this bounded completion sequence (about 15.3 seconds at the default timeout). Other status codes are not retried. The handler is not rerun, and `onCompleted` fires once only after a successful acknowledgment. Intermediate retryable errors are suppressed; terminal/exhausted errors reach `onError`. Exhaustion still leaves the completion outcome uncertain. Heartbeats stop when the handler settles; an uncommitted completion that outlives its lease is rejected by the broker.

Every enqueue (`add`, `addBulk` items, and `enqueueChild`) uses the bounded, auto-keyed retry policy in [Producer durability](#producer-durability); FailJob is not automatically retried. Polling errors retry with exponential backoff (see [Transport resilience](#transport-resilience)). A completion RPC error is never converted into a failure acknowledgment. Deploy the broker's idempotent completion support before this client: an older broker may reject an otherwise successful replay.

## Transport resilience

The client keeps the gRPC channel healthy at the transport layer and reacts to broker outages with bounded backoff and explicit connection events. Together these remove the need for an external connection-monitor sidecar, an application-level active-PING loop, or a scheduled worker restart to recover from a half-open channel — the client detects and recovers on its own.

### HTTP/2 keepalive

Every connection is built with HTTP/2 keepalive so a half-open channel (an idle load-balancer timeout, a silently dead peer) is detected by a missed keepalive PING instead of only surfacing when a real RPC finally hits its per-call deadline. Tune it via `ConnectionOptions.keepalive` on any `Queue`, `Worker`, `RateLimits`, or `IngressLimits`:

| Option | Default | Meaning |
| --- | --- | --- |
| `timeMs` | `20000` | Interval between keepalive PINGs on an otherwise idle channel. |
| `timeoutMs` | `10000` | How long to wait for a PING ack before dropping the channel. |
| `permitWithoutCalls` | `true` | Keep sending PINGs even when no RPCs are in flight. |
| `maxReconnectBackoffMs` | `10000` | Upper bound on the transport's own reconnect backoff. |

Each supplied numeric is validated as a positive finite number, like `rpcTimeoutMs`.

### Poll-loop backoff

A `Worker` no longer re-polls on a fixed interval during an outage. A poll that **returns** (a claim found, or an empty result) proves the broker is reachable and resets the worker's failure state. A poll that **throws** is a failure: the worker waits an exponentially growing delay — `pollIntervalMs`, doubling each consecutive failure, capped at `pollBackoffMaxMs` (default 5000; must be `>= pollIntervalMs`) — jittered to a random 50–100% of that delay so a fleet does not thunder back in lockstep when the broker restarts. This replaces the previous fixed retry storm. Heartbeat and completion failures are deliberately *not* folded into this signal; the poll loop is the single source of connection liveness. `close()` interrupts an in-progress backoff sleep and returns promptly.

### Connection events

After `connectionLostThreshold` (default 3) consecutive poll failures, the worker fires `onConnectionLost` exactly once; when polling next succeeds it fires `onConnectionRestored` exactly once with how long it was down. A transient blip below the threshold emits nothing.

```typescript
const worker = new Worker("emails", handler, {
  pollIntervalMs: 250,
  pollBackoffMaxMs: 5000,       // cap the backoff during an outage
  connectionLostThreshold: 3,   // consecutive poll failures before "lost"
  onError: () => {},            // per-attempt errors (silenced here)
  onConnectionLost: ({ consecutiveFailures }) =>
    console.warn(`broker unreachable after ${consecutiveFailures} polls`),
  onConnectionRestored: ({ downForMs, failuresWhileDown }) =>
    console.info(`broker back after ${downForMs}ms and ${failuresWhileDown} failed polls`),
});
```

Both callbacks are invoked defensively: a throw inside one is routed to `onError` and never breaks the poll loop.

## Kubernetes health

A `Worker` exposes a synchronous, I/O-free health snapshot designed for Kubernetes probes. It draws a hard line between two questions:

- **Liveness — "my loop is turning."** `worker.health().live` fails only when the worker is genuinely wedged (a blocked event loop, or a dead poll task). A broker outage must **not** fail liveness: restarting a pod cannot fix a down broker, and an outage-driven restart loop is exactly the failure this surface removes. Wire it to `livenessProbe -> /livez`.
- **Readiness — "I can reach the broker and do work."** `worker.health().ready` fails during a broker outage, before the first successful poll (a fresh pod is gated until it reaches the broker once), and during graceful shutdown. Wire it to `readinessProbe -> /readyz` so traffic and work are routed around a worker that cannot reach the broker.

`worker.health()` reads in-memory fields plus one `Date.now()`; it issues no RPC and never awaits, so a probe does zero I/O.

### The staleness subtlety

`lastPollOkAt` advances on every reachable poll, but the poll loop is suspended inside your handler for the whole duration of a long job, so it stops advancing while a job runs. A naive "no poll in N seconds => unhealthy" would therefore flap NOT-ready during every long job. The surface uses two separate clocks instead:

- **`lastBrokerContactAt`** — refreshed on any successful broker round-trip: a poll, a heartbeat, or a completion. During a long job the automatic heartbeats keep contact fresh, so readiness holds. Readiness staleness reads this clock.
- **`loopAliveAt`** — refreshed at the top of every poll-loop iteration and on every heartbeat tick. A long async job keeps it fresh via heartbeat ticks; a blocked event loop stops both poll iterations and timer callbacks, so it goes stale and liveness correctly fails.

Only **successful** round-trips refresh these clocks. Heartbeat and completion **failures** are never folded into connection health (that remains the poll loop's job, exactly as in [Transport resilience](#transport-resilience)); this surface never emits connection events and never changes `connected`.

### `WorkerHealth` fields

| Field | Meaning |
| --- | --- |
| `live` | Liveness: the poll loop is turning. |
| `ready` | `live && connected && !stale && !stopping && lastPollOkAt > 0`. |
| `connected` | Poll-loop connection state (from Transport resilience). |
| `stale` | Broker-contact staleness exceeded `readinessStaleMs`. |
| `stopping` | `close()` has begun. |
| `consecutivePollFailures` | Consecutive poll-failure streak (0 while reachable). |
| `inFlight` | Jobs currently processing (0 or 1 today; concurrency is future work). |
| `lastPollOkAt` | Epoch ms of the last successful poll, `0` if never. |
| `lastBrokerContactAt` | Epoch ms of the last poll, heartbeat, or completion round-trip, `0` if never. |
| `loopAliveAt` | Epoch ms of the last poll-loop or heartbeat tick. |
| `uptimeMs` | Milliseconds since construction. |
| `now` | `Date.now()` at snapshot, so a reader is self-contained. |

### Options and defaults

| Option | Default | Meaning |
| --- | --- | --- |
| `livenessStaleMs` | `max(heartbeatIntervalMs, pollBackoffMaxMs) * 3`, floored at `30000` | How long the loop may stop turning before liveness fails. |
| `readinessStaleMs` | `max(heartbeatIntervalMs, pollBackoffMaxMs) * 2`, floored at `20000` | How long without a broker round-trip before readiness fails. |

Each is validated as a positive finite number if supplied. `readinessStaleMs` must be **greater than** `heartbeatIntervalMs`, otherwise a long job's heartbeats could not keep readiness fresh and it would flap; construction throws if this is violated.

### Serving the probes

The library never opens a socket or imports `node:http`, so it stays runtime-agnostic and side-effect-free on import. It exports two framework-agnostic handlers instead; each maps `GET {livePath}` to 200/503 on `live`, `GET {readyPath}` to 200/503 on `ready`, returns the full `WorkerHealth` as a JSON body in both cases (so a 503 is debuggable), and answers an unknown path with 404 and a non-GET with 405. Paths default to `/livez` and `/readyz`.

On Node's `http` server:

```typescript
import { createServer } from "node:http";
import { Worker, nodeHealthListener } from "./src/index";

const worker = new Worker("emails", handler);
createServer(nodeHealthListener(worker)).listen(8080);
```

On a `fetch`-style server:

```typescript
import { Worker, fetchHealthHandler } from "./src/index";

const worker = new Worker("emails", handler);
Bun.serve({ port: 8080, fetch: fetchHealthHandler(worker) });
```

```yaml
livenessProbe:
  httpGet:
    path: /livez
    port: 8080
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  periodSeconds: 5
  failureThreshold: 2
```

Together with [Transport resilience](#transport-resilience), this retires the external connection-monitor sidecar and the scheduled-restart CronJob: the pod restarts itself only when truly wedged and is routed around automatically when it cannot reach the broker.

**Security:** the health port is unauthenticated plaintext (transport auth and TLS are future work). Expose it on a cluster-internal address or listener only — never on a public interface — and rely on the probe's own network path rather than exposing it externally.

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

Keys are scoped to the queue name. Matching requests return one job ID; conflicting payload/options return `AlreadyExists` without retry. The client accepts nonblank keys up to 256 UTF-8 bytes. The reply is an enqueue receipt, not a status query: a matching replay returns the job's ORIGINAL enqueue state (`Waiting`, or `Delayed` if a delay was set) even after the job has run. Treat `result.replayed === true` as "nothing new was created" and use `result.id` to look up live progress separately. See [Idempotent enqueue](../README.md#idempotent-enqueue) for the full model.

Every `add` (and every `enqueueChild`, and every item of `addBulk`) now retries `Unavailable` and `DeadlineExceeded` under one unified, bounded policy — see [Producer durability](#producer-durability) for how the retry stays duplicate-free even without an explicit key. Request data is serialized and options copied once before retrying, so a retry never picks up a mutated caller object. After the window elapses the outcome is still uncertain; retain the same key and original request for a later retry. Changing the key could create duplicate work.

Receipts have no standalone TTL and are reclaimed only once the job is gone from both the live and history tables, so the dedup window tracks job retention. The producer should reuse a stable business-operation ID or persist a generated UUID before its first request. Preserve payload serialization and options across restarts; JSON property ordering is significant. New intended work needs a new key. This prevents duplicate insertion, not repeated handler side effects.

Delivery is at least once within the server's attempt and durability limits. Side effects must tolerate duplicates. The `leaseExpiresAtMs` on the job is the initial claim deadline; the worker renews it internally.

## Producer durability

A producer survives a **broker hiccup** (a short outage: a rolling restart, a brief network partition, a load-balancer failover) without losing an enqueue and without creating a duplicate, and it has a clearly-scoped stop-gap for a **producer crash** (the producer process itself dying before the broker acknowledges). The distinction is the whole design:

| Failure | What the client does | Guarantee |
| --- | --- | --- |
| Broker hiccup (broker down, producer alive) | Bounded, jittered enqueue retry across a wall-clock window; the same effective key rides every retry so the broker dedupes | At-least-once, no client-made duplicates |
| Producer crash (producer dies before ack) | The in-memory outbox buffers the pending add, but that buffer is process-local | Best-effort; anything still pending is **lost on crash** unless you back the outbox with durable storage |

### Auto-key: safe retries with or without your own key

`add` derives an **effective idempotency key** once per call and reuses it across every retry of that call:

1. `options.idempotencyKey` if you supply one (your stable business-operation ID — always preferred), else
2. a UUID minted once at the top of the call and held for the whole retry loop.

Because the same key is sent on every attempt, the broker collapses the client's own retries into a single job: at-least-once delivery with **zero duplicates from the retry loop**, even for a caller that never passed a key. Tuning lives on `AddOptions`:

| Option | Default | Meaning |
| --- | --- | --- |
| `enqueueRetryMaxMs` | `15000` | Total wall-clock window for bounded enqueue retries. |
| `enqueueRetryBaseMs` | `200` | First backoff step; doubles each attempt, capped at `enqueueRetryMaxMs`, jittered to 50–100%. |
| `noIdempotencyKey` | `false` | Opt out: send no key, attempt once, accept duplicate risk. |

Retry is attempted only for `Unavailable`/`DeadlineExceeded`; any other status (e.g. `AlreadyExists`, `InvalidArgument`, `ResourceExhausted`) throws immediately. Set `noIdempotencyKey: true` only for a fire-and-forget add where a duplicate is acceptable and you explicitly do not want a minted key.

### `addBulk`: bounded client-side fan-out

There is no batch RPC; `addBulk` fans out to one independent `add` per item through a bounded worker pool (default concurrency 8) so a large batch never spawns unbounded promises. Results are **positional** — one `AddResult` per input, in order:

```typescript
const results = await queue.addBulk(
  [
    { data: { orderId: "a" }, options: { idempotencyKey: "order:a" } },
    { data: { orderId: "b" }, options: { idempotencyKey: "order:b" } },
  ],
  { concurrency: 8, stopOnError: false },
);
for (const r of results) {
  if (r.ok) console.log(r.id, r.replayed);
  else console.error(r.error);
}
```

With `stopOnError: false` (the default) a mid-batch failure is captured as `{ ok: false, error }` and does not abort the rest. With `stopOnError: true` the call rejects on the first failure, with the partial positional results attached as `error.results`.

### Enqueue-before-complete: exactly-once fan-out at the broker

To fan a job out to children crash-safely, **enqueue every child idempotently, then complete the parent**. If the parent crashes after enqueuing but before completing, the broker redelivers it; the re-run re-enqueues the identical keys, the broker dedupes them, and the net effect is exactly-once fan-out *at the broker*. `enqueueChildBulk` assigns each child a positional index and a deterministic default key `${parentId}:${childName}:${index}`, which is what makes the replay dedupe:

```typescript
const worker = new Worker<{ orderId: string }>("orders", async job => {
  // 1. Enqueue all children first. Deterministic keys => a redelivered parent replays,
  //    it does not duplicate.
  await job.enqueueChildBulk([
    { name: "shipments", data: { orderId: job.data.orderId } },
    { name: "receipts", data: { orderId: job.data.orderId } },
  ]);
  // 2. Only now return, which completes the parent. A crash before this point is safe.
});
```

A single `job.enqueueChild(name, data)` still works; with no explicit key it mints a per-call UUID (no stable index), so prefer `enqueueChildBulk` — or pass your own `idempotencyKey` — when you need replay-safe fan-out. The broker makes *insertion* exactly-once; child **handler side effects remain at-least-once** and must tolerate duplicates.

### In-memory outbox stop-gap (and plugging in your own store)

`OutboxForwarder` is a store-and-forward buffer: `enqueue()` records a pending add and returns immediately, and a background tick loop (or a manual `drainOnce()`) forwards pending records to the broker, always re-using each record's held key so a re-forward is deduped. Transient broker errors leave a record pending for the next tick; a permanent error (e.g. `InvalidArgument`) poisons the record via `markFailed` and is surfaced through `onForwarded` — never silently dropped.

```typescript
import { Queue, OutboxForwarder, MemoryOutboxStore } from "./src/index";

const queue = new Queue("emails");
const forwarder = new OutboxForwarder(queue, new MemoryOutboxStore(), {
  onForwarded: (key, result) => { if (!result.ok) console.error("poisoned", key, result.error); },
  onDrained: () => console.info("outbox empty"),
});
forwarder.start();
await forwarder.enqueue({ to: "user@example.com" }, { idempotencyKey: "welcome:user-1" });
// ... on shutdown:
await forwarder.stop();
```

`MemoryOutboxStore` is the **only** store shipped and it is in-memory by design: it absorbs a short broker outage without the producer blocking, but a producer-process crash loses anything still pending — this is a stop-gap, not durable persistence, and it adds no database or storage dependency. The `OutboxStore` interface (`put` / `claimPending` / `markSent` / `markFailed`) is the user-space seam for real durability: implement it against your own durable medium and the `OutboxForwarder` works unchanged, giving you producer-crash survival.

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
