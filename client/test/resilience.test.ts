import { test, expect } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import * as grpc from "@grpc/grpc-js";
import { Queue, Worker, __setClientFactory } from "../src/index";

async function until(check: () => boolean | Promise<boolean>, timeout = 30000) {
  const end = Date.now() + timeout;
  while (!(await check())) { if (Date.now() > end) throw new Error("condition timed out"); await sleep(25); }
}
async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>(r => server.close(() => r()));
  return port;
}

/** A fully in-memory client whose per-method outcome is driven by `next()`. Lets the
 * poll loop be exercised deterministically with no broker, proto, or server change. */
type Outcome = { error?: unknown; result?: unknown };
function scriptedClient(next: () => Outcome): grpc.Client {
  const call = (_req: unknown, _opts: unknown, cb: (e: unknown, r: unknown) => void) => {
    const o = next();
    queueMicrotask(() => (o.error ? cb(o.error, undefined) : cb(null, o.result ?? { found: false })));
  };
  return { getNextJob: call, heartbeat: call, completeJob: call, failJob: call, addJob: call, close: () => {} } as unknown as grpc.Client;
}

test("keepalive: channel is constructed with the four keepalive options", () => {
  let captured: grpc.ClientOptions | undefined;
  const previous = __setClientFactory((address, creds, options) => {
    captured = options;
    return previous(address, creds, options);
  });
  try {
    // Defaults.
    const q = new Queue("keepalive-defaults", { address: "127.0.0.1:1" });
    q.close();
    expect(captured!["grpc.keepalive_time_ms"]).toBe(20000);
    expect(captured!["grpc.keepalive_timeout_ms"]).toBe(10000);
    expect(captured!["grpc.keepalive_permit_without_calls"]).toBe(1);
    expect(captured!["grpc.max_reconnect_backoff_ms"]).toBe(10000);
    // Overrides, including permitWithoutCalls:false -> 0.
    const q2 = new Queue("keepalive-custom", {
      address: "127.0.0.1:1",
      keepalive: { timeMs: 5000, timeoutMs: 2000, permitWithoutCalls: false, maxReconnectBackoffMs: 3000 },
    });
    q2.close();
    expect(captured!["grpc.keepalive_time_ms"]).toBe(5000);
    expect(captured!["grpc.keepalive_timeout_ms"]).toBe(2000);
    expect(captured!["grpc.keepalive_permit_without_calls"]).toBe(0);
    expect(captured!["grpc.max_reconnect_backoff_ms"]).toBe(3000);
  } finally { __setClientFactory(previous); }
  // A non-positive keepalive value is rejected like rpcTimeoutMs.
  expect(() => new Queue("bad", { keepalive: { timeMs: 0 } })).toThrow();
  expect(() => new Queue("bad", { keepalive: { timeoutMs: -1 } })).toThrow();
});

test("events: one onConnectionLost at threshold, one onConnectionRestored on recovery", async () => {
  let calls = 0;
  const previous = __setClientFactory(() => scriptedClient(() => {
    calls++;
    // Fail the first four polls (crosses threshold 3), then recover.
    return calls <= 4 ? { error: new Error("broker down") } : { result: { found: false } };
  }));
  const lost: { consecutiveFailures: number }[] = [];
  const restored: { downForMs: number; failuresWhileDown: number }[] = [];
  let worker: Worker | undefined;
  try {
    worker = new Worker("events", async () => {}, {
      address: "127.0.0.1:1", pollIntervalMs: 10, pollBackoffMaxMs: 40, connectionLostThreshold: 3,
      onError: () => {},
      onConnectionLost: info => lost.push({ consecutiveFailures: info.consecutiveFailures }),
      onConnectionRestored: info => restored.push(info),
    });
    await until(() => restored.length === 1);
    await sleep(60); // give any spurious duplicate a chance to (not) fire
    expect(lost.length).toBe(1);
    expect(lost[0].consecutiveFailures).toBe(3);
    expect(restored.length).toBe(1);
    expect(restored[0].downForMs).toBeGreaterThanOrEqual(0);
    expect(restored[0].failuresWhileDown).toBeGreaterThanOrEqual(3);
    expect(worker.isConnected).toBe(true);
    expect(worker.consecutivePollFailures).toBe(0);
  } finally { __setClientFactory(previous); if (worker) await worker.close(); }
});

test("events: a single sub-threshold transient does not emit", async () => {
  let calls = 0;
  const previous = __setClientFactory(() => scriptedClient(() => {
    calls++;
    // Only two failures, never reaching threshold 3.
    return calls <= 2 ? { error: new Error("transient") } : { result: { found: false } };
  }));
  const lost: unknown[] = [];
  const restored: unknown[] = [];
  let worker: Worker | undefined;
  try {
    worker = new Worker("transient", async () => {}, {
      address: "127.0.0.1:1", pollIntervalMs: 10, pollBackoffMaxMs: 40, connectionLostThreshold: 3,
      onError: () => {},
      onConnectionLost: info => lost.push(info),
      onConnectionRestored: info => restored.push(info),
    });
    await until(() => calls > 12);
    expect(lost.length).toBe(0);
    expect(restored.length).toBe(0);
    expect(worker.isConnected).toBe(true);
    expect(worker.consecutivePollFailures).toBe(0);
  } finally { __setClientFactory(previous); if (worker) await worker.close(); }
});

test("close during backoff returns promptly", async () => {
  const previous = __setClientFactory(() => scriptedClient(() => ({ error: new Error("always down") })));
  let worker: Worker | undefined;
  try {
    worker = new Worker("backoff-close", async () => {}, {
      address: "127.0.0.1:1", pollIntervalMs: 50, pollBackoffMaxMs: 3000, connectionLostThreshold: 2,
      onError: () => {},
    });
    // Let it climb into a long backoff sleep.
    await until(() => (worker as Worker).consecutivePollFailures >= 4);
    const start = Date.now();
    await worker.close();
    expect(Date.now() - start).toBeLessThan(1000); // must not wait out the ~3s backoff
  } finally { __setClientFactory(previous); if (worker) await worker.close(); }
});

test("real gRPC: outage backoff grows and bounded, recovery resets health", async () => {
  const dir = await mkdtemp(join(tmpdir(), "anvil-resilience-"));
  const address = `127.0.0.1:${await freePort()}`;
  const httpAddress = `127.0.0.1:${await freePort()}`;
  const dbPath = join(dir, "test.db");
  const executable = resolve("../target/debug/" + (process.platform === "win32" ? "rusty-queue.exe" : "rusty-queue"));
  const spawnBroker = () => Bun.spawn([executable], { cwd: dir, env: { ...process.env, RUST_LOG: "info", ANVILMQ_ADDR: address, ANVILMQ_HTTP_ADDR: httpAddress, ANVILMQ_DB_PATH: dbPath }, stdout: "pipe", stderr: "pipe" });

  let server = spawnBroker();
  const drainOut = (s: ReturnType<typeof Bun.spawn>) => { void (async () => { try { for await (const _ of s.stdout as ReadableStream<Uint8Array>) { /* drain */ } } catch { /* ignore */ } })(); void new Response(s.stderr as ReadableStream<Uint8Array>).text().catch(() => {}); };
  drainOut(server);

  const processed: string[] = [];
  const failureTimes: number[] = [];
  const restored: { downForMs: number; failuresWhileDown: number }[] = [];
  const lost: number[] = [];
  const pollMs = 100;
  const pollBackoffMaxMs = 800;
  let worker: Worker<{ n: number }> | undefined;
  let queue: Queue<{ n: number }> | undefined;
  try {
    // Broker's HTTP readiness precedes the gRPC socket; retry the first enqueue.
    queue = new Queue<{ n: number }>("outage", { address });
    await until(async () => { try { return (await fetch(`http://${httpAddress}/readyz`)).status === 200; } catch { return false; } }, 10000);

    worker = new Worker<{ n: number }>("outage", async job => { processed.push(String(job.data.n)); }, {
      address, pollIntervalMs: pollMs, pollBackoffMaxMs, connectionLostThreshold: 3,
      onError: () => { failureTimes.push(Date.now()); },
      onConnectionLost: info => lost.push(info.consecutiveFailures),
      onConnectionRestored: info => restored.push(info),
    });

    // Confirm normal operation before the outage.
    await until(async () => { try { await queue!.add({ n: 1 }); return true; } catch { return false; } }, 10000);
    await until(() => processed.includes("1"), 10000);
    expect(worker.isConnected).toBe(true);

    // Induce the outage.
    server.kill(); await server.exited;
    // Collect enough failures to see the backoff climb and plateau.
    await until(() => failureTimes.length >= 6, 20000);

    const gaps: number[] = [];
    for (let i = 1; i < failureTimes.length; i++) gaps.push(failureTimes[i] - failureTimes[i - 1]);
    const maxGap = Math.max(...gaps);
    // Grew beyond the fixed poll interval (not a constant 100ms storm)...
    expect(maxGap).toBeGreaterThan(pollMs * 1.5);
    // ...yet stayed bounded by the configured cap (with scheduler slack).
    expect(maxGap).toBeLessThan(pollBackoffMaxMs + 600);
    // Connection was declared lost exactly once.
    expect(lost.length).toBe(1);
    expect(worker.isConnected).toBe(false);

    // Restart on the SAME address + db path.
    server = spawnBroker(); drainOut(server);
    await until(async () => { try { return (await fetch(`http://${httpAddress}/readyz`)).status === 200; } catch { return false; } }, 15000);

    // Worker resumes claiming; health resets; exactly one restored event.
    await until(() => restored.length === 1, 15000);
    await until(async () => { try { await queue!.add({ n: 2 }); return true; } catch { return false; } }, 10000);
    await until(() => processed.includes("2"), 15000);
    expect(worker.isConnected).toBe(true);
    expect(worker.consecutivePollFailures).toBe(0);
    expect(restored[0].downForMs).toBeGreaterThan(0);
  } finally {
    if (worker) await worker.close();
    if (queue) queue.close();
    if (server.exitCode === null) { server.kill(); await server.exited; }
    await rm(dir, { recursive: true, force: true });
  }
}, 90000);
