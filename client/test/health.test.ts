import { test, expect } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import * as grpc from "@grpc/grpc-js";
import { Queue, Worker, __setClientFactory, nodeHealthListener, fetchHealthHandler, type WorkerHealth } from "../src/index";

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

/** In-memory client whose per-method outcome is driven by `next()`; mirrors resilience.test.ts. */
type Outcome = { error?: unknown; result?: unknown };
function scriptedClient(next: () => Outcome): grpc.Client {
  const call = (_req: unknown, _opts: unknown, cb: (e: unknown, r: unknown) => void) => {
    const o = next();
    queueMicrotask(() => (o.error ? cb(o.error, undefined) : cb(null, o.result ?? { found: false })));
  };
  return { getNextJob: call, heartbeat: call, completeJob: call, failJob: call, addJob: call, close: () => {} } as unknown as grpc.Client;
}

/** A client whose poll resolves only after a long delay, so the loop is parked in the
 * poll await (loopAliveAt frozen) long enough to observe a synthetic wedge, yet close()
 * still returns once the pending poll settles. */
function slowPollClient(delayMs: number): grpc.Client {
  const call = (_req: unknown, _opts: unknown, cb: (e: unknown, r: unknown) => void) => { setTimeout(() => cb(null, { found: false }), delayMs); };
  return { getNextJob: call, heartbeat: call, completeJob: call, failJob: call, addJob: call, close: () => {} } as unknown as grpc.Client;
}

test("health() is synchronous and issues no RPC", async () => {
  let calls = 0;
  const previous = __setClientFactory(() => scriptedClient(() => { calls++; return { result: { found: false } }; }));
  let worker: Worker | undefined;
  try {
    worker = new Worker("sync", async () => {}, { address: "127.0.0.1:1", pollIntervalMs: 10, onError: () => {} });
    // health() returns a plain object, not a promise.
    const snapshot = worker.health();
    expect(snapshot).not.toBeInstanceOf(Promise);
    expect(typeof snapshot.now).toBe("number");
    // Calling health() must not itself drive the scripted client: measure synchronously
    // (no await between reads, so the poll loop cannot run in between).
    const before = calls;
    worker.health();
    worker.health();
    expect(calls).toBe(before);
  } finally { __setClientFactory(previous); if (worker) await worker.close(); }
});

test("startup gate: not ready until the first successful poll, but live from the start", async () => {
  let calls = 0;
  const previous = __setClientFactory(() => scriptedClient(() => {
    calls++;
    // Fail the first two polls (below threshold 3, so still connected), then succeed.
    return calls <= 2 ? { error: new Error("starting up") } : { result: { found: false } };
  }));
  let worker: Worker | undefined;
  try {
    worker = new Worker("startup", async () => {}, { address: "127.0.0.1:1", pollIntervalMs: 10, onError: () => {} });
    // Synchronously after construction: no poll has resolved yet.
    const initial = worker.health();
    expect(initial.ready).toBe(false);
    expect(initial.live).toBe(true);
    expect(initial.lastPollOkAt).toBe(0);
    // After the broker is reachable once, readiness flips true.
    await until(() => worker!.health().ready === true);
    const ok = worker.health();
    expect(ok.ready).toBe(true);
    expect(ok.live).toBe(true);
    expect(ok.lastPollOkAt).toBeGreaterThan(0);
    expect(ok.lastBrokerContactAt).toBeGreaterThan(0);
  } finally { __setClientFactory(previous); if (worker) await worker.close(); }
});

test("wedge: a stale loopAliveAt fails liveness", async () => {
  const previous = __setClientFactory(() => slowPollClient(1000));
  let worker: Worker | undefined;
  try {
    worker = new Worker("wedge", async () => {}, { address: "127.0.0.1:1", pollIntervalMs: 10, heartbeatIntervalMs: 500, livenessStaleMs: 1000, readinessStaleMs: 800, onError: () => {} });
    // The loop is parked in the slow poll, so loopAliveAt is frozen near now.
    expect(worker.health().live).toBe(true);
    // Backdate the loop-liveness clock beyond livenessStaleMs to simulate a wedged loop.
    worker.__setLoopAliveAt(Date.now() - 5000);
    expect(worker.health().live).toBe(false);
    expect(worker.health().ready).toBe(false);
  } finally { __setClientFactory(previous); if (worker) await worker.close(); }
});

test("readinessStaleMs <= heartbeatIntervalMs is rejected at construction", () => {
  expect(() => new Worker("bad", async () => {}, { address: "127.0.0.1:1", heartbeatIntervalMs: 10000, readinessStaleMs: 5000 })).toThrow();
  expect(() => new Worker("bad", async () => {}, { address: "127.0.0.1:1", livenessStaleMs: 0 })).toThrow();
  expect(() => new Worker("bad", async () => {}, { address: "127.0.0.1:1", readinessStaleMs: -1 })).toThrow();
});

test("shutdown: close() makes the worker stopping and not-ready", async () => {
  const previous = __setClientFactory(() => scriptedClient(() => ({ result: { found: false } })));
  let worker: Worker | undefined;
  try {
    worker = new Worker("shutdown", async () => {}, { address: "127.0.0.1:1", pollIntervalMs: 10, onError: () => {} });
    await until(() => worker!.health().ready === true);
    const closing = worker.close();
    // stopping flips synchronously when close() aborts the controller.
    const during = worker.health();
    expect(during.stopping).toBe(true);
    expect(during.ready).toBe(false);
    await closing;
    const after = worker.health();
    expect(after.stopping).toBe(true);
    expect(after.ready).toBe(false);
  } finally { __setClientFactory(previous); if (worker) await worker.close(); }
});

test("HTTP surface: nodeHealthListener and fetchHealthHandler map live/ready to 200/503", async () => {
  const live: WorkerHealth = { live: true, ready: true, connected: true, stale: false, stopping: false, consecutivePollFailures: 0, inFlight: 1, lastPollOkAt: 1, lastBrokerContactAt: 1, loopAliveAt: 1, uptimeMs: 5, now: 6 };
  const notReady: WorkerHealth = { ...live, ready: false, connected: false };
  const notLive: WorkerHealth = { ...live, live: false, ready: false };

  // node:http-shaped listener.
  const callNode = (health: WorkerHealth, url: string, method = "GET") => {
    const listener = nodeHealthListener({ health: () => health });
    const res = { statusCode: 0, headers: {} as Record<string, string>, body: "", setHeader(k: string, v: string) { this.headers[k.toLowerCase()] = v; }, end(b?: string) { this.body = b ?? ""; } };
    listener({ url, method }, res);
    return res;
  };
  let r = callNode(live, "/livez");
  expect(r.statusCode).toBe(200);
  expect(r.headers["content-type"]).toBe("application/json");
  expect((JSON.parse(r.body) as WorkerHealth).live).toBe(true);
  expect(callNode(notLive, "/livez").statusCode).toBe(503);
  expect(callNode(live, "/readyz").statusCode).toBe(200);
  expect(callNode(notReady, "/readyz").statusCode).toBe(503);
  // Query string on the path is tolerated (pathname compared).
  expect(callNode(live, "/livez?probe=1").statusCode).toBe(200);
  // Unknown path -> 404, non-GET -> 405.
  expect(callNode(live, "/nope").statusCode).toBe(404);
  expect(callNode(live, "/livez", "POST").statusCode).toBe(405);

  // fetch-shaped handler.
  const handler = fetchHealthHandler({ health: () => live });
  const readyRes = handler({ url: "http://pod.local/readyz", method: "GET" });
  expect(readyRes.status).toBe(200);
  expect(readyRes.headers.get("content-type")).toBe("application/json");
  expect(((await readyRes.json()) as WorkerHealth).ready).toBe(true);
  expect(fetchHealthHandler({ health: () => notLive })({ url: "http://pod.local/livez" }).status).toBe(503);
  expect(handler({ url: "http://pod.local/missing" }).status).toBe(404);
  expect(handler({ url: "http://pod.local/livez", method: "DELETE" }).status).toBe(405);
});

/** Spawn the real broker (mirrors resilience.test.ts) so heartbeats actually flow. */
function brokerHarness() {
  return (async () => {
    const dir = await mkdtemp(join(tmpdir(), "anvil-health-"));
    const address = `127.0.0.1:${await freePort()}`;
    const httpAddress = `127.0.0.1:${await freePort()}`;
    const dbPath = join(dir, "test.db");
    const executable = resolve("../target/debug/" + (process.platform === "win32" ? "rusty-queue.exe" : "rusty-queue"));
    const spawnBroker = () => Bun.spawn([executable], { cwd: dir, env: { ...process.env, RUST_LOG: "info", ANVILMQ_ADDR: address, ANVILMQ_HTTP_ADDR: httpAddress, ANVILMQ_DB_PATH: dbPath }, stdout: "pipe", stderr: "pipe" });
    const drainOut = (s: ReturnType<typeof Bun.spawn>) => { void (async () => { try { for await (const _ of s.stdout as ReadableStream<Uint8Array>) { /* drain */ } } catch { /* ignore */ } })(); void new Response(s.stderr as ReadableStream<Uint8Array>).text().catch(() => {}); };
    return { dir, address, httpAddress, dbPath, spawnBroker, drainOut };
  })();
}

test("long job keeps readiness fresh via heartbeats while polls are suspended", async () => {
  const h = await brokerHarness();
  let server = h.spawnBroker(); h.drainOut(server);
  let worker: Worker<{ n: number }> | undefined;
  let queue: Queue<{ n: number }> | undefined;
  let inHandler = false;
  try {
    await until(async () => { try { return (await fetch(`http://${h.httpAddress}/readyz`)).status === 200; } catch { return false; } }, 15000);
    queue = new Queue<{ n: number }>("longjob", { address: h.address });
    // heartbeatMs (300) < readinessStaleMs (1500) < job duration (4000): without the
    // heartbeat-refreshed contact clock, readiness would flap during the job.
    worker = new Worker<{ n: number }>("longjob", async () => {
      inHandler = true;
      const end = Date.now() + 4000;
      while (Date.now() < end) await sleep(100);
      inHandler = false;
    }, { address: h.address, pollIntervalMs: 100, heartbeatIntervalMs: 300, readinessStaleMs: 1500, livenessStaleMs: 5000, onError: () => {} });

    await until(() => worker!.health().ready === true, 10000);
    await until(async () => { try { await queue!.add({ n: 1 }); return true; } catch { return false; } }, 10000);
    await until(() => inHandler, 10000);

    // Sample readiness across a window longer than readinessStaleMs while the job runs.
    const end = Date.now() + 2500;
    while (Date.now() < end) {
      const snap = worker.health();
      expect(snap.ready).toBe(true);
      expect(snap.inFlight).toBe(1);
      await sleep(150);
    }
    // After the job settles, inFlight returns to 0.
    await until(() => worker!.health().inFlight === 0, 10000);
    expect(worker.health().ready).toBe(true);
  } finally {
    if (worker) await worker.close();
    if (queue) queue.close();
    if (server.exitCode === null) { server.kill(); await server.exited; }
    await rm(h.dir, { recursive: true, force: true });
  }
}, 90000);

test("outage flips readiness off while liveness stays on; recovery restores readiness", async () => {
  const h = await brokerHarness();
  let server = h.spawnBroker(); h.drainOut(server);
  let worker: Worker | undefined;
  try {
    await until(async () => { try { return (await fetch(`http://${h.httpAddress}/readyz`)).status === 200; } catch { return false; } }, 15000);
    // Small readiness window and heartbeat so the outage surfaces quickly; readinessStaleMs > heartbeatMs.
    worker = new Worker("outage-health", async () => {}, {
      address: h.address, pollIntervalMs: 100, pollBackoffMaxMs: 400, heartbeatIntervalMs: 500,
      readinessStaleMs: 1000, livenessStaleMs: 30000, connectionLostThreshold: 3, onError: () => {},
    });
    await until(() => worker!.health().ready === true, 10000);

    server.kill(); await server.exited;
    // Within a bounded window readiness drops while liveness holds (loop keeps turning).
    await until(() => worker!.health().ready === false, 15000);
    expect(worker.health().live).toBe(true);

    server = h.spawnBroker(); h.drainOut(server);
    await until(async () => { try { return (await fetch(`http://${h.httpAddress}/readyz`)).status === 200; } catch { return false; } }, 15000);
    await until(() => worker!.health().ready === true, 15000);
    expect(worker.health().live).toBe(true);
  } finally {
    if (worker) await worker.close();
    if (server.exitCode === null) { server.kill(); await server.exited; }
    await rm(h.dir, { recursive: true, force: true });
  }
}, 90000);
