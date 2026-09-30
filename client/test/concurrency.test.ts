import { test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import * as grpc from "@grpc/grpc-js";
import { Queue, Worker, __setClientFactory } from "../src/index";

async function until(check: () => boolean | Promise<boolean>, timeout = 30000) {
  const end = Date.now() + timeout;
  while (!(await check())) { if (Date.now() > end) throw new Error("condition timed out"); await sleep(10); }
}
async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>(r => server.close(() => r()));
  return port;
}

// One real broker shared by every real-broker test; each test uses its own queue name.
let dir: string;
let address: string;
let httpAddress: string;
let server: ReturnType<typeof Bun.spawn>;
let output: Promise<void>;
let errors: Promise<string>;

beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), "anvil-c4-"));
  address = `127.0.0.1:${await freePort()}`;
  httpAddress = `127.0.0.1:${await freePort()}`;
  const executable = resolve("../target/debug/" + (process.platform === "win32" ? "rusty-queue.exe" : "rusty-queue"));
  server = Bun.spawn([executable], { cwd: dir, env: { ...process.env, RUST_LOG: "info", ANVILMQ_ADDR: address, ANVILMQ_HTTP_ADDR: httpAddress, ANVILMQ_DB_PATH: join(dir, "test.db") }, stdout: "pipe", stderr: "pipe" });
  let log = "";
  output = (async () => { for await (const chunk of server.stdout as ReadableStream<Uint8Array>) log += new TextDecoder().decode(chunk); })();
  errors = new Response(server.stderr as ReadableStream<Uint8Array>).text();
  await until(() => log.includes("daemon listening"), 15000);
});

afterAll(async () => {
  server.kill(); await server.exited; await output.catch(() => {}); await errors.catch(() => {});
  await rm(dir, { recursive: true, force: true });
});

// 1. Parallelism: concurrency N runs N slow handlers at once; wall time ~= one job, not N.
test("parallelism: concurrency N runs N slow jobs at once", async () => {
  const N = 4;
  const d = 400;
  const q = new Queue<{ i: number }>("c4-parallel", { address });
  for (let i = 0; i < N; i++) await q.add({ i });
  let active = 0, peak = 0, completed = 0;
  const startedAt = Date.now();
  const worker = new Worker<{ i: number }>("c4-parallel", async () => {
    active++; peak = Math.max(peak, active);
    await sleep(d);
    active--; completed++;
  }, { address, concurrency: N, pollIntervalMs: 20, onError: () => {} });
  try {
    await until(() => completed === N, 10000);
    const wall = Date.now() - startedAt;
    expect(peak).toBe(N);                       // all N handlers overlapped
    expect(wall).toBeLessThan(d * 2);           // ~= d, not N*d
  } finally { await worker.close(); q.close(); }
});

// 2. Capacity cap: never more than N in flight; saturated true at peak.
test("capacity cap: inFlight never exceeds concurrency and saturates", async () => {
  const N = 4;
  const total = 3 * N;
  const q = new Queue<{ i: number }>("c4-cap", { address });
  for (let i = 0; i < total; i++) await q.add({ i });
  let active = 0, peakActive = 0, completed = 0;
  const worker = new Worker<{ i: number }>("c4-cap", async () => {
    active++; peakActive = Math.max(peakActive, active);
    await sleep(120);
    active--; completed++;
  }, { address, concurrency: N, pollIntervalMs: 20, onError: () => {} });
  let maxHealthInFlight = 0, saturatedSeen = false;
  const sampler = setInterval(() => {
    const h = worker.health();
    maxHealthInFlight = Math.max(maxHealthInFlight, h.inFlight);
    if (h.saturated && h.inFlight === N) saturatedSeen = true;
  }, 5);
  try {
    await until(() => completed === total, 20000);
    clearInterval(sampler);
    expect(peakActive).toBeLessThanOrEqual(N);
    expect(maxHealthInFlight).toBeLessThanOrEqual(N);
    expect(maxHealthInFlight).toBe(N);
    expect(saturatedSeen).toBe(true);
  } finally { clearInterval(sampler); await worker.close(); q.close(); }
});

// 3. Serial back-compat: default concurrency 1 finishes one job before claiming the next.
test("serial back-compat: default concurrency 1 keeps max inFlight at 1", async () => {
  const q = new Queue<{ i: number }>("c4-serial", { address });
  for (let i = 0; i < 3; i++) await q.add({ i });
  let active = 0, peak = 0, completed = 0;
  const worker = new Worker<{ i: number }>("c4-serial", async () => {
    active++; peak = Math.max(peak, active);
    await sleep(80);
    active--; completed++;
  }, { address, pollIntervalMs: 20, onError: () => {} });
  let maxHealthInFlight = 0;
  const sampler = setInterval(() => { maxHealthInFlight = Math.max(maxHealthInFlight, worker.health().inFlight); }, 5);
  try {
    await until(() => completed === 3, 10000);
    clearInterval(sampler);
    expect(peak).toBe(1);
    expect(maxHealthInFlight).toBe(1);
    expect(worker.health().concurrency).toBe(1);
  } finally { clearInterval(sampler); await worker.close(); q.close(); }
});

// 4. Drain-on-close: close() resolves only after every in-flight handler completes.
test("drain-on-close: close() waits for all in-flight handlers", async () => {
  const N = 4;
  const q = new Queue<{ i: number }>("c4-drain", { address });
  for (let i = 0; i < N; i++) await q.add({ i });
  let started = 0;
  const completions: number[] = [];
  const worker = new Worker<{ i: number }>("c4-drain", async () => {
    started++;
    await sleep(300);
    completions.push(Date.now());
  }, { address, concurrency: N, pollIntervalMs: 20, onError: () => {} });
  try {
    await until(() => started === N, 10000);
    const inflightAtClose = N - completions.length;
    expect(inflightAtClose).toBeGreaterThan(0);   // real work is mid-flight
    await worker.close();
    const closeResolvedAt = Date.now();
    expect(completions.length).toBe(N);           // all drained
    for (const at of completions) expect(at).toBeLessThanOrEqual(closeResolvedAt);
  } finally { q.close(); }
});

// 5. Error isolation: one throwing handler fails its own job; the loop and peers keep going.
test("error isolation: a throwing handler does not stop the loop", async () => {
  const N = 4;
  const q = new Queue<{ i: number; fail?: boolean }>("c4-erriso", { address });
  const completed = new Set<number>();
  const errorsSeen: unknown[] = [];
  const worker = new Worker<{ i: number; fail?: boolean }>("c4-erriso", async job => {
    if (job.data.fail) throw new Error("boom");
    completed.add(job.data.i);
  }, { address, concurrency: N, pollIntervalMs: 20, onError: e => errorsSeen.push(e) });
  try {
    for (let i = 0; i < N; i++) await q.add({ i, fail: i === 1 }, { maxAttempts: 1 });
    await until(() => completed.has(0) && completed.has(2) && completed.has(3), 15000);
    expect(errorsSeen.length).toBeGreaterThanOrEqual(1);   // the failing job reported
    // Loop still alive: a job enqueued afterward is still picked up and completed.
    await q.add({ i: 99 });
    await until(() => completed.has(99), 15000);
    expect(worker.health().ready).toBe(true);
  } finally { await worker.close(); q.close(); }
});

// 7. Readiness under saturation: parked at capacity, per-job heartbeats keep ready true.
test("readiness under saturation stays ready past a poll interval", async () => {
  const N = 2;
  const q = new Queue<{ i: number }>("c4-sat-ready", { address });
  for (let i = 0; i < N; i++) await q.add({ i });
  let active = 0;
  const worker = new Worker<{ i: number }>("c4-sat-ready", async () => {
    active++;
    await sleep(2000);
    active--;
  }, { address, concurrency: N, pollIntervalMs: 100, heartbeatIntervalMs: 200, readinessStaleMs: 1500, onError: () => {} });
  try {
    await until(() => active === N, 10000);       // both slots busy -> parked at capacity
    const span = 600;                             // > pollIntervalMs
    const end = Date.now() + span;
    while (Date.now() < end) {
      const h = worker.health();
      expect(h.ready).toBe(true);
      expect(h.saturated).toBe(true);
      await sleep(50);
    }
  } finally { await worker.close(); q.close(); }
});

// ---- Scripted-seam tests (no broker) ----
type Outcome = { error?: unknown; result?: unknown };
function methodClient(handlers: Partial<Record<string, () => Outcome>>): grpc.Client {
  const wrap = (fn?: () => Outcome) => (_req: unknown, _opts: unknown, cb: (e: unknown, r: unknown) => void) => {
    const o = fn ? fn() : { result: {} };
    queueMicrotask(() => (o.error ? cb(o.error, undefined) : cb(null, o.result ?? {})));
  };
  return {
    getNextJob: wrap(handlers.getNextJob), heartbeat: wrap(handlers.heartbeat),
    completeJob: wrap(handlers.completeJob), failJob: wrap(handlers.failJob),
    addJob: wrap(handlers.addJob), close: () => {},
  } as unknown as grpc.Client;
}

// 6. Adaptive idle: empty-poll delays grow toward the cap and reset after a found job,
//    never reaching readinessStaleMs, with readiness staying true throughout.
test("adaptive idle: empty-poll delay grows then resets, readiness stays true", async () => {
  const pollTimes: number[] = [];
  let emitJob = false;
  const claim = {
    found: true, id: "j1", name: "c4-idle", payload: Buffer.from("{}"),
    attempts: 1, metadata: { parentId: "", traceId: "", executionDepth: 0 }, leaseExpiresAtMs: Date.now() + 60000,
  };
  const previous = __setClientFactory(() => methodClient({
    getNextJob: () => {
      pollTimes.push(Date.now());
      if (emitJob) { emitJob = false; return { result: claim }; }
      return { result: { found: false } };
    },
    heartbeat: () => ({ result: {} }), completeJob: () => ({ result: {} }),
  }));
  let worker: Worker | undefined;
  try {
    worker = new Worker("c4-idle", async () => {}, {
      address: "127.0.0.1:1", pollIntervalMs: 20, idlePollMaxMs: 640,
      heartbeatIntervalMs: 1000, readinessStaleMs: 5000, onError: () => {},
    });
    await until(() => worker!.health().ready === true, 5000);
    // Let several empty polls accumulate so the idle delay ramps up.
    await until(() => pollTimes.length >= 7, 10000);
    const deltas: number[] = [];
    for (let i = 1; i < pollTimes.length; i++) deltas.push(pollTimes[i] - pollTimes[i - 1]);
    const early = deltas[0];
    const late = Math.max(...deltas.slice(-3));
    expect(late).toBeGreaterThan(early);                 // delay grew
    expect(Math.max(...deltas)).toBeLessThan(5000);      // never reached readinessStaleMs
    expect(worker.health().ready).toBe(true);            // idle worker stays ready
    // Inject one found job; the next inter-poll delay resets toward pollMs.
    const before = pollTimes.length;
    emitJob = true;
    await until(() => pollTimes.length >= before + 3, 10000);
    const resetDeltas: number[] = [];
    for (let i = before; i < pollTimes.length; i++) resetDeltas.push(pollTimes[i] - pollTimes[i - 1]);
    expect(Math.min(...resetDeltas)).toBeLessThan(early + 100);   // back near pollMs
  } finally { __setClientFactory(previous); if (worker) await worker.close(); }
});
