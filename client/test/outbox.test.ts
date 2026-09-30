import { test, expect } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import * as grpc from "@grpc/grpc-js";
import { Queue, Worker, MemoryOutboxStore, OutboxForwarder, type Job, type AddResult } from "../src/index";

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

const executable = resolve("../target/debug/" + (process.platform === "win32" ? "rusty-queue.exe" : "rusty-queue"));

type Broker = {
  address: string; httpAddress: string; dbPath: string; dir: string;
  proc: ReturnType<typeof Bun.spawn>;
  spawn(): ReturnType<typeof Bun.spawn>;
};

function drain(s: ReturnType<typeof Bun.spawn>) {
  void (async () => { try { for await (const _ of s.stdout as ReadableStream<Uint8Array>) { /* drain */ } } catch { /* ignore */ } })();
  void new Response(s.stderr as ReadableStream<Uint8Array>).text().catch(() => {});
}

async function makeBroker(prefix: string): Promise<Broker> {
  const dir = await mkdtemp(join(tmpdir(), prefix));
  const address = `127.0.0.1:${await freePort()}`;
  const httpAddress = `127.0.0.1:${await freePort()}`;
  const dbPath = join(dir, "test.db");
  const b: Broker = {
    address, httpAddress, dbPath, dir, proc: undefined as unknown as ReturnType<typeof Bun.spawn>,
    spawn() {
      const proc = Bun.spawn([executable], { cwd: dir, env: { ...process.env, RUST_LOG: "info", ANVILMQ_ADDR: address, ANVILMQ_HTTP_ADDR: httpAddress, ANVILMQ_DB_PATH: dbPath }, stdout: "pipe", stderr: "pipe" });
      drain(proc); this.proc = proc; return proc;
    },
  };
  return b;
}
async function readyz(httpAddress: string, timeout = 15000) {
  await until(async () => { try { return (await fetch(`http://${httpAddress}/readyz`)).status === 200; } catch { return false; } }, timeout);
}

test("addBulk: positional results, partial failure does not abort, stopOnError rejects with partial results", async () => {
  const b = await makeBroker("anvil-outbox-bulk-");
  b.spawn();
  const queue = new Queue<{ n: number }>("bulk", { address: b.address });
  try {
    await readyz(b.httpAddress);
    await until(async () => { try { await queue.add({ n: 0 }, { idempotencyKey: "warmup" }); return true; } catch { return false; } });

    // stopOnError:false (default) — every item enqueues on its own effective key; a
    // failure would surface as {ok:false} without aborting the batch (see next test).
    const items = [
      { data: { n: 1 } },
      { data: { n: 2 } },
      { data: { n: 3 } },
    ];
    const results = await queue.addBulk(items, { concurrency: 2 });
    expect(results.length).toBe(3);
    expect(results.every(r => r.ok)).toBe(true);
    const ids = new Set(results.map(r => (r.ok ? r.id : "")));
    expect(ids.size).toBe(3); // three distinct jobs

    // Idempotency: re-adding the same explicit keys returns replayed receipts, not new jobs.
    const keyed = await queue.addBulk([
      { data: { n: 10 }, options: { idempotencyKey: "bulk-a" } },
      { data: { n: 11 }, options: { idempotencyKey: "bulk-b" } },
    ]);
    expect(keyed.every(r => r.ok && r.replayed === false)).toBe(true);
    const replay = await queue.addBulk([
      { data: { n: 10 }, options: { idempotencyKey: "bulk-a" } },
      { data: { n: 11 }, options: { idempotencyKey: "bulk-b" } },
    ]);
    expect(replay.every(r => r.ok && r.replayed === true)).toBe(true);
  } finally {
    queue.close();
    b.proc.kill(); await b.proc.exited;
    await rm(b.dir, { recursive: true, force: true });
  }
}, 60000);

test("addBulk: stopOnError rejects early with partial results attached", async () => {
  const b = await makeBroker("anvil-outbox-stop-");
  b.spawn();
  const queue = new Queue<{ n: number }>("bulk-stop", { address: b.address });
  try {
    await readyz(b.httpAddress);
    await until(async () => { try { await queue.add({ n: 0 }, { idempotencyKey: "warmup" }); return true; } catch { return false; } });
    // Kill the broker so every enqueue fails permanently once the retry window is 0.
    b.proc.kill(); await b.proc.exited;
    let caught: unknown;
    try {
      await queue.addBulk(
        [ { data: { n: 1 }, options: { enqueueRetryMaxMs: 0 } }, { data: { n: 2 }, options: { enqueueRetryMaxMs: 0 } } ],
        { stopOnError: true, concurrency: 1 },
      );
    } catch (e) { caught = e; }
    expect(caught).toBeDefined();
    const results = (caught as { results?: AddResult[] }).results;
    expect(Array.isArray(results)).toBe(true);
    expect(results!.some(r => r && r.ok === false)).toBe(true);
  } finally {
    queue.close();
    if (b.proc.exitCode === null) { b.proc.kill(); await b.proc.exited; }
    await rm(b.dir, { recursive: true, force: true });
  }
}, 60000);

test("MemoryOutboxStore + forwarder: drains a backlog accrued while the broker is down", async () => {
  const b = await makeBroker("anvil-outbox-drain-");
  b.spawn();
  const queue = new Queue<{ n: number }>("outbox", { address: b.address });
  const store = new MemoryOutboxStore();
  const forwarded: string[] = [];
  let drainedFired = 0;
  const forwarder = new OutboxForwarder<{ n: number }>(queue, store, {
    intervalMs: 50, onForwarded: key => forwarded.push(key), onDrained: () => { drainedFired++; },
  });
  try {
    await readyz(b.httpAddress);
    await until(async () => { try { await queue.add({ n: -1 }, { idempotencyKey: "warmup" }); return true; } catch { return false; } });

    // Take the broker down and enqueue N into the outbox; all stay pending.
    b.proc.kill(); await b.proc.exited;
    const N = 12;
    for (let i = 0; i < N; i++) await forwarder.enqueue({ n: i }, { idempotencyKey: `job-${i}` });
    expect(store.count("pending")).toBe(N);
    // A drain against a down broker forwards nothing (transient) and leaves them pending.
    forwarder.start();
    await sleep(200);
    expect(store.count("sent")).toBe(0);
    expect(store.count("pending")).toBe(N);

    // Bring the broker back on the SAME address/db; the loop drains everything to sent.
    b.spawn();
    await readyz(b.httpAddress);
    await until(() => store.count("sent") === N, 20000);
    await until(() => drainedFired >= 1);
    expect(forwarded.length).toBe(N);
    expect(store.count("pending")).toBe(0);
  } finally {
    await forwarder.stop();
    queue.close();
    if (b.proc.exitCode === null) { b.proc.kill(); await b.proc.exited; }
    await rm(b.dir, { recursive: true, force: true });
  }
}, 90000);

test("forwarder: a re-forward is deduped by the broker and a second drainOnce does not re-enqueue", async () => {
  const b = await makeBroker("anvil-outbox-dedupe-");
  b.spawn();
  const queue = new Queue<{ n: number }>("outbox-dedupe", { address: b.address });
  const store = new MemoryOutboxStore();
  const results: AddResult[] = [];
  const forwarder = new OutboxForwarder<{ n: number }>(queue, store, { onForwarded: (_k, r) => results.push(r) });
  try {
    await readyz(b.httpAddress);
    await until(async () => { try { await queue.add({ n: -1 }, { idempotencyKey: "warmup" }); return true; } catch { return false; } });

    const { key } = await forwarder.enqueue({ n: 1 }, { idempotencyKey: "dedupe-1" });
    const first = await forwarder.drainOnce();
    expect(first).toBe(1);
    expect(store.count("sent")).toBe(1);
    expect(results[0].ok && results[0].replayed).toBe(false);

    // A forced second drain finds nothing pending: no double enqueue.
    const second = await forwarder.drainOnce();
    expect(second).toBe(0);

    // Independently prove broker-side dedup: re-adding the held key replays the receipt.
    const replay = await queue.add({ n: 1 }, { idempotencyKey: key });
    expect(replay.replayed).toBe(true);
  } finally {
    await forwarder.stop();
    queue.close();
    b.proc.kill(); await b.proc.exited;
    await rm(b.dir, { recursive: true, force: true });
  }
}, 60000);

test("forwarder: a permanent error poisons the record and is surfaced, not swallowed", async () => {
  const b = await makeBroker("anvil-outbox-poison-");
  b.spawn();
  const queue = new Queue<unknown>("outbox-poison", { address: b.address });
  const store = new MemoryOutboxStore();
  const surfaced: AddResult[] = [];
  const forwarder = new OutboxForwarder(queue, store, { onForwarded: (_k, r) => surfaced.push(r) });
  try {
    await readyz(b.httpAddress);
    await until(async () => { try { await queue.add({ n: -1 }, { idempotencyKey: "warmup" }); return true; } catch { return false; } });

    // A depth far past the circuit-breaker limit is a permanent RESOURCE_EXHAUSTED:
    // put a poison record whose forwarded options carry an over-limit execution depth.
    await store.put({ key: "poison-1", queue: "outbox-poison", payload: { n: 1 }, options: { metadata: { executionDepth: 1_000_000 } }, state: "pending", attempts: 0, createdAt: Date.now() });
    const forwarded = await forwarder.drainOnce();
    expect(forwarded).toBe(1);
    expect(store.count("failed")).toBe(1);
    expect(store.get("poison-1")!.state).toBe("failed");
    expect(store.get("poison-1")!.lastError).toBeDefined();
    expect(surfaced.length).toBe(1);
    expect(surfaced[0].ok).toBe(false);
  } finally {
    await forwarder.stop();
    queue.close();
    b.proc.kill(); await b.proc.exited;
    await rm(b.dir, { recursive: true, force: true });
  }
}, 60000);

test("worker fan-out: enqueue-before-complete is exactly-once at the broker across a crash", async () => {
  const b = await makeBroker("anvil-outbox-fanout-");
  b.spawn();
  const parent = new Queue<{ k: number }>("fanout-parent", { address: b.address });
  const K = 4;
  const childrenSeen = new Set<string>();
  const replayedKeys = new Set<string>();
  let parentRuns = 0;
  // The parent 'crashes' (throws before completing) on its first attempt AFTER enqueuing
  // all children; the broker redelivers it, and the deterministic child keys replay.
  const worker = new Worker<{ k: number }>("fanout-parent", async (job: Job<{ k: number }>) => {
    parentRuns++;
    const children = Array.from({ length: K }, (_, i) => ({ name: "fanout-child", data: { i } }));
    const receipts = await job.enqueueChildBulk(children);
    for (const r of receipts) if (r.replayed) replayedKeys.add(r.id);
    if (job.attempts === 1) throw new Error("simulated crash before complete");
  }, { address: b.address, pollIntervalMs: 50, onError: () => {} });

  const childWorker = new Worker<{ i: number }>("fanout-child", async (job: Job<{ i: number }>) => {
    childrenSeen.add(job.id);
  }, { address: b.address, pollIntervalMs: 50, onError: () => {} });

  try {
    await readyz(b.httpAddress);
    await until(async () => { try { await parent.add({ k: 0 }, { idempotencyKey: "parent-1", maxAttempts: 3, retryBackoffMs: 100 }); return true; } catch { return false; } });
    // Parent must run at least twice (crash then redelivery) and children dedupe.
    await until(() => parentRuns >= 2, 30000);
    await until(() => childrenSeen.size === K, 30000);
    await sleep(500); // allow any stray duplicate child to (not) appear
    expect(childrenSeen.size).toBe(K); // exactly K distinct children despite re-enqueue
    expect(replayedKeys.size).toBeGreaterThan(0); // redelivery replayed the deterministic keys
  } finally {
    await worker.close();
    await childWorker.close();
    parent.close();
    b.proc.kill(); await b.proc.exited;
    await rm(b.dir, { recursive: true, force: true });
  }
}, 90000);
