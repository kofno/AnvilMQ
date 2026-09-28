// Bounded soak + leak check for the C1 transport-resilience machinery.
//
// It drives induced connect/drop cycles (short-lived workers that connect, claim,
// and close; plus broker kill/restart outages that exercise the poll-loop backoff and
// connection-health bookkeeping) and samples process memory after each cycle, failing
// if RSS/heap trends monotonically upward or grows past tolerance. It then runs an idle
// keepalive window to confirm the broker does not answer client keepalive PINGs with a
// too_many_pings GOAWAY (permit-without-calls verification).
//
// TODO(C7 publishing): once the client ships transpiled JS (not raw .ts via `exports`),
// add a Node-LTS-native runner for this soak. Node's strip-only .ts loader rejects the
// library's TypeScript parameter properties, so today the soak runs under Bun only.
//
// Run:  bun test/soak.ts
// Env:  SOAK_CYCLES (default 30), SOAK_OUTAGES (default 4), SOAK_IDLE_MS (default 90000),
//       SOAK_RSS_TOLERANCE (default 0.35)

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import { Queue, Worker } from "../src/index";

const CYCLES = Number(process.env.SOAK_CYCLES ?? 30);
const WARMUP = Number(process.env.SOAK_WARMUP ?? 6);
const OUTAGES = Number(process.env.SOAK_OUTAGES ?? 4);
const IDLE_MS = Number(process.env.SOAK_IDLE_MS ?? 90000);
const RSS_TOLERANCE = Number(process.env.SOAK_RSS_TOLERANCE ?? 0.35);

async function until(check: () => boolean | Promise<boolean>, timeout = 20000, label = "condition") {
  const end = Date.now() + timeout;
  while (!(await check())) { if (Date.now() > end) throw new Error(`${label} timed out`); await sleep(25); }
}
async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>(r => server.close(() => r()));
  return port;
}
function mib(bytes: number): number { return Math.round((bytes / 1024 / 1024) * 100) / 100; }

const dir = await mkdtemp(join(tmpdir(), "anvil-soak-"));
const address = `127.0.0.1:${await freePort()}`;
const httpAddress = `127.0.0.1:${await freePort()}`;
const dbPath = join(dir, "soak.db");
const executable = resolve("../target/debug/" + (process.platform === "win32" ? "rusty-queue.exe" : "rusty-queue"));
const spawnBroker = () => Bun.spawn([executable], { cwd: dir, env: { ...process.env, RUST_LOG: "info", ANVILMQ_ADDR: address, ANVILMQ_HTTP_ADDR: httpAddress, ANVILMQ_DB_PATH: dbPath }, stdout: "pipe", stderr: "pipe" });

let server = spawnBroker();
let brokerLog = "";
const drain = (s: ReturnType<typeof Bun.spawn>) => {
  void (async () => { try { for await (const c of s.stdout as ReadableStream<Uint8Array>) brokerLog += new TextDecoder().decode(c); } catch { /* ignore */ } })();
  void (async () => { try { for await (const c of s.stderr as ReadableStream<Uint8Array>) brokerLog += new TextDecoder().decode(c); } catch { /* ignore */ } })();
};
drain(server);

const readyz = async () => { try { return (await fetch(`http://${httpAddress}/readyz`)).status === 200; } catch { return false; } };
async function outageCycle() {
  const idleWorker = new Worker("soak", async () => {}, { address, pollIntervalMs: 25, pollBackoffMaxMs: 400, connectionLostThreshold: 3, onError: () => {} });
  server.kill(); await server.exited;
  await sleep(600); // let workers climb into backoff
  server = spawnBroker(); drain(server);
  await until(readyz, 10000, "readyz after restart");
  await until(() => idleWorker.isConnected, 10000, "reconnect");
  await idleWorker.close();
}
const rss: number[] = [];
const heap: number[] = [];
function sample() { Bun.gc(true); const m = process.memoryUsage(); rss.push(m.rss); heap.push(m.heapUsed); }

let failed = false;
try {
  await until(readyz, 10000, "initial readyz");

  // Phase A: induced connect/drop cycles with periodic outages. The first WARMUP cycles
  // are unsampled so grpc/proto/JIT one-time allocations don't masquerade as a leak.
  const total = WARMUP + CYCLES;
  const outageEvery = Math.max(1, Math.floor(total / Math.max(1, OUTAGES)));
  for (let i = 0; i < total; i++) {
    const queue = new Queue<{ n: number }>("soak", { address, rpcTimeoutMs: 3000 });
    let processed = false;
    const worker = new Worker<{ n: number }>("soak", async () => { processed = true; }, {
      address, pollIntervalMs: 25, pollBackoffMaxMs: 400, connectionLostThreshold: 3,
      heartbeatIntervalMs: 5000, onError: () => {},
    });
    try {
      await until(async () => { try { await queue.add({ n: i }); return true; } catch { return false; } }, 8000, "enqueue");
      await until(() => processed, 8000, "processed");
    } finally { await worker.close(); queue.close(); }

    // Every so often, drop the broker and bring it back to exercise backoff + reconnect.
    if (OUTAGES > 0 && i > 0 && i % outageEvery === 0) await outageCycle();
    if (i < WARMUP) {
      // Pay the one-time reconnect-path allocation cost during warmup so a single step
      // doesn't masquerade as a leak in the measured window.
      if (i === WARMUP - 1 && OUTAGES > 0) await outageCycle();
      Bun.gc(true); process.stdout.write(`warmup ${i + 1}/${WARMUP}\n`); continue;
    }
    if (i < WARMUP) { Bun.gc(true); process.stdout.write(`warmup ${i + 1}/${WARMUP}\n`); continue; }
    sample();
    process.stdout.write(`cycle ${i - WARMUP + 1}/${CYCLES} rss=${mib(rss[rss.length - 1])}MiB heap=${mib(heap[heap.length - 1])}MiB\n`);
  }

  // Phase B: idle keepalive / GOAWAY verification.
  process.stdout.write(`idle keepalive window: ${IDLE_MS}ms (permit-without-calls PINGs on an idle channel)\n`);
  const idleQueue = new Queue<{ probe: number }>("soak-idle", { address, rpcTimeoutMs: 3000 });
  const idleLost: number[] = [];
  const idleWorker = new Worker("soak-idle-worker", async () => {}, {
    address, pollIntervalMs: 250, connectionLostThreshold: 3, onError: () => {},
    onConnectionLost: info => idleLost.push(info.consecutiveFailures),
  });
  try {
    await idleQueue.add({ probe: 0 }); // establish the channel
    await sleep(IDLE_MS);
    await idleQueue.add({ probe: 1 }); // must still succeed after the idle window
    process.stdout.write("post-idle probe RPC succeeded\n");
  } finally { await idleWorker.close(); idleQueue.close(); }

  const goaway = /too_many_pings|GOAWAY|ENHANCE_YOUR_CALM/i.test(brokerLog);
  if (goaway) { failed = true; process.stdout.write("FAIL: broker emitted a GOAWAY/too_many_pings during idle keepalive\n"); }
  else process.stdout.write("keepalive OK: no GOAWAY/too_many_pings observed during idle window\n");
  if (idleLost.length > 0) { failed = true; process.stdout.write(`FAIL: idle worker lost its connection ${idleLost.length}x during keepalive window\n`); }

  // Trend analysis on RSS (primary) and heap (secondary).
  const analyze = (series: number[], name: string) => {
    const third = Math.max(1, Math.floor(series.length / 3));
    const avg = (a: number[]) => a.reduce((s, v) => s + v, 0) / a.length;
    const first = avg(series.slice(0, third));
    const last = avg(series.slice(-third));
    const growth = (last - first) / first;
    let monotonic = true;
    for (let i = 1; i < series.length; i++) if (series[i] <= series[i - 1]) { monotonic = false; break; }
    process.stdout.write(`${name}: first-third=${mib(first)}MiB last-third=${mib(last)}MiB growth=${(growth * 100).toFixed(1)}% monotonic=${monotonic}\n`);
    // A genuine leak shows sustained growth. A strictly-increasing series is only a red
    // flag when it also creeps past a floor; coarse RSS often ratchets up trivially.
    if (monotonic && series.length > 4 && growth > 0.15) { process.stdout.write(`FAIL: ${name} increased monotonically with ${(growth * 100).toFixed(1)}% growth\n`); return false; }
    if (growth > RSS_TOLERANCE) { process.stdout.write(`FAIL: ${name} grew ${(growth * 100).toFixed(1)}% > tolerance ${(RSS_TOLERANCE * 100).toFixed(0)}%\n`); return false; }
    return true;
  };
  if (!analyze(rss, "rss")) failed = true;
  if (!analyze(heap, "heap")) failed = true;
} finally {
  if (server.exitCode === null) { server.kill(); await server.exited; }
  await rm(dir, { recursive: true, force: true });
}

process.stdout.write(failed ? "SOAK RESULT: FAIL\n" : "SOAK RESULT: PASS\n");
process.exit(failed ? 1 : 0);
