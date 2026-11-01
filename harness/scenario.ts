import { Queue, Worker } from "../client/src/index";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { cpus, totalmem } from "node:os";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

// C1 — representative cron + bulk workload simulation.
//
// Unlike `load.ts` (a closed-loop Queue.add/Worker throughput test), this scenario drives the two
// Milestone 1 broker features that `load.ts` never exercises: A5 cron schedulers
// (UpsertJobScheduler + the background producer loop) and A3 atomic bulk enqueue (AddJobBulk). It
// asserts exactly-once, atomicity, and no dupes/errors, then reports throughput/latency.
//
// The client library only wraps AddJob/GetNextJob/Complete/Fail, so the bulk and scheduler RPCs are
// driven over a raw gRPC client built the same way the client package does (mirrors crash-active.ts).

const require = createRequire(new URL("../client/package.json", import.meta.url));
const grpc = require("@grpc/grpc-js");
const loader = require("@grpc/proto-loader");
// loadSync WITHOUT keepCase => proto-loader exposes camelCase field names on the wire objects.
const api = grpc.loadPackageDefinition(loader.loadSync(fileURLToPath(new URL("../proto/queue.proto", import.meta.url)), {
  longs: Number, defaults: true, bytes: Buffer,
}));

function number(name: string, fallback: number, min: number, max: number) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < min || value > max) throw new Error(`${name} must be an integer in [${min}, ${max}]`);
  return value;
}
function float(name: string, fallback: number, min: number, max: number) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isFinite(value) || value < min || value > max) throw new Error(`${name} must be a number in [${min}, ${max}]`);
  return value;
}
const config = {
  schedulers: number("SCENARIO_SCHEDULERS", 50, 1, 500),
  minutes: number("SCENARIO_MINUTES", 3, 1, 15),
  workers: number("SCENARIO_WORKERS", 8, 1, 128),
  bulkBatches: number("SCENARIO_BULK_BATCHES", 5, 1, 1000),
  bulkSize: number("SCENARIO_BULK_SIZE", 2000, 1, 5000),
  bulkIntervalMs: number("SCENARIO_BULK_INTERVAL_MS", 1000, 0, 60000),
  payloadBytes: number("SCENARIO_PAYLOAD_BYTES", 256, 0, 1000000),
  drainSeconds: number("SCENARIO_DRAIN_SECONDS", 120, 1, 600),
  pollMs: number("SCENARIO_POLL_MS", 10, 1, 1000),
};
// Ensure the configured float helper is referenced (kept for parity with load.ts validation style).
void float;

const address = process.env.ANVILMQ_ADDR ?? "127.0.0.1:50061";
const http = process.env.ANVILMQ_HTTP_URL ?? "http://127.0.0.1:9091";
const results = process.env.ANVILMQ_RESULTS_DIR ?? "harness/artifacts";
const runId = new Date().toISOString().replaceAll(/[:.]/g, "-") + "-" + crypto.randomUUID().slice(0, 8);

const grpcClient = new api.queue.v1.QueueService(address, grpc.credentials.createInsecure());
function call<T>(method: string, request: object, timeoutMs = 15000): Promise<T> {
  return new Promise((resolve, reject) => grpcClient[method](request, { deadline: Date.now() + timeoutMs },
    (error: unknown, response: T) => error ? reject(error) : resolve(response)));
}

class Samples {
  values: number[] = []; count = 0; sum = 0; max = 0;
  add(ms: number) {
    this.count++; this.sum += ms; this.max = Math.max(this.max, ms);
    if (this.values.length < 200000) this.values.push(ms);
    else { const index = Math.floor(Math.random() * this.count); if (index < this.values.length) this.values[index] = ms; }
  }
  report() {
    const values = this.values.sort((a, b) => a - b);
    const percentile = (p: number) => values.length ? values[Math.ceil(p * values.length) - 1] : null;
    return { count: this.count, sampled: values.length, mean: this.count ? this.sum / this.count : null, p50: percentile(.5), p95: percentile(.95), p99: percentile(.99), max: this.count ? this.max : null };
  }
}

async function metrics() {
  const response = await fetch(`${http}/metrics`, { signal: AbortSignal.timeout(5000) });
  if (!response.ok) throw new Error(`metrics HTTP ${response.status}`);
  return response.text();
}
function counter(text: string, name: string): number {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const m = text.match(new RegExp(`^${escaped} (-?\\d+(?:\\.\\d+)?)$`, "m"));
  return m ? Number(m[1]) : NaN;
}
function completedJobs(text: string): number {
  return Number(text.match(/anvilmq_jobs\{state="Completed"\} (\d+)/)?.[1] ?? NaN);
}
function schedulerSnapshot(text: string) {
  return {
    produced: counter(text, "anvilmq_scheduler_jobs_produced_total"),
    errors: counter(text, "anvilmq_scheduler_errors_total"),
    active: counter(text, "anvilmq_schedulers_active"),
    completed: completedJobs(text),
  };
}

async function waitReady() {
  const deadline = Date.now() + 60000;
  while (Date.now() < deadline) {
    try { if ((await fetch(`${http}/readyz`, { signal: AbortSignal.timeout(2000) })).ok) return; } catch { /* retry */ }
    await sleep(500);
  }
  throw new Error("broker not ready within 60s");
}

interface TimelinePoint { elapsedSeconds: number; delivered: number; completed: number; produced: number }

/** A recurring 1s sampler that records a local/remote counter timeline without overlapping fetches. */
function startSampler(begin: number, snapshot: () => Promise<TimelinePoint>) {
  const timeline: TimelinePoint[] = [];
  let busy = false;
  const handle = setInterval(() => {
    if (busy) return; busy = true;
    void (async () => { try { timeline.push(await snapshot()); } catch { /* ignore transient */ } })().finally(() => { busy = false; });
  }, 1000);
  return { timeline, stop: () => clearInterval(handle) };
}

async function phaseCron() {
  const beforeText = await metrics();
  const before = schedulerSnapshot(beforeText);
  const sharedName = `c1-cron-${runId}`;
  const payload = Buffer.from(JSON.stringify({ padding: "c".repeat(config.payloadBytes) }));

  // Register N schedulers with DISTINCT ids but the SAME template.name. The produced job's queue
  // name == scheduler.job_name == template.name (verified in src/scheduler.rs), so all produced
  // jobs land under `sharedName` and a single Worker pool drains them.
  const schedulerIds: string[] = [];
  const nextRuns: number[] = [];
  for (let i = 0; i < config.schedulers; i++) {
    const id = `c1-cron-${i}-${runId}`;
    const resp = await call<{ id: string; nextRunAt: number }>("UpsertJobScheduler", {
      id,
      template: { name: sharedName, payload, maxAttempts: 1 },
      repeat: { cron: "* * * * *", timezone: "" },
    });
    schedulerIds.push(id);
    nextRuns.push(resp.nextRunAt);
  }

  const delivered = new Set<string>();
  let duplicates = 0, workerErrors = 0, completed = 0;
  const errorExamples: string[] = [];
  const recordError = (e: unknown) => { if (errorExamples.length < 10) errorExamples.push(String(e)); };
  const workers = Array.from({ length: config.workers }, () => new Worker<unknown>(sharedName, async job => {
    if (delivered.has(job.id)) duplicates++; else delivered.add(job.id);
  }, { address, pollIntervalMs: config.pollMs, onError: e => { workerErrors++; recordError(e); }, onCompleted: () => { completed++; } }));

  const begin = performance.now();
  const sampler = startSampler(begin, async () => ({
    elapsedSeconds: (performance.now() - begin) / 1000,
    delivered: delivered.size,
    completed,
    produced: counter(await metrics(), "anvilmq_scheduler_jobs_produced_total") - before.produced,
  }));

  // Observe several minute boundaries. Standard 5-field cron has 1-minute granularity, so this must
  // run whole real minutes to see multiple fires per scheduler.
  await sleep(config.minutes * 60 * 1000);

  // Settle: wait until the produced counter is stable across two consecutive reads, so we snapshot
  // and tear down mid-minute (no fire in flight). This makes produced/producedCount/unique/completed
  // mutually consistent for the exactly-once verdict.
  let last = NaN, stable = 0;
  const settleDeadline = performance.now() + 65000;
  while (stable < 2 && performance.now() < settleDeadline) {
    await sleep(1000);
    const p = counter(await metrics(), "anvilmq_scheduler_jobs_produced_total");
    if (p === last) stable++; else { stable = 0; last = p; }
  }

  const atWindowText = await metrics();
  const atWindow = schedulerSnapshot(atWindowText);

  // Read per-scheduler produced_count while the schedulers still exist (deltas off a fresh id == 0).
  const list = await call<{ schedulers: Array<Record<string, unknown>> }>("ListJobSchedulers", {});
  const mine = list.schedulers.filter(s => schedulerIds.includes(String(s.id)));
  const producedCounts = mine.map(s => Number(s.producedCount));
  const casingSample = mine[0] ?? null;

  // Teardown: stop every scheduler we created BEFORE draining so production halts and `completed`
  // can converge to a fixed `produced` total.
  for (const id of schedulerIds) await call("DeleteJobScheduler", { id });

  const producedDelta = atWindow.produced - before.produced;
  const deadline = performance.now() + config.drainSeconds * 1000;
  while (completed < producedDelta && performance.now() < deadline) await sleep(50);
  const drainTimedOut = completed < producedDelta;

  sampler.stop();
  await Promise.all(workers.map(w => w.close()));

  const afterText = await metrics();
  const after = schedulerSnapshot(afterText);

  const activeDelta = atWindow.active - before.active;
  const errorsDelta = atWindow.errors - before.errors;
  const uniqueDelivered = delivered.size;
  const sumProducedCounts = producedCounts.reduce((a, b) => a + b, 0);
  const minProduced = producedCounts.length ? Math.min(...producedCounts) : 0;
  const maxProduced = producedCounts.length ? Math.max(...producedCounts) : 0;

  const gateActive = activeDelta === config.schedulers;
  const gateEachProduced = producedCounts.length === config.schedulers && producedCounts.every(c => c >= 1);
  const gateUniform = producedCounts.length > 0 && (maxProduced - minProduced) <= 1;
  const gateNoSchedulerErrors = errorsDelta === 0;
  const gateExactlyOnce = uniqueDelivered === producedDelta && sumProducedCounts === producedDelta
    && duplicates === 0 && completed === producedDelta && !drainTimedOut;
  const gateNoWorkerErrors = workerErrors === 0;
  const passed = gateActive && gateEachProduced && gateUniform && gateNoSchedulerErrors && gateExactlyOnce && gateNoWorkerErrors;

  return {
    passed,
    gates: { active: gateActive, eachProduced: gateEachProduced, uniform: gateUniform, noSchedulerErrors: gateNoSchedulerErrors, exactlyOnce: gateExactlyOnce, noWorkerErrors: gateNoWorkerErrors },
    schedulers: config.schedulers, workers: config.workers, minutes: config.minutes,
    activeDelta, producedMetricDelta: producedDelta, sumProducedCounts, errorsDelta,
    perSchedulerProduced: { min: minProduced, max: maxProduced }, uniqueDelivered, completed, duplicates, workerErrors, drainTimedOut,
    firstNextRunAt: nextRuns[0] ?? null, casingSample, errorExamples,
    metricsDeltas: { completedJobs: after.completed - before.completed },
    timeline: sampler.timeline,
  };
}

async function phaseBulk() {
  const beforeText = await metrics();
  const baseCompleted = completedJobs(beforeText);
  const name = `c1-bulk-${runId}`;
  const payload = Buffer.from(JSON.stringify({ padding: "b".repeat(config.payloadBytes) }));

  const delivered = new Set<string>();
  let duplicates = 0, workerErrors = 0, completed = 0;
  const errorExamples: string[] = [];
  const recordError = (e: unknown) => { if (errorExamples.length < 10) errorExamples.push(String(e)); };
  const workers = Array.from({ length: config.workers }, () => new Worker<unknown>(name, async job => {
    if (delivered.has(job.id)) duplicates++; else delivered.add(job.id);
  }, { address, pollIntervalMs: config.pollMs, onError: e => { workerErrors++; recordError(e); }, onCompleted: () => { completed++; } }));

  const enqueue = new Samples();
  const batchSizes: number[] = [];
  let totalEnqueued = 0, emptyIds = 0;
  const enqueueBegin = performance.now();
  for (let b = 0; b < config.bulkBatches; b++) {
    const requests = Array.from({ length: config.bulkSize }, () => ({ name, payload, maxAttempts: 1 }));
    const sent = performance.now();
    const resp = await call<{ responses: Array<{ id: string }> }>("AddJobBulk", { requests }, 60000);
    enqueue.add(performance.now() - sent);
    const ids = (resp.responses ?? []).map(r => r.id);
    batchSizes.push(ids.length);
    for (const id of ids) if (!id) emptyIds++;
    totalEnqueued += ids.length;
    if (b < config.bulkBatches - 1 && config.bulkIntervalMs) await sleep(config.bulkIntervalMs);
  }
  const enqueueSeconds = (performance.now() - enqueueBegin) / 1000;

  const expected = config.bulkBatches * config.bulkSize;
  const drainBegin = performance.now();
  const deadline = drainBegin + config.drainSeconds * 1000;
  while (completed < expected && performance.now() < deadline) await sleep(50);
  const drainTimedOut = completed < expected;
  const totalSeconds = (performance.now() - enqueueBegin) / 1000;

  await Promise.all(workers.map(w => w.close()));

  const afterText = await metrics();

  const gateAtomic = batchSizes.length === config.bulkBatches && batchSizes.every(s => s === config.bulkSize) && emptyIds === 0;
  const gateCompleted = completed === expected && !drainTimedOut;
  const gateNoDupes = duplicates === 0;
  const gateNoWorkerErrors = workerErrors === 0;
  const passed = gateAtomic && gateCompleted && gateNoDupes && gateNoWorkerErrors;

  return {
    passed,
    gates: { atomic: gateAtomic, completed: gateCompleted, noDupes: gateNoDupes, noWorkerErrors: gateNoWorkerErrors },
    batches: config.bulkBatches, bulkSize: config.bulkSize, workers: config.workers, expected,
    totalEnqueued, emptyIds, batchSizesMin: Math.min(...batchSizes), batchSizesMax: Math.max(...batchSizes),
    uniqueDelivered: delivered.size, completed, duplicates, workerErrors, drainTimedOut,
    enqueueSeconds, enqueueOpsPerSecond: enqueueSeconds > 0 ? totalEnqueued / enqueueSeconds : null,
    completionPerSecond: totalSeconds > 0 ? completed / totalSeconds : null,
    enqueueRpcLatencyMs: enqueue.report(),
    metricsDeltas: { completedJobs: completedJobs(afterText) - baseCompleted },
    errorExamples,
  };
}

await mkdir(results, { recursive: true });
let beforeAll = "";
try {
  await waitReady();
  beforeAll = await metrics();
  const cron = await phaseCron();
  const bulk = await phaseBulk();
  const afterAll = await metrics();
  const passed = cron.passed && bulk.passed;

  const report = {
    runId, mode: "scenario", timestamp: new Date().toISOString(), passed, config,
    environment: { bun: Bun.version, platform: process.platform, arch: process.arch, visibleCpuCount: cpus().length, visibleMemoryBytes: totalmem() },
    phaseA: cron, phaseB: bulk,
  };

  const f = (n: number | null | undefined) => n === null || n === undefined || Number.isNaN(n) ? "n/a" : n.toFixed(2);
  const yn = (b: boolean) => (b ? "yes" : "no");
  const lat = bulk.enqueueRpcLatencyMs;
  const markdown = `# AnvilMQ scenario report (C1 cron + bulk)

Run: ${runId}

Result: ${passed ? "PASS" : "FAIL"}

Representative workload: ${config.schedulers} every-minute cron schedulers over ${config.minutes} min, plus ${config.bulkBatches} atomic bulk batches of ${config.bulkSize} jobs. ${config.workers} workers, ${config.payloadBytes} payload bytes. Cron has a hard 1-minute granularity, so Phase A runs whole real minutes to observe multiple fires.

## Phase A — cron schedulers: ${cron.passed ? "PASS" : "FAIL"}

- Schedulers active delta: ${cron.activeDelta} (expected ${config.schedulers}) — ${yn(cron.gates.active)}
- Jobs produced (metric delta): ${cron.producedMetricDelta}; sum of per-scheduler produced_count: ${cron.sumProducedCounts}
- Per-scheduler produced_count: min ${cron.perSchedulerProduced.min}, max ${cron.perSchedulerProduced.max} (uniform ±1 — ${yn(cron.gates.uniform)}; each ≥ 1 — ${yn(cron.gates.eachProduced)})
- Scheduler errors delta: ${cron.errorsDelta} (expected 0 — ${yn(cron.gates.noSchedulerErrors)})
- Exactly-once: unique delivered ${cron.uniqueDelivered} == produced ${cron.producedMetricDelta} == completed ${cron.completed}, duplicates ${cron.duplicates} — ${yn(cron.gates.exactlyOnce)}
- Worker errors: ${cron.workerErrors}; drain timeout: ${yn(cron.drainTimedOut)}

## Phase B — atomic bulk enqueue: ${bulk.passed ? "PASS" : "FAIL"}

- Atomicity: ${bulk.batches} batches, each returned exactly ${config.bulkSize} ids (min ${bulk.batchSizesMin}, max ${bulk.batchSizesMax}), empty ids ${bulk.emptyIds} — ${yn(bulk.gates.atomic)}
- Completion: ${bulk.completed}/${bulk.expected} completed, duplicates ${bulk.duplicates}, drain timeout ${yn(bulk.drainTimedOut)} — ${yn(bulk.gates.completed && bulk.gates.noDupes)}
- Worker errors: ${bulk.workerErrors}
- Enqueue throughput: ${f(bulk.enqueueOpsPerSecond)} jobs/s over ${f(bulk.enqueueSeconds)}s; completion throughput ${f(bulk.completionPerSecond)} jobs/s

| Bulk enqueue-RPC latency (ms) | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: |
| AddJobBulk round-trip | ${f(lat.p50)} | ${f(lat.p95)} | ${f(lat.p99)} | ${f(lat.max)} |

These are client-observed timings, including container networking and JS scheduling; they are not a capacity guarantee. Cron fire counts follow wall-clock minute boundaries, not an exact predicted count. Enqueue latency is the AddJobBulk round-trip (one atomic transaction admitting ${config.bulkSize} jobs). Run one scenario at a time against a dedicated broker, since completion/scheduler counters include all clients.
`;

  for (const prefix of [runId, "latest"]) {
    await writeFile(join(results, `${prefix}.json`), JSON.stringify(report, null, 2));
    await writeFile(join(results, `${prefix}.md`), markdown);
  }
  await writeFile(join(results, `${runId}-metrics-before.txt`), beforeAll);
  await writeFile(join(results, `${runId}-metrics-after.txt`), afterAll);
  console.log(markdown);
  if (cron.casingSample) console.log("ListJobSchedulers sample row (field casing):", JSON.stringify(cron.casingSample));
  if (!passed) process.exitCode = 1;
} catch (error) {
  await writeFile(join(results, `${runId}-error.json`), JSON.stringify({ runId, mode: "scenario", config, passed: false, error: String(error) }, null, 2)).catch(() => {});
  console.error(error); process.exitCode = 1;
}
