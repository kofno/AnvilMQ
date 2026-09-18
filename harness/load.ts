import { Queue, Worker } from "../client/src/index";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { cpus, totalmem } from "node:os";

function number(name: string, fallback: number, min: number, max: number) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < min || value > max) throw new Error(`${name} must be an integer in [${min}, ${max}]`);
  return value;
}
const config = {
  producers: number("LOAD_PRODUCERS", 4, 1, 128), workers: number("LOAD_WORKERS", 4, 1, 128),
  durationSeconds: number("LOAD_DURATION_SECONDS", 15, 1, 300), warmupSeconds: number("LOAD_WARMUP_SECONDS", 3, 0, 60),
  payloadBytes: number("LOAD_PAYLOAD_BYTES", 1024, 0, 1000000), workMs: number("LOAD_WORK_MS", 0, 0, 30000),
  targetRate: number("LOAD_RATE", 0, 0, 100000), pollMs: number("LOAD_POLL_MS", 10, 1, 1000),
  drainSeconds: number("LOAD_DRAIN_SECONDS", 60, 1, 600),
};
const address = process.env.ANVILMQ_ADDR ?? "127.0.0.1:50061";
const http = process.env.ANVILMQ_HTTP_URL ?? "http://127.0.0.1:9091";
const results = process.env.ANVILMQ_RESULTS_DIR ?? "harness/artifacts";
const runId = new Date().toISOString().replaceAll(/[:.]/g, "-") + "-" + crypto.randomUUID().slice(0, 8);
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
async function phase(seconds: number, name: string) {
  const enqueue = new Samples(), startLatency = new Samples(), completion = new Samples();
  let submitted = 0, acknowledged = 0, completed = 0, duringWindow = 0, producerErrors = 0, workerErrors = 0, duplicates = 0;
  const examples: string[] = [];
  const started = new Map<string, number>();
  const completedIds = new Set<string>();
  const begin = performance.now(), end = begin + seconds * 1000;
  let slot = begin;
  const error = (e: unknown) => { if (examples.length < 10) examples.push(String(e)); };
  const workers = Array.from({ length: config.workers }, () => new Worker<{ sent: number; padding: string }>(name, async (job, signal) => {
    if (started.has(job.id) || completedIds.has(job.id)) duplicates++;
    started.set(job.id, job.data.sent);
    startLatency.add(performance.now() - job.data.sent);
    if (config.workMs) await sleep(config.workMs, undefined, { signal });
  }, { address, pollIntervalMs: config.pollMs, onError: e => { workerErrors++; error(e); }, onCompleted: id => {
    const now = performance.now();
    const sent = started.get(id);
    if (sent !== undefined) completion.add(now - sent);
    if (!completedIds.has(id)) { completedIds.add(id); completed++; if (now <= end) duringWindow++; }
    started.delete(id);
  } }));
  const queues = Array.from({ length: config.producers }, () => new Queue(name, { address }));
  const padding = "x".repeat(config.payloadBytes);
  const timeline: { elapsedSeconds: number; submitted: number; acknowledged: number; completed: number; backlog: number }[] = [];
  const sample = () => timeline.push({ elapsedSeconds: (performance.now() - begin) / 1000, submitted, acknowledged, completed, backlog: Math.max(0, acknowledged - completed) });
  const sampler = setInterval(sample, 1000);
  let drainTimedOut = false;
  try {
    await Promise.all(queues.map(async queue => {
      while (performance.now() < end) {
        if (config.targetRate) {
          const due = slot; slot += 1000 / config.targetRate;
          if (due >= end) break;
          if (due > performance.now()) await sleep(due - performance.now());
          if (performance.now() >= end) break;
        }
        const sent = performance.now(); submitted++;
        try { await queue.add({ sent, padding }, { maxAttempts: 1 }); acknowledged++; enqueue.add(performance.now() - sent); }
        catch (e) { producerErrors++; error(e); }
      }
    }));
    const deadline = performance.now() + config.drainSeconds * 1000;
    while (completed < acknowledged && performance.now() < deadline) await sleep(50);
    drainTimedOut = completed < acknowledged;
  } finally {
    queues.forEach(q => q.close());
    await Promise.all(workers.map(w => w.close()));
    clearInterval(sampler); sample();
  }
  const elapsedSeconds = (performance.now() - begin) / 1000;
  return {
    passed: !drainTimedOut && !producerErrors && !workerErrors && !duplicates && completed === acknowledged,
    timeline, submitted, acknowledged, completed, completedDuringWindow: duringWindow, producerErrors, workerErrors, duplicates, drainTimedOut, errorExamples: examples,
    measurementSeconds: seconds, elapsedIncludingDrainSeconds: elapsedSeconds, drainSeconds: Math.max(0, elapsedSeconds - seconds),
    enqueuePerSecond: acknowledged / seconds, completionsPerSecondDuringWindow: duringWindow / seconds, completionsPerSecondIncludingDrain: completed / elapsedSeconds,
    latencyMs: { enqueueRpc: enqueue.report(), submissionToHandler: startLatency.report(), submissionToCompletionAck: completion.report() },
  };
}
await mkdir(results, { recursive: true });
let before = "";
try {
  before = await metrics();
  if (config.warmupSeconds) {
    const warmup = await phase(config.warmupSeconds, `warmup-${runId}`);
    if (!warmup.passed) throw new Error(`Warmup failed: ${JSON.stringify(warmup)}`);
  }
  before = await metrics();
  const result = await phase(config.durationSeconds, `load-${runId}`);
  const after = await metrics();
  const report = { runId, timestamp: new Date().toISOString(), config, environment: { bun: Bun.version, platform: process.platform, arch: process.arch, visibleCpuCount: cpus().length, visibleMemoryBytes: totalmem() }, ...result };
  const f = (n: number | null) => n === null ? "n/a" : n.toFixed(2);
  const rows = Object.entries(result.latencyMs).map(([name, v]) => `| ${name} | ${f(v.p50)} | ${f(v.p95)} | ${f(v.p99)} | ${f(v.max)} |`).join("\n");
  const markdown = `# AnvilMQ load report\n\nRun: ${runId}\n\nResult: ${result.passed ? "PASS" : "FAIL"}\n\n${config.producers} producers, ${config.workers} workers, ${config.durationSeconds}s measured, ${config.warmupSeconds}s warmup, ${config.payloadBytes} padding bytes, ${config.workMs}ms handler delay; target ${config.targetRate || "unlimited"} jobs/s.\n\n- Enqueue: ${f(result.enqueuePerSecond)} jobs/s\n- Completion during window: ${f(result.completionsPerSecondDuringWindow)} jobs/s\n- Completion including drain: ${f(result.completionsPerSecondIncludingDrain)} jobs/s\n- Acknowledged/completed: ${result.acknowledged}/${result.completed}\n- Producer/worker errors: ${result.producerErrors}/${result.workerErrors}\n- Duplicate deliveries: ${result.duplicates}; drain timeout: ${result.drainTimedOut}\n- Drain/shutdown: ${f(result.drainSeconds)}s\n\n| Client latency (ms) | p50 | p95 | p99 | max |\n| --- | ---: | ---: | ---: | ---: |\n${rows}\n\nThese are client-observed timings, including container networking and JS scheduling. Submission-to-handler includes enqueue and queue wait; completion includes handler work and the completion RPC. Padding excludes JSON metadata/envelope bytes. Closed-loop producers have at most one outstanding enqueue each; target rate is a ceiling, not guaranteed offered load. No fault injection. Successful RPC samples only. Reservoir sampling begins after 200,000 observations per distribution. Compare only runs with matching durability, logging, host resources, and database history size.\n`;
  for (const prefix of [runId, "latest"]) {
    await writeFile(join(results, `${prefix}.json`), JSON.stringify(report, null, 2));
    await writeFile(join(results, `${prefix}.md`), markdown);
  }
  await writeFile(join(results, `${runId}-metrics-before.txt`), before);
  await writeFile(join(results, `${runId}-metrics-after.txt`), after);
  console.log(markdown);
  if (!result.passed) process.exitCode = 1;
} catch (error) {
  await writeFile(join(results, `${runId}-error.json`), JSON.stringify({ runId, config, passed: false, error: String(error) }, null, 2));
  console.error(error); process.exitCode = 1;
}
