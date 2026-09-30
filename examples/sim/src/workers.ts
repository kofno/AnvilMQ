// Synthetic workers, running against AnvilMQ with simulated side effects only.
//
// In a real deployment these handlers would call downstream services (a database, an
// event bus, etc.). Here they use bounded sleeps or CPU work and the local broker only.
import {
  Worker, gracefulShutdown, fetchHealthHandler,
  type Job, type WorkerHealth, type WorkerMetrics, type Logger,
} from "../../../client/src/index";
import { setTimeout as sleep } from "node:timers/promises";
import {
  ADDRESS, ALL_JOB_NAMES, JobName, num, closeAllQueues, GRPC_RESOURCE_EXHAUSTED,
  type TaskData, type SubtaskData, type RecordData,
  type RecursiveProbeData, type JobData, type JobName as JobNameT,
} from "./queues";

// Per-name parallelism: SIM_WORKER_CONCURRENCY now maps to the client's `concurrency`
// option, so one Worker per name runs up to this many handlers in parallel over a single
// connection (replacing the previous N distinct single-job Worker objects per name).
const concurrency = num("SIM_WORKER_CONCURRENCY", 2, 1, 64);
const healthPort = num("SIM_HEALTH_PORT", 9095, 1, 65535);
const workMs = num("SIM_WORK_MS", 15, 0, 5000);
const tasksPerEvent = num("SIM_TASKS_PER_EVENT", 2, 0, 20);
const subtasksPerTask = num("SIM_SUBTASKS_PER_TASK", 3, 0, 20);
const pollMs = num("SIM_POLL_MS", 10, 1, 1000);

// Jittered simulated I/O so latency histograms look realistic instead of a single spike.
function ioLatency(): number {
  return workMs === 0 ? 0 : Math.round(workMs * (0.5 + Math.random()));
}
async function simulateIo(signal: AbortSignal): Promise<void> {
  const ms = ioLatency();
  if (ms) await sleep(ms, undefined, { signal });
}

// One processor per job name. Fan-out handlers enqueue real child jobs.
const processors: { [Name in JobNameT]: (job: Job<JobData[Name]>, signal: AbortSignal) => Promise<void> } = {
  async IngestEvent(job, signal) {
    await simulateIo(signal);
    for (let i = 0; i < tasksPerEvent; i++) {
      // Built-in parentage: server forces this job as the parent and derives depth + trace.
      await job.enqueueChild<TaskData>(JobName.ProcessTask,
        { recordId: `${job.data.tenantId}-${job.id}-task-${i}`, payloadBytes: job.data.payloadBytes, subtaskCount: subtasksPerTask },
        { priority: job.data.trigger === "manual" ? 0 : 5 });
    }
  },
  async ProcessTask(job, signal) {
    await simulateIo(signal);
    for (let i = 0; i < job.data.subtaskCount; i++) {
      await job.enqueueChild<SubtaskData>(JobName.ProcessSubtask,
        { recordId: `${job.data.recordId}-subtask-${i}`, payloadBytes: job.data.payloadBytes });
    }
  },
  async ProcessSubtask(job, signal) {
    await simulateIo(signal);
    await job.enqueueChild<RecordData>(JobName.WriteRecord,
      { recordId: job.data.recordId, payloadBytes: job.data.payloadBytes });
  },
  async WriteRecord(_job, signal) {
    await simulateIo(signal);
  },
  // Diagnostic -----------------------------------------------------------------
  async Delay(job, signal) {
    const ms = Math.min(Math.max(0, job.data.waitTimeInMilliseconds ?? 0), 60000);
    if (ms) await sleep(ms, undefined, { signal });
  },
  async Stress(job, _signal) {
    const ms = Math.min(Math.max(0, job.data.stressTimeInMilliseconds ?? 0), 2000);
    const end = performance.now() + ms;
    while (performance.now() < end) { /* spin */ }
  },
  async Throw(job, _signal) {
    throw new Error(job.data.message || "intentional diagnostic failure");
  },
  async RecursiveProbe(job, signal) {
    // Deliberate runaway recursion. Each generation enqueues one child of itself; the server
    // derives execution_depth = parent + 1, so around the 11th generation it trips the depth-10
    // circuit breaker and REJECTS the child enqueue with RESOURCE_EXHAUSTED. That rejection is
    // the whole point: it increments anvilmq_ancestry_rejections_total and halts the lineage.
    // The breaker, not this handler, stops the recursion; a rejected enqueue creates no child.
    await simulateIo(signal);
    try {
      await job.enqueueChild<RecursiveProbeData>(JobName.RecursiveProbe,
        { hops: (job.data.hops ?? 1) + 1, origin: job.data.origin });
    } catch (error) {
      const code = (error as { code?: number }).code;
      if (code !== GRPC_RESOURCE_EXHAUSTED) throw error;
      recursionTripped++;
      console.log(JSON.stringify({
        event: "ancestry-breaker-tripped", origin: job.data.origin,
        depth: job.metadata.executionDepth, hops: job.data.hops,
      }));
    }
  },
};

// Runaway lineages halted by the ancestry-depth circuit breaker (expected, not a job failure).
let recursionTripped = 0;

let completed = 0;
let failed = 0;
let retried = 0;
function createWorker<Name extends JobNameT>(name: Name) {
  return new Worker<JobData[Name]>(name, processors[name], {
    address: ADDRESS,
    pollIntervalMs: pollMs,
    concurrency,
    // Broker-authoritative lifecycle hooks drive the counters: onFailed fires only when the
    // broker moved a job to its terminal failed state, onRetry when it will redeliver.
    onCompleted: () => { completed++; },
    onFailed: () => { failed++; },
    onRetry: () => { retried++; },
    onError: () => { /* per-attempt errors are expected (e.g. the Throw job); counters above track outcomes */ },
  });
}
// One Worker per job name (8 total), each internally parallel up to `concurrency`.
const workers = ALL_JOB_NAMES.map((name) => createWorker(name));

// Aggregate the 8 per-name workers into a single health/metrics surface so Prometheus scrapes
// one series set (concatenating per-worker renderPrometheus output would duplicate HELP/TYPE
// lines for the same metric, which Prometheus rejects).
function aggregateHealth(): WorkerHealth {
  const hs = workers.map((w) => w.health());
  return {
    live: hs.every((h) => h.live),
    ready: hs.every((h) => h.ready),
    connected: hs.every((h) => h.connected),
    stale: hs.some((h) => h.stale),
    stopping: hs.some((h) => h.stopping),
    consecutivePollFailures: Math.max(...hs.map((h) => h.consecutivePollFailures)),
    inFlight: hs.reduce((s, h) => s + h.inFlight, 0),
    concurrency: hs.reduce((s, h) => s + h.concurrency, 0),
    saturated: hs.every((h) => h.saturated),
    lastPollOkAt: Math.max(...hs.map((h) => h.lastPollOkAt)),
    lastBrokerContactAt: Math.max(...hs.map((h) => h.lastBrokerContactAt)),
    loopAliveAt: Math.max(...hs.map((h) => h.loopAliveAt)),
    uptimeMs: Math.max(...hs.map((h) => h.uptimeMs)),
    now: Date.now(),
  };
}
function aggregateMetrics(): WorkerMetrics {
  const ms = workers.map((w) => w.metrics());
  return {
    jobsProcessedTotal: ms.reduce((s, m) => s + m.jobsProcessedTotal, 0),
    jobsFailedTotal: ms.reduce((s, m) => s + m.jobsFailedTotal, 0),
    jobsRetriedTotal: ms.reduce((s, m) => s + m.jobsRetriedTotal, 0),
    jobsActive: ms.reduce((s, m) => s + m.jobsActive, 0),
    pollFailuresTotal: ms.reduce((s, m) => s + m.pollFailuresTotal, 0),
    reconnectsTotal: ms.reduce((s, m) => s + m.reconnectsTotal, 0),
    connectionUp: ms.every((m) => m.connectionUp === 1) ? 1 : 0,
  };
}
const agg = { health: aggregateHealth, metrics: aggregateMetrics };

const logger: Logger = {
  error: (...args: unknown[]) => console.error(...args),
  warn: (...args: unknown[]) => console.warn(...args),
  info: (...args: unknown[]) => console.log(...args),
};

// One aggregated Kubernetes-style probe + Prometheus surface for the whole fleet.
const server = Bun.serve({
  port: healthPort,
  hostname: "0.0.0.0",
  fetch: fetchHealthHandler(agg, { metricsPath: "/metrics" }),
});

console.log(JSON.stringify({
  event: "workers-started", address: ADDRESS, jobNames: ALL_JOB_NAMES,
  totalWorkers: workers.length, concurrency, healthPort, workMs, tasksPerEvent, subtasksPerTask,
}));

const report = setInterval(() => {
  const m = aggregateMetrics();
  console.log(JSON.stringify({
    event: "workers-progress", completed, failed, retried, recursionTripped,
    jobsProcessedTotal: m.jobsProcessedTotal, jobsActive: m.jobsActive,
    jobsFailedTotal: m.jobsFailedTotal, jobsRetriedTotal: m.jobsRetriedTotal,
  }));
}, 10000);

// Clean pod termination: drain in-flight handlers, stop the health server, and close the
// queue clients concurrently against the shutdown deadline.
gracefulShutdown([
  ...workers,
  { close() { server.stop(); } },
  { close() {
    clearInterval(report);
    closeAllQueues();
    console.log(JSON.stringify({ event: "workers-stopped", completed, failed, retried, recursionTripped }));
  } },
], { logger });
