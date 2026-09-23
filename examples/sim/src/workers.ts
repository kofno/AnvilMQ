// Synthetic workers, running against AnvilMQ with simulated side effects only.
//
// In a real deployment these handlers would call downstream services (a database, an
// event bus, etc.). Here they use bounded sleeps or CPU work and the local broker only.
import { Worker, type Job } from "../../../client/src/index";
import { setTimeout as sleep } from "node:timers/promises";
import {
  ADDRESS, ALL_JOB_NAMES, JobName, num, closeAllQueues, GRPC_RESOURCE_EXHAUSTED,
  type TaskData, type SubtaskData, type RecordData,
  type RecursiveProbeData, type JobData, type JobName as JobNameT,
} from "./queues";

const concurrency = num("SIM_WORKER_CONCURRENCY", 2, 1, 64);
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
function createWorker<Name extends JobNameT>(name: Name) {
  return new Worker<JobData[Name]>(name, processors[name], {
    address: ADDRESS,
    pollIntervalMs: pollMs,
    onCompleted: () => { completed++; },
    onError: () => { failed++; },
  });
}
const workers = ALL_JOB_NAMES.flatMap((name) =>
  Array.from({ length: concurrency }, () => createWorker(name)));

console.log(JSON.stringify({
  event: "workers-started", address: ADDRESS, jobNames: ALL_JOB_NAMES,
  workersPerName: concurrency, totalWorkers: workers.length, workMs, tasksPerEvent, subtasksPerTask,
}));

const report = setInterval(() => {
  console.log(JSON.stringify({ event: "workers-progress", completed, failed, recursionTripped }));
}, 10000);

async function shutdown() {
  clearInterval(report);
  console.log(JSON.stringify({ event: "workers-draining" }));
  await Promise.all(workers.map((w) => w.close()));
  closeAllQueues();
  console.log(JSON.stringify({ event: "workers-stopped", completed, failed, recursionTripped }));
  process.exit(0);
}
for (const s of ["SIGINT", "SIGTERM"] as const) process.once(s, shutdown);
