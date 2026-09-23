// Sliding-window ingress velocity demonstration for the AnvilMQ sim.
//
// Ingress limits are an ADMISSION-side control: the broker rejects AddJob up front when a
// facet's recent enqueue velocity exceeds its configured limit, protecting the embedded
// writer from bursty enqueue overload. (This is distinct from the dispatch-side rate limits,
// which throttle the rate at which workers CLAIM jobs.)
//
// This script:
//   1. Installs a sliding-window ingress rule for a dedicated demo facet (maxJobs per window).
//   2. Fires a sustained burst of enqueues for that facet WELL above the limit, tagging each
//      job with the facet so the rule applies, and counts admits vs. rejections.
//   3. Reports the admit rate, which converges on the configured limit while the surplus is
//      shed as RESOURCE_EXHAUSTED. Watch `anvilmq_ingress_rejected_total` climb in Grafana
//      (the "AnvilMQ ingress velocity limits" dashboard) during the run.
//
// The demo facet is unique to this run, so only these enqueues are throttled; the steady
// workload is untouched. Jobs are enqueued under the drained ProcessTask name so the
// admitted ones complete normally instead of piling up. Isolated: loopback broker only.
import { setTimeout as sleep } from "node:timers/promises";
import { Queue, IngressLimits } from "../../../client/src/index";
import { ADDRESS, JobName, num, type TaskData } from "./queues";

const facet = process.env.SIM_INGRESS_FACET ?? `tenant:ingress-demo-${Math.random().toString(36).slice(2, 6)}`;
const maxJobs = num("SIM_INGRESS_MAX", 40, 1, 1_000_000); // admitted per window
const windowMs = num("SIM_INGRESS_WINDOW_MS", 2000, 100, 600000); // sliding-window duration
const attemptRate = num("SIM_INGRESS_RATE", 200, 1, 200000); // attempted enqueues/s (>> the limit)
const seconds = num("SIM_INGRESS_SECONDS", 30, 1, 3600);
const lanes = num("SIM_INGRESS_LANES", 8, 1, 256);

const admitLimitPerSec = (maxJobs / windowMs) * 1000;
const GRPC_RESOURCE_EXHAUSTED = 8;

let attempted = 0;
let admitted = 0;
let rejected = 0;
let errors = 0;

const q = new Queue<TaskData>(JobName.ProcessTask, { address: ADDRESS });
const limits = new IngressLimits({ address: ADDRESS });

console.log(JSON.stringify({
  event: "ingress-demo-started", address: ADDRESS, facet,
  rule: { maxJobs, windowMs, admitLimitPerSec: Number(admitLimitPerSec.toFixed(1)) },
  burst: { attemptRate, seconds, lanes },
}));

// 1. Install the sliding-window ingress rule for this demo's facet.
await limits.upsert(facet, maxJobs, windowMs);
const before = await limits.status(facet);
console.log(JSON.stringify({ event: "rule-installed", status: before }));

// 2. Burst enqueues, paced to attemptRate across lanes, for the configured duration. Each add
//    that the sliding-window control rejects surfaces as gRPC RESOURCE_EXHAUSTED (code 8).
const start = performance.now();
const deadline = start + seconds * 1000;
const step = 1000 / attemptRate; // ms between enqueue attempts across all lanes
let slot = start;

async function lane(): Promise<void> {
  while (performance.now() < deadline) {
    const due = slot;
    slot += step;
    const wait = due - performance.now();
    if (wait > 0) await sleep(wait);
    if (performance.now() >= deadline) break;
    attempted++;
    try {
      await q.add({ recordId: `ingress-${Math.random().toString(36).slice(2, 10)}`, payloadBytes: 256, subtaskCount: 0 }, { rateLimitFacet: facet });
      admitted++;
    } catch (error) {
      const code = (error as { code?: number }).code;
      if (code === GRPC_RESOURCE_EXHAUSTED) rejected++;
      else { errors++; if (errors <= 5) console.error(JSON.stringify({ event: "unexpected-error", error: String(error) })); }
    }
  }
}

const report = setInterval(() => {
  const elapsed = (performance.now() - start) / 1000;
  console.log(JSON.stringify({
    event: "ingress-progress", elapsedSeconds: Number(elapsed.toFixed(1)),
    attempted, admitted, rejected, errors,
    admitPerSecond: Number((admitted / elapsed).toFixed(1)),
    rejectPerSecond: Number((rejected / elapsed).toFixed(1)),
  }));
}, 5000);

await Promise.all(Array.from({ length: lanes }, lane));
clearInterval(report);

const elapsed = (performance.now() - start) / 1000;
const after = await limits.status(facet);

// 3. The admit rate should track the configured limit (within sliding-window tolerance) while
//    the large surplus is shed as rejections.
const admitPerSecond = admitted / elapsed;
const withinTolerance = admitted > 0 && admitPerSecond <= admitLimitPerSec * 2.0 && rejected > 0;

let passed = 0, failed = 0;
function check(name: string, ok: boolean, detail: string) {
  console.log(`${ok ? "PASS" : "FAIL"}  ${name} :: ${detail}`);
  if (ok) passed++; else failed++;
}
check("surplus was rejected", rejected > 0, `rejected=${rejected} of attempted=${attempted}`);
check("admit rate tracks the configured limit", withinTolerance,
  `admit=${admitPerSecond.toFixed(1)}/s vs limit=${admitLimitPerSec.toFixed(1)}/s (2x tolerance), rejected=${rejected}`);
check("no unexpected (non-ingress) errors", errors === 0, `unexpected errors=${errors}`);

console.log(JSON.stringify({
  event: "ingress-demo-finished",
  elapsedSeconds: Number(elapsed.toFixed(1)),
  attempted, admitted, rejected, errors,
  admitPerSecond: Number(admitPerSecond.toFixed(1)),
  configuredLimitPerSecond: Number(admitLimitPerSec.toFixed(1)),
  finalStatus: after,
  passed, failed,
}, null, 2));

// 4. Clean up the demo rule so the sim returns to its baseline.
await limits.delete(facet);
q.close();
limits.close();
process.exit(failed ? 1 : 0);
