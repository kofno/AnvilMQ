// Workload producer for the AnvilMQ sim. Two profiles:
//   prod  - steady synthetic workload at ~13 top-level jobs/s by default.
//           Dominated by IngestEvent, which fans out into the task chain,
//           so internal throughput is a multiple of the top-level enqueue rate.
//   soak  - high-volume push of cheap jobs toward SIM_MAX_JOBS to prove scale (millions).
//
// Isolated: enqueues only to the local sim broker. No demo/prod endpoints.
import { setTimeout as sleep } from "node:timers/promises";
import { RateLimits, IngressLimits, type AddOptions } from "../../../client/src/index";
import { ADDRESS, HTTP, JobName, num, enqueue, closeAllQueues, LimitFacet, GRPC_RESOURCE_EXHAUSTED, type JobData, type JobName as JobNameT } from "./queues";

const profile = (process.env.SIM_PROFILE ?? "prod").toLowerCase();
const rate = num("SIM_RATE", profile === "soak" ? 1000 : 13, 0, 200000); // aggregate jobs/s; 0 = unlimited
const lanes = num("SIM_PRODUCER_LANES", profile === "soak" ? 16 : 4, 1, 256);
const durationSeconds = num("SIM_DURATION_SECONDS", 0, 0, 604800); // 0 = run until maxJobs or forever
const maxJobs = num("SIM_MAX_JOBS", 0, 0, 1_000_000_000); // 0 = unlimited

// --- Limit-tripping demo (woven into the steady prod workload) ---------------------------
// Periodically bursts a dedicated facet past an admission-side ingress rule (surplus enqueues
// are rejected -> anvilmq_ingress_rejected_total) and past a dispatch-side claim rate rule
// (jobs pool in Waiting and drain slowly -> anvilmq_throttled_polls_total, GetRateLimitStatus
// is_throttled). Distinct facets keep each control isolated on the dashboards. On by default for
// prod; disabled for soak. Set SIM_LIMIT_DEMO=0 to turn off.
const limitDemo = (process.env.SIM_LIMIT_DEMO || (profile === "soak" ? "0" : "1")) !== "0";
const ingressMax = num("SIM_INGRESS_DEMO_MAX", 10, 1, 1_000_000);
const ingressWindowMs = num("SIM_INGRESS_DEMO_WINDOW_MS", 2000, 100, 600000);
const dispatchMax = num("SIM_DISPATCH_DEMO_MAX", 5, 1, 1_000_000);
const dispatchWindowMs = num("SIM_DISPATCH_DEMO_WINDOW_MS", 2000, 100, 600000);
const limitTickMs = num("SIM_LIMIT_DEMO_INTERVAL_MS", 15000, 1000, 3600000);
const ingressBurst = num("SIM_INGRESS_DEMO_BURST", 30, 1, 100000);
const dispatchBurst = num("SIM_DISPATCH_DEMO_BURST", 25, 1, 100000);

type Weighted = { [Name in JobNameT]: { name: Name; weight: number; build: (n: number) => { data: JobData[Name]; options: AddOptions } } }[JobNameT];

const rid = () => Math.random().toString(36).slice(2, 10);
const tenants = ["tenant-alpha", "tenant-bravo", "tenant-charlie", "tenant-delta"];
const tenant = () => tenants[Math.floor(Math.random() * tenants.length)];

// Invented, infrastructure-generic failure reasons for the Throw job. Each carries a distinct
// token (timeout / schema / rate / dependency / checksum ...) so the full-text search demo
// (search-demo.ts) can retrieve a meaningful subset of failures by keyword from the
// FTS5-indexed last_error column. These phrases reference no real system, service, or data.
const failureReasons = [
  "connection timeout contacting upstream service",
  "invalid payload schema rejected by validator",
  "rate limit exceeded, retry scheduled",
  "downstream dependency temporarily unavailable",
  "checksum mismatch detected on stored record",
];
const failureReason = () => failureReasons[Math.floor(Math.random() * failureReasons.length)];

// Mixed profile: mostly events, some direct tasks, and diagnostics.
const prodMix: Weighted[] = [
  { name: JobName.IngestEvent, weight: 50, build: () => ({
      data: { tenantId: tenant(), trigger: Math.random() < 0.2 ? "manual" : "scheduled", payloadBytes: 1024 },
      options: { metadata: { executionDepth: 1 }, priority: 5 } }) },
  { name: JobName.ProcessTask, weight: 35, build: () => ({
      data: { recordId: `record-${rid()}`, payloadBytes: 256, subtaskCount: 0 },
      options: { maxAttempts: 3, retryBackoffMs: 500, retryBackoffMaxMs: 5000 } }) },
  { name: JobName.Delay, weight: 8, build: () => ({
      data: { waitTimeInMilliseconds: 200 + Math.floor(Math.random() * 800) }, options: {} }) },
  { name: JobName.Stress, weight: 4, build: () => ({
      data: { stressTimeInMilliseconds: 50 + Math.floor(Math.random() * 250) }, options: {} }) },
  // Throw exercises the retry/failure metrics and the Failure Ratio panel. The message varies so
  // the FTS-indexed last_error column carries distinct tokens for the search demo.
  { name: JobName.Throw, weight: 3, build: () => ({
      data: { message: failureReason() }, options: { maxAttempts: 2, retryBackoffMs: 300 } }) },
  // Occasionally launch a self-recursive chain that climbs past the depth-10 circuit breaker.
  // Each pick spawns one lineage that runs ~10 deep, then a single enqueue is rejected and
  // reported via anvilmq_ancestry_rejections_total. See workers.ts RecursiveProbe.
  { name: JobName.RecursiveProbe, weight: 1, build: () => ({
      data: { hops: 1, origin: rid() }, options: {} }) },
];

// Soak pushes direct tasks without fan-out to reach high cumulative counts quickly.
const soakMix: Weighted[] = [
  { name: JobName.ProcessTask, weight: 100, build: () => ({
      data: { recordId: `record-${rid()}`, payloadBytes: 256, subtaskCount: 0 }, options: {} }) },
];

const mix = profile === "soak" ? soakMix : prodMix;
const totalWeight = mix.reduce((s, m) => s + m.weight, 0);
function pick(): Weighted {
  let r = Math.random() * totalWeight;
  for (const m of mix) { if ((r -= m.weight) <= 0) return m; }
  return mix[mix.length - 1];
}

let submitted = 0;
let acknowledged = 0;
let errors = 0;
let ingressAdmitted = 0;
let ingressRejected = 0;
let dispatchQueued = 0;
const start = performance.now();
const deadline = durationSeconds ? start + durationSeconds * 1000 : Infinity;
let slot = start;
const step = rate ? 1000 / rate : 0;

function done(): boolean {
  if (performance.now() >= deadline) return true;
  if (maxJobs && submitted >= maxJobs) return true;
  return false;
}

async function lane(): Promise<void> {
  while (!done()) {
    if (rate) {
      const due = slot; slot += step;
      const wait = due - performance.now();
      if (wait > 0) await sleep(wait);
      if (done()) break;
    }
    submitted++;
    const job = pick();
    const { data, options } = job.build(submitted);
    try { await enqueue(job.name, data, options); acknowledged++; }
    catch (e) { errors++; if (errors <= 5) console.error(JSON.stringify({ event: "enqueue-error", error: String(e) })); }
  }
}

async function currentDepth(): Promise<number | null> {
  try {
    const text = await (await fetch(`${HTTP}/metrics`, { signal: AbortSignal.timeout(3000) })).text();
    let waiting = 0, active = 0;
    for (const line of text.split("\n")) {
      const m = line.match(/anvilmq_backlog_jobs\{[^}]*scope="all"[^}]*kind="(waiting|delayed|active)"\}\s+([0-9.]+)/);
      if (m) { if (m[1] === "active") active += Number(m[2]); else waiting += Number(m[2]); }
    }
    return waiting + active;
  } catch { return null; }
}

console.log(JSON.stringify({ event: "producer-started", profile, address: ADDRESS, rate: rate || "unlimited", lanes, durationSeconds: durationSeconds || "forever", maxJobs: maxJobs || "unlimited" }));

const rateLimits = limitDemo ? new RateLimits({ address: ADDRESS }) : null;
const ingressLimits = limitDemo ? new IngressLimits({ address: ADDRESS }) : null;
if (limitDemo && rateLimits && ingressLimits) {
  await rateLimits.upsert(LimitFacet.DispatchThrottle, dispatchMax, dispatchWindowMs);
  await ingressLimits.upsert(LimitFacet.IngressBurst, ingressMax, ingressWindowMs);
  console.log(JSON.stringify({ event: "limit-demo-rules-installed",
    ingress: { facet: LimitFacet.IngressBurst, maxJobs: ingressMax, windowMs: ingressWindowMs },
    dispatch: { facet: LimitFacet.DispatchThrottle, maxJobs: dispatchMax, windowMs: dispatchWindowMs },
    intervalMs: limitTickMs, ingressBurst, dispatchBurst }));
}

// Every limitTickMs: burst each limited facet. Ingress sheds surplus as RESOURCE_EXHAUSTED (the
// admitted remainder drains normally); dispatch admits all at enqueue but workers claim them only
// at the fixed-window rate, so they pool in Waiting. Jobs use ProcessTask without fan-out.
async function limitDemoTick(): Promise<void> {
  await Promise.all(Array.from({ length: ingressBurst }, async () => {
    try { await enqueue(JobName.ProcessTask, { recordId: `ingress-${rid()}`, payloadBytes: 256, subtaskCount: 0 }, { rateLimitFacet: LimitFacet.IngressBurst }); ingressAdmitted++; }
    catch (e) { if ((e as { code?: number }).code === GRPC_RESOURCE_EXHAUSTED) ingressRejected++; else if (errors <= 5) console.error(JSON.stringify({ event: "ingress-demo-error", error: String(e) })); }
  }));
  await Promise.all(Array.from({ length: dispatchBurst }, async () => {
    try { await enqueue(JobName.ProcessTask, { recordId: `throttle-${rid()}`, payloadBytes: 256, subtaskCount: 0 }, { rateLimitFacet: LimitFacet.DispatchThrottle }); dispatchQueued++; }
    catch (e) { if (errors <= 5) console.error(JSON.stringify({ event: "dispatch-demo-error", error: String(e) })); }
  }));
}
const limitTimer = limitDemo ? setInterval(() => { void limitDemoTick(); }, limitTickMs) : null;

const report = setInterval(async () => {
  const elapsed = (performance.now() - start) / 1000;
  console.log(JSON.stringify({ event: "producer-progress", elapsedSeconds: Number(elapsed.toFixed(1)), submitted, acknowledged, errors, enqueuePerSecond: Number((acknowledged / elapsed).toFixed(1)), backlog: await currentDepth(), ingressAdmitted, ingressRejected, dispatchQueued }));
}, 5000);

let stopping = false;
async function shutdown() {
  if (stopping) return; stopping = true;
  if (limitTimer) clearInterval(limitTimer);
}
for (const s of ["SIGINT", "SIGTERM"] as const) process.once(s, shutdown);

await Promise.all(Array.from({ length: lanes }, lane));
clearInterval(report);
if (limitTimer) clearInterval(limitTimer);
if (limitDemo && rateLimits && ingressLimits) {
  try { await rateLimits.delete(LimitFacet.DispatchThrottle); await ingressLimits.delete(LimitFacet.IngressBurst); }
  catch (e) { console.error(JSON.stringify({ event: "limit-demo-cleanup-error", error: String(e) })); }
  rateLimits.close(); ingressLimits.close();
}
const elapsed = (performance.now() - start) / 1000;
console.log(JSON.stringify({ event: "producer-finished", profile, elapsedSeconds: Number(elapsed.toFixed(1)), submitted, acknowledged, errors, enqueuePerSecond: Number((acknowledged / elapsed).toFixed(1)), ingressAdmitted, ingressRejected, dispatchQueued }));
closeAllQueues();
process.exit(errors ? 1 : 0);
