// Tenant-fairness demonstration for the AnvilMQ sim.
//
// The broker in this cluster runs with ANVILMQ_FAIRNESS_ENABLED=true, so dequeue rotates
// among due jobs of equal priority by least-recently-served `rate_limit_facet` instead of
// strict created-at FIFO. This script makes that rotation observable end to end:
//
//   1. It enqueues a lopsided multi-tenant burst on a dedicated, isolated job name: one
//      "flood" tenant with a large head start (enqueued first, so it wins every FIFO tie),
//      plus three small tenants, all at the SAME priority.
//   2. It drains them with a single serial Worker and records the tenant of each job in the
//      exact order the broker handed it out (the claim order, which is where fairness acts).
//   3. It reports the observed order and infers whether round-robin fairness was active
//      (each tenant served in turn) or the legacy FIFO path monopolized the flood tenant.
//
// The job name is unique to this run and not in the workers' catalog, so the cluster's
// steady workers never touch it -- the claim order is fully controlled and deterministic.
// The facet is not exposed on the claimed job, so each job also carries its tenant in the
// payload. Isolated: talks only to the local sim broker over loopback; throwaway jobs.
import { setTimeout as sleep } from "node:timers/promises";
import { Queue, Worker, type Job } from "../../../client/src/index";
import { ADDRESS } from "./queues";

// A unique job name isolates this run from the steady workload and from prior runs; the
// steady workers subscribe only to the synthetic catalog, so nothing else claims these jobs.
const NAME = `FairnessProbe_${Math.random().toString(36).slice(2, 8)}`;
const PRIORITY = 5; // equal for every job: fairness only ever rotates within one priority tier.

// One flood tenant with a big head start vs. three small tenants. Under strict FIFO the flood
// tenant's earlier-enqueued jobs are all served before any small tenant's; under fairness each
// tenant takes an equal turn regardless of backlog depth.
const FLOOD = { tenant: "flood", count: 60 };
const SMALL = [
  { tenant: "alpha", count: 12 },
  { tenant: "bravo", count: 12 },
  { tenant: "charlie", count: 12 },
];
const TENANTS = [FLOOD, ...SMALL];
const TOTAL = TENANTS.reduce((s, t) => s + t.count, 0);

interface Probe { tenant: string; i: number }

let passed = 0;
let failed = 0;
function check(name: string, ok: boolean, detail: string) {
  console.log(`${ok ? "PASS" : "FAIL"}  ${name} :: ${detail}`);
  if (ok) passed++;
  else failed++;
}

console.log(JSON.stringify({ event: "fairness-demo-started", address: ADDRESS, name: NAME, total: TOTAL }));

// 1. Enqueue the flood tenant FIRST so it owns the earliest created_at (and wins every FIFO
//    tie), then pause so the small tenants are strictly later. Facet drives fairness; the
//    tenant is duplicated into the payload because the claim does not echo the facet back.
const q = new Queue<Probe>(NAME, { address: ADDRESS });
for (let i = 0; i < FLOOD.count; i++) {
  await q.add({ tenant: FLOOD.tenant, i }, { rateLimitFacet: `tenant:${FLOOD.tenant}`, priority: PRIORITY, maxAttempts: 1 });
}
await sleep(60); // ensure the small tenants land in a strictly later millisecond than the flood.
for (const t of SMALL) {
  for (let i = 0; i < t.count; i++) {
    await q.add({ tenant: t.tenant, i }, { rateLimitFacet: `tenant:${t.tenant}`, priority: PRIORITY, maxAttempts: 1 });
  }
}
q.close();
console.log(JSON.stringify({ event: "seeded", perTenant: TENANTS.map((t) => `${t.tenant}:${t.count}`) }));

// 2. Drain with a single serial Worker and record the claim order. A lone Worker claims one
//    job at a time, so the recorded sequence is exactly the order the broker dispatched them.
const order: string[] = [];
const drained = new Promise<void>((resolve) => {
  const worker = new Worker<Probe>(
    NAME,
    async (job: Job<Probe>) => {
      order.push(job.data.tenant);
      if (order.length >= TOTAL) {
        // Close asynchronously so this handler can still ack cleanly.
        void worker.close().then(resolve);
      }
    },
    { address: ADDRESS, pollIntervalMs: 5 },
  );
});
const timeout = sleep(60000).then(() => "timeout");
const outcome = await Promise.race([drained.then(() => "drained"), timeout]);
check("drained the burst", outcome === "drained" && order.length === TOTAL, `claimed ${order.length}/${TOTAL} (${outcome})`);

// 3. Analyze the claim order.
// Leading monopolization: how many jobs the flood tenant claimed before ANY small tenant ran.
let lead = 0;
while (lead < order.length && order[lead] === FLOOD.tenant) lead++;

// Rotation window: over the first (tenants * smallCount) claims -- while every tenant still has
// work -- a fair scheduler serves each tenant an equal number of times.
const smallCount = SMALL[0].count;
const windowLen = Math.min(order.length, TENANTS.length * smallCount);
const window = order.slice(0, windowLen);
const counts = new Map<string, number>();
for (const t of window) counts.set(t, (counts.get(t) ?? 0) + 1);
const distribution = TENANTS.map((t) => `${t.tenant}=${counts.get(t.tenant) ?? 0}`).join(" ");
const floodShare = (counts.get(FLOOD.tenant) ?? 0) / windowLen;

// Fair: no tenant dominates the window and the flood tenant did not front-run the small ones.
const fairObserved = lead <= 2 && floodShare <= 1 / TENANTS.length + 0.1;
const mode = fairObserved ? "round-robin fairness ACTIVE" : "FIFO / fairness OFF (flood monopolized)";

console.log(JSON.stringify({
  event: "claim-order",
  firstClaims: order.slice(0, 24),
  leadingFloodRun: lead,
  window: { length: windowLen, distribution, floodShare: Number(floodShare.toFixed(3)) },
  observed: mode,
}, null, 2));

check(
  "no single tenant monopolizes the rotation window",
  fairObserved,
  `leadingFloodRun=${lead}, distribution[${distribution}] -> ${mode}`,
);
console.log(
  "NOTE: set ANVILMQ_FAIRNESS_ENABLED=false in compose.yaml and recreate the broker to see " +
  "the legacy FIFO contrast (the flood tenant's 60 jobs drain before any small tenant runs).",
);

console.log(JSON.stringify({ event: "fairness-demo-finished", total: passed + failed, passed, failed, observed: mode }));
process.exit(failed ? 1 : 0);
