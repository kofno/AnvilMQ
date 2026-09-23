// Check AnvilMQ queue semantics
// (priority ordering, delays, retry with exponential backoff, and idempotent enqueue).
// Runs against the sim broker using fresh unique queue names so it never disturbs other work.
import { Queue, Worker } from "../../../client/src/index";
import { setTimeout as sleep } from "node:timers/promises";
import { ADDRESS } from "./queues";

const options = { address: ADDRESS };
const uniq = (p: string) => `semantics-${p}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
type Result = { name: string; passed: boolean; detail: string };
const results: Result[] = [];
function check(name: string, passed: boolean, detail: string) {
  results.push({ name, passed, detail });
  console.log(`${passed ? "PASS" : "FAIL"}  ${name} :: ${detail}`);
}

// Priority: lower numeric priority must be claimed first among jobs waiting together.
async function priorityOrdering() {
  const q = new Queue<{ priority: number }>(uniq("priority"), options);
  const name = q.name;
  const inputs = [9, 1, 5, 3];
  for (const p of inputs) await q.add({ priority: p }, { priority: p });
  const seen: number[] = [];
  const done = new Promise<void>((resolve) => {
    const w = new Worker<{ priority: number }>(name, async (job) => {
      seen.push(job.data.priority);
      if (seen.length === inputs.length) { resolve(); void w.close(); }
    }, { ...options, pollIntervalMs: 10 });
  });
  await Promise.race([done, sleep(15000)]);
  q.close();
  const expected = [...inputs].sort((a, b) => a - b);
  check("priority ordering", JSON.stringify(seen) === JSON.stringify(expected), `claimed ${JSON.stringify(seen)}, expected ${JSON.stringify(expected)}`);
}

// Delay: a delayed job must not be delivered before its delay elapses.
async function delayScheduling() {
  const q = new Queue<{ tag: string }>(uniq("delay"), options);
  const name = q.name;
  const t0 = performance.now();
  await q.add({ tag: "immediate" });
  await q.add({ tag: "delayed" }, { delayMs: 1500 });
  const order: { tag: string; atMs: number }[] = [];
  const done = new Promise<void>((resolve) => {
    const w = new Worker<{ tag: string }>(name, async (job) => {
      order.push({ tag: job.data.tag, atMs: performance.now() - t0 });
      if (order.length === 2) { resolve(); void w.close(); }
    }, { ...options, pollIntervalMs: 10 });
  });
  await Promise.race([done, sleep(15000)]);
  q.close();
  const immediate = order.find((o) => o.tag === "immediate");
  const delayed = order.find((o) => o.tag === "delayed");
  const ok = !!immediate && !!delayed && order[0]?.tag === "immediate" && delayed.atMs >= 1400;
  check("delay scheduling", ok, `order ${JSON.stringify(order.map((o) => o.tag))}, delayed delivered at ${delayed ? Math.round(delayed.atMs) : "n/a"}ms`);
}

// Retry + exponential backoff: attempts increment 1..N, backoff grows, job completes once.
async function retryBackoff() {
  const q = new Queue(uniq("retry"), options);
  const name = q.name;
  await q.add({}, { maxAttempts: 3, retryBackoffMs: 400, retryBackoffMaxMs: 4000 });
  const attemptTimes: number[] = [];
  let completions = 0;
  const t0 = performance.now();
  const done = new Promise<void>((resolve) => {
    const w = new Worker(name, async (job) => {
      attemptTimes.push(performance.now() - t0);
      if (job.attempts < 3) throw new Error(`fail attempt ${job.attempts}`);
    }, { ...options, pollIntervalMs: 10, onError: () => {}, onCompleted: () => { completions++; resolve(); void w.close(); } });
  });
  await Promise.race([done, sleep(20000)]);
  q.close();
  const gap1 = attemptTimes[1] - attemptTimes[0];
  const gap2 = attemptTimes[2] - attemptTimes[1];
  const ok = attemptTimes.length === 3 && completions === 1 && gap1 >= 300 && gap2 > gap1;
  check("retry + exponential backoff", ok, `attempts=${attemptTimes.length}, completions=${completions}, gaps=${Math.round(gap1)}ms/${Math.round(gap2)}ms (expect ~400 then ~800)`);
}

// Idempotent enqueue: same key returns the original id with replayed=true, one job only.
async function idempotency() {
  const q = new Queue<{ v: number }>(uniq("idem"), options);
  const key = `key-${Math.random().toString(36).slice(2)}`;
  const first = await q.add({ v: 1 }, { idempotencyKey: key });
  const second = await q.add({ v: 1 }, { idempotencyKey: key });
  q.close();
  const ok = first.replayed === false && second.replayed === true && first.id === second.id;
  check("idempotent enqueue", ok, `first.replayed=${first.replayed}, second.replayed=${second.replayed}, sameId=${first.id === second.id}`);
}

console.log(JSON.stringify({ event: "semantics-check-started", address: ADDRESS }));
await priorityOrdering();
await delayScheduling();
await retryBackoff();
await idempotency();
const failed = results.filter((r) => !r.passed);
console.log(JSON.stringify({ event: "semantics-check-finished", total: results.length, passed: results.length - failed.length, failed: failed.length }));
process.exit(failed.length ? 1 : 0);
