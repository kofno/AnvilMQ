if (process.argv[2] === "load") { await import("./load"); process.exit(process.exitCode ?? 0); }
import { Queue, Worker } from "../client/src/index";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

const address = process.env.ANVILMQ_ADDR ?? "127.0.0.1:50061";
const http = process.env.ANVILMQ_HTTP_URL ?? "http://127.0.0.1:9091";
const results = process.env.ANVILMQ_RESULTS_DIR ?? "harness/artifacts";
const mode = process.argv[2] ?? "smoke";
const start = Date.now();
const abort = new AbortController();
const deadline = setTimeout(() => abort.abort(new Error("harness timed out")), 60000);
function check(ok: unknown, message: string): asserts ok { if (!ok) throw new Error(message); }
async function until(check: () => Promise<boolean>) {
  while (!await check()) { abort.signal.throwIfAborted(); await sleep(100, undefined, { signal: abort.signal }); }
}
async function completed() {
  const response = await fetch(`${http}/metrics`, { signal: abort.signal });
  check(response.ok, "metrics unavailable");
  const text = await response.text();
  return Number(text.match(/anvilmq_jobs\{state="Completed"\} (\d+)/)?.[1] ?? NaN);
}
async function processJobs(name: string, expected: string[], retry: boolean) {
  const baseline = await completed();
  const seen = new Map<string, number[]>();
  const errors: unknown[] = [];
  const worker = new Worker<{ kind: string }>(name, async (job, signal) => {
    signal.throwIfAborted();
    check(expected.includes(job.id), `unexpected job ${job.id}`);
    seen.set(job.id, [...(seen.get(job.id) ?? []), job.attempts]);
    if (retry && job.data.kind === "retry" && job.attempts === 1) throw new Error("expected smoke retry");
  }, { address, pollIntervalMs: 100, onError: error => {
    if (!(error instanceof Error && error.message === "expected smoke retry")) errors.push(String(error));
  } });
  try {
    await until(async () => {
      check(errors.length === 0, `worker errors: ${errors}`);
      return (await completed()) === baseline + expected.length;
    });
  } finally { await worker.close(); }
  check(expected.every(id => seen.has(id)), "not all persisted IDs were processed");
  check(errors.length === 0, `worker errors: ${errors}`);
  return Object.fromEntries(seen);
}
try {
  await mkdir(results, { recursive: true });
  await until(async () => {
    try { return (await fetch(`${http}/readyz`, { signal: abort.signal })).ok; } catch { return false; }
  });
  let report: object;
  if (mode === "smoke") {
    const name = `smoke-${crypto.randomUUID()}`;
    const queue = new Queue(name, { address });
    let ids: string[];
    try {
      ids = [(await queue.add({ kind: "success" })).id,
        (await queue.add({ kind: "retry" }, { maxAttempts: 2, retryBackoffMs: 500 })).id,
        (await queue.add({ kind: "delayed" }, { delayMs: 1500 })).id];
    } finally { queue.close(); }
    const attempts = await processJobs(name, ids, true);
    check(JSON.stringify(attempts[ids[1]]) === "[1,2]", "retry did not use two attempts");
    report = { scenario: mode, ids, attempts };
  } else if (mode === "seed") {
    const name = `persist-${crypto.randomUUID()}`;
    const queue = new Queue(name, { address });
    const ids: string[] = [];
    try { for (let n = 0; n < 10; n++) ids.push((await queue.add({ kind: "persist", n })).id); }
    finally { queue.close(); }
    report = { scenario: mode, name, ids };
    await writeFile(join(results, "pending.json"), JSON.stringify(report, null, 2));
  } else if (mode === "verify") {
    const seed = JSON.parse(await readFile(join(results, "pending.json"), "utf8")) as { name: string; ids: string[] };
    check(seed.ids.length === 10, "invalid seed manifest");
    const attempts = await processJobs(seed.name, seed.ids, false);
    check(Object.values(attempts).every(a => JSON.stringify(a) === "[1]"), "unexpected persistence redelivery");
    report = { scenario: mode, ids: seed.ids, attempts };
  } else { throw new Error(`unknown scenario ${mode}`); }
  const output = { passed: true, elapsedMs: Date.now() - start, ...report };
  await writeFile(join(results, `${mode}.json`), JSON.stringify(output, null, 2));
  console.log(JSON.stringify(output));
} catch (error) {
  console.error(error); process.exitCode = 1;
  await writeFile(join(results, `${mode}.json`), JSON.stringify({ passed: false, elapsedMs: Date.now() - start, error: String(error) }, null, 2)).catch(() => {});
}
finally { clearTimeout(deadline); }
