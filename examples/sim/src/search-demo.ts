// Full-text search demonstration for the AnvilMQ sim.
//
// The broker in this cluster runs with ANVILMQ_FTS_ENABLED=1, so `/v1/search` resolves free-text
// `q` through the opt-in FTS5 index over job_history metadata (name / trace_id / last_error)
// instead of an escaped-LIKE scan. This script seeds a batch of failing jobs whose error messages
// carry distinct, invented tokens, waits for the trigger-maintained index to catch up, then runs a
// series of searches that show tokenized retrieval, implicit-AND narrowing, FTS + structured-filter
// combination, name-column matching, and the token-vs-substring behavior.
//
// Isolated: it talks only to the local sim broker over loopback and enqueues throwaway jobs.
import { setTimeout as sleep } from "node:timers/promises";
import { ADDRESS, HTTP, JobName, enqueue, closeAllQueues } from "./queues";

type Row = {
  id: string;
  name: string;
  state: string;
  last_error: string | null;
  trace_id: string | null;
};

async function search(params: Record<string, string>): Promise<Row[]> {
  const qs = new URLSearchParams(params).toString();
  const res = await fetch(`${HTTP}/v1/search?${qs}`, { signal: AbortSignal.timeout(5000) });
  if (!res.ok) throw new Error(`search ?${qs} -> HTTP ${res.status}`);
  return (await res.json()) as Row[];
}

// A unique, single-token run tag isolates this run's failures from any other traffic in the
// cluster, so token counts are exact regardless of what the producer is doing.
const runTag = `ftsdemo${Math.random().toString(36).slice(2, 10)}`;
const categories = [
  { token: "timeout", message: `${runTag} connection timeout contacting upstream service` },
  { token: "schema", message: `${runTag} invalid payload schema rejected by validator` },
  { token: "checksum", message: `${runTag} checksum mismatch detected on stored record` },
];
const perCategory = 2;
const expectedTotal = categories.length * perCategory;

let passed = 0;
let failed = 0;
function check(name: string, ok: boolean, detail: string) {
  console.log(`${ok ? "PASS" : "FAIL"}  ${name} :: ${detail}`);
  if (ok) passed++;
  else failed++;
}

console.log(JSON.stringify({ event: "search-demo-started", address: ADDRESS, http: HTTP, runTag }));

// 1. Seed failing jobs. maxAttempts:1 means each fails on its first attempt (no backoff) and lands
//    in job_history with our message as last_error. The sim's Throw worker rethrows job.data.message.
for (const c of categories) {
  for (let i = 0; i < perCategory; i++) {
    await enqueue(JobName.Throw, { message: c.message }, { maxAttempts: 1 });
  }
}
console.log(JSON.stringify({ event: "seeded", total: expectedTotal }));

// 2. Wait for the workers to fail them and the FTS index (kept in sync by the AFTER INSERT trigger
//    on job_history) to catch up.
const deadline = performance.now() + 30000;
let indexed = 0;
while (performance.now() < deadline) {
  indexed = (await search({ q: runTag, state: "Failed", limit: "100" })).length;
  if (indexed >= expectedTotal) break;
  await sleep(500);
}
check("index caught up", indexed >= expectedTotal, `${indexed}/${expectedTotal} tagged failures indexed`);

// 3. Tokenized retrieval: the unique run tag returns exactly our seeded failures.
const all = await search({ q: runTag, limit: "100" });
check(
  "token retrieval (run tag)",
  all.length === expectedTotal,
  `q="${runTag}" -> ${all.length} rows (expect ${expectedTotal})`,
);

// 4. Implicit AND: two whitespace-separated tokens must BOTH be present, narrowing to one category.
for (const c of categories) {
  const rows = await search({ q: `${runTag} ${c.token}`, limit: "100" });
  check(
    `token AND (${c.token})`,
    rows.length === perCategory,
    `q="${runTag} ${c.token}" -> ${rows.length} rows (expect ${perCategory})`,
  );
}

// 5. FTS + structured filter combine: same tag, restricted to the Failed state.
const failedOnly = await search({ q: runTag, state: "Failed", limit: "100" });
check(
  "FTS + state filter",
  failedOnly.length === expectedTotal && failedOnly.every((r) => r.state === "Failed"),
  `q="${runTag}" & state=Failed -> ${failedOnly.length} rows, all Failed=${failedOnly.every((r) => r.state === "Failed")}`,
);

// 6. Name-column indexing: searching the job name matches by name. Other Throw failures from the
//    producer count too, so this is a lower bound.
const byName = await search({ q: "Throw", state: "Failed", limit: "100" });
check("name-column match", byName.length >= expectedTotal, `q="Throw" & state=Failed -> ${byName.length} rows (>= ${expectedTotal})`);

// 7. Token vs substring: FTS matches whole tokens, not substrings. A fragment of a real token that
//    is not itself a token returns nothing (an escaped-LIKE scan would have matched it). Combined
//    with the run tag so the result is deterministic.
const substr = await search({ q: `${runTag} hecksum`, limit: "100" });
check(
  "substring is not a token match",
  substr.length === 0,
  `q="${runTag} hecksum" -> ${substr.length} rows (expect 0; FTS is token-based, not substring)`,
);

// Sample output so the demo is legible at a glance.
console.log(
  JSON.stringify(
    {
      event: "sample",
      rows: all.slice(0, 3).map((r) => ({ id: r.id, name: r.name, state: r.state, last_error: r.last_error })),
    },
    null,
    2,
  ),
);

console.log(JSON.stringify({ event: "search-demo-finished", total: passed + failed, passed, failed }));
closeAllQueues();
process.exit(failed ? 1 : 0);
