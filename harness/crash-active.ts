import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { mkdir, readFile, writeFile, rename } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

// Resolve the runner's existing dependencies through the client package.
const require = createRequire(new URL("../client/package.json", import.meta.url));
const grpc = require("@grpc/grpc-js");
const loader = require("@grpc/proto-loader");
const api = grpc.loadPackageDefinition(loader.loadSync(fileURLToPath(new URL("../proto/queue.proto", import.meta.url)), {
  longs: Number, defaults: true, bytes: Buffer,
}));
const client = new api.queue.v1.QueueService(process.env.ANVILMQ_ADDR ?? "127.0.0.1:50061", grpc.credentials.createInsecure());
const runId = process.env.CRASH_RUN_ID;
if (!runId || !/^[a-zA-Z0-9-]+$/.test(runId)) throw new Error("CRASH_RUN_ID must be an alphanumeric/hyphen run identifier");
const results = join(process.env.ANVILMQ_RESULTS_DIR ?? "harness/artifacts", runId);
const name = `crash-${runId}`;
const workerId = `worker-${runId}`;
const start = performance.now();
const deadline = start + 150000;
interface Claim { found: boolean; id: string; payload: Buffer; attempts: number; leaseExpiresAtMs: number }
interface RpcError { code?: number }
const acknowledged = new Map<string, number>();
const ambiguousAdds: { sequence: number; code: number }[] = [];
const deliveries = new Map<string, number[]>();
const completed = new Set<string>();
const held = new Map<string, Claim>();
const recovered = new Set<string>();
const unacknowledged = new Map<string, number>();
let sequence = 0;
let stopProducers = false;
let producerFailure: unknown;
let producers: Promise<void>[] = [];
let outageAt: number | undefined;
let firstRecoveredAt: number | undefined;
let staleCompletionsRejected = 0;
let staleHeartbeatsRejected = 0;
let orchestration: unknown;
function check(ok: unknown, message: string): asserts ok { if (!ok) throw new Error(message); }
function withinDeadline() { check(performance.now() < deadline, "active-crash scenario timed out"); }
function call<T>(method: string, request: object): Promise<T> {
  return new Promise((resolve, reject) => client[method](request, { deadline: Date.now() + 2000 },
    (error: RpcError | null, response: T) => error ? reject(error) : resolve(response)));
}
async function save(file: string, value: unknown) {
  const path = join(results, file);
  await writeFile(`${path}.tmp`, JSON.stringify(value, null, 2));
  await rename(`${path}.tmp`, path);
}
async function marker(file: string): Promise<any> {
  while (true) {
    withinDeadline();
    try { return JSON.parse(await readFile(join(results, file), "utf8")); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    await sleep(100);
  }
}
async function add() {
  const seq = sequence++;
  try {
    const result = await call<{ id: string }>("addJob", { name, payload: Buffer.from(JSON.stringify({ sequence: seq })), maxAttempts: 3 });
    check(!acknowledged.has(result.id), "duplicate enqueue ID");
    acknowledged.set(result.id, seq);
  } catch (error) {
    const code = (error as RpcError).code;
    if (code === undefined || (code !== grpc.status.UNAVAILABLE && code !== grpc.status.DEADLINE_EXCEEDED)) throw error;
    ambiguousAdds.push({ sequence: seq, code });
    outageAt ??= performance.now();
    throw error;
  }
}
async function produce() {
  try {
    while (!stopProducers) {
      withinDeadline();
      await add();
      await sleep(10);
    }
  } catch (error) {
    const code = (error as RpcError).code;
    if (code !== grpc.status.UNAVAILABLE && code !== grpc.status.DEADLINE_EXCEEDED) producerFailure = error;
  }
}
async function claim(): Promise<Claim> {
  return call("getNextJob", { workerId, queueNames: [name] });
}
function observe(job: Claim) {
  const attempts = deliveries.get(job.id) ?? [];
  check(!attempts.includes(job.attempts), `duplicate delivery of the same attempt: ${job.id}/${job.attempts}`);
  attempts.push(job.attempts);
  deliveries.set(job.id, attempts);
}
async function rejected(method: string, id: string, attempt: number) {
  try { await call(method, { id, workerId, attempt }); }
  catch (error) {
    check((error as RpcError).code === grpc.status.FAILED_PRECONDITION, `${method}: expected stale-attempt rejection, got ${String(error)}`);
    return;
  }
  throw new Error(`${method} accepted stale attempt ${id}/${attempt}`);
}

await mkdir(results, { recursive: true });
let failure: unknown;
try {
  // These four handlers simulate unfinished work. They intentionally stop renewing
  // leases after the broker crashes; no completion RPC crosses the crash boundary.
  for (let i = 0; i < 20; i++) await add();
  for (let i = 0; i < 4; i++) {
    const job = await claim();
    check(job.found && job.attempts === 1 && acknowledged.has(job.id), "initial claim missing or invalid");
    observe(job); held.set(job.id, job);
  }
  producers = [produce(), produce()];
  while (acknowledged.size < 40) {
    withinDeadline(); check(!producerFailure && outageAt === undefined, "producer failed before crash readiness");
    await sleep(10);
  }
  check([...held.values()].every(job => job.leaseExpiresAtMs - Date.now() > 15000), "held leases too close to expiry before crash");
  await save("ready.json", { name, workerId, acknowledged: Object.fromEntries(acknowledged), held: [...held.values()], readyAt: new Date().toISOString() });
  await marker("killed.json");
  await Promise.all(producers);
  check(!producerFailure, `unexpected producer failure: ${String(producerFailure)}`);
  check(outageAt !== undefined && ambiguousAdds.length === 2, "both producers must observe the broker outage");
  await save("outage.json", { acknowledged: Object.fromEntries(acknowledged), ambiguousAdds });
  orchestration = await marker("restarted.json");

  let emptySince: number | undefined;
  while (true) {
    withinDeadline();
    const job = await claim();
    if (!job.found) {
      if ([...acknowledged.keys()].every(id => completed.has(id))) {
        emptySince ??= performance.now();
        if (performance.now() - emptySince >= 1000) break;
      }
      await sleep(50); continue;
    }
    emptySince = undefined;
    check(!completed.has(job.id), `completed job was delivered again: ${job.id}`);
    const seq = JSON.parse(job.payload.toString("utf8")).sequence;
    if (acknowledged.has(job.id)) check(acknowledged.get(job.id) === seq, `payload mismatch: ${job.id}`);
    else {
      check(ambiguousAdds.some(a => a.sequence === seq), `unexpected job: ${job.id}`);
      check(![...unacknowledged.values()].includes(seq), `duplicate enqueue for sequence ${seq}`);
      unacknowledged.set(job.id, seq);
    }
    observe(job);
    const prior = held.get(job.id);
    if (prior) {
      check(job.attempts === prior.attempts + 1, `unexpected recovered attempt: ${job.id}`);
      firstRecoveredAt ??= performance.now();
      // Reuse the same worker ID so the attempt token itself must fence the old claim.
      await rejected("completeJob", job.id, prior.attempts); staleCompletionsRejected++;
      await rejected("heartbeat", job.id, prior.attempts); staleHeartbeatsRejected++;
      recovered.add(job.id);
    } else check(job.attempts === 1, `unexpected retry: ${job.id}`);
    const result = await call<{ success: boolean }>("completeJob", { id: job.id, workerId, attempt: job.attempts });
    check(result.success, `completion failed: ${job.id}`);
    completed.add(job.id);
  }
  check(recovered.size === held.size, "not all abandoned claims recovered");
  check(staleCompletionsRejected === 4 && staleHeartbeatsRejected === 4, "fencing checks incomplete");
} catch (error) { failure = error; process.exitCode = 1; }
finally {
  stopProducers = true;
  await Promise.allSettled(producers);
  client.close();
}
const missing = [...acknowledged.keys()].filter(id => !completed.has(id));
const report = {
  passed: !failure, scenario: "crash-active", runId, name, error: failure ? String(failure) : null,
  elapsedMs: performance.now() - start, orchestration,
  acknowledged: acknowledged.size, completed: completed.size, missingAcknowledgedIds: missing,
  abandonedClaims: held.size, recoveredClaims: recovered.size,
  redeliveries: [...deliveries.values()].reduce((sum, attempts) => sum + Math.max(0, attempts.length - 1), 0),
  staleCompletionsRejected, staleHeartbeatsRejected,
  outageToFirstRecoveredClaimMs: firstRecoveredAt !== undefined && outageAt !== undefined ? firstRecoveredAt - outageAt : null,
  outageToVerificationMs: outageAt !== undefined ? performance.now() - outageAt : null,
  ambiguousAdds, committedAmbiguousAdds: Object.fromEntries(unacknowledged),
  acknowledgedIds: Object.fromEntries(acknowledged), completedIds: [...completed], attempts: Object.fromEntries(deliveries),
};
await save("report.json", report);
const markdown = `# Active broker crash recovery\n\nResult: ${report.passed ? "PASS" : "FAIL"}\n\nRun: ${runId}\n\n` +
  `- Acknowledged: ${report.acknowledged}; completed: ${report.completed}; missing acknowledged: ${missing.length}\n` +
  `- Abandoned/recovered claims: ${held.size}/${recovered.size}; redeliveries: ${report.redeliveries}\n` +
  `- Stale completions/heartbeats rejected: ${staleCompletionsRejected}/${staleHeartbeatsRejected}\n` +
  `- Outage to first recovered claim: ${report.outageToFirstRecoveredClaimMs?.toFixed(0) ?? "n/a"} ms\n` +
  `- Outage to verification: ${report.outageToVerificationMs?.toFixed(0) ?? "n/a"} ms\n` +
  `- Ambiguous enqueue RPCs: ${ambiguousAdds.length}; observed committed: ${unacknowledged.size}\n` +
  (failure ? `\nFailure: ${String(failure)}\n` : "") +
  "\nSIGKILL of one broker with the same volume. Held claims use the normal 30-second lease. No power/node loss or completion-RPC fault injection. Redelivery is expected; external side effects are not deduplicated by this test.\n";
await writeFile(join(results, "report.md"), markdown);
console.log(markdown);
