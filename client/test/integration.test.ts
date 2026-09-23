import { test, expect } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import { Queue, Worker, RateLimits, IngressLimits } from "../src/index";
import { verifyCompletionAmbiguity } from "./completion-proxy";

async function until(check: () => boolean, timeout = 60000) {
  const end = Date.now() + timeout;
  while (!check()) { if (Date.now() > end) throw new Error("condition timed out"); await sleep(50); }
}
async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>(r => server.close(() => r()));
  return port;
}

test("real gRPC: lifecycle, lease recovery, and ambiguous completion retries", async () => {
  const dir = await mkdtemp(join(tmpdir(), "anvil-client-"));
  const address = `127.0.0.1:${await freePort()}`;
  const httpAddress = `127.0.0.1:${await freePort()}`;
  const executable = resolve("../target/debug/" + (process.platform === "win32" ? "rusty-queue.exe" : "rusty-queue"));
  const server = Bun.spawn([executable], { cwd: dir, env: { ...process.env, RUST_LOG: "info", ANVILMQ_ADDR: address, ANVILMQ_HTTP_ADDR: httpAddress, ANVILMQ_DB_PATH: join(dir, "test.db") }, stdout: "pipe", stderr: "pipe" });
  let log = "";
  const output = (async () => { for await (const chunk of server.stdout) log += new TextDecoder().decode(chunk); })();
  const errors = new Response(server.stderr).text();
  const queues: Queue<any>[] = [];
  const workers: Worker<any>[] = [];
  let crash: ReturnType<typeof Bun.spawn> | undefined;
  try {
    await until(() => log.includes("daemon listening"), 10000);
    expect((await fetch(`http://${httpAddress}/healthz`)).status).toBe(200);
    expect((await fetch(`http://${httpAddress}/readyz`)).status).toBe(200);
    const limits = new RateLimits({ address });
    try {
      await limits.upsert("demo:paused", 0, 1000);
      expect((await limits.status("demo:paused")).isThrottled).toBe(true);
      expect(await limits.delete("demo:paused")).toBe(true);
      expect((await limits.status("demo:paused")).ruleExists).toBe(false);
    } finally { limits.close(); }
    const ingress = new IngressLimits({ address });
    const paused = new Queue<{ n: number }>("ingress-demo", { address }); queues.push(paused);
    try {
      // A one-per-window rule admits the first enqueue and rejects the second in-window.
      await ingress.upsert("ingress:tenant", 1, 60000);
      await paused.add({ n: 1 }, { rateLimitFacet: "ingress:tenant" });
      expect((await ingress.status("ingress:tenant")).estimatedCount).toBe(1);
      await expect(paused.add({ n: 2 }, { rateLimitFacet: "ingress:tenant" })).rejects.toBeDefined();
      expect(await ingress.delete("ingress:tenant")).toBe(true);
      expect((await ingress.status("ingress:tenant")).ruleExists).toBe(false);
    } finally { ingress.close(); }
    const queue = new Queue<{ kind: string }>("test", { address }); queues.push(queue);
    const seen: { kind: string; attempt: number; at: number }[] = [];
    const added = Date.now();
    await queue.add({ kind: "success" });
    await queue.add({ kind: "delayed" }, { delayMs: 600 });
    await queue.add({ kind: "retry" }, { retryBackoffMs: 300, maxAttempts: 2 });
    const worker = new Worker<{ kind: string }>("test", async job => {
      seen.push({ kind: job.data.kind, attempt: job.attempts, at: Date.now() });
      if (job.data.kind === "retry" && job.attempts === 1) throw new Error("expected failure");
    }, { address, pollIntervalMs: 50, onError: () => {} }); workers.push(worker);
    await until(() => seen.length === 4, 10000); await worker.close();
    expect(seen.find(x => x.kind === "delayed")!.at - added).toBeGreaterThanOrEqual(550);
    const retries = seen.filter(x => x.kind === "retry");
    expect(retries.map(x => x.attempt)).toEqual([1, 2]);
    expect(retries[1].at - retries[0].at).toBeGreaterThanOrEqual(280);

    // Start a real process, kill it after claim, and recover that persisted claim.
    const cq = new Queue("crash", { address }); queues.push(cq); await cq.add({ crash: true });
    crash = Bun.spawn([process.execPath, "test/crash-worker.ts"], { env: { ...process.env, ANVILMQ_ADDR: address }, stdout: "pipe", stderr: "inherit" });
    let claimed = false;
    const reader = (async () => { for await (const chunk of crash!.stdout as ReadableStream<Uint8Array>) { if (new TextDecoder().decode(chunk).includes("CLAIMED")) claimed = true; } })();
    await until(() => claimed, 10000); crash.kill(); await crash.exited; await reader;
    let recovered = 0;
    const recovery = new Worker("crash", async job => { recovered = job.attempts; }, { address, pollIntervalMs: 100 }); workers.push(recovery);

    // Run longer than a lease and call close during work: heartbeats must continue.
    const lq = new Queue("long", { address }); queues.push(lq); await lq.add({});
    let started = false; let finished = false; const longErrors: unknown[] = [];
    const long = new Worker("long", async (_job, signal) => {
      started = true; await sleep(32000, undefined, { signal }); finished = true;
    }, { address, onError: e => longErrors.push(e) }); workers.push(long);
    await until(() => started, 5000); await long.close();
    expect(finished).toBe(true); expect(longErrors).toEqual([]);
    await until(() => recovered > 0, 10000); await recovery.close();
    expect(recovered).toBe(2);
    const records = log.trim().split("\n").map(line => JSON.parse(line));
    expect(records.some(record => record.fields?.message === "job transition" && record.fields?.to === "Active" && record.fields?.worker_id)).toBe(true);
    const metrics = await (await fetch(`http://${httpAddress}/metrics`)).text();
    expect(metrics).toContain('anvilmq_jobs{state="Completed"} 5');
    expect(metrics).toContain('anvilmq_jobs{state="Active"} 0');
    expect(metrics).toContain('anvilmq_transitions_total{event="retried"} 2');
    expect(metrics).toContain('anvilmq_transitions_total{event="lease_expired"} 1');
    expect(metrics).toContain('anvilmq_rpc_duration_seconds_bucket{method="Heartbeat"');
    await verifyCompletionAmbiguity(address, httpAddress);
  } finally {
    if (crash && crash.exitCode === null) { crash.kill(); await crash.exited; }
    await Promise.all(workers.map(w => w.close()));
    queues.forEach(q => q.close());
    server.kill(); await server.exited; await output; await errors;
    await rm(dir, { recursive: true, force: true });
  }
}, 90000);
