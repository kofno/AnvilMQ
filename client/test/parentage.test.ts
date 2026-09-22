import { test, expect } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import { Queue, Worker, type Job } from "../src/index";

async function until(check: () => boolean | Promise<boolean>, timeout = 10000) {
  const end = Date.now() + timeout;
  while (!(await check())) { if (Date.now() > end) throw new Error("condition timed out"); await sleep(50); }
}
async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>(r => server.close(() => r()));
  return port;
}

test("enqueueChild forces parentage and lets the server derive depth/trace", async () => {
  const dir = await mkdtemp(join(tmpdir(), "anvil-parentage-"));
  const address = `127.0.0.1:${await freePort()}`;
  const httpAddress = `127.0.0.1:${await freePort()}`;
  const executable = resolve("../target/debug/" + (process.platform === "win32" ? "rusty-queue.exe" : "rusty-queue"));
  const server = Bun.spawn([executable], { cwd: dir, env: { ...process.env, RUST_LOG: "info", ANVILMQ_ADDR: address, ANVILMQ_HTTP_ADDR: httpAddress, ANVILMQ_DB_PATH: join(dir, "test.db") }, stdout: "pipe", stderr: "pipe" });
  let log = "";
  const output = (async () => { for await (const chunk of server.stdout) log += new TextDecoder().decode(chunk); })();
  const errors = new Response(server.stderr).text();
  const queue = new Queue<{ kind: string }>("chain", { address });
  const seen: Job<{ kind: string }>[] = [];
  const worker = new Worker<{ kind: string }>("chain", async job => {
    seen.push(job);
    if (job.data.kind === "root") {
      // A caller-supplied parentId must be ignored/overridden with this job's id.
      const res = await (job.enqueueChild as unknown as (n: string, d: unknown, o: unknown) => Promise<{ id: string }>)(
        "chain", { kind: "child" }, { metadata: { parentId: "attacker" } },
      );
      expect(typeof res.id).toBe("string");
    }
  }, { address, pollIntervalMs: 50, onError: () => {} });
  try {
    await until(() => log.includes("daemon listening"));
    // readyz (HTTP) comes up a beat before the gRPC socket accepts; the enqueue is
    // idempotent (same key replays), so retry until the channel is live.
    await until(async () => { try { await queue.add({ kind: "root" }, { idempotencyKey: "root-1" }); return true; } catch { return false; } });
    await until(() => seen.length === 2);
    const root = seen.find(j => j.data.kind === "root")!;
    const child = seen.find(j => j.data.kind === "child")!;
    // enqueueChild forced this job as the parent (not the spoofed "attacker").
    expect(child.metadata.parentId).toBe(root.id);
    // Server derived depth (root 0 -> child 1) and inherited the lineage trace.
    expect(child.metadata.executionDepth).toBe(1);
    expect(child.metadata.traceId).toBe(root.metadata.traceId);
  } finally {
    await worker.close();
    queue.close();
    server.kill(); await server.exited; await output; await errors;
    await rm(dir, { recursive: true, force: true });
  }
}, 30000);
