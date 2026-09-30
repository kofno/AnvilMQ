import { test, expect } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import * as grpc from "@grpc/grpc-js";
import {
  Queue, Worker, __setClientFactory, nodeHealthListener, fetchHealthHandler,
  renderPrometheus, gracefulShutdown,
  type WorkerHealth, type WorkerMetrics, type Logger,
} from "../src/index";

async function until(check: () => boolean | Promise<boolean>, timeout = 30000) {
  const end = Date.now() + timeout;
  while (!(await check())) { if (Date.now() > end) throw new Error("condition timed out"); await sleep(25); }
}
async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>(r => server.close(() => r()));
  return port;
}

/** In-memory client whose per-method outcome is driven by `next()`; mirrors resilience.test.ts. */
type Outcome = { error?: unknown; result?: unknown };
function scriptedClient(next: () => Outcome): grpc.Client {
  const call = (_req: unknown, _opts: unknown, cb: (e: unknown, r: unknown) => void) => {
    const o = next();
    queueMicrotask(() => (o.error ? cb(o.error, undefined) : cb(null, o.result ?? { found: false })));
  };
  return { getNextJob: call, heartbeat: call, completeJob: call, failJob: call, addJob: call, close: () => {} } as unknown as grpc.Client;
}

/** Spawn the real broker (mirrors health.test.ts) so lifecycle events are broker-authoritative. */
function brokerHarness() {
  return (async () => {
    const dir = await mkdtemp(join(tmpdir(), "anvil-obs-"));
    const address = `127.0.0.1:${await freePort()}`;
    const httpAddress = `127.0.0.1:${await freePort()}`;
    const dbPath = join(dir, "test.db");
    const executable = resolve("../target/debug/" + (process.platform === "win32" ? "rusty-queue.exe" : "rusty-queue"));
    const spawnBroker = () => Bun.spawn([executable], { cwd: dir, env: { ...process.env, RUST_LOG: "info", ANVILMQ_ADDR: address, ANVILMQ_HTTP_ADDR: httpAddress, ANVILMQ_DB_PATH: dbPath }, stdout: "pipe", stderr: "pipe" });
    const drainOut = (s: ReturnType<typeof Bun.spawn>) => { void (async () => { try { for await (const _ of s.stdout as ReadableStream<Uint8Array>) { /* drain */ } } catch { /* ignore */ } })(); void new Response(s.stderr as ReadableStream<Uint8Array>).text().catch(() => {}); };
    return { dir, address, httpAddress, dbPath, spawnBroker, drainOut };
  })();
}

// 1. Hook firing (real broker): success fires onActive then onCompleted; an always-throwing
//    handler with maxAttempts>1 fires onActive+onRetry on non-final deliveries and
//    onActive+onFailed exactly once on the final delivery.
test("hooks: onActive/onCompleted on success; onRetry (maxAttempts-1) then onFailed once on terminal failure", async () => {
  const h = await brokerHarness();
  const server = h.spawnBroker(); h.drainOut(server);
  let worker: Worker<{ mode: string }> | undefined;
  let queue: Queue<{ mode: string }> | undefined;
  const active: string[] = [];
  const completed: string[] = [];
  const retried: number[] = [];
  const failed: string[] = [];
  const order: string[] = [];
  try {
    await until(async () => { try { return (await fetch(`http://${h.httpAddress}/readyz`)).status === 200; } catch { return false; } }, 15000);
    queue = new Queue<{ mode: string }>("hooks", { address: h.address });
    worker = new Worker<{ mode: string }>("hooks", async job => {
      if (job.data.mode === "boom") throw new Error("always fails");
    }, {
      address: h.address, pollIntervalMs: 50, onError: () => {},
      onActive: info => { active.push(info.id); order.push(`active:${info.id}`); },
      onCompleted: id => { completed.push(id); order.push(`completed:${id}`); },
      onRetry: info => { retried.push(info.attempt); order.push(`retry:${info.id}`); },
      onFailed: info => { failed.push(info.id); order.push(`failed:${info.id}`); },
    });
    await until(() => worker!.health().ready === true, 10000);

    // Success job.
    const ok = await queue.add({ mode: "ok" });
    await until(() => completed.includes(ok.id), 15000);
    // onActive fired before onCompleted for the success job.
    expect(active).toContain(ok.id);
    expect(order.indexOf(`active:${ok.id}`)).toBeLessThan(order.indexOf(`completed:${ok.id}`));

    // Always-throwing job with maxAttempts 3: two retries then one terminal failure.
    const maxAttempts = 3;
    const bad = await queue.add({ mode: "boom" }, { maxAttempts, retryBackoffMs: 100, retryBackoffMaxMs: 200 });
    await until(() => failed.includes(bad.id), 20000);
    // Exactly maxAttempts-1 retries (moved_to_failed_state=false) and one terminal (=true).
    expect(retried.length).toBe(maxAttempts - 1);
    expect(failed.filter(id => id === bad.id).length).toBe(1);
    // onActive fired for every delivery of the bad job (maxAttempts times).
    expect(active.filter(id => id === bad.id).length).toBe(maxAttempts);
  } finally {
    if (worker) await worker.close();
    if (queue) queue.close();
    if (server.exitCode === null) { server.kill(); await server.exited; }
    await rm(h.dir, { recursive: true, force: true });
  }
}, 90000);

// 2. Hook isolation: a throwing onActive callback is swallowed by safeEmit and the job +
//    loop continue (a subsequent job still completes).
test("hooks: a throwing onActive is swallowed and the loop keeps processing", async () => {
  const h = await brokerHarness();
  const server = h.spawnBroker(); h.drainOut(server);
  let worker: Worker<{ n: number }> | undefined;
  let queue: Queue<{ n: number }> | undefined;
  const completed: string[] = [];
  const errors: unknown[] = [];
  try {
    await until(async () => { try { return (await fetch(`http://${h.httpAddress}/readyz`)).status === 200; } catch { return false; } }, 15000);
    queue = new Queue<{ n: number }>("iso", { address: h.address });
    worker = new Worker<{ n: number }>("iso", async () => {}, {
      address: h.address, pollIntervalMs: 50,
      onError: e => { errors.push(e); },
      onActive: () => { throw new Error("hook blew up"); },
      onCompleted: id => { completed.push(id); },
    });
    await until(() => worker!.health().ready === true, 10000);
    const a = await queue.add({ n: 1 });
    const b = await queue.add({ n: 2 });
    await until(() => completed.includes(a.id) && completed.includes(b.id), 15000);
    // The throwing hook was routed to onError, and both jobs still completed.
    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(completed).toContain(a.id);
    expect(completed).toContain(b.id);
  } finally {
    if (worker) await worker.close();
    if (queue) queue.close();
    if (server.exitCode === null) { server.kill(); await server.exited; }
    await rm(h.dir, { recursive: true, force: true });
  }
}, 90000);

// 3a. Metrics counters (real broker): after N successes + M terminal failures, metrics()
//     shows jobsProcessedTotal==N, jobsFailedTotal==M, jobsActive back to 0.
test("metrics: processed/failed counters track a real broker; jobsActive returns to 0", async () => {
  const h = await brokerHarness();
  const server = h.spawnBroker(); h.drainOut(server);
  let worker: Worker<{ mode: string }> | undefined;
  let queue: Queue<{ mode: string }> | undefined;
  const N = 3, M = 2;
  const done: string[] = [];
  const failed: string[] = [];
  try {
    await until(async () => { try { return (await fetch(`http://${h.httpAddress}/readyz`)).status === 200; } catch { return false; } }, 15000);
    queue = new Queue<{ mode: string }>("metrics", { address: h.address });
    worker = new Worker<{ mode: string }>("metrics", async job => {
      if (job.data.mode === "boom") throw new Error("terminal");
    }, {
      address: h.address, pollIntervalMs: 50, onError: () => {},
      onCompleted: id => { done.push(id); },
      onFailed: info => { failed.push(info.id); },
    });
    await until(() => worker!.health().ready === true, 10000);
    for (let i = 0; i < N; i++) await queue.add({ mode: "ok" });
    for (let i = 0; i < M; i++) await queue.add({ mode: "boom" }, { maxAttempts: 1 });
    await until(() => done.length === N && failed.length === M, 20000);
    // Let inFlight settle.
    await until(() => worker!.metrics().jobsActive === 0, 10000);
    const m = worker.metrics();
    expect(m.jobsProcessedTotal).toBe(N);
    expect(m.jobsFailedTotal).toBe(M);
    expect(m.jobsRetriedTotal).toBe(0);
    expect(m.jobsActive).toBe(0);
    expect(m.connectionUp).toBe(1);
  } finally {
    if (worker) await worker.close();
    if (queue) queue.close();
    if (server.exitCode === null) { server.kill(); await server.exited; }
    await rm(h.dir, { recursive: true, force: true });
  }
}, 90000);

// 3b. poll_failures_total and reconnects_total via the scripted client seam.
test("metrics: poll failures and reconnects move via a scripted UNAVAILABLE-then-recover seam", async () => {
  let calls = 0;
  const previous = __setClientFactory(() => scriptedClient(() => {
    calls++;
    // Fail the first four polls (crosses threshold 3) then recover.
    return calls <= 4 ? { error: { code: grpc.status.UNAVAILABLE } } : { result: { found: false } };
  }));
  let worker: Worker | undefined;
  try {
    worker = new Worker("reconnect", async () => {}, {
      address: "127.0.0.1:1", pollIntervalMs: 10, pollBackoffMaxMs: 40, connectionLostThreshold: 3, onError: () => {},
    });
    await until(() => worker!.metrics().reconnectsTotal === 1);
    const m = worker.metrics();
    expect(m.pollFailuresTotal).toBeGreaterThanOrEqual(4);
    expect(m.reconnectsTotal).toBe(1);
    expect(m.connectionUp).toBe(1);
  } finally { __setClientFactory(previous); if (worker) await worker.close(); }
});

// 4. Prometheus text: TYPE line + one sample per series, labels render as {k="v"}.
test("renderPrometheus: valid exposition with TYPE lines, samples, and optional labels", () => {
  const m: WorkerMetrics = {
    jobsProcessedTotal: 5, jobsFailedTotal: 2, jobsRetriedTotal: 3,
    jobsActive: 1, pollFailuresTotal: 7, reconnectsTotal: 4, connectionUp: 1,
  };
  const text = renderPrometheus(m);
  expect(text).toContain("# TYPE anvilmq_client_jobs_processed_total counter");
  expect(text).toContain("# HELP anvilmq_client_jobs_processed_total");
  expect(text).toContain("# TYPE anvilmq_client_jobs_active gauge");
  expect(text).toContain("# TYPE anvilmq_client_connection_up gauge");
  expect(text.endsWith("\n")).toBe(true);
  // One sample line per series with the expected numeric value (no labels => no braces).
  const sample = (name: string, value: number) => {
    const line = text.split("\n").find(l => l.startsWith(`anvilmq_client_${name} `));
    expect(line).toBeDefined();
    expect(line).toBe(`anvilmq_client_${name} ${value}`);
  };
  sample("jobs_processed_total", 5);
  sample("jobs_failed_total", 2);
  sample("jobs_retried_total", 3);
  sample("poll_failures_total", 7);
  sample("reconnects_total", 4);
  sample("jobs_active", 1);
  sample("connection_up", 1);
  // Seven series each emit HELP + TYPE + sample = 21 non-empty lines.
  expect(text.split("\n").filter(l => l.length > 0).length).toBe(21);
  // Labels render as {k="v",...} on every sample.
  const labelled = renderPrometheus(m, { labels: { worker: "w1", zone: "us" } });
  expect(labelled).toContain('anvilmq_client_jobs_processed_total{worker="w1",zone="us"} 5');
  expect(labelled).toContain('anvilmq_client_connection_up{worker="w1",zone="us"} 1');
});

// 5. /metrics route on both handlers; unset metricsPath keeps /metrics a 404; live/ready JSON unchanged.
test("/metrics route: served only when metricsPath is set; probes stay JSON", async () => {
  const health: WorkerHealth = { live: true, ready: true, connected: true, stale: false, stopping: false, consecutivePollFailures: 0, inFlight: 0, concurrency: 1, saturated: false, lastPollOkAt: 1, lastBrokerContactAt: 1, loopAliveAt: 1, uptimeMs: 5, now: 6 };
  const metrics: WorkerMetrics = { jobsProcessedTotal: 4, jobsFailedTotal: 1, jobsRetriedTotal: 0, jobsActive: 0, pollFailuresTotal: 0, reconnectsTotal: 0, connectionUp: 1 };
  const worker = { health: () => health, metrics: () => metrics };

  // node:http-shaped listener with metricsPath.
  const callNode = (opts: Parameters<typeof nodeHealthListener>[1], url: string, method = "GET") => {
    const listener = nodeHealthListener(worker, opts);
    const res = { statusCode: 0, headers: {} as Record<string, string>, body: "", setHeader(k: string, v: string) { this.headers[k.toLowerCase()] = v; }, end(b?: string) { this.body = b ?? ""; } };
    listener({ url, method }, res);
    return res;
  };
  const withMetrics = { metricsPath: "/metrics" };
  const mres = callNode(withMetrics, "/metrics");
  expect(mres.statusCode).toBe(200);
  expect(mres.headers["content-type"]).toBe("text/plain; version=0.0.4");
  expect(mres.body).toContain("# TYPE anvilmq_client_jobs_processed_total counter");
  expect(mres.body).toBe(renderPrometheus(metrics));
  // Live/ready still JSON.
  expect(callNode(withMetrics, "/livez").statusCode).toBe(200);
  expect(callNode(withMetrics, "/livez").headers["content-type"]).toBe("application/json");
  expect(callNode(withMetrics, "/readyz").statusCode).toBe(200);
  // Non-GET on metrics -> 405.
  expect(callNode(withMetrics, "/metrics", "POST").statusCode).toBe(405);
  // With metricsPath UNSET, /metrics is a 404.
  expect(callNode({}, "/metrics").statusCode).toBe(404);

  // fetch-shaped handler.
  const handler = fetchHealthHandler(worker, withMetrics);
  const fres = handler({ url: "http://pod.local/metrics", method: "GET" });
  expect(fres.status).toBe(200);
  expect(fres.headers.get("content-type")).toBe("text/plain; version=0.0.4");
  expect(await fres.text()).toBe(renderPrometheus(metrics));
  expect(handler({ url: "http://pod.local/livez" }).status).toBe(200);
  expect(handler({ url: "http://pod.local/readyz" }).headers.get("content-type")).toBe("application/json");
  // Unset metricsPath -> 404.
  expect(fetchHealthHandler(worker, {})({ url: "http://pod.local/metrics" }).status).toBe(404);
});

// 6. Logger: an injected logger captures internal errors and console.error is NOT touched;
//    the default (no logger, no onError) still reaches console.error.
test("logger: injection routes internal errors to the logger, not console.error", async () => {
  const captured: unknown[] = [];
  const logger: Logger = { error: (...a) => { captured.push(a); }, warn: () => {}, info: () => {}, debug: () => {} };
  const realConsoleError = console.error;
  let consoleErrorCalls = 0;
  console.error = (...a: unknown[]) => { consoleErrorCalls++; void a; };
  const previous = __setClientFactory(() => scriptedClient(() => ({ error: new Error("scripted poll failure") })));
  let worker: Worker | undefined;
  try {
    // No onError and an injected logger: report() must route poll failures to logger.error.
    worker = new Worker("logged", async () => {}, { address: "127.0.0.1:1", pollIntervalMs: 10, pollBackoffMaxMs: 40, logger });
    await until(() => captured.length >= 1);
    expect(captured.length).toBeGreaterThanOrEqual(1);
    expect(consoleErrorCalls).toBe(0);
  } finally { __setClientFactory(previous); if (worker) await worker.close(); console.error = realConsoleError; }

  // Default (no logger, no onError) reaches console.error.
  let defaultConsoleCalls = 0;
  const realConsoleError2 = console.error;
  console.error = (...a: unknown[]) => { defaultConsoleCalls++; void a; };
  const previous2 = __setClientFactory(() => scriptedClient(() => ({ error: new Error("default poll failure") })));
  let worker2: Worker | undefined;
  try {
    worker2 = new Worker("default-log", async () => {}, { address: "127.0.0.1:1", pollIntervalMs: 10, pollBackoffMaxMs: 40 });
    await until(() => defaultConsoleCalls >= 1);
    expect(defaultConsoleCalls).toBeGreaterThanOrEqual(1);
  } finally { __setClientFactory(previous2); if (worker2) await worker2.close(); console.error = realConsoleError2; }
});

// 7. gracefulShutdown (no real signals): stub globalThis.process and drive handlers.
test("gracefulShutdown: closes all closables, unregisters handlers, and hits the timeout branch", async () => {
  const original = (globalThis as { process?: unknown }).process;
  try {
    // (a) Happy path: every closable's close() is awaited; unregister detaches handlers.
    {
      const handlers: Record<string, Function[]> = {};
      let exited: number | undefined;
      const fakeProc = {
        on(sig: string, h: Function) { (handlers[sig] ??= []).push(h); },
        removeListener(sig: string, h: Function) { handlers[sig] = (handlers[sig] ?? []).filter(x => x !== h); },
        exit(code?: number) { exited = code; },
      };
      (globalThis as { process?: unknown }).process = fakeProc;
      let closedA = false, closedB = false;
      const unregister = gracefulShutdown(
        [{ close: async () => { await sleep(5); closedA = true; } }, { close: () => { closedB = true; } }],
        { exit: false, timeoutMs: 1000 },
      );
      expect(handlers["SIGTERM"].length).toBe(1);
      expect(handlers["SIGINT"].length).toBe(1);
      handlers["SIGTERM"][0]();
      await until(() => closedA && closedB);
      expect(exited).toBeUndefined(); // exit:false never calls proc.exit
      unregister();
      expect(handlers["SIGTERM"].length).toBe(0);
      expect(handlers["SIGINT"].length).toBe(0);
    }

    // (b) Idempotency: a second signal while draining is ignored.
    {
      const handlers: Record<string, Function[]> = {};
      const fakeProc = { on(sig: string, h: Function) { (handlers[sig] ??= []).push(h); }, removeListener() {}, exit() {} };
      (globalThis as { process?: unknown }).process = fakeProc;
      let closeCount = 0;
      gracefulShutdown([{ close: async () => { closeCount++; await sleep(30); } }], { exit: false, timeoutMs: 1000 });
      handlers["SIGTERM"][0]();
      handlers["SIGTERM"][0]();
      await sleep(60);
      expect(closeCount).toBe(1);
    }

    // (c) Timeout branch: a close() that outlives timeoutMs triggers a warn/error.
    {
      const handlers: Record<string, Function[]> = {};
      const fakeProc = { on(sig: string, h: Function) { (handlers[sig] ??= []).push(h); }, removeListener() {}, exit() {} };
      (globalThis as { process?: unknown }).process = fakeProc;
      const warns: unknown[] = [];
      const logger: Logger = { error: () => {}, warn: (...a) => { warns.push(a); } };
      gracefulShutdown([{ close: () => new Promise<void>(r => setTimeout(r, 500)) }], { exit: false, timeoutMs: 40, logger });
      handlers["SIGINT"][0]();
      await until(() => warns.length >= 1);
      expect(warns.length).toBeGreaterThanOrEqual(1);
    }

    // (d) Off-Node: no process.on -> logs once and returns a no-op.
    {
      (globalThis as { process?: unknown }).process = undefined;
      const warns: unknown[] = [];
      const logger: Logger = { error: () => {}, warn: (...a) => { warns.push(a); } };
      const unregister = gracefulShutdown([{ close: () => {} }], { logger });
      expect(warns.length).toBe(1);
      expect(typeof unregister).toBe("function");
      unregister(); // no-op, must not throw
    }
  } finally {
    (globalThis as { process?: unknown }).process = original;
  }
});
