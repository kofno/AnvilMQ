import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { expect } from "bun:test";
import { Queue, Worker } from "../src/index";

/** Test-only gRPC proxy: discard a real successful response after the broker commits. */
export async function verifyCompletionAmbiguity(address: string, httpAddress: string) {
  const api = grpc.loadPackageDefinition(loadSync(fileURLToPath(new URL("../../proto/queue.proto", import.meta.url)), {
    longs: Number, defaults: true, bytes: Buffer,
  })) as any;
  const upstream = new api.queue.v1.QueueService(address, grpc.credentials.createInsecure());
  const proxy = new grpc.Server();
  let mode: "unavailable" | "deadline" | "exhausted" | "rejected" = "unavailable";
  let completionCalls: object[] = [];
  let failCalls = 0;
  let committedResponses = 0;
  const handlers: grpc.UntypedServiceImplementation = {};
  for (const method of ["addJob", "getNextJob", "completeJob", "failJob", "heartbeat"]) {
    handlers[method] = (call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) => {
      if (method === "failJob") failCalls++;
      if (method === "completeJob") {
        completionCalls.push(call.request);
        if (mode === "rejected") { callback({ code: grpc.status.PERMISSION_DENIED, details: "injected terminal error" }); return; }
      }
      upstream[method](call.request, { deadline: Date.now() + 2000 }, (error: grpc.ServiceError | null, result: any) => {
        if (!error && method === "completeJob") {
          committedResponses++;
          if (mode === "exhausted" || completionCalls.length === 1) {
            if (mode === "deadline") return; // Caller deadline expires; successful response is discarded.
            callback({ code: grpc.status.UNAVAILABLE, details: "injected lost completion response" }); return;
          }
        }
        callback(error, result);
      });
    };
  }
  proxy.addService(api.queue.v1.QueueService.service, handlers);
  const port = await new Promise<number>((resolve, reject) => proxy.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (e, p) => e ? reject(e) : resolve(p)));
  const proxyAddress = `127.0.0.1:${port}`;
  async function metrics() { return (await (await fetch(`http://${httpAddress}/metrics`)).text()); }
  function completedCount(text: string) { return Number(text.match(/anvilmq_jobs\{state="Completed"\} (\d+)/)?.[1]); }
  function transitionCount(text: string) { return Number(text.match(/anvilmq_transitions_total\{event="completed"\} (\d+)/)?.[1]); }
  try {
    for (const scenario of ["unavailable", "deadline", "exhausted", "rejected"] as const) {
      mode = scenario; completionCalls = []; committedResponses = 0;
      const before = await metrics();
      const name = `ambiguity-${scenario}-${crypto.randomUUID()}`;
      const queue = new Queue(name, { address });
      const { id } = await queue.add({}); queue.close();
      let processed = 0; let acknowledged = 0;
      const errors: grpc.ServiceError[] = [];
      const worker = new Worker(name, async () => { processed++; }, {
        address: proxyAddress, rpcTimeoutMs: 500, pollIntervalMs: 20,
        onCompleted: completedId => { expect(completedId).toBe(id); acknowledged++; },
        onError: error => errors.push(error as grpc.ServiceError),
      });
      try {
        const end = Date.now() + 10000;
        while (!acknowledged && !errors.length) {
          if (Date.now() > end) throw new Error(`completion scenario timed out: ${scenario}`);
          await sleep(20);
        }
      } finally { await worker.close(); }
      expect(processed).toBe(1);
      expect(failCalls).toBe(0);
      expect(completionCalls.length).toBe(scenario === "exhausted" ? 3 : scenario === "rejected" ? 1 : 2);
      for (const request of completionCalls) expect(request).toEqual(completionCalls[0]);
      if (scenario === "rejected") {
        expect(errors.map(e => e.code)).toEqual([grpc.status.PERMISSION_DENIED]);
        expect(acknowledged).toBe(0);
        expect(committedResponses).toBe(0);
      } else {
        expect(committedResponses).toBe(completionCalls.length);
        expect(acknowledged).toBe(scenario === "exhausted" ? 0 : 1);
        expect(errors.map(e => e.code)).toEqual(scenario === "exhausted" ? [grpc.status.UNAVAILABLE] : []);
        const after = await metrics();
        expect(completedCount(after) - completedCount(before)).toBe(1);
        expect(transitionCount(after) - transitionCount(before)).toBe(1);
        const next = await new Promise<any>((resolve, reject) => upstream.getNextJob({ workerId: "probe", queueNames: [name] }, { deadline: Date.now() + 2000 }, (e: Error | null, r: any) => e ? reject(e) : resolve(r)));
        expect(next.found).toBe(false);
      }
    }
  } finally { proxy.forceShutdown(); upstream.close(); }
}
