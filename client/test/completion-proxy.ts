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
  let upstream = new api.queue.v1.QueueService(address, grpc.credentials.createInsecure(), { "grpc.use_local_subchannel_pool": 1 });
  const proxy = new grpc.Server();
  let mode: "unavailable" | "deadline" | "exhausted" | "rejected" = "unavailable";
  let completionCalls: object[] = [];
  let failCalls = 0;
  let committedResponses = 0;
  let enqueueFault: "unavailable" | "deadline" | "exhausted" | "unkeyed" | undefined;
  let addRequests: object[] = [];
  let committedIds: string[] = [];
  const handlers: grpc.UntypedServiceImplementation = {};
  for (const method of ["addJob", "getNextJob", "completeJob", "failJob", "heartbeat"]) {
    handlers[method] = (call: grpc.ServerUnaryCall<any, any>, callback: grpc.sendUnaryData<any>) => {
      if (method === "addJob" && enqueueFault) addRequests.push(call.request);
      if (method === "failJob") failCalls++;
      if (method === "completeJob") {
        completionCalls.push(call.request);
        if (mode === "rejected") { callback({ code: grpc.status.PERMISSION_DENIED, details: "injected terminal error" }); return; }
      }
      upstream[method](call.request, { deadline: Date.now() + 2000 }, (error: grpc.ServiceError | null, result: any) => {
        if (!error && method === "addJob" && enqueueFault) {
          committedIds.push(result.id);
          if (enqueueFault === "exhausted" || addRequests.length === 1) {
            if (enqueueFault === "deadline" || enqueueFault === "exhausted" || enqueueFault === "unkeyed") return;
            callback({ code: grpc.status.UNAVAILABLE, details: "injected lost enqueue response" }); return;
          }
        }
        if (!error && method === "completeJob") {
          committedResponses++;
          if (mode === "exhausted" || completionCalls.length === 1) {
            if (mode === "deadline") return; // Caller deadline expires; successful response is discarded.
            callback({ code: grpc.status.UNAVAILABLE, details: "injected lost completion response" }); return;
          }
        }
        // Do not forward upstream transport metadata as downstream trailers.
        callback(error ? { code: error.code, details: error.details } : null, result);
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
    for (const scenario of ["unavailable", "deadline", "exhausted", "unkeyed"] as const) {
      // Isolate transport state left by deliberately abandoned calls between scenarios.
      upstream.close();
      upstream = new api.queue.v1.QueueService(address, grpc.credentials.createInsecure(), { "grpc.use_local_subchannel_pool": 1 });
      enqueueFault = scenario; addRequests = []; committedIds = [];
      const queue = new Queue(`enqueue-${crypto.randomUUID()}`, { address: proxyAddress, rpcTimeoutMs: 500 });
      const options = scenario === "unkeyed" ? {} : { idempotencyKey: "business-operation-123" };
      try {
        if (scenario === "exhausted" || scenario === "unkeyed") {
          await expect(queue.add({ invoiceId: 123 }, options)).rejects.toMatchObject({ code: grpc.status.DEADLINE_EXCEEDED });
        } else {
          const result = await queue.add({ invoiceId: 123 }, options);
          expect(result.replayed).toBe(true);
          expect(result.id).toBe(committedIds[0]);
        }
        expect(addRequests.length).toBe(scenario === "exhausted" ? 3 : scenario === "unkeyed" ? 1 : 2);
        expect(new Set(committedIds).size).toBe(1);
        for (const request of addRequests) expect(request).toEqual(addRequests[0]);
        if (scenario !== "unkeyed") {
          // Exercise actual broker status handling directly; the fault proxy is
          // used only to discard successful replies, not to translate errors.
          const conflicts = async () => Number((await metrics()).match(/anvilmq_enqueue_conflicts_total (\d+)/)?.[1]);
          const before = await conflicts();
          const direct = new Queue(queue.name, { address, rpcTimeoutMs: 2000 });
          try { await expect(direct.add({ invoiceId: 456 }, options)).rejects.toMatchObject({ code: grpc.status.ALREADY_EXISTS }); }
          finally { direct.close(); }
          expect(await conflicts()).toBe(before + 1); // Conflicts must not retry.
        }
        const poll = () => new Promise<any>((resolve, reject) => upstream.getNextJob({ workerId: "probe", queueNames: [queue.name] }, { deadline: Date.now() + 2000 }, (e: Error | null, r: any) => e ? reject(e) : resolve(r)));
        expect((await poll()).id).toBe(committedIds[0]);
        expect((await poll()).found).toBe(false);
      } finally { queue.close(); }
    }
    enqueueFault = undefined;
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
