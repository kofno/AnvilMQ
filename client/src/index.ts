import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { existsSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";

const bundledProto = new URL("../proto/queue.proto", import.meta.url);
const definition = loadSync(fileURLToPath(existsSync(bundledProto) ? bundledProto : new URL("../../proto/queue.proto", import.meta.url)), {
  longs: Number, defaults: true, bytes: Buffer,
});
const api = grpc.loadPackageDefinition(definition) as unknown as {
  queue: { v1: { QueueService: grpc.ServiceClientConstructor } };
};

export interface ConnectionOptions { address?: string; rpcTimeoutMs?: number }
export interface JobMetadata { parentId: string; traceId: string; executionDepth: number }
export interface AddOptions {
  priority?: number; delayMs?: number; maxAttempts?: number;
  retryBackoffMs?: number; retryBackoffMaxMs?: number;
  metadata?: Partial<JobMetadata>; rateLimitFacet?: string;
  idempotencyKey?: string;
}
/** Options for {@link Job.enqueueChild}: everything except the lineage fields, which
 * the server owns (parent is forced to the current job; depth/trace are derived). */
export type ChildAddOptions = Omit<AddOptions, "metadata">;
export interface Job<T> {
  id: string; name: string; data: T; attempts: number;
  metadata: JobMetadata; leaseExpiresAtMs: number;
  /**
   * Enqueue a child of this job over the worker's existing connection. Parentage is
   * built-in: `parentId` is forced to this job's id and the server derives the child's
   * `executionDepth` (parent depth + 1) and inherits the lineage `traceId`. Lineage
   * fields cannot be set by the caller.
   */
  enqueueChild<C = unknown>(name: string, data: C, options?: ChildAddOptions): Promise<{ id: string; state: string; replayed: boolean }>;
}
interface Claim { found: boolean; id: string; name: string; payload: Buffer; attempts: number; metadata: JobMetadata; leaseExpiresAtMs: number }

/** Validates options, freezes the request, and sends AddJob with idempotency-safe
 * retries over the given connection. Shared by {@link Queue.add} and
 * {@link Job.enqueueChild} so both behave identically regardless of the channel used. */
async function sendAdd<T>(connection: Connection, name: string, data: T, options: AddOptions = {}): Promise<{ id: string; state: string; replayed: boolean }> {
  if (options.idempotencyKey !== undefined && (typeof options.idempotencyKey !== "string" || !options.idempotencyKey.trim() || Buffer.byteLength(options.idempotencyKey, "utf8") > 256)) throw new Error("idempotencyKey must be nonblank and at most 256 UTF-8 bytes");
  for (const key of ["delayMs", "retryBackoffMs", "retryBackoffMaxMs", "maxAttempts"] as const) {
    const value = options[key];
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 0)) throw new Error(`${key} must be a nonnegative safe integer`);
  }
  if (options.maxAttempts !== undefined && options.maxAttempts > 0xffffffff) throw new Error("maxAttempts exceeds uint32");
  if (options.priority !== undefined && (!Number.isInteger(options.priority) || options.priority < -2147483648 || options.priority > 2147483647)) throw new Error("priority exceeds int32");
  const json = JSON.stringify(data);
  if (json === undefined) throw new Error("payload must be JSON serializable");
  // Freeze the request once; a retry must not pick up mutated caller options/data.
  const request = { ...options, metadata: options.metadata ? { ...options.metadata } : undefined, name, payload: Buffer.from(json) };
  for (let attempt = 0; ; attempt++) {
    try { return await connection.call("addJob", request); }
    catch (error) {
      const code = (error as grpc.ServiceError).code;
      if (!request.idempotencyKey || attempt >= 2 || (code !== grpc.status.UNAVAILABLE && code !== grpc.status.DEADLINE_EXCEEDED)) throw error;
      await sleep(100 * 2 ** attempt);
    }
  }
}

class Connection {
  private client: grpc.Client;
  private timeout: number;
  constructor(options: ConnectionOptions) {
    this.timeout = options.rpcTimeoutMs ?? 5000;
    if (!Number.isFinite(this.timeout) || this.timeout <= 0) throw new Error("rpcTimeoutMs must be positive");
    this.client = new api.queue.v1.QueueService(options.address ?? "[::1]:50051", grpc.credentials.createInsecure());
  }
  call<T>(method: string, request: object): Promise<T> {
    return new Promise((resolve, reject) => {
      const fn = (this.client as unknown as Record<string, Function>)[method];
      fn.call(this.client, request, { deadline: Date.now() + this.timeout }, (error: grpc.ServiceError | null, result: T) => {
        if (error) reject(error); else resolve(result);
      });
    });
  }
  close() { this.client.close(); }
}

/** A queue name maps directly to the protocol's job name; payloads are JSON. */
export class Queue<T = unknown> {
  private connection: Connection;
  constructor(public readonly name: string, options: ConnectionOptions = {}) {
    if (!name.trim()) throw new Error("queue name must not be blank");
    this.connection = new Connection(options);
  }
  async add(data: T, options: AddOptions = {}): Promise<{ id: string; state: string; replayed: boolean }> {
    return sendAdd(this.connection, this.name, data, options);
  }
  close() { this.connection.close(); }
}

export interface WorkerOptions extends ConnectionOptions {
  workerId?: string; pollIntervalMs?: number; heartbeatIntervalMs?: number;
  onError?: (error: unknown) => void;
  onCompleted?: (id: string) => void;
}
export type Processor<T> = (job: Job<T>, signal: AbortSignal) => Promise<void>;

/** Starts polling immediately. close() drains the current handler while retaining heartbeats. */
export class Worker<T = unknown> {
  readonly workerId: string;
  private connection: Connection;
  private stopping = new AbortController();
  private running: Promise<void>;
  private pollMs: number;
  private heartbeatMs: number;
  constructor(private name: string, private processor: Processor<T>, private options: WorkerOptions = {}) {
    this.workerId = options.workerId ?? crypto.randomUUID();
    this.pollMs = options.pollIntervalMs ?? 250;
    this.heartbeatMs = options.heartbeatIntervalMs ?? 10000;
    if (!name.trim() || !this.workerId.trim()) throw new Error("queue and worker ID must not be blank");
    if (!Number.isFinite(this.pollMs) || this.pollMs <= 0 || !Number.isFinite(this.heartbeatMs) || this.heartbeatMs <= 0 || this.heartbeatMs > 10000) throw new Error("invalid poll/heartbeat interval (heartbeat maximum is 10000ms)");
    this.connection = new Connection(options);
    this.running = this.run();
  }
  private report(error: unknown) {
    try { (this.options.onError ?? console.error)(error); } catch (callbackError) { console.error(callbackError); }
  }
  private async run() {
    try {
      while (!this.stopping.signal.aborted) {
        try {
          const claim = await this.connection.call<Claim>("getNextJob", { workerId: this.workerId, queueNames: [this.name] });
          // A claim returned during shutdown still belongs to us and must be drained.
          if (claim.found) { await this.process(claim); continue; }
        } catch (error) { this.report(error); }
        await sleep(this.pollMs, undefined, { signal: this.stopping.signal }).catch(() => {});
      }
    } finally { this.connection.close(); }
  }
  private async process(claim: Claim) {
    const handler = new AbortController();
    const heartbeatStop = new AbortController();
    const identity = { id: claim.id, workerId: this.workerId, attempt: claim.attempts };
    const beats = (async () => {
      while (!heartbeatStop.signal.aborted) {
        await sleep(this.heartbeatMs, undefined, { signal: heartbeatStop.signal }).catch(() => {});
        if (heartbeatStop.signal.aborted) break;
        try { await this.connection.call("heartbeat", identity); }
        catch (error) {
          // Even a timeout leaves ownership uncertain. Never acknowledge after this.
          handler.abort(error); this.report(error); break;
        }
      }
    })();
    let failed = false;
    let failure: unknown;
    try {
      const data = JSON.parse(claim.payload.toString("utf8")) as T;
      // Reuse the worker's multiplexed channel; enqueueChild forces this job as the
      // parent and leaves depth/trace for the server to derive.
      const enqueueChild = <C = unknown>(name: string, childData: C, options?: ChildAddOptions) =>
        sendAdd(this.connection, name, childData, { ...options, metadata: { parentId: claim.id } });
      await this.processor({ id: claim.id, name: claim.name, data, attempts: claim.attempts, metadata: claim.metadata, leaseExpiresAtMs: claim.leaseExpiresAtMs, enqueueChild }, handler.signal);
    } catch (error) { failed = true; failure = error; }
    heartbeatStop.abort();
    await beats;
    if (handler.signal.aborted) return;
    // Do not turn an ambiguous completion RPC failure into a FailJob request.
    if (failed) {
      this.report(failure);
      await this.connection.call("failJob", { ...identity, errorMessage: failure instanceof Error ? failure.message : String(failure) });
    } else {
      // Completion is idempotent. Keep the same claim token on every retry;
      // never rerun the handler or turn an ambiguous result into FailJob.
      for (let attempt = 0; ; attempt++) {
        try { await this.connection.call("completeJob", identity); break; }
        catch (error) {
          const code = (error as grpc.ServiceError).code;
          if (attempt >= 2 || (code !== grpc.status.UNAVAILABLE && code !== grpc.status.DEADLINE_EXCEEDED)) throw error;
          await sleep(100 * 2 ** attempt);
        }
      }
      this.options.onCompleted?.(claim.id);
    }
  }
  async close(): Promise<void> {
    this.stopping.abort();
    await this.running;
  }
}

export interface RateLimitStatus {
  facetKey: string; ruleExists: boolean; maxJobs: number; currentCount: number;
  windowDurationMs: number; windowExpiresAt: number; isThrottled: boolean;
}
/** Administrative RPCs; the server currently relies on trusted-network access. */
export class RateLimits {
  private connection: Connection;
  constructor(options: ConnectionOptions = {}) { this.connection = new Connection(options); }
  async upsert(facetKey: string, maxJobs: number, windowDurationMs: number): Promise<void> {
    if (!Number.isInteger(maxJobs) || maxJobs < 0 || maxJobs > 0xffffffff) throw new Error("maxJobs must fit uint32");
    if (!Number.isSafeInteger(windowDurationMs) || windowDurationMs <= 0) throw new Error("windowDurationMs must be a positive safe integer");
    await this.connection.call("upsertRateLimitRule", { facetPattern: facetKey, maxJobs, windowDurationMs });
  }
  async delete(facetKey: string): Promise<boolean> {
    return (await this.connection.call<{ deleted: boolean }>("deleteRateLimitRule", { facetKey })).deleted;
  }
  status(facetKey: string): Promise<RateLimitStatus> { return this.connection.call("getRateLimitStatus", { facetKey }); }
  close() { this.connection.close(); }
}

export interface IngressLimitStatus {
  facetKey: string; ruleExists: boolean; maxJobs: number; estimatedCount: number;
  windowDurationMs: number; windowStartedAt: number; isThrottled: boolean;
}
/**
 * Administrative RPCs for the admission-side sliding-window ingress velocity control.
 * Distinct from {@link RateLimits}, which throttles the dispatch side (workers claiming
 * jobs); these throttle the ENQUEUE rate for a facet and reject `AddJob` when a facet's
 * recent enqueue velocity exceeds its limit. The server currently relies on trusted-network
 * access.
 */
export class IngressLimits {
  private connection: Connection;
  constructor(options: ConnectionOptions = {}) { this.connection = new Connection(options); }
  async upsert(facetKey: string, maxJobs: number, windowDurationMs: number): Promise<void> {
    if (!Number.isInteger(maxJobs) || maxJobs < 0 || maxJobs > 0xffffffff) throw new Error("maxJobs must fit uint32");
    if (!Number.isSafeInteger(windowDurationMs) || windowDurationMs <= 0) throw new Error("windowDurationMs must be a positive safe integer");
    await this.connection.call("upsertIngressLimitRule", { facetPattern: facetKey, maxJobs, windowDurationMs });
  }
  async delete(facetKey: string): Promise<boolean> {
    return (await this.connection.call<{ deleted: boolean }>("deleteIngressLimitRule", { facetKey })).deleted;
  }
  status(facetKey: string): Promise<IngressLimitStatus> { return this.connection.call("getIngressLimitStatus", { facetKey }); }
  close() { this.connection.close(); }
}
