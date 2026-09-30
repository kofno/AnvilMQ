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

export interface ConnectionOptions {
  address?: string;
  rpcTimeoutMs?: number;
  /**
   * HTTP/2 keepalive channel tuning. Keepalive PINGs detect a half-open channel
   * (idle load-balancer timeout, silently dead peer) at the transport layer instead
   * of waiting for a real RPC to hit its per-call deadline.
   */
  keepalive?: {
    timeMs?: number;                // default 20000: PING interval on an idle channel
    timeoutMs?: number;             // default 10000: PING ack deadline before the channel is dropped
    permitWithoutCalls?: boolean;   // default true: keep PINGing even with no active RPCs
    maxReconnectBackoffMs?: number; // default 10000: cap on the transport reconnect backoff
  };
}

/**
 * Seam over the generated gRPC client constructor so tests can observe the channel
 * arguments (keepalive tuning) without a proto or server change. Not part of the
 * supported API.
 */
type ClientFactory = (address: string, credentials: grpc.ChannelCredentials, channelOptions: grpc.ClientOptions) => grpc.Client;
let clientFactory: ClientFactory = (address, credentials, channelOptions) => new api.queue.v1.QueueService(address, credentials, channelOptions);
/** Test-only: replace the client factory and return the previous one for restoration. */
export function __setClientFactory(factory: ClientFactory): ClientFactory {
  const previous = clientFactory;
  clientFactory = factory;
  return previous;
}
/**
 * Runtime-agnostic UUID mint. Prefers the WHATWG crypto available on Node 18+, Bun,
 * and Deno; falls back to a Math.random v4 shape only when it is entirely absent, so
 * the library never imports `node:*` on the enqueue hot path.
 */
function mintKey(): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  if (c && typeof c.randomUUID === "function") return c.randomUUID();
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, ch => {
    const r = (Math.random() * 16) | 0;
    return (ch === "x" ? r : (r & 0x3) | 0x8).toString(16);
  });
}

/** Backoff with [50%,100%] jitter, mirroring the C1 poll-loop backoff so a producer
 * fleet does not thunder back in lockstep when the broker restarts. */
function jitteredBackoff(base: number, attempt: number, cap: number): number {
  const exp = Math.min(cap, base * 2 ** attempt);
  return Math.floor(exp * (0.5 + Math.random() * 0.5));
}

/** Run `task` for indices 0..count-1 with at most `concurrency` in flight. `task` must
 * settle its own outcome (never reject) so the pool cannot leak an unhandled rejection. */
async function boundedPool(count: number, concurrency: number, task: (index: number) => Promise<void>): Promise<void> {
  let next = 0;
  const worker = async () => { for (let i = next++; i < count; i = next++) await task(i); };
  await Promise.all(Array.from({ length: Math.max(1, Math.min(concurrency, count)) }, worker));
}

/**
 * A minimal structured-logging seam. Only `error` is required; `warn`/`info`/`debug`
 * are optional so a caller can supply a console, pino, winston, or bunyan-shaped object
 * unchanged. Injecting a logger is how you keep the library off `console.*` entirely.
 */
export interface Logger {
  error(...args: unknown[]): void;
  warn?(...args: unknown[]): void;
  info?(...args: unknown[]): void;
  debug?(...args: unknown[]): void;
}

/**
 * The single console-backed {@link Logger} used when no logger is injected. This object
 * is the ONLY place the library touches `console.*` on any runtime path; every other
 * diagnostic goes through an injected or defaulted `Logger`.
 */
export const defaultLogger: Logger = {
  error: (...args: unknown[]) => console.error(...args),
  warn: (...args: unknown[]) => console.warn(...args),
  info: (...args: unknown[]) => console.info(...args),
  debug: (...args: unknown[]) => console.debug(...args),
};

export interface JobMetadata { parentId: string; traceId: string; executionDepth: number }
export interface AddOptions {
  priority?: number; delayMs?: number; maxAttempts?: number;
  retryBackoffMs?: number; retryBackoffMaxMs?: number;
  metadata?: Partial<JobMetadata>; rateLimitFacet?: string;
  idempotencyKey?: string;
  /** Total wall-clock window for bounded enqueue retries on transient errors. Default 15000. */
  enqueueRetryMaxMs?: number;
  /** First backoff step; doubles each attempt, capped at enqueueRetryMaxMs. Default 200. */
  enqueueRetryBaseMs?: number;
  /** Opt out of the auto-minted idempotency key: send no key, attempt once, accept duplicate risk. Default false. */
  noIdempotencyKey?: boolean;
}
/** One positional outcome from {@link Queue.addBulk}: a receipt on success, or the raw error. */
export type AddResult =
  | { ok: true; id: string; state: string; replayed: boolean }
  | { ok: false; error: unknown };
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
  /**
   * Enqueue a fan-out of children of this job with bounded concurrency. Each child is
   * assigned a positional index (0..n-1) and, unless the caller supplies its own
   * `idempotencyKey`, a deterministic default key `${parentId}:${childName}:${index}`.
   * That determinism is what makes the enqueue-before-complete pattern exactly-once at
   * the broker: if the parent is redelivered after a crash, the identical keys replay
   * and the broker dedupes them. Rejects on the first child that fails permanently.
   */
  enqueueChildBulk(children: { name: string; data: unknown; options?: ChildAddOptions }[]): Promise<{ id: string; state: string; replayed: boolean }[]>;
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
  for (const key of ["enqueueRetryMaxMs", "enqueueRetryBaseMs"] as const) {
    const value = options[key];
    if (value !== undefined && (!Number.isFinite(value) || value < 0)) throw new Error(`${key} must be a nonnegative finite number`);
  }
  if (options.maxAttempts !== undefined && options.maxAttempts > 0xffffffff) throw new Error("maxAttempts exceeds uint32");
  if (options.priority !== undefined && (!Number.isInteger(options.priority) || options.priority < -2147483648 || options.priority > 2147483647)) throw new Error("priority exceeds int32");
  const json = JSON.stringify(data);
  if (json === undefined) throw new Error("payload must be JSON serializable");
  // Derive the effective idempotency key ONCE per call and reuse it across every retry,
  // so the broker dedupes our own retries: at-least-once delivery, zero client-made
  // duplicates. An explicit key wins; otherwise mint a UUID — UNLESS the caller opted
  // out, in which case we send no key and make a single attempt (duplicate risk theirs).
  const effectiveKey = options.noIdempotencyKey ? undefined : (options.idempotencyKey ?? mintKey());
  const maxMs = options.enqueueRetryMaxMs ?? 15000;
  const baseMs = options.enqueueRetryBaseMs ?? 200;
  // Freeze the request once; a retry must not pick up mutated caller options/data, and
  // client-only knobs (retry window, opt-out flag) never travel to the broker.
  const { enqueueRetryMaxMs: _rm, enqueueRetryBaseMs: _rb, noIdempotencyKey: _ni, idempotencyKey: _ik, metadata, ...rest } = options;
  const request = { ...rest, metadata: metadata ? { ...metadata } : undefined, name, payload: Buffer.from(json), ...(effectiveKey ? { idempotencyKey: effectiveKey } : {}) };
  const started = Date.now();
  for (let attempt = 0; ; attempt++) {
    try { return await connection.call("addJob", request); }
    catch (error) {
      const code = (error as grpc.ServiceError).code;
      const retryable = code === grpc.status.UNAVAILABLE || code === grpc.status.DEADLINE_EXCEEDED;
      // No key ⇒ a retry could duplicate, so never retry. Non-transient ⇒ retry is futile.
      // Window elapsed ⇒ stop and surface the last error to the caller.
      if (!effectiveKey || !retryable || Date.now() - started >= maxMs) throw error;
      await sleep(jitteredBackoff(baseMs, attempt, maxMs));
    }
  }
}

class Connection {
  private client: grpc.Client;
  private timeout: number;
  constructor(options: ConnectionOptions) {
    this.timeout = options.rpcTimeoutMs ?? 5000;
    if (!Number.isFinite(this.timeout) || this.timeout <= 0) throw new Error("rpcTimeoutMs must be positive");
    const k = options.keepalive ?? {};
    for (const [label, value] of [["keepalive.timeMs", k.timeMs], ["keepalive.timeoutMs", k.timeoutMs], ["keepalive.maxReconnectBackoffMs", k.maxReconnectBackoffMs]] as const) {
      if (value !== undefined && (!Number.isFinite(value) || value <= 0)) throw new Error(`${label} must be a positive finite number`);
    }
    this.client = clientFactory(options.address ?? "[::1]:50051", grpc.credentials.createInsecure(), {
      "grpc.keepalive_time_ms": k.timeMs ?? 20000,
      "grpc.keepalive_timeout_ms": k.timeoutMs ?? 10000,
      "grpc.keepalive_permit_without_calls": (k.permitWithoutCalls ?? true) ? 1 : 0,
      "grpc.max_reconnect_backoff_ms": k.maxReconnectBackoffMs ?? 10000,
    });
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
  /**
   * Client-side fan-out: one independent {@link sendAdd} per item (each with its own
   * effective key and bounded retry), run through a bounded worker pool (default
   * concurrency 8) so a large batch never spawns unbounded promises. Results are
   * positional, one per input. With `stopOnError:false` (the default) a mid-batch
   * failure is captured as `{ ok:false }` and does not abort the rest. With
   * `stopOnError:true` the returned promise rejects on the first failure, with the
   * partial positional results attached as `error.results`.
   */
  async addBulk(
    items: { data: T; options?: AddOptions }[],
    bulkOpts: { concurrency?: number; stopOnError?: boolean } = {},
  ): Promise<AddResult[]> {
    const concurrency = bulkOpts.concurrency ?? 8;
    if (!Number.isInteger(concurrency) || concurrency < 1) throw new Error("concurrency must be a positive integer");
    const stopOnError = bulkOpts.stopOnError ?? false;
    const results = new Array<AddResult>(items.length);
    let firstError: unknown;
    let stopped = false;
    await boundedPool(items.length, concurrency, async i => {
      if (stopped) return;
      const item = items[i];
      try {
        const r = await sendAdd(this.connection, this.name, item.data, item.options);
        results[i] = { ok: true, id: r.id, state: r.state, replayed: r.replayed };
      } catch (error) {
        results[i] = { ok: false, error };
        if (stopOnError && !stopped) { stopped = true; firstError = error; }
      }
    });
    if (stopOnError && stopped) {
      const err = firstError instanceof Error ? firstError : new Error(String(firstError));
      (err as { results?: AddResult[] }).results = results;
      throw err;
    }
    return results;
  }
  close() { this.connection.close(); }
}

export interface WorkerOptions extends ConnectionOptions {
  workerId?: string; pollIntervalMs?: number; heartbeatIntervalMs?: number;
  onError?: (error: unknown) => void;
  onCompleted?: (id: string) => void;
  /** Fired via safeEmit immediately before a job's handler runs. */
  onActive?: (info: { id: string; name: string; attempt: number; metadata: JobMetadata }) => void;
  /** Fired via safeEmit when the broker moves a job to its terminal failed state (attempts exhausted). */
  onFailed?: (info: { id: string; name: string; attempt: number; error: unknown }) => void;
  /** Fired via safeEmit when the broker will redeliver a failed job (attempts remain). */
  onRetry?: (info: { id: string; name: string; attempt: number; error: unknown }) => void;
  /** Injectable structured logger. Defaults to a console-backed {@link defaultLogger}. */
  logger?: Logger;
  /** Upper bound on the exponential poll backoff during an outage. Default 5000; must be >= pollIntervalMs. */
  pollBackoffMaxMs?: number;
  /** Consecutive poll failures before the connection is declared lost. Default 3. */
  connectionLostThreshold?: number;
  /** Fired once when consecutive poll failures first reach connectionLostThreshold. */
  onConnectionLost?: (info: { error: unknown; consecutiveFailures: number }) => void;
  /** Fired once when polling succeeds again after a lost connection. */
  onConnectionRestored?: (info: { downForMs: number; failuresWhileDown: number }) => void;
  /**
   * Liveness staleness budget. Liveness fails only when the poll loop stops turning
   * (a blocked event loop or a dead poll task) for longer than this. A broker outage
   * must NOT fail liveness. Default: max(heartbeatMs, pollBackoffMaxMs) * 3, floored at 30000.
   */
  livenessStaleMs?: number;
  /**
   * Readiness staleness budget. Readiness fails when no successful broker round-trip
   * (poll, heartbeat, or completion) has happened within this window. Must exceed
   * heartbeatIntervalMs so a long job's heartbeats keep readiness fresh instead of
   * flapping. Default: max(heartbeatMs, pollBackoffMaxMs) * 2, floored at 20000.
   */
  readinessStaleMs?: number;
  /**
   * Maximum number of job handlers a single worker runs in parallel. The dispatcher
   * still polls serially (getNextJob is a single-claim RPC) but holds up to this many
   * concurrent single-job leases, each multiplexed over the one shared connection.
   * Default 1, which reproduces the strictly serial poll-one-process-one behaviour.
   */
  concurrency?: number;
  /**
   * Adaptive-idle ceiling. When greater than pollIntervalMs, the idle poll delay grows
   * exponentially from pollIntervalMs toward this cap on consecutive empty polls and
   * resets to pollIntervalMs on the next found job, trading a little latency for far
   * fewer wasted polls on an idle queue. Default = pollIntervalMs (flat, off). Must be
   * less than readinessStaleMs so an idle worker never flaps readiness.
   */
  idlePollMaxMs?: number;
}
export type Processor<T> = (job: Job<T>, signal: AbortSignal) => Promise<void>;

/**
 * A synchronous, I/O-free snapshot of a worker's k8s health surface. The distinction
 * that drives everything: `live` ("my loop is turning") must survive a broker outage,
 * while `ready` ("I can reach the broker and do work") must fail during one.
 */
export interface WorkerHealth {
  /** Liveness: the poll loop is turning (map to livenessProbe -> /livez). */
  live: boolean;
  /** Readiness: live && connected && !stale && !stopping && polled at least once (map to readinessProbe -> /readyz). */
  ready: boolean;
  /** C1 poll-loop connection state. */
  connected: boolean;
  /** Broker-contact staleness exceeded readinessStaleMs. */
  stale: boolean;
  /** close() has begun. */
  stopping: boolean;
  /** Consecutive poll-failure streak (0 while reachable). */
  consecutivePollFailures: number;
  /** Jobs currently processing (0..concurrency). */
  inFlight: number;
  /** Max handlers this worker runs in parallel (mirrors the concurrency option). */
  concurrency: number;
  /** True when inFlight >= concurrency: every slot is busy and no new job can be claimed. */
  saturated: boolean;
  /** Epoch ms of the last successful poll, 0 if never. */
  lastPollOkAt: number;
  /** Epoch ms of the last successful broker round-trip (poll | heartbeat | completion), 0 if never. */
  lastBrokerContactAt: number;
  /** Epoch ms of the last poll-loop or heartbeat tick. */
  loopAliveAt: number;
  /** Milliseconds since worker construction. */
  uptimeMs: number;
  /** Date.now() at snapshot, so a reader is self-contained. */
  now: number;
}

  /**
   * A synchronous, I/O-free counter/gauge snapshot for a worker. Counters are monotonic
   * since construction; gauges read live. Feed it to {@link renderPrometheus} for a
   * `/metrics` endpoint, or scrape the numbers directly.
   */
  export interface WorkerMetrics {
    /** Jobs that completed successfully (completeJob acknowledged). */
    jobsProcessedTotal: number;
    /** Jobs the broker moved to its terminal failed state. */
    jobsFailedTotal: number;
    /** Jobs that failed but will be redelivered by the broker. */
    jobsRetriedTotal: number;
    /** Jobs currently processing (live gauge, mirrors inFlight). */
    jobsActive: number;
    /** Poll failures observed by the dispatcher loop. */
    pollFailuresTotal: number;
    /** Number of times the connection recovered after being declared lost. */
    reconnectsTotal: number;
    /** 1 when the poll loop currently considers the broker reachable, else 0 (live gauge). */
    connectionUp: number;
  }

/** Starts polling immediately. close() drains the current handler while retaining heartbeats. */
export class Worker<T = unknown> {
  readonly workerId: string;
  private connection: Connection;
  private stopping = new AbortController();
  private running: Promise<void>;
  private pollMs: number;
  private heartbeatMs: number;
  private pollBackoffMaxMs: number;
  private connectionLostThreshold: number;
  private consecutiveFailures = 0;
  private lastPollOkAt = 0;
  private connected = true;
  private lostSince = 0;
  private failuresWhileDown = 0;
  private inFlight = 0;
  private lastBrokerContactAt = 0;
  private loopAliveAt = Date.now();
  private readonly startedAt = Date.now();
  private livenessStaleMs: number;
  private readinessStaleMs: number;
  private concurrency: number;
  private idlePollMaxMs: number;
  private readonly logger: Logger;
  /** Monotonic lifecycle counters (plain numbers; JS is single-threaded so no atomics). */
  private jobsProcessedTotal = 0;
  private jobsFailedTotal = 0;
  private jobsRetriedTotal = 0;
  private pollFailuresTotal = 0;
  private reconnectsTotal = 0;
  /** Live set of dispatched process() promises; close() drains all of them. */
  private tracked = new Set<Promise<void>>();
  /** Re-armable slot-freed waiter: the capacity-parked branch awaits `slotFreed`, and
   * each settling job resolves the current one and re-arms a fresh promise. */
  private slotFreed!: Promise<void>;
  private resolveSlotFreed!: () => void;
  /** Resolves once when close() aborts, so a capacity-parked loop wakes on shutdown.
   * A single shared promise avoids leaking a per-park listener on the abort signal. */
  private abortedPromise: Promise<void>;
  constructor(private name: string, private processor: Processor<T>, private options: WorkerOptions = {}) {
    this.workerId = options.workerId ?? crypto.randomUUID();
    this.logger = options.logger ?? defaultLogger;
    this.pollMs = options.pollIntervalMs ?? 250;
    this.heartbeatMs = options.heartbeatIntervalMs ?? 10000;
    this.pollBackoffMaxMs = options.pollBackoffMaxMs ?? 5000;
    this.connectionLostThreshold = options.connectionLostThreshold ?? 3;
    if (!name.trim() || !this.workerId.trim()) throw new Error("queue and worker ID must not be blank");
    if (!Number.isFinite(this.pollMs) || this.pollMs <= 0 || !Number.isFinite(this.heartbeatMs) || this.heartbeatMs <= 0 || this.heartbeatMs > 10000) throw new Error("invalid poll/heartbeat interval (heartbeat maximum is 10000ms)");
    if (!Number.isFinite(this.pollBackoffMaxMs) || this.pollBackoffMaxMs <= 0 || this.pollBackoffMaxMs < this.pollMs) throw new Error("pollBackoffMaxMs must be a positive finite number and >= pollIntervalMs");
    if (!Number.isInteger(this.connectionLostThreshold) || this.connectionLostThreshold < 1) throw new Error("connectionLostThreshold must be an integer >= 1");
    if (options.livenessStaleMs !== undefined && (!Number.isFinite(options.livenessStaleMs) || options.livenessStaleMs <= 0)) throw new Error("livenessStaleMs must be a positive finite number");
    if (options.readinessStaleMs !== undefined && (!Number.isFinite(options.readinessStaleMs) || options.readinessStaleMs <= 0)) throw new Error("readinessStaleMs must be a positive finite number");
    const staleBase = Math.max(this.heartbeatMs, this.pollBackoffMaxMs);
    this.livenessStaleMs = options.livenessStaleMs ?? Math.max(30000, staleBase * 3);
    this.readinessStaleMs = options.readinessStaleMs ?? Math.max(20000, staleBase * 2);
    // A readiness window at or below the heartbeat interval would flap NOT-ready during
    // every long job, since only heartbeats (not polls) refresh contact while a job runs.
    if (this.readinessStaleMs <= this.heartbeatMs) throw new Error("readinessStaleMs must be greater than heartbeatIntervalMs so long jobs do not flap readiness");
    this.concurrency = options.concurrency ?? 1;
    if (!Number.isInteger(this.concurrency) || this.concurrency < 1) throw new Error("concurrency must be an integer >= 1");
    this.idlePollMaxMs = options.idlePollMaxMs ?? this.pollMs;
    if (!Number.isFinite(this.idlePollMaxMs) || this.idlePollMaxMs < this.pollMs) throw new Error("idlePollMaxMs must be a finite number >= pollIntervalMs");
    if (this.idlePollMaxMs >= this.readinessStaleMs) throw new Error("idlePollMaxMs must be less than readinessStaleMs so an idle worker does not flap readiness");
    this.armSlotFreed();
    this.abortedPromise = new Promise<void>(resolve => {
      if (this.stopping.signal.aborted) return resolve();
      this.stopping.signal.addEventListener("abort", () => resolve(), { once: true });
    });
    this.connection = new Connection(options);
    this.running = this.run();
  }
  private report(error: unknown) {
    try { (this.options.onError ?? ((e: unknown) => this.logger.error(e)))(error); } catch (callbackError) { this.logger.error(callbackError); }
  }
  private async run() {
    const tracked = this.tracked;
    let idleStreak = 0;
    try {
      while (!this.stopping.signal.aborted) {
        // Refresh loop liveness at the top of every iteration (including outage backoff
        // and capacity-park iterations) so a broker outage never fails liveness.
        this.loopAliveAt = Date.now();
        // At capacity: park until a slot frees rather than busy-polling. Per-job
        // heartbeats keep loopAliveAt fresh while parked, so liveness never wedges.
        if (this.inFlight >= this.concurrency) {
          await Promise.race([this.slotFreed, this.abortedPromise]);
          continue;
        }
        let ok = false;
        try {
          const claim = await this.connection.call<Claim>("getNextJob", { workerId: this.workerId, queueNames: [this.name] });
          ok = true;
          this.onPollSuccess();
          // A claim returned during shutdown still belongs to us and must be drained.
          if (claim.found) {
            this.inFlight++;
            idleStreak = 0;
            // Dispatch concurrently; the promise settles its own outcome (never rejects)
            // so the tracked set can never leak an unhandled rejection.
            let entry!: Promise<void>;
            entry = this.process(claim)
              .catch(error => this.report(error))
              .finally(() => { this.inFlight--; tracked.delete(entry); this.signalSlotFreed(); });
            tracked.add(entry);
            // Fast path: immediately re-poll to fill any remaining slots, no sleep.
            continue;
          }
          idleStreak++;
        } catch (error) { this.onPollFailure(error); this.report(error); idleStreak = 0; }
        const delay = ok ? this.nextIdleDelay(idleStreak) : this.nextBackoffDelay();
        await sleep(delay, undefined, { signal: this.stopping.signal }).catch(() => {});
      }
    } finally {
      // Drain: close() resolves only after the loop has broken AND every dispatched
      // handler (including one already in flight when abort fired) has settled.
      await Promise.allSettled([...tracked]);
      this.connection.close();
    }
  }
  /** (Re)arm the single-shot slot-freed waiter. */
  private armSlotFreed() {
    this.slotFreed = new Promise<void>(resolve => { this.resolveSlotFreed = resolve; });
  }
  /** Wake a capacity-parked loop and re-arm a fresh waiter for the next fill. */
  private signalSlotFreed() {
    const resolve = this.resolveSlotFreed;
    this.armSlotFreed();
    resolve();
  }
  /** Adaptive idle backoff (distinct from the C1 failure-path nextBackoffDelay): grows
   * from pollMs toward idlePollMaxMs on consecutive empty polls, [50%,100%] jittered.
   * Collapses to ~pollMs when idlePollMaxMs === pollMs (flat, off). */
  private nextIdleDelay(streak: number): number {
    const exp = Math.min(this.idlePollMaxMs, this.pollMs * 2 ** (streak - 1));
    return Math.floor(exp * (0.5 + Math.random() * 0.5));
  }
  /** Refresh both staleness clocks off a SUCCESSFUL broker round-trip (poll, heartbeat,
   * or completion). Never emits connection events and never changes `connected`. */
  private markContact() { this.lastBrokerContactAt = this.loopAliveAt = Date.now(); }
  /** A poll that returns (found OR not-found) proves the broker is reachable. */
  private onPollSuccess() {
    this.lastPollOkAt = Date.now();
    this.markContact();
    if (!this.connected) {
      const info = { downForMs: Date.now() - this.lostSince, failuresWhileDown: this.failuresWhileDown };
      this.connected = true;
      this.reconnectsTotal++;
      this.safeEmit(() => this.options.onConnectionRestored?.(info));
    }
    this.consecutiveFailures = 0;
    this.failuresWhileDown = 0;
  }
  private onPollFailure(error: unknown) {
    this.consecutiveFailures++;
    this.pollFailuresTotal++;
    if (this.connected && this.consecutiveFailures >= this.connectionLostThreshold) {
      this.connected = false;
      this.lostSince = Date.now();
      this.failuresWhileDown = this.consecutiveFailures;
      this.safeEmit(() => this.options.onConnectionLost?.({ error, consecutiveFailures: this.consecutiveFailures }));
    } else if (!this.connected) {
      this.failuresWhileDown++;
    }
  }
  /** Exponential backoff capped by pollBackoffMaxMs, jittered to [50%,100%] to avoid a thundering herd. */
  private nextBackoffDelay(): number {
    const exp = Math.min(this.pollBackoffMaxMs, this.pollMs * 2 ** (this.consecutiveFailures - 1));
    return Math.floor(exp * (0.5 + Math.random() * 0.5));
  }
  private safeEmit(fn: () => void) {
    try { fn(); } catch (e) { try { (this.options.onError ?? ((err: unknown) => this.logger.error(err)))(e); } catch { /* ignore */ } }
  }
  /** Current consecutive poll-failure streak (0 while reachable). Observability/testing hook. */
  get consecutivePollFailures(): number { return this.consecutiveFailures; }
  /** Whether the poll loop currently considers the broker reachable. Observability/testing hook. */
  get isConnected(): boolean { return this.connected; }
  /**
   * Synchronous, I/O-free health snapshot for k8s probes. Reads only in-memory fields
   * plus one Date.now(); issues no RPC and never awaits.
   */
  health(): WorkerHealth {
    const now = Date.now();
    const stale = this.lastBrokerContactAt > 0 && (now - this.lastBrokerContactAt) > this.readinessStaleMs;
    const live = (now - this.loopAliveAt) <= this.livenessStaleMs;
    const stopping = this.stopping.signal.aborted;
    // Startup gate: a fresh pod is not ready until it reaches the broker once, even
    // though C1 seeds connected = true optimistically.
    const ready = live && this.connected && !stale && !stopping && this.lastPollOkAt > 0;
    return {
      live, ready, connected: this.connected, stale, stopping,
      consecutivePollFailures: this.consecutiveFailures, inFlight: this.inFlight,
      concurrency: this.concurrency, saturated: this.inFlight >= this.concurrency,
      lastPollOkAt: this.lastPollOkAt, lastBrokerContactAt: this.lastBrokerContactAt,
      loopAliveAt: this.loopAliveAt, uptimeMs: now - this.startedAt, now,
    };
  }
  /**
   * Synchronous, I/O-free counter/gauge snapshot. Counters are monotonic since
   * construction; `jobsActive`/`connectionUp` read live. Issues no RPC and never awaits.
   */
  metrics(): WorkerMetrics {
    return {
      jobsProcessedTotal: this.jobsProcessedTotal,
      jobsFailedTotal: this.jobsFailedTotal,
      jobsRetriedTotal: this.jobsRetriedTotal,
      jobsActive: this.inFlight,
      pollFailuresTotal: this.pollFailuresTotal,
      reconnectsTotal: this.reconnectsTotal,
      connectionUp: this.connected ? 1 : 0,
    };
  }
  /** Test-only: backdate the loop-liveness clock to simulate a wedged event loop. Not part of the supported API. */
  __setLoopAliveAt(value: number): void { this.loopAliveAt = value; }
  private async process(claim: Claim) {
    const handler = new AbortController();
    const heartbeatStop = new AbortController();
    const identity = { id: claim.id, workerId: this.workerId, attempt: claim.attempts };
    const beats = (async () => {
      while (!heartbeatStop.signal.aborted) {
        await sleep(this.heartbeatMs, undefined, { signal: heartbeatStop.signal }).catch(() => {});
        if (heartbeatStop.signal.aborted) break;
        // Refresh loop liveness on every heartbeat tick so a long async job keeps
        // liveness fresh; a blocked event loop stops this callback and liveness fails.
        this.loopAliveAt = Date.now();
        try { await this.connection.call("heartbeat", identity); this.markContact(); }
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
      // Deterministic fan-out: default child key is `${parentId}:${childName}:${index}`
      // unless the caller overrides idempotencyKey. Redelivery of the parent replays the
      // identical keys, so the broker dedupes ⇒ exactly-once fan-out at the broker.
      const enqueueChildBulk = async (children: { name: string; data: unknown; options?: ChildAddOptions }[]) => {
        const results = new Array<{ id: string; state: string; replayed: boolean }>(children.length);
        let firstError: unknown;
        let stopped = false;
        await boundedPool(children.length, 8, async i => {
          if (stopped) return;
          const child = children[i];
          const key = child.options?.idempotencyKey ?? `${claim.id}:${child.name}:${i}`;
          try {
            results[i] = await sendAdd(this.connection, child.name, child.data, { ...child.options, idempotencyKey: key, metadata: { parentId: claim.id } });
          } catch (error) { if (!stopped) { stopped = true; firstError = error; } }
        });
        if (stopped) throw firstError;
        return results;
      };
      // Fired via safeEmit immediately before the handler runs; the broker-authoritative
      // lifecycle events (onFailed/onRetry) come from the failJob response below.
      this.safeEmit(() => this.options.onActive?.({ id: claim.id, name: claim.name, attempt: claim.attempts, metadata: claim.metadata }));
      await this.processor({ id: claim.id, name: claim.name, data, attempts: claim.attempts, metadata: claim.metadata, leaseExpiresAtMs: claim.leaseExpiresAtMs, enqueueChild, enqueueChildBulk }, handler.signal);
    } catch (error) { failed = true; failure = error; }
    heartbeatStop.abort();
    await beats;
    if (handler.signal.aborted) return;
    // Do not turn an ambiguous completion RPC failure into a FailJob request.
    if (failed) {
      this.report(failure);
      const resp = await this.connection.call<{ success: boolean; movedToFailedState: boolean }>("failJob", { ...identity, errorMessage: failure instanceof Error ? failure.message : String(failure) });
      // The broker owns the retry decision: movedToFailedState distinguishes a terminal
      // failure (attempts exhausted) from a transient one it will redeliver.
      if (resp.movedToFailedState === true) {
        this.jobsFailedTotal++;
        this.safeEmit(() => this.options.onFailed?.({ id: claim.id, name: claim.name, attempt: claim.attempts, error: failure }));
      } else {
        this.jobsRetriedTotal++;
        this.safeEmit(() => this.options.onRetry?.({ id: claim.id, name: claim.name, attempt: claim.attempts, error: failure }));
      }
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
      this.jobsProcessedTotal++;
      this.markContact();
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

export interface HealthHttpOptions {
  /** Liveness path (default /livez). */
  livePath?: string;
  /** Readiness path (default /readyz). */
  readyPath?: string;
  /** Prometheus metrics path. When unset there is NO metrics route (`/metrics` -> 404). */
  metricsPath?: string;
}

/**
 * Render a {@link WorkerMetrics} snapshot as Prometheus text exposition (version 0.0.4).
 * Pure and zero-dependency: no clock, no I/O. Every series is prefixed `anvilmq_client_`.
 * An optional `labels` bag is applied to every sample; omit it (or pass `{}`) to emit
 * bare sample lines with no `{...}` braces.
 */
export function renderPrometheus(m: WorkerMetrics, opts?: { labels?: Record<string, string> }): string {
  const labels = opts?.labels ?? {};
  const entries = Object.entries(labels);
  const labelStr = entries.length === 0
    ? ""
    : `{${entries.map(([k, v]) => `${k}="${String(v).replace(/\\/g, "\\\\").replace(/"/g, "\\\"").replace(/\n/g, "\\n")}"`).join(",")}}`;
  const series: { name: string; type: "counter" | "gauge"; help: string; value: number }[] = [
    { name: "jobs_processed_total", type: "counter", help: "Jobs completed successfully.", value: m.jobsProcessedTotal },
    { name: "jobs_failed_total", type: "counter", help: "Jobs moved to the terminal failed state.", value: m.jobsFailedTotal },
    { name: "jobs_retried_total", type: "counter", help: "Jobs that failed and will be redelivered.", value: m.jobsRetriedTotal },
    { name: "poll_failures_total", type: "counter", help: "Dispatcher poll failures.", value: m.pollFailuresTotal },
    { name: "reconnects_total", type: "counter", help: "Connection recoveries after being declared lost.", value: m.reconnectsTotal },
    { name: "jobs_active", type: "gauge", help: "Jobs currently processing.", value: m.jobsActive },
    { name: "connection_up", type: "gauge", help: "1 when the broker is considered reachable, else 0.", value: m.connectionUp },
  ];
  let out = "";
  for (const s of series) {
    const full = `anvilmq_client_${s.name}`;
    out += `# HELP ${full} ${s.help}\n`;
    out += `# TYPE ${full} ${s.type}\n`;
    out += `${full}${labelStr} ${s.value}\n`;
  }
  return out;
}

/** Extract the pathname from a request URL that may be absolute or a bare path, and
 * may carry a query string or fragment. Compares pathname only. */
function healthPathname(url: string | undefined): string {
  const raw = url ?? "/";
  try { return new URL(raw).pathname; } catch { return raw.split("?")[0].split("#")[0]; }
}

/**
 * A `node:http`-shaped request listener for k8s probes. `GET {livePath}` returns 200 iff
 * `worker.health().live`, `GET {readyPath}` returns 200 iff `worker.health().ready`, else
 * 503; the body is the full {@link WorkerHealth} JSON in both cases. When `opts.metricsPath`
 * is set, `GET {metricsPath}` returns 200 `text/plain` Prometheus text from
 * `worker.metrics()`; when unset there is no metrics route. Unknown path -> 404,
 * non-GET -> 405. Does zero I/O and opens no socket: pass it to `http.createServer(...)`.
 * The library never imports `node:http`, keeping it runtime-agnostic.
 */
export function nodeHealthListener(
  worker: { health(): WorkerHealth; metrics?(): WorkerMetrics },
  opts: HealthHttpOptions = {},
): (req: { url?: string; method?: string }, res: { statusCode: number; setHeader(k: string, v: string): void; end(b?: string): void }) => void {
  const livePath = opts.livePath ?? "/livez";
  const readyPath = opts.readyPath ?? "/readyz";
  const metricsPath = opts.metricsPath;
  return (req, res) => {
    const path = healthPathname(req.url);
    const isMetrics = metricsPath !== undefined && path === metricsPath;
    if (path !== livePath && path !== readyPath && !isMetrics) { res.setHeader("content-type", "application/json"); res.statusCode = 404; res.end(JSON.stringify({ error: "not found" })); return; }
    if ((req.method ?? "GET") !== "GET") { res.setHeader("content-type", "application/json"); res.setHeader("allow", "GET"); res.statusCode = 405; res.end(JSON.stringify({ error: "method not allowed" })); return; }
    if (isMetrics) {
      res.setHeader("content-type", "text/plain; version=0.0.4");
      res.statusCode = 200;
      res.end(renderPrometheus(worker.metrics!()));
      return;
    }
    res.setHeader("content-type", "application/json");
    const health = worker.health();
    res.statusCode = (path === livePath ? health.live : health.ready) ? 200 : 503;
    res.end(JSON.stringify(health));
  };
}

/**
 * A WHATWG-`fetch`-shaped handler for k8s probes, for runtimes whose server takes a
 * `(Request) => Response` function. Same semantics as {@link nodeHealthListener}:
 * `GET {livePath}` -> 200/503 on `live`, `GET {readyPath}` -> 200/503 on `ready`, full
 * {@link WorkerHealth} JSON body, `GET {metricsPath}` -> 200 Prometheus text when set,
 * unknown path -> 404, non-GET -> 405. Does zero I/O.
 */
export function fetchHealthHandler(
  worker: { health(): WorkerHealth; metrics?(): WorkerMetrics },
  opts: HealthHttpOptions = {},
): (request: { url: string; method?: string }) => Response {
  const livePath = opts.livePath ?? "/livez";
  const readyPath = opts.readyPath ?? "/readyz";
  const metricsPath = opts.metricsPath;
  const headers = { "content-type": "application/json" };
  return (request) => {
    const path = healthPathname(request.url);
    const isMetrics = metricsPath !== undefined && path === metricsPath;
    if (path !== livePath && path !== readyPath && !isMetrics) return new Response(JSON.stringify({ error: "not found" }), { status: 404, headers });
    if ((request.method ?? "GET") !== "GET") return new Response(JSON.stringify({ error: "method not allowed" }), { status: 405, headers: { ...headers, allow: "GET" } });
    if (isMetrics) return new Response(renderPrometheus(worker.metrics!()), { status: 200, headers: { "content-type": "text/plain; version=0.0.4" } });
    const health = worker.health();
    const ok = path === livePath ? health.live : health.ready;
    return new Response(JSON.stringify(health), { status: ok ? 200 : 503, headers });
  };
}

/**
 * Register OS-signal handlers that gracefully close a set of resources (workers,
 * forwarders, health servers) on the first `SIGTERM`/`SIGINT`, then optionally exit.
 * Returns an unregister function that detaches every handler.
 *
 * On the first signal it calls `close()` on every closable concurrently and races the
 * combined drain against `timeoutMs`. A clean drain exits 0 (when `exit`); a timeout
 * logs a warning and exits 1 (when `exit`). Further signals during the drain are ignored
 * (idempotent). Off Node (no `globalThis.process.on`) it logs once and returns a no-op,
 * so importing in a browser/edge runtime never throws. Reaches `process` via
 * `globalThis` only — it never imports `node:process`.
 */
export function gracefulShutdown(
  closables: { close(): Promise<void> | void }[],
  opts: { signals?: string[]; timeoutMs?: number; logger?: Logger; exit?: boolean } = {},
): () => void {
  const signals = opts.signals ?? ["SIGTERM", "SIGINT"];
  const timeoutMs = opts.timeoutMs ?? 25000;
  const logger = opts.logger ?? defaultLogger;
  const exit = opts.exit ?? true;
  const proc = (globalThis as { process?: { on?: Function; removeListener?: Function; exit?: (code?: number) => void } }).process;
  if (!proc || typeof proc.on !== "function") {
    logger.warn?.("gracefulShutdown: no process.on available; signal handling is disabled");
    return () => {};
  }
  let draining = false;
  const handler = (signal: string) => {
    if (draining) return;
    draining = true;
    logger.info?.(`gracefulShutdown: received ${signal}, draining ${closables.length} resource(s)`);
    void (async () => {
      const closes = closables.map(c => Promise.resolve().then(() => c.close()));
      let timedOut = false;
      const timeout = new Promise<void>(resolve => setTimeout(() => { timedOut = true; resolve(); }, timeoutMs));
      await Promise.race([Promise.allSettled(closes), timeout]);
      if (timedOut) {
        (logger.warn ?? logger.error)(`gracefulShutdown: drain exceeded ${timeoutMs}ms; forcing exit`);
        if (exit) proc.exit?.(1);
        return;
      }
      if (exit) proc.exit?.(0);
    })();
  };
  const bound = signals.map(sig => { const h = () => handler(sig); proc.on!(sig, h); return { sig, h }; });
  return () => { for (const { sig, h } of bound) proc.removeListener?.(sig, h); };
}

/**
 * A store-and-forward record. `payload` is the caller's data (not yet JSON-encoded);
 * `key` is the effective idempotency key, minted once and held for the record's whole
 * lifetime so re-forwards are deduped by the broker.
 */
export interface OutboxRecord {
  key: string;
  queue: string;
  payload: unknown;
  options?: AddOptions;
  state: "pending" | "sent" | "failed";
  attempts: number;
  createdAt: number;
  lastError?: unknown;
}

/**
 * The pluggable durability seam. {@link MemoryOutboxStore} is the ONLY implementation
 * shipped and it is in-memory (a producer crash loses un-forwarded records — by design).
 * To make submissions survive producer-process loss, implement this interface against
 * your own durable medium; the {@link OutboxForwarder} works unchanged against any store.
 */
export interface OutboxStore {
  /** Insert or replace a record keyed by `record.key`. */
  put(record: OutboxRecord): Promise<void>;
  /** Return up to `limit` records still in the `pending` state. */
  claimPending(limit: number): Promise<OutboxRecord[]>;
  /** Mark the record forwarded (broker accepted it). */
  markSent(key: string): Promise<void>;
  /** Mark the record poisoned (a permanent, non-retryable enqueue error). */
  markFailed(key: string, error: unknown): Promise<void>;
}

/**
 * In-memory {@link OutboxStore} backed by a `Map`. Suitable as a best-effort stop-gap
 * that absorbs a short broker outage without the producer blocking or losing the add.
 * It does NOT persist: anything still `pending` when the process dies is lost. Swap in
 * a durable {@link OutboxStore} when the buffer must outlive the producer process.
 */
export class MemoryOutboxStore implements OutboxStore {
  private records = new Map<string, OutboxRecord>();
  async put(record: OutboxRecord): Promise<void> {
    // First writer for a key wins its createdAt/attempts; a re-put of the same key
    // (idempotent enqueue) does not reset an in-flight record's bookkeeping.
    if (!this.records.has(record.key)) this.records.set(record.key, { ...record });
  }
  async claimPending(limit: number): Promise<OutboxRecord[]> {
    const out: OutboxRecord[] = [];
    for (const r of this.records.values()) {
      if (r.state === "pending") { out.push({ ...r }); if (out.length >= limit) break; }
    }
    return out;
  }
  async markSent(key: string): Promise<void> {
    const r = this.records.get(key);
    if (r) { r.state = "sent"; r.attempts++; }
  }
  async markFailed(key: string, error: unknown): Promise<void> {
    const r = this.records.get(key);
    if (r) { r.state = "failed"; r.attempts++; r.lastError = error; }
  }
  /** Introspection: a snapshot copy of a single record, or undefined. */
  get(key: string): OutboxRecord | undefined { const r = this.records.get(key); return r ? { ...r } : undefined; }
  /** Introspection: snapshot copies of every record. */
  all(): OutboxRecord[] { return [...this.records.values()].map(r => ({ ...r })); }
  /** Introspection: number of records in a given state (all states if omitted). */
  count(state?: OutboxRecord["state"]): number {
    if (!state) return this.records.size;
    let n = 0; for (const r of this.records.values()) if (r.state === state) n++; return n;
  }
}

export interface OutboxForwarderOptions {
  /** Drain tick interval. Default 250. */
  intervalMs?: number;
  /** Maximum records claimed per drain. Default 32. */
  batchSize?: number;
  /** Concurrent forwards within a drain. Default 8. */
  concurrency?: number;
  /** Fired per record once it reaches a terminal outcome (sent, or poisoned). */
  onForwarded?: (key: string, result: AddResult) => void;
  /** Fired once each time a non-empty backlog is drained to zero pending records. */
  onDrained?: () => void;
}

/**
 * Drains an {@link OutboxStore} to a {@link Queue}. `enqueue()` records a pending add and
 * returns immediately (best-effort store-and-forward); a background tick loop (start/stop)
 * or a manual {@link drainOnce} forwards pending records. Every forward uses the record's
 * held key, so a re-forward is deduped by the broker (at-least-once, no duplicate jobs).
 * Transient broker errors (`UNAVAILABLE`/`DEADLINE_EXCEEDED`) leave the record pending for
 * the next tick; a permanent error (e.g. `INVALID_ARGUMENT`) poisons the record via
 * `markFailed` and is surfaced through `onForwarded` — never silently dropped.
 */
export class OutboxForwarder<T = unknown> {
  private readonly intervalMs: number;
  private readonly batchSize: number;
  private readonly concurrency: number;
  private running?: Promise<void>;
  private wake = new AbortController();
  private stopping = false;
  constructor(private queue: Queue<T>, private store: OutboxStore, private opts: OutboxForwarderOptions = {}) {
    this.intervalMs = opts.intervalMs ?? 250;
    this.batchSize = opts.batchSize ?? 32;
    this.concurrency = opts.concurrency ?? 8;
    if (!Number.isFinite(this.intervalMs) || this.intervalMs <= 0) throw new Error("intervalMs must be a positive finite number");
    if (!Number.isInteger(this.batchSize) || this.batchSize < 1) throw new Error("batchSize must be a positive integer");
    if (!Number.isInteger(this.concurrency) || this.concurrency < 1) throw new Error("concurrency must be a positive integer");
  }
  /** Mint the effective key (business idempotencyKey else UUID) and store a pending record. */
  async enqueue(data: T, options?: AddOptions): Promise<{ key: string }> {
    const key = options?.idempotencyKey ?? mintKey();
    await this.store.put({ key, queue: this.queue.name, payload: data, options, state: "pending", attempts: 0, createdAt: Date.now() });
    return { key };
  }
  /** Forward one batch of pending records. Returns how many reached a terminal outcome
   * (sent or poisoned) this pass; transient failures stay pending and are not counted. */
  async drainOnce(): Promise<number> {
    const batch = await this.store.claimPending(this.batchSize);
    if (batch.length === 0) return 0;
    let forwarded = 0;
    await boundedPool(batch.length, this.concurrency, async i => {
      const record = batch[i];
      try {
        // Always force the held key and a single attempt: the tick loop, not add()'s own
        // window, owns retry cadence so a down broker does not block a drain for 15s.
        const r = await this.queue.add(record.payload as T, { ...record.options, idempotencyKey: record.key, noIdempotencyKey: false, enqueueRetryMaxMs: 0 });
        await this.store.markSent(record.key);
        forwarded++;
        this.opts.onForwarded?.(record.key, { ok: true, id: r.id, state: r.state, replayed: r.replayed });
      } catch (error) {
        const code = (error as grpc.ServiceError).code;
        if (code === grpc.status.UNAVAILABLE || code === grpc.status.DEADLINE_EXCEEDED) return; // transient: retry next tick
        await this.store.markFailed(record.key, error);
        forwarded++;
        this.opts.onForwarded?.(record.key, { ok: false, error });
      }
    });
    return forwarded;
  }
  /** Start the background drain loop. Idempotent; a second call while running is a no-op. */
  start(): void {
    if (this.running) return;
    this.stopping = false;
    this.wake = new AbortController();
    this.running = (async () => {
      while (!this.stopping) {
        try {
          const forwarded = await this.drainOnce();
          if (forwarded > 0 && !this.stopping) {
            const remaining = await this.store.claimPending(1);
            if (remaining.length === 0) { try { this.opts.onDrained?.(); } catch { /* ignore */ } }
          }
        } catch { /* a store error should not kill the loop */ }
        await sleep(this.intervalMs, undefined, { signal: this.wake.signal }).catch(() => {});
      }
    })();
  }
  /** Stop the background drain loop and await its exit. */
  async stop(): Promise<void> {
    this.stopping = true;
    this.wake.abort();
    if (this.running) { await this.running; this.running = undefined; }
  }
}
