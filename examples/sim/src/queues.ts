// Shared job-name catalog and helpers for the synthetic workload.
//
// AnvilMQ has a single "name" dimension (name == queue). These strings MUST match the
// ANVILMQ_METRICS_QUEUES allowlist in compose.yaml so per-name metrics are emitted.
import { Queue, type AddOptions } from "../../../client/src/index";

export const ADDRESS = process.env.ANVILMQ_ADDR ?? "127.0.0.1:50071";
export const HTTP = process.env.ANVILMQ_HTTP_URL ?? "http://127.0.0.1:9092";

export const JobName = {
  // Fan-out chain: IngestEvent -> ProcessTask -> ProcessSubtask -> WriteRecord.
  IngestEvent: "IngestEvent",
  ProcessTask: "ProcessTask",
  ProcessSubtask: "ProcessSubtask",
  WriteRecord: "WriteRecord",
  // Diagnostic queue
  Delay: "Delay",
  Stress: "Stress",
  Throw: "Throw",
  // Self-recursive probe: each generation enqueues a child of itself until the server's
  // execution-depth circuit breaker (max 10) rejects the enqueue. Demonstrates how an
  // ancestry-depth / runaway-chain rejection is reported (anvilmq_ancestry_rejections_total).
  RecursiveProbe: "RecursiveProbe",
} as const;
export type JobName = (typeof JobName)[keyof typeof JobName];
export const ALL_JOB_NAMES: JobName[] = Object.values(JobName);

export interface IngestEventData { tenantId: string; trigger: string; payloadBytes: number }
// Event children fan out; directly submitted tasks use subtaskCount=0.
export interface TaskData { recordId: string; payloadBytes: number; subtaskCount: number }
export interface SubtaskData { recordId: string; payloadBytes: number }
export interface RecordData { recordId: string; payloadBytes: number }
export interface DelayData { waitTimeInMilliseconds: number }
export interface StressData { stressTimeInMilliseconds: number }
export interface ThrowData { message: string }
export interface RecursiveProbeData { hops: number; origin: string }

export interface JobData {
  IngestEvent: IngestEventData;
  ProcessTask: TaskData;
  ProcessSubtask: SubtaskData;
  WriteRecord: RecordData;
  Delay: DelayData;
  Stress: StressData;
  Throw: ThrowData;
  RecursiveProbe: RecursiveProbeData;
}

// Facets for the limit-tripping demo woven into the steady producer. Deliberately distinct so
// each control is isolated on the dashboards: IngressBurst carries only an admission-side
// (sliding-window) ingress rule; DispatchThrottle carries only a dispatch-side (fixed-window)
// claim rate rule. Both are keyed solely by facet, independent of the job `name`.
export const LimitFacet = {
  IngressBurst: "tenant:ingress-burst",
  DispatchThrottle: "tenant:dispatch-throttle",
} as const;

// gRPC status code surfaced when an enqueue is rejected (ingress velocity limit or the
// ancestry-depth circuit breaker). Dispatch throttling does not reject enqueues.
export const GRPC_RESOURCE_EXHAUSTED = 8;

export function num(name: string, fallback: number, min: number, max: number): number {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isFinite(value) || value < min || value > max) {
    throw new Error(`${name} must be a number in [${min}, ${max}]`);
  }
  return value;
}

// Reusable queue clients keyed by job name for enqueue and fan-out.
const clients = new Map<string, Queue>();
export function queueFor(name: JobName): Queue {
  let q = clients.get(name);
  if (!q) {
    q = new Queue(name, { address: ADDRESS });
    clients.set(name, q);
  }
  return q;
}
export function closeAllQueues(): void {
  for (const q of clients.values()) q.close();
  clients.clear();
}

export async function enqueue<Name extends JobName>(name: Name, data: JobData[Name], options: AddOptions = {}) {
  return queueFor(name).add(data, options);
}
