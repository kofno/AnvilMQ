import { Queue, Worker } from "../src/index";
import { setTimeout as sleep } from "node:timers/promises";

type Data = { kind: "success" | "retry" | "delayed" | "long" };
const options = { address: process.env.ANVILMQ_ADDR ?? "[::1]:50051" };
const mode = process.argv[2] ?? "worker";
if (mode === "seed") {
  const queue = new Queue<Data>("demo", options);
  try {
    console.log(await queue.add({ kind: "success" }));
    console.log(await queue.add({ kind: "retry" }, { maxAttempts: 3, retryBackoffMs: 1000, retryBackoffMaxMs: 4000 }));
    console.log(await queue.add({ kind: "delayed" }, { delayMs: 5000 }));
    console.log(await queue.add({ kind: "long" }, { maxAttempts: 3 }));
  } finally { queue.close(); }
} else if (mode === "worker") {
  const worker = new Worker<Data>("demo", async (job, signal) => {
    console.log(`START ${job.id} ${job.data.kind} attempt=${job.attempts}`);
    if (job.data.kind === "retry" && job.attempts < 3) throw new Error("intentional demo failure");
    if (job.data.kind === "long") await sleep(40000, undefined, { signal });
    console.log(`HANDLER DONE ${job.id}`);
  }, options);
  for (const event of ["SIGINT", "SIGTERM"] as const) process.once(event, () => {
    console.log("Draining active work...");
    void worker.close().then(() => console.log("Worker stopped"));
  });
} else { throw new Error("Usage: bun demo/run.ts [seed|worker]"); }
