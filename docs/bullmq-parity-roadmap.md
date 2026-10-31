# BullMQ feature parity roadmap

Status: **Milestone 1 delivered.** The broker features that bring AnvilMQ to
functional parity with the BullMQ capabilities a typical cron-and-bulk workload
relies on — **A1, A2, A3, A4, A5, and A7** — have merged (PRs #51–#56). Only
**A6 (flows)** remains, deferred to Milestone 2.

This document tracks AnvilMQ's parity with [BullMQ](https://docs.bullmq.io/)'s
job-queue feature set. The scope is derived from the BullMQ features a
cron-and-bulk workload depends on, reconciled against AnvilMQ's real wire
contract (`proto/queue.proto`) and documented behavior. "Missing" below means the
capability is absent from the broker contract, not merely undocumented.

## Scope

Parity is organized around the BullMQ capabilities common to cron-and-bulk
workloads: timezone-aware cron schedulers, bulk enqueue, idempotent/singleton
enqueue, non-retryable failures, backoff jitter, and job result storage. Flows
(parent/child dependency trees) are a larger, separable capability tracked as
Milestone 2.

A feature only needs to be built if it is on the critical path to running a
representative cron-and-bulk workload. Flows are the one commonly-used BullMQ
capability deliberately sequenced after Milestone 1.

## Classification: broker vs. client

Each BullMQ feature falls into one of three buckets: a **broker gap** (the wire
contract must change), **client-library parity** (the client wraps existing
broker behavior), or **deferred**.

### A. Broker gaps — A1–A5 + A7 **delivered** (Milestone 1); A6 is Milestone 2

| ID | Feature | Prior reality | Size | Status |
|----|---------|---------------|------|--------|
| A1 | **Non-retryable failure** (`UnrecoverableError`) | `FailJob` always reschedules while attempts remain; no terminal flag | XS | ✅ Delivered (#51) |
| A2 | **Backoff jitter** | Exponential backoff + per-job attempts exist; "no jitter is applied" | XS | ✅ Delivered (#51) |
| A3 | **Bulk enqueue** (`addBulk`) | Single `AddJob` only; no atomic batch | S/M | ✅ Delivered (#52) |
| A4 | **Release-on-finish dedup** | `idempotency_key` exists but receipts are permanent; no release/lookup/delete by key | S | ✅ Delivered (#53) |
| A5 | **Scheduler / cron** (tz-aware) | No scheduler, cron, timezone, or declarative reconcile exists | **L** | ✅ Delivered (#55 + #56) |
| A6 | **Flows** (parent waits for children) | `parent_id` is lineage metadata only; dequeue has no dependency gate | **XL** | ⬜ Milestone 2 |
| A7 | **Job result storage** | `CompleteJob` stores no result; BullMQ retains the return value for *all* jobs (debugging use outside flows; child values inside) | XS/S | ✅ Delivered (#54) |

**A5 (cron) was the critical path for Milestone 1** — timezone-aware declarative
schedulers, which cron-heavy workloads lean on most — and shipped across #55
(CRUD + tz-aware cron) and #56 (the background producer loop). **A6 (flows) moves
to Milestone 2** as the largest, most invasive change. A1–A4 and A7 landed as
small correctness/realism items alongside cron.

#### A4 note — the dedup semantics gap

BullMQ's `deduplication: { id }` and custom `jobId` release the de-dup slot when
the job **finishes**, so the next cron tick can enqueue again. AnvilMQ's
`idempotency_key` is a *permanent enqueue receipt* retained until retention prunes
the underlying job, and the contract exposes no way to fetch or delete a receipt
by key. The common "reuse one logical key, let it release on completion" singleton
pattern therefore needs a server affordance.

**Recommended design (delivered in #53):** a dedup *mode* on `AddJob` where the
receipt is released automatically when the job reaches a terminal state
(completed/failed), matching BullMQ singleton semantics with no producer
coordination. This shipped as the `AddJobRequest.release_on_finish` flag
(requires `idempotency_key`); the current retain-forever behavior stays the
default. It is small and server-side, and it subsumes the "fetch/remove by key"
ask for the common case.

#### A6 note — flows is the largest single item (Milestone 2)

Flows are **deferred to Milestone 2** as the largest, most invasive change. When
built, delivering flows means, at minimum:
- a **dependency gate** in dequeue so a parent is not claimable until all children
  are terminal;
- **child return values** — a parent reads its children's results
  (`GetChildrenValues` equivalent), built on the general result field from **A7**
  (result storage is *not* flow-specific — BullMQ stores results for all jobs);
- **`failParentOnFailure`** — propagate a child's final failure up to ancestors
  while allowing siblings to continue (BullMQ's behavior);
- **bulk flow creation** (`FlowProducer.addBulk`) building parent/child trees in
  one atomic call (shares machinery with A3);
- **flow merge by jobId** — re-sending a flow structure attaches new jobs to an
  existing flow via parent references (depends on A4/custom-key semantics);
- **per-tree depth + tree-size cap** — a flow tree is created in one atomic action
  and is finite, so every node shares a single tree depth rather than per-node
  ancestry accumulation; cap **tree size** (node count) the way bulk fan-outs are
  capped, to bound runaway trees.

This naturally **subsumes Phase 3's "validate ancestry and quarantine runaway
chains"** — real dependencies are a superset of ancestry validation.

### B. Client-library parity (build in the client, not the broker)

These are BullMQ *client* concerns; the broker does not need to change.

| ID | Feature | Notes |
|----|---------|-------|
| B1 | **Worker concurrency** (`concurrency: N`) | BullMQ enforces this worker-side; AnvilMQ workers can self-limit in-flight claims |
| B2 | **Worker health monitoring** | The AnvilMQ client exposes Prometheus endpoints scrapeable into Grafana; provide an equivalent worker-side liveness signal for setups used to a worker→Redis health check |
| B3 | **BullMQ-shaped client API** (keep thin) | The client is explicitly *not* BullMQ-compatible today. A **thin** compatibility shim lets cron + bulk jobs port with minimal rewrites, easing migration. Stay thin while validating; a fuller rewrite waits until the direction is proven |

### C. Deferred — not required for Milestone 1 parity

| Feature | Rationale for deferral |
|---------|------------------------|
| **High availability / Raft (Phase 5)** | Single-broker AnvilMQ at **NORMAL** durability (FULL is off the table for throughput — see below), backed by the client **outbox** to catch producers up across a broker restart and periodic **WAL backup** for disaster recovery, is the current target resilience posture. Multi-node consensus is future work, not a Milestone 1 blocker. |
| **Admin-UI mutations** (retry / promote / remove / create) | `/console` is read-only; useful for ops parity but not needed to run a throughput/correctness workload. Revisit later. |
| **`job.log` / `step.sleep`** | Nice-to-have. Per-job logs are cosmetic; `step.sleep` is expressible as a delayed follow-up job. |
| **Chain/depth configurability** | Default depth 10 and the (off-by-default) chain-size cap are fine for shallow trees. When flows (A6) land, prefer a **per-tree depth + tree-size cap** (atomic, finite trees share one depth) over per-node ancestry accumulation; make configurable only if a ported flow needs it. |

#### Durability note

**AnvilMQ targets NORMAL durability by default.** FULL's per-commit fsync caps
sustained completion throughput (see [harness/BENCHMARKS.md](../harness/BENCHMARKS.md)),
so the shipped production profile runs at NORMAL. NORMAL can lose recently
acknowledged writes after an OS/power failure, so the durability story rests on
two compensating mechanisms rather than synchronous commit:
1. the client **outbox**, which lets producers catch up after a broker restart
   with no lost enqueues;
2. periodic **WAL-backup restore**, which recovers broker state within a bounded
   window.

Operators who need a stronger durability posture than NORMAL + outbox + WAL
backup should treat HA/Raft (Phase 5) as a prerequisite.

## Sequencing

Work splits into two milestones. **Milestone 1 delivers functional parity for
cron + bulk workloads without flows**; Milestone 2 adds flows.

### Milestone 1 — cron + bulk parity (no flows) — **delivered**

```
Track A (broker)   A1 A2 ──▶ A3 A4 A7 ──▶ A5 (cron)   [all merged, #51–#56]
Track B (client)        B1 B2 ──────────▶ B3 (thin BullMQ-shaped shim)
```

The XS/S items landed first for quick parity wins and to de-risk — A1 + A2 (#51),
A3 (#52), A4 (#53), A7 (#54) — then **cron (A5)**, the single largest Milestone 1
item, across #55 (CRUD + tz-aware cron) and #56 (background producer loop). The
broker track is complete; the client stays **thin** while validating (B3 is a
compatibility shim, not a rewrite).

### Milestone 2 — flows

**A6 (flows)** — the largest, most invasive change — plus a flow-aware client
surface. Built with the per-tree depth + tree-size-cap design above, and A7
(result storage) already in place as the foundation for child return values.

## Relationship to the existing README roadmap

| README phase | Disposition under this plan |
|--------------|-----------------------------|
| Phase 1 — Core plumbing | Unchanged (libSQL migration still open, independent of parity) |
| Phase 2 — Transactional lifecycle | Largely complete; the parity track builds on it |
| Phase 3 — Safety controls | **Ancestry validation merges into A6 (flows), now Milestone 2**; ingress velocity stays. Flows ship a per-tree depth + tree-size cap as the bound |
| Phase 4 — Faceted rate limiting | Unchanged; fairness still open |
| **Phase 5 — Regional HA (Raft)** | **Stays deferred.** Mitigated by NORMAL durability + client outbox + WAL backup; multi-node consensus is future work |
| Phase 6 — Observability | Largely complete |

## Decisions

- **Durability: NORMAL by default.** FULL is incompatible with the throughput the
  broker targets and is not the default. The durability story is NORMAL + client
  outbox + periodic WAL backup.
- **Resilience posture: single-broker with compensating mechanisms.** Multi-node
  HA/Raft (Phase 5) stays deferred behind the parity work.

## Resolved (from review)

- **Flows are not required for Milestone 1.** Cron + bulk + dedup (A1–A5, A7)
  deliver parity for cron-and-bulk workloads; **A6 (flows) defers to Milestone 2.**
- **Keep the client thin while validating.** B3 is a thin BullMQ-compatible shim,
  not a fuller rewrite, until the direction is proven.
- **Result storage is general, not flow-specific** (A7): BullMQ retains the return
  value for all jobs (debugging outside flows), so it lands in Milestone 1
  independent of flows and is the foundation for flow child values later.
- **Flow trees get a per-tree depth + tree-size cap** (atomic, finite trees share
  one depth) rather than per-node ancestry accumulation, when A6 is built.
