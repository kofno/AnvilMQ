# BullMQ workload parity roadmap

Status: **Draft / for discussion.** This document proposes a modified, test-driven
slice of the roadmap whose goal is a **head-to-head evaluation** of AnvilMQ
against the current BullMQ/Valkey deployment, running a representative (simulated)
copy of the production workload. It does not replace the [README roadmap](../README.md#roadmap);
it sequences a near-term **workload-parity track** ahead of the deferred
high-availability work.

The scope here is derived from an audit of the BullMQ features the application
actually uses, reconciled against AnvilMQ's real wire contract
(`proto/queue.proto`) and documented behavior. "Missing" below means the
capability is absent from the broker contract, not merely undocumented.

## Goal

Stand up a test scenario in which the application's workload — ~35 cron jobs
(intervals 30s to 10 days) and ~25 flow (parent/child) jobs, plus bulk fan-outs —
runs against AnvilMQ and against BullMQ/Valkey side by side, on the same cluster,
under the same load, with comparable dashboards. **No real demo or production
data is touched**; the workload is simulated.

A feature only needs to be built if it is on the critical path to *running that
workload* or to *making the comparison fair*. Everything else is explicitly
deferred.

## Classification: broker vs. client vs. deferred

Each BullMQ feature the application uses falls into one of four buckets.

### A. Broker gaps on the critical path (must build)

| ID | Feature | Current reality | Size |
|----|---------|-----------------|------|
| A1 | **Non-retryable failure** (`UnrecoverableError`) | `FailJob` always reschedules while attempts remain; no terminal flag | XS |
| A2 | **Backoff jitter** | Exponential backoff + per-job attempts exist; "no jitter is applied" | XS |
| A3 | **Bulk enqueue** (`addBulk`) | Single `AddJob` only; fan-outs batch up to 5000 | S/M |
| A4 | **Release-on-finish dedup** | `idempotency_key` exists but receipts are permanent; no release/lookup/delete by key | S |
| A5 | **Scheduler / cron** (tz-aware) | No scheduler, cron, timezone, or declarative reconcile exists | **L** |
| A6 | **Flows** (parent waits for children) | `parent_id` is lineage metadata only; dequeue has no dependency gate; `CompleteJob` stores no result | **XL** |

A5 and A6 are the whole ballgame: without them the workload cannot even be
ported. A1–A4 are small correctness/realism items.

#### A4 note — the dedup semantics gap

BullMQ's `deduplication: { id }` and custom `jobId` release the de-dup slot when
the job **finishes**, so the next cron tick can enqueue again. AnvilMQ's
`idempotency_key` is a *permanent enqueue receipt* retained until retention prunes
the underlying job, and the contract exposes no way to fetch or delete a receipt
by key. The application pattern of "reuse one logical key, let it release on
completion" therefore needs a server affordance.

**Recommended design:** a dedup *mode* on `AddJob` where the receipt is released
automatically when the job reaches a terminal state (completed/failed), matching
BullMQ singleton semantics with no producer coordination. The current
retain-forever behavior stays available as the other mode. This is small and
server-side, and it subsumes the "fetch/remove by key" ask for the common case.

#### A6 note — flows is the largest single item

Delivering flows means, at minimum:
- a **dependency gate** in dequeue so a parent is not claimable until all children
  are terminal;
- **child return values** — add a result field to `CompleteJob` and a way for a
  parent to read its children's results (`GetChildrenValues` equivalent);
- **`failParentOnFailure`** — propagate a child's final failure up to ancestors
  while allowing siblings to continue (BullMQ's behavior);
- **bulk flow creation** (`FlowProducer.addBulk`) building parent/child trees in
  one atomic call (shares machinery with A3);
- **flow merge by jobId** — re-sending a flow structure attaches new jobs to an
  existing flow via parent references (depends on A4/custom-key semantics).

This naturally **subsumes Phase 3's "validate ancestry and quarantine runaway
chains"** — real dependencies are a superset of ancestry validation.

### B. Client-library parity (build in the client, not the broker)

These are BullMQ *client* concerns; the broker does not need to change.

| ID | Feature | Notes |
|----|---------|-------|
| B1 | **Worker concurrency** (`concurrency: N`) | BullMQ enforces this worker-side; AnvilMQ workers can self-limit in-flight claims |
| B2 | **Worker health monitoring** | The application's monitor is a worker→Redis check; the AnvilMQ client already exposes Prometheus endpoints scrapeable into Grafana. Provide an equivalent worker-side liveness signal |
| B3 | **BullMQ-shaped client API** | The client is explicitly *not* BullMQ-compatible today. A compatibility surface lets the ~60 cron+flow jobs port with minimal rewrites — essential for a *fair* comparison and for a real migration later |

### C. Deferred — not blockers for the evaluation

| Feature | Rationale for deferral |
|---------|------------------------|
| **High availability / Raft (Phase 5)** | The application's current Valkey runs Sentinel, but the Sentinels are **co-located on a single node**, so a node outage still drops messages — the deployment is **not truly HA today**. Single-broker AnvilMQ therefore only has to match that real-world resilience bar, not textbook HA. AnvilMQ runs at **NORMAL** durability (FULL is off the table for throughput — see below) and relies on the client **outbox** to catch producers up across a broker restart, plus periodic **WAL backup** for disaster recovery. HA is not required to make the comparison fair — see the caveat below. |
| **Admin-UI mutations** (retry / promote / remove / create) | `/console` is read-only; useful for ops parity but not needed to run a throughput/correctness test. Revisit post-evaluation. |
| **`job.log` / `step.sleep`** | Nice-to-have. Per-job logs are cosmetic for the test; `step.sleep` is expressible as a delayed follow-up job. |
| **Chain/depth configurability** | Default depth 10 and the (off-by-default) chain-size cap are fine for the expected shallow trees; make configurable only if a ported flow needs it. |

#### HA caveat (must be tested, not assumed)

**The resilience bar is the *actual* current deployment, not textbook HA.** Today's
Valkey runs Sentinel with all Sentinels co-located on one node, so a node outage
already drops in-flight messages. AnvilMQ only has to match that, and the client
outbox plausibly exceeds it. The asserted "HA is the biggest blocker" framing
overstates a requirement the current system does not itself meet.

**Durability choice: run at NORMAL.** FULL is incompatible with the throughput the
workload needs (the baseline shows FULL capped well below observed peak), so the
evaluation runs AnvilMQ at NORMAL. NORMAL can lose recently acknowledged writes
after an OS/power failure, so the durability story rests on two compensating
mechanisms rather than synchronous commit.

The parity argument for deferring HA therefore rests on two claims the evaluation
must **validate as explicit test cases**, not take on faith:
1. the client **outbox** actually lets producers catch up after a broker restart
   with no lost enqueues (test C4);
2. **WAL-backup restore** recovers broker state within an acceptable window.

If either fails under test, HA (or a stronger durability posture) re-enters the
conversation.

## Proposed sequencing

Three tracks run partly in parallel. Track A is the critical path; B and C
proceed alongside.

```
Track A (broker)   A1 A2 ──▶ A3 A4 ──▶ A5 (cron) ──▶ A6 (flows)
Track B (client)        B1 B2 ──────────────────────▶ B3 (BullMQ-shaped API)
Track C (scenario)                     C1 harness ──▶ C2 side-by-side ──▶ C3 dashboards ──▶ C4 failure injection
```

**Recommended milestone order within Track A:** land the XS/S items first (A1,
A2, A3, A4) to de-risk and get quick parity wins, then **cron (A5)** — it unblocks
the most jobs (~35) and is more self-contained than flows — then **flows (A6)**,
the largest and most invasive change.

### Track C — the evaluation itself

- **C1.** Port a representative *simulated* workload: cron spread across the real
  interval range (30s–10d), a few flow trees mirroring real shapes, and a bulk
  fan-out. No real tenant data.
- **C2.** Deploy AnvilMQ and BullMQ/Valkey side by side on the same cluster under
  identical load.
- **C3.** Comparable dashboards: reuse the bull-der-dash panels for BullMQ and the
  AnvilMQ sim/Prometheus dashboards for AnvilMQ — throughput (ops/s), latency
  (p50/p95/p99), backlog/age, and durability settings in view.
- **C4.** Failure injection: **node-outage parity** — reproduce the real failure
  mode (lose the node hosting the broker, mirroring the co-located-Sentinel Valkey
  risk) and verify the client **outbox** catches producers up with no lost
  enqueues on restart; plus a **WAL-backup restore** drill (see HA caveat).

## Relationship to the existing README roadmap

| README phase | Disposition under this plan |
|--------------|-----------------------------|
| Phase 1 — Core plumbing | Unchanged (libSQL migration still open, independent of parity) |
| Phase 2 — Transactional lifecycle | Largely complete; the parity track builds on it |
| Phase 3 — Safety controls | **Ancestry validation merges into A6 (flows)**; ingress velocity stays |
| Phase 4 — Faceted rate limiting | Unchanged; fairness still open, not needed for the test |
| **Phase 5 — Regional HA (Raft)** | **Stays deferred.** Not a blocker for the evaluation; mitigated by durability + client outbox, which the test validates |
| Phase 6 — Observability | Largely complete; reused for Track C dashboards |

## Decisions

- **Durability: NORMAL.** FULL is incompatible with the workload's throughput
  needs and is not a candidate. The durability story is NORMAL + client outbox +
  periodic WAL backup, validated by test C4.
- **Resilience bar: match the current deployment, not textbook HA.** Today's
  co-located-Sentinel Valkey is not truly HA, so HA/Raft (Phase 5) stays deferred.

## Open questions

- Milestone cut for the *first* evaluation: do we need full flow semantics (A6)
  in the first head-to-head, or can an initial round cover cron + bulk + dedup
  (A1–A5) with flows following in a second round?
- B3 scope: a thin BullMQ-compatible shim vs. a fuller client rewrite.
