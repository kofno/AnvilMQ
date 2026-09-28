# Failure-testing drills

An operator-facing catalog of failure-testing drills for AnvilMQ's **single-broker +
persistent-volume** model. Each drill has a fixed shape — failure model, local
reproduction, Kubernetes procedure, expected outcome, acceptance criteria, and evidence —
so you can rehearse recovery and confirm the guarantees below hold on your storage stack.

## 1. Purpose & scope

These drills validate that a single AnvilMQ broker recovers from process, pod, node, and
volume failures within stated recovery-time and recovery-point objectives, with **no loss
of acknowledged jobs** inside the stated failure model.

**In scope:** broker process crash, pod reschedule, volume detach/reattach, and
restore-from-snapshot on one broker backed by one persistent volume.

**Out of scope:** replicated / high-availability failover. A single broker is
**unavailable** during the interruption — there is no standby to take over. Replicated
failover (Raft or an external HA datastore) is deferred to **Phase 5b**; its requirements
and acceptance tests live in [availability requirements](availability.md), not here.

This document is a consolidation layer. It **cross-references** rather than duplicates:

- [availability requirements](availability.md) — the failure model, the single-broker +
  persistent-volume alternative, and the deferred replicated-HA work.
- [durability contract](durability.md) — what "acknowledged" means and WAL/NORMAL vs FULL
  semantics.
- [restore-from-snapshot runbook](restore-runbook.md) — the full local and Kubernetes
  restore procedure referenced by Drill D.
- [harness/README.md](../harness/README.md) and
  [harness/CRASH-RECOVERY.md](../harness/CRASH-RECOVERY.md) — the automated drills and
  their measured results.

## 2. Objectives: RTO / RPO / invariant

Targets below are **derived from measured evidence** (see
[harness/CRASH-RECOVERY.md](../harness/CRASH-RECOVERY.md)) and the lease / recovery / backup
defaults — they are not invented. Numbers are configuration-dependent; treat them as
targets to verify on your cluster, not guarantees.

### Recovery-time objective (RTO — queue availability)

The queue is unavailable from the moment the broker stops until a broker is back and
`/readyz` passes. The local harness measured **crash-request → broker readiness at ~7.8 s**
(7.76 s and 7.85 s across two runs), which already includes Docker/Compose orchestration
and health-check polling, not intrinsic startup latency.

- **Pod restart / reschedule, surviving volume:** target **≤ 60 s** to restored
  availability. This budgets the ~8 s process-crash → readiness plus Kubernetes failure
  detection, rescheduling, volume re-attach, and probe cadence
  (`readinessProbe` runs every 5 s).
- **Node loss + zonal volume reattach:** honestly **a few minutes** — node-failure
  detection and controller-driven volume detach/reattach dominate and are outside the
  broker's control. Alert and automate for this wider band; do not assume the ≤ 60 s
  target applies to node loss.

### In-flight (leased) job redelivery bound

A job that was **Active** (claimed under a worker lease) when the broker stopped is not
lost, but it is not redelivered instantly. After the broker is back, redelivery is bounded
by the **lease TTL + one recovery-sweep interval**:

- lease TTL: `LEASE_DURATION_MS` default **30 s** (runtime-configurable via
  `ANVILMQ_LEASE_DURATION_MS`).
- recovery sweep cadence: `DEFAULT_RECOVERY_INTERVAL_MS` default **5 s** (runtime-configurable
  via `ANVILMQ_RECOVERY_INTERVAL_MS`).

So expect leased work to become claimable again within **~35 s after the broker is ready**.
The harness observed **outage → first recovered claim at ~32 s** and **→ completed
verification at ~33 s**, consistent with this bound.

### Recovery-point objective (RPO — data loss window)

- **Volume survives** (crash, pod reschedule, detach/reattach): **RPO = 0** — zero
  acknowledged-job loss. Committed mutations are in the WAL on the retained volume.
- **Volume lost** and restored from a snapshot: **RPO = one snapshot interval**,
  `ANVILMQ_BACKUP_INTERVAL_MS` default **120000 ms (2 min)**. Work committed **after** the
  last snapshot is lost **by design** — the broker cannot recover what it never snapshotted.
  Producers that cannot lose submissions must keep an outbox and resubmit; see the
  [restore runbook](restore-runbook.md).

### Invariant under test

**No loss of acknowledged jobs** within the stated failure model. An "acknowledged" job is
one whose enqueue transaction committed (see the [durability contract](durability.md)).
Delivery remains **at-least-once**: worker side effects may repeat across recovery, and the
broker cannot make external effects exactly-once. Handlers must stay idempotent.

## 3. Recovery mechanisms

Brief pointers only — each mechanism is owned and explained by another document:

- **WAL / NORMAL durability.** Committed mutations survive process crash on retained
  storage; NORMAL may roll back the most recent commits only on OS/power loss. See the
  [durability contract](durability.md).
- **Worker leases + expired-lease recovery sweep.** Active jobs whose lease lapses are
  requeued so another worker can claim them (bound in
  [§2](#in-flight-leased-job-redelivery-bound)).
- **Graceful-shutdown WAL checkpoint.** On `SIGTERM`/`Ctrl-C` the daemon drains in-flight
  work and runs `PRAGMA wal_checkpoint(TRUNCATE)`; the StatefulSet allows
  `terminationGracePeriodSeconds: 30` for this.
- **`VACUUM INTO` snapshots + restore.** Scheduled self-contained snapshots plus the
  documented restore flow bound RPO when the volume is lost. See
  [Backups](../README.md#backups) and the [restore runbook](restore-runbook.md).

## 4. Drill catalog

All drills assume a **dedicated** broker: run one scenario at a time. Kubernetes object
names follow the chart — StatefulSet / pod prefix `<release>-anvilmq`, pod
`<release>-anvilmq-0`, PVC `data-<release>-anvilmq-0`, `volumeClaimTemplate` named `data`
mounted at `/data`, `ReadWriteOnce`, `terminationGracePeriodSeconds: 30`, UID 10001.

### Drill A — Broker process crash under load

**Failure model.** The broker process is killed (`SIGKILL`) while workers hold active
claims and producers keep enqueuing. Storage is retained.

**Local reproduction.** `./harness/crash-active.ps1` — seeds 20 jobs, holds four
first-attempt claims to simulate unfinished handlers, runs two producers, then `SIGKILL`s
only this project's broker and restarts it on the same `broker-data` volume. The `smoke` CI
job also runs `./harness/smoke.ps1` (a `SIGKILL` + restart of acknowledged waiting jobs) on
every pull request and push to `main`.

**Kubernetes procedure.**

```bash
kubectl delete pod <release>-anvilmq-0
# StatefulSet recreates the pod on the same PVC data-<release>-anvilmq-0
kubectl rollout status statefulset/<release>-anvilmq
```

**Expected outcome & mechanism.** WAL durability preserves committed work; the expired-lease
recovery sweep redelivers the four abandoned claims as **attempt 2**. Stale attempt-1
completion and heartbeat calls are rejected with `FailedPrecondition` while attempt 2 runs.

**Acceptance criteria.** 0 missing acknowledged IDs; every held claim redelivered exactly
once as attempt 2; stale attempt-1 completion/heartbeat rejected (`FailedPrecondition`);
availability restored within the RTO in [§2](#recovery-time-objective-rto--queue-availability).

**Evidence.** [harness/CRASH-RECOVERY.md](../harness/CRASH-RECOVERY.md) (0 missing of 80/85
acknowledged; 4/4 abandoned claims recovered; 4 stale completion + 4 stale heartbeat
rejections; ~7.8 s crash→readiness; ~32 s outage→first recovered claim; ~33 s →completed)
plus the CI `smoke` gate.

### Drill B — Pod reschedule (graceful)

**Failure model.** The pod is terminated **gracefully** (`SIGTERM`, honored drain) and
rescheduled — a rolling update, node drain, or `kubectl delete pod`. Storage is retained.

**Local reproduction.** `./harness/smoke.ps1` exercises a kill + restart on the same named
volume. Unlike Drill A's `SIGKILL`, a **graceful** stop exercises the `SIGTERM` drain and
`PRAGMA wal_checkpoint(TRUNCATE)`; the chart's `terminationGracePeriodSeconds: 30` gives the
daemon time to finish it.

**Kubernetes procedure.**

```bash
# graceful single-pod restart:
kubectl delete pod <release>-anvilmq-0

# node maintenance (drains and reschedules the pod elsewhere):
kubectl cordon <node>
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data
```

**Expected outcome & mechanism.** The graceful-shutdown checkpoint leaves a clean WAL; the
replacement pod reopens the volume and recovers quickly. In-flight leased jobs redeliver
within the bound in [§2](#in-flight-leased-job-redelivery-bound).

**Acceptance criteria.** No acknowledged-job loss; a clean WAL checkpoint on `SIGTERM`
(no forced kill before drain completes); availability restored within the pod-reschedule
RTO target (≤ 60 s).

**Evidence.** [harness/smoke.ps1](../harness/README.md#container-reliability-harness) via the
CI `smoke` gate; graceful-shutdown checkpoint behavior described in the
[README roadmap](../README.md#phase-5a-production-operability-next).

### Drill C — Volume detach/reattach

**Failure model.** The broker stops and its persistent volume detaches, then reattaches to a
replacement broker. Models a node failure where the PVC moves to the rescheduled pod. The
volume — and all committed data — survives.

**Local reproduction.** Stop the broker and bring the project **down without `-v`** so the
`broker-data` volume is preserved, then bring it back up so the broker reopens the same
volume:

```powershell
docker compose -p anvilmq-harness -f harness/compose.yaml down
docker compose -p anvilmq-harness -f harness/compose.yaml up -d --wait broker
```

Then verify every committed job is still present. (`down -v` would destroy the volume — do
not use it here.)

**Kubernetes procedure.** On node failure the `ReadWriteOnce` PVC
`data-<release>-anvilmq-0` detaches from the lost node and reattaches to the rescheduled
pod. No manual step is normally required; observe with:

```bash
kubectl get pod <release>-anvilmq-0 -o wide
kubectl describe pvc data-<release>-anvilmq-0
```

> **Operational hazard — zone affinity.** A `ReadWriteOnce` **zonal** disk can attach only
> to a node in the **same zone**. If the pod is rescheduled into a different zone, the
> volume cannot follow and the pod stays **Pending** (`FailedAttachVolume` /
> `node(s) had volume node affinity conflict`). Constrain scheduling to the volume's zone,
> or the pod will not start.

**Expected outcome & mechanism.** The durable PVC plus WAL recovery on reopen restore all
committed state; lapsed Active jobs requeue via lease recovery.

**Acceptance criteria.** After reattach + restart: all committed jobs present, database
integrity intact (`PRAGMA integrity_check` → `ok`), no acknowledged-job loss.

**Evidence.** Manual drill; record job counts before/after and the `integrity_check` result.

### Drill D — Restore from snapshot (volume lost)

**Failure model.** The live volume is **lost or corrupt** and cannot be reopened. Recovery
is from the most recent `VACUUM INTO` snapshot, so committed work after that snapshot is
gone by design.

**Local reproduction.** Follow the local flow in the
[restore runbook](restore-runbook.md#local--non-kubernetes-procedure): seed work → take a
`VACUUM INTO` snapshot → simulate loss by deleting `anvil.db`, `anvil.db-wal`, and
`anvil.db-shm` → copy the newest `snapshot-*.db` over `anvil.db` → delete the stale
`-wal`/`-shm` sidecars → restart → verify.

**Kubernetes procedure.** Use the maintenance-pod procedure in the
[restore runbook](restore-runbook.md#kubernetes-procedure): scale the StatefulSet to 0,
start a maintenance pod on the same PVC, restore the snapshot over `anvil.db`, drop the
stale WAL sidecars, then scale back to 1. Do **not** restore while a broker is running —
the broker is the sole writer.

**Expected outcome & mechanism.** The snapshot is a self-contained, non-WAL database;
deleting the stale `-wal`/`-shm` prevents SQLite from replaying an old WAL onto it. On
startup the broker recreates its WAL, migrates, and runs lease recovery to requeue lapsed
Active jobs.

**Acceptance criteria.** Broker starts cleanly; `PRAGMA integrity_check` → `ok`; all
**pre-snapshot** committed jobs present (bounded RPO = one snapshot interval, default 2 min;
post-snapshot work lost by design); previously Active jobs requeued via lease recovery.

**Evidence.** Cross-reference the [restore runbook](restore-runbook.md) — it was validated
once end-to-end. Do not duplicate its steps here.

## 5. Cadence & artifacts

- **Automated, CI-gated.** The `smoke` job runs `./harness/smoke.ps1` under `pwsh` on every
  pull request and push to `main`, uploading `harness/artifacts` and gating merges on
  crash-recovery passing (Drill A/B core path).
- **Reports land under `harness/artifacts/`.** `crash-active.ps1` writes
  `harness/artifacts/crash-<timestamp>-<id>/` (`report.json`, `report.md`, runner logs, and
  acknowledged-ID manifests); `smoke.ps1` writes `smoke.json`, `seed.json`, `verify.json`.
- **Recommended rehearsal.** Periodically run `./harness/crash-active.ps1` and a full
  restore drill (Drill D) against a dedicated broker. Run **one scenario at a time** — the
  completion-gauge checks assume a dedicated instance (see
  [harness/README.md](../harness/README.md#isolation-and-results)).

## 6. Limitations

- **Single broker, no HA.** During any of these interruptions the queue **is unavailable** —
  there is no standby. Replicated failover is [Phase 5b](availability.md).
- **At-least-once side effects.** Recovery may redeliver in-flight jobs; the broker cannot
  make external handler effects exactly-once. Keep handlers idempotent.
- **Power-loss durability caveats.** These drills use process/pod/volume faults on retained
  storage. They do **not** establish OS/power-loss durability; under NORMAL the most recent
  commits may roll back on power loss. Use `ANVILMQ_DURABILITY=FULL` and validate your
  storage stack — see the [durability contract](durability.md).
- **Snapshot RPO on volume loss.** When the volume is lost, up to one snapshot interval of
  committed work is lost by design (Drill D).
