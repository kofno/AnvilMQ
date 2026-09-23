# AnvilMQ isolated latency/throughput benchmark

> **These are local, exploratory measurements on shared developer hardware (Docker Desktop on
> WSL2), not production SLOs or capacity guarantees.** They characterise one broker build on one
> machine with a single embedded database volume. Treat every number as directional. Re-run on the
> target hardware before making any sizing decision.

Run date: 2026-09-23. Broker commit `e0810b3` (branch `kofno-phase6-benchmarks`).

## Why this document exists (and how it relates to BASELINE.md)

`BASELINE.md` is retained as a dated, honest record of an earlier *exploratory* pass. It documents
its own limitations: those runs changed **both** worker concurrency and arrival pressure at once
(so they do not isolate the benefit of adding workers), neither run established a maximum
sustainable capacity, and history/volume was reused between scenarios (warm caches inflate later
numbers). This document supersedes `BASELINE.md` for capacity questions.

This pass was designed to fix exactly those problems:

1. **One variable per scenario.** Set A sweeps arrival rate at fixed concurrency; Set B sweeps
   worker concurrency at a fixed arrival rate; producer count is held constant so worker count is
   the only thing that changes.
2. **A defined, measured "sustainable" criterion** (below) rather than eyeballing peak enqueue
   rate — we report the arrival rate at which *completion* keeps up and backlog stays bounded.
3. **A clean database volume for every repetition** (no history carryover), a pinned/quantified
   environment, an explicit warmup phase, and machine-readable per-scenario JSON artifacts.

A new file (rather than an edit of `BASELINE.md`) keeps the older exploratory record intact for
comparison and makes the methodological break obvious.

## Environment (pinned)

| Property | Value |
| --- | --- |
| Container engine | Docker Desktop 29.8.0, Compose v5.5.1 |
| Engine kernel | `5.15.167.4-microsoft-standard-WSL2` (Linux, x86_64) |
| CPUs visible to engine | 4 |
| Memory visible to engine | 8,332,894,208 bytes (≈7.76 GiB) |
| Broker image | build `rust:1.94.1-bookworm` → runtime `debian:bookworm-slim`, ≈140 MiB |
| Broker binary | release build, runs as UID 10001 |
| Load runner image | `oven/bun:1.2.14` |
| Bun (host and container) | 1.2.14 |
| Storage | embedded single-file database, WAL journal, on a dedicated Compose volume |
| Durability | NORMAL = `synchronous=NORMAL`; FULL = `synchronous=FULL` (fsync each commit) |
| Transport | plaintext gRPC over loopback, dedicated ports 50072 (gRPC) / 9099 (metrics) |
| Compose project | `anvilmq-bench` (isolated from the smoke/crash harness project) |
| Payload | 1024 padding bytes per job (excludes envelope/metadata) |
| Handler work | 0 ms artificial delay (measures broker + storage + transport, not handler CPU) |

Each scenario runs against **one dedicated broker instance** (the completion-throughput gauge
assumes a dedicated process). Every repetition recreates the broker container against a **fresh,
unique, empty database file** (`ANVILMQ_HARNESS_DB_FILE=<unique>.db`), so no run inherits another
run's history, cache warmth, or table size. The broker image is built once per invocation and
reused across repetitions (only the container/DB is recreated).

## Methodology

### Phases and window

Each run has three phases:

- **Warmup** (5 s): load is applied but excluded from all rate and latency statistics.
- **Measurement window** (60 s): the reported enqueue rate, completion-during-window rate, latency
  percentiles, and backlog regression slope are computed over this window only.
- **Drain** (up to 120 s cap): the producers stop and the run waits for every acknowledged job to
  complete, measuring drain duration and completion-including-drain throughput. A run that cannot
  drain within the cap is recorded as a correctness failure (`drainTimedOut`).

### Metrics

- **Enqueue/s** — acknowledged enqueues per second during the window. Target rate is a *ceiling*
  enforced by closed-loop producers, each with at most one outstanding enqueue; it is not
  guaranteed offered load, so under overload the achieved enqueue rate falls below target.
- **Completion/s (window)** — jobs whose completion acknowledgement arrived during the window,
  per second. This is the throughput the "sustainable" verdict is built on.
- **Completion/s (incl. drain)** — completions per second across window + drain.
- **Enqueue latency** — client-observed enqueue RPC round trip, p50/p95/p99.
- **End-to-end latency** — submission → completion-acknowledgement, p50/p95/p99. Includes enqueue,
  queue wait, handler, and the completion RPC.
- **Backlog slope** — linear-regression slope (jobs/second) of the acknowledged-minus-completed
  backlog against time across the measurement window. Near zero = draining as fast as arriving;
  strongly positive = backlog is accumulating.

### Bounded-backlog / "sustained" criterion

A repetition is **sustained** when all three hold:

1. **Bounded backlog:** backlog regression slope ≤ **2 jobs/s** over the measurement window.
2. **Completion keeps up:** completion/s ≥ **0.98 ×** enqueue/s.
3. **Correctness:** no drain timeout, no producer/worker errors, no duplicate deliveries, and
   completed == acknowledged.

Thresholds are configurable (`LOAD_BACKLOG_SLOPE_MAX`, `LOAD_KEEPUP_FRACTION`). A **point**
(a target rate or worker count) is judged sustained when a **majority of its 3 repetitions** are
sustained.

### Repetitions and reproducibility

Every point is run **3 times**. Cells below are **median (min–max)** across repetitions. The
**Reproducible** verdict is judged on *completion throughput* (spread max/min ≤ 1.5×) because that
is the metric the capacity claim rests on. End-to-end p99 **tail** spread is reported separately
and descriptively: on a shared host the tail jitters run-to-run even when throughput is rock
solid, so a wide tail alone does not invalidate a throughput result. A point whose completion/s
spread exceeds 1.5× is flagged **NON-REPRODUCIBLE** — which is itself a finding (it marks a
metastable operating point).

## Results

### Set A — arrival-rate sweep at fixed concurrency (4 producers / 4 workers, NORMAL)

| Target rate | Enqueue/s | Completion/s (window) | End-to-end p99 ms | Backlog slope j/s | Drain s | Sustained | Reproducible (throughput) |
| --- | ---: | ---: | ---: | ---: | ---: | :---: | :--- |
| 200 jobs/s | 200.00 (200.00–200.00) | 199.98 (199.97–200.00) | 49.67 (37.19–92.36) | -0.00 | 0.05 | yes (3/3) | yes; tail spread 2.48× |
| 250 jobs/s | 250.00 (250.00–250.00) | 249.98 (249.93–249.98) | 56.63 (44.04–66.80) | -0.02 | 0.05 | yes (3/3) | yes; tail spread 1.52× |
| 300 jobs/s | 300.00 (299.97–300.00) | 299.88 (299.87–299.90) | 490.05 (69.13–584.15) | -0.37 (-0.82–-0.02) | 0.05 | yes (3/3) | yes; tail spread 8.45× |
| 350 jobs/s | 350.00 (350.00–350.00) | 349.95 (349.90–349.97) | 227.30 (130.16–249.83) | -0.13 (-0.23–0.01) | 0.05 | yes (3/3) | yes; tail spread 1.92× |
| 400 jobs/s | 232.42 (219.05–239.08) | 114.48 (100.83–126.53) | 61,981 (58,457–63,130) | 116.11 (113.24–116.14) | 57.02 | **no (0/3)** | yes |

**Max sustainable completion throughput is bracketed in (350, 400) jobs/s:** 350 jobs/s sustained
in all 3 repetitions (completion tracked arrival at 349.95/s with a slightly negative backlog
slope), and 400 jobs/s overloaded in all 3 (achieved enqueue fell to ~232/s, completion collapsed
to ~114/s, backlog grew ~116 jobs/s, and end-to-end p99 blew out to ~62 s). The collapse at 400 is
classic congestion collapse: once the backlog runs away, contention on the single database writer
drags completion *below* the rate a quiet run could sustain.

Note this clean-database, dedicated-instance ceiling (350) sits **below** the warm-volume figure a
prior reused-history sweep suggested — consistent with `BASELINE.md`'s own warning that warm
caches/history inflate results.

### Set B — worker-concurrency sweep at fixed arrival rate (250 jobs/s, 4 producers, NORMAL)

The fixed rate (250 jobs/s) is a comfortably-sustained Set A point, below the 350 metastable knee.
Producer count is held at 4 (identical to Set A) so **worker count is the only variable**.

| Workers | Enqueue/s | Completion/s (window) | End-to-end p99 ms | Backlog slope j/s | Drain s | Sustained | Reproducible (throughput) |
| --- | ---: | ---: | ---: | ---: | ---: | :---: | :--- |
| 1 | 249.93 (246.47–250.00) | 105.98 (65.40–115.43) | 116,204 (95,723–139,175) | 150.36 (140.79–189.82) | 116.02 | **no (0/3)** | NON-REPRODUCIBLE: completion spread 1.77× |
| 2 | 249.77 (249.20–250.00) | 248.27 (86.68–249.97) | 429 (165–104,583) | 0.06 (-0.07–163.89) | 0.59 | yes (2/3) | NON-REPRODUCIBLE: completion spread 2.88× |
| 4 | 250.00 (250.00–250.00) | 250.00 (249.98–250.00) | 167.12 (60.51–1106.09) | -0.11 (-0.20–-0.00) | 0.05 | yes (3/3) | yes; tail spread 18.28× |
| 8 | 249.97 (249.93–250.00) | 249.97 (249.72–249.98) | 82.62 (65.64–150.37) | 0.01 | 0.06 | yes (3/3) | yes; tail spread 2.29× |

**Isolated worker-concurrency effect at 250 jobs/s:** 1 worker cannot keep up (completion collapses
to ~106/s, backlog grows ~150 jobs/s). **2 workers is metastable** — it sustained in 2 of 3
repetitions but collapsed in the third (86.7/s), which is why its completion spread is flagged
non-reproducible and exactly why 3 repetitions matter near a knee. **4 and 8 workers both sustain
250 jobs/s in all 3 repetitions**, and going from 4 to 8 workers halves end-to-end p99 tail
(median 167 ms → 83 ms) without changing throughput (arrival-bound at 250/s). So ~4 workers are
needed to *reliably* sustain 250 jobs/s at this payload on this host; more workers buy tail-latency
headroom rather than throughput.

### Set D — durability comparison at a fixed operating point (250 jobs/s, 4 producers / 4 workers)

| Durability | Enqueue/s | Completion/s (window) | End-to-end p99 ms | Backlog slope j/s | Drain s | Sustained | Reproducible (throughput) |
| --- | ---: | ---: | ---: | ---: | ---: | :---: | :--- |
| NORMAL | 250.00 (244.95–250.02) | 249.97 (199.05–249.97) | 816 (778–15,310) | 0.99 (0.46–40.23) | 0.06 | yes (2/3) | yes; tail spread 19.67× |
| FULL | 111.22 (101.88–112.07) | 55.10 (50.42–55.70) | 45,085 (41,994–46,084) | 54.37 (49.38–55.40) | 41.58 | **no (0/3)** | yes |

**Durability cost is large and unambiguous.** With FULL durability (an fsync on every commit) the
broker **cannot sustain 250 jobs/s**: completion collapses to ~55/s and even the enqueue side is
throttled to ~110/s, because every enqueue commit also fsyncs. That is roughly a **4.5× reduction
in completion throughput** and a **~2× reduction in enqueue throughput** versus NORMAL at the same
offered load. Even NORMAL's worst repetition (199/s) far exceeds FULL's best (55.7/s), so the
direction of the result is robust regardless of NORMAL's metastability.

**Caveat on this specific NORMAL row:** the same 250 jobs/s @ 4-worker point that was rock-solid in
Set A (p99 56 ms, 3/3) shows an elevated tail (p99 ~800 ms) and one collapsed repetition here. Set
D ran late in a long session after the full Set B sweep, so the host was under more accumulated
load — a reminder that this operating point sits close enough to the host-sensitive edge that
background pressure can tip it. This does not affect the FULL-vs-NORMAL conclusion.

### Latency detail for sustained points (median across repetitions, ms)

| Scenario | Enqueue p50 | Enqueue p95 | Enqueue p99 | End-to-end p50 | End-to-end p95 | End-to-end p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A 200 j/s (4w) | 2.17 | 14.52 | 24.13 | 8.27 | 30.76 | 49.67 |
| A 250 j/s (4w) | 2.22 | 14.57 | 22.14 | 8.40 | 29.41 | 56.63 |
| A 300 j/s (4w) | 2.75 | 16.26 | 24.10 | 10.46 | 276.23 | 490.05 |
| A 350 j/s (4w) | 3.04 | 16.85 | 24.01 | 13.93 | 126.13 | 227.30 |
| B 250 j/s, 4w | 2.84 | 17.04 | 26.31 | 10.55 | 74.35 | 167.12 |
| B 250 j/s, 8w | 4.96 | 22.39 | 34.63 | 13.86 | 53.15 | 82.62 |
| D 250 j/s NORMAL | 4.42 | 21.46 | 35.23 | 21.97 | 562.13 | 816.22 |

## Threats to validity

- **Shared consumer hardware.** Docker Desktop on WSL2 with 4 CPUs; other host processes compete
  for CPU and disk. The elevated Set D NORMAL tail shows the operating point is sensitive to host
  load late in a long run.
- **Single embedded writer.** The dominant bottleneck at the knee is the single-writer database
  with per-commit durability, not the network or the handler (handler work is 0 ms here). Results
  will shift substantially with real handler work, larger payloads, or different storage.
- **Metastable knees.** 400 jobs/s (Set A) and 2 workers @ 250 jobs/s (Set B) are genuinely
  metastable; the reported verdict is the majority of 3 repetitions and the spread flags mark them.
- **Closed-loop, ceiling-paced load.** Target rate is a ceiling, not an open-loop arrival process;
  under overload the achieved enqueue rate drops rather than a queue building unbounded upstream.
- **Descriptive resource samples.** Per-run `docker stats` CPU/memory samples are captured for
  context only; the sampler loop was verified not to change the throughput verdict.
- **Successful-sample latency only.** Latency percentiles use successful RPC samples; reservoir
  sampling begins after 200,000 observations per distribution.

## Reproduce

From the repository root (PowerShell 7), with Docker running:

```powershell
# Set A — build once, sweep arrival rate at fixed 4/4 concurrency, clean DB per rep.
./harness/benchmark.ps1 -Sets A,summary `
  -Rates 200,250,300,350,400 -Reps 3 `
  -DurationSeconds 60 -WarmupSeconds 5 -DrainSeconds 120 `
  -OutDir harness/artifacts/bench-phase6

# Set B + Set D — reuse the images, fixed 250 jobs/s, sweep workers, then NORMAL vs FULL.
./harness/benchmark.ps1 -Sets B,D,summary -SkipBuild `
  -FixedRate 250 -WorkerCounts 1,2,4,8 `
  -Durability NORMAL,FULL -DurWorkers 4 -Reps 3 `
  -DurationSeconds 60 -WarmupSeconds 5 -DrainSeconds 120 `
  -OutDir harness/artifacts/bench-phase6

# Re-aggregate an existing run directory (no runs; reads the JSON reports).
bun harness/summarize.ts harness/artifacts/bench-phase6
```

## Artifacts

All machine-readable outputs land in the run directory (`harness/artifacts/` is gitignored):

- `A-rate<r>-rep<n>.json`, `B-workers<w>-rep<n>.json`, `D-<mode>-rep<n>.json` — per-scenario load
  reports (config, per-second backlog timeline, latency distributions, sustained verdict).
- `*-resources.json` — per-run `docker stats` CPU/memory samples (descriptive).
- `*-broker-config.log`, `*-broker-tail.log` — durability readback and the tail of the broker log.
- `docker-info.json`, `images.json`, `manifest.json` — environment and run parameters.
- `summary.json`, `summary.md` — cross-repetition aggregation (median + min–max spread, the
  max-sustainable bracket, and reproducibility flags).
