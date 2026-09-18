# NORMAL/FULL comparison — 2026-09-17

Four sequential runs used the same release broker/runner images, four producers and four workers, a 400 jobs/s rate ceiling, 60-second input window, three-second warmup, 1 KiB payload padding, zero handler delay, and info logging. Each run started with a fresh database in the same Docker volume. The second pair reversed mode order. Startup readback confirmed WAL and synchronous=1 for NORMAL or 2 for FULL.

Host: Docker Desktop Linux VM, Docker 29.7.2, four CPUs, 8,332,894,208 bytes visible RAM. Client: Bun 1.2.14. No CPU/memory limits or concurrent benchmark workloads were configured. Ordinary host contention and storage caches were uncontrolled. Unlike earlier rate sweeps, this script does not run repeated Docker resource sampling.

| Order / mode | Actual enqueue/s | Completion/s during input | Enqueue p99 ms | Completion p99 ms | Drain s |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 / NORMAL | 400.00 | 399.90 | 21.44 | 69.57 | 0.07 |
| 2 / FULL | 176.55 | 87.98 | 36.81 | 32257.97 | 32.28 |
| 3 / FULL | 171.77 | 85.67 | 36.88 | 32947.55 | 33.02 |
| 4 / NORMAL | 400.00 | 399.95 | 19.82 | 37.19 | 0.05 |

All 68,899 acknowledged measured jobs completed. All four runs reported zero producer/worker errors and duplicate deliveries. FULL's sampled backlog peaked at 5,310 and 5,161 jobs. Passing eventual-completion checks does not mean FULL sustained the 400/s target; it plainly did not. Producers are closed-loop, so their actual submission rate fell below the ceiling when RPCs slowed.

## What this tells us

The FULL cost was substantial and repeatable on this local storage stack. NORMAL kept up both before and after the FULL pair, which strengthens the evidence that the configured durability mode drove the difference. This experiment does not isolate hardware sync latency, establish a production capacity limit, or show that FULL cannot meet the workload on different storage.

Completion p99 in the FULL runs is dominated by accumulated queue wait, not a 32-second individual commit. The lower completion rate during input also includes contention from concurrent enqueue traffic; it is not a universal maximum for FULL. A lower-rate FULL run would be needed to measure latency without overload.

Preserve the acknowledgment guarantee when choosing the next architecture. If OS/power-loss durability is required, configure FULL and validate the intended storage rather than substituting NORMAL to meet a throughput number. Future batching/group commit may reduce sync overhead, but requires its own correctness and latency tests. Raft remains an availability decision and also needs durable persistent-state rules.

No power loss, node loss, or storage failure was injected. These measurements validate configuration and performance only; see the [durability contract](../docs/durability.md) for guarantees and assumptions. The broker was restored to its original default database and NORMAL configuration after the test; benchmark databases were retained.

## Reproduce

```powershell
./harness/durability.ps1 -DurationSeconds 60 -Rate 400
```

Raw paired reports, startup readbacks, and Docker details are in `harness/artifacts/durability-20260917T224646-6f74ab98/` (gitignored). Timestamped load reports also retain before/after metrics snapshots. Validation: 31 Rust tests, debug/release builds, invalid-mode startup rejection, formatting/whitespace checks, and all four actual container workloads passed.
