# 60-second rate sweep — 2026-09-16

Four producers and four workers; 1,024 padding bytes; zero simulated handler time; three-second warmup. Same persistent broker, WAL/NORMAL, info logging, Docker VM (four CPUs). Backlog sampled once per second; Docker resource samples include warmup/drain, so CPU means are descriptive rather than a measurement-window comparison.

| Target jobs/s | Actual enqueue/s | Completions/s during window | End-to-end p99 ms | Drain seconds | Peak sampled backlog | Mean CPU %, broker / runner |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 100.00 | 100.00 | 27.38 | 0.05 | 2 | 27.6 / 46.8 |
| 200 | 200.00 | 199.95 | 36.56 | 0.05 | 2 | 45.4 / 65.1 |
| 400 | 400.00 | 399.98 | 27.65 | 0.05 | 5 | 58.5 / 70.8 |
| 800 | 346.30 | 184.78 | 65277.13 | 58.20 | 9688 | 101.3 / 45.2 |

All runs eventually completed every acknowledged job with zero reported RPC/worker errors or duplicate deliveries. PASS means correctness/drain checks passed, not that the offered-rate or latency target was met.

100–400 jobs/s kept up in these individual runs. The 800 target did not achieve 800: it accepted only 346 jobs/s while completing 185/s during input, and required 58 seconds to drain. Its p99 was 65 seconds. This is overload behavior, not evidence that 346/s is the stable ceiling. Closed-loop producer pacing reduces achieved input under contention. Existing history, warm caches, shared host resources, and queue buildup can affect results.

Next experiment: repeat 400 as a control, then test 500 and 600 jobs/s to localize the transition and inspect RPC timings/query plans under backlog. Do not select a production capacity limit from this sweep alone. No storage or Raft change is justified by these results alone.

Raft is separately evaluated against the availability requirement in [availability.md](../docs/availability.md). It is intended to preserve service and committed state through a broker/node failure, not improve throughput.

Raw reports, backlog timelines, logs, resource samples, and before/after Prometheus snapshots are in `harness/artifacts/sweep-*` and the timestamped run files. The database was not reset between rates.
