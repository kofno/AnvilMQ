# No-rules dequeue scan fix — 2026-09-16

Dequeue's throttled-poll telemetry previously scanned due, queue-matching jobs even when the rate-limit rule table was empty. The probe now uses a lazy SQL `CASE`: check whether any rules exist, evaluate the existing probe only if they do, otherwise return false. This remains inside the immediate claim transaction, preserving serialization with rule updates and existing quota/metric semantics.

A regression test compares SQLite VM instruction counts with 1 versus 10,001 queued jobs: the no-rules cost is identical. It also checks that adding a matching rule activates the probe and deleting the rule restores the fast path. All 28 Rust tests and the build passed; formatting and diff whitespace checks passed.

## Container results

Rebuilt the release broker and ran four producers/workers, 60 seconds per rate, three seconds warmup, 1 KiB padding, zero handler delay, WAL/NORMAL, info logging, and Docker resource sampling. The runner image stayed unchanged. The same Docker VM (4 CPUs, about 8.3 GB visible RAM) and persisted database volume were reused; history continues to accumulate.

| Target jobs/s | Actual enqueue/s | Completion/s during input | End-to-end p99 ms | Drain s | Peak sampled backlog | Mean broker CPU % |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 400 | 400.00 | 399.98 | 27.40 | 0.05 | 6 | 54.7 |
| 600 | 600.02 | 599.93 | 42.97 | 0.05 | 35 | 74.4 |
| 800 | 800.00 | 762.63 | 2757.62 | 2.33 | 2199 | 82.0 |

All 108,001 acknowledged measured jobs completed, with zero reported producer/worker errors or duplicate deliveries. CPU samples include warmup and drain; 100% is approximately one core. Backlog is sampled once per second, so peaks between samples are not captured.

Compared with the earlier follow-up at 600/s, completion during input rose from 186.97 to 599.93/s, p99 fell from 61.34s to 42.97ms, and drain fell from 54.28s to 0.05s. Compared with the original 800/s sweep, completion rose from 184.78 to 762.63/s and p99 fell from 65.28s to 2.76s.

The scan's removal is directly verified by the regression test; these end-to-end improvements are observations, not an isolated causal measurement. Runs occurred sequentially, with a broker restart, different accumulated history, and uncontrolled host contention. Earlier 400/s results already varied substantially. No production capacity guarantee follows from these short runs.

The 800/s run still accumulated backlog and required drainage: passing eventual-completion checks does not mean it sustained 800/s. The next performance experiment should repeat longer 600–800/s runs to establish stability before targeting another bottleneck. Workloads with configured rules retain the original telemetry scan and need separate profiling. Raft remains an independent availability decision.

## Reproduce

```powershell
docker compose -p anvilmq-harness -f harness/compose.yaml build broker
docker compose -p anvilmq-harness -f harness/compose.yaml up -d --wait broker
./harness/sweep.ps1 -Rates 400,600,800 -Label scanfix
```

Raw reports, resource samples, and logs are under `harness/artifacts/scanfix-*` (gitignored). The table above preserves the measured summary.
