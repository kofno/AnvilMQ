# Follow-up load tests and query diagnosis — 2026-09-16

Same broker/configuration as the first sweep: four producers/workers, 60 seconds, 3-second warmup, 1 KiB padding, zero work delay, WAL/NORMAL, info logging, reused volume, 4-CPU Docker VM. No production code changes were made during this test.

| Target jobs/s | Actual enqueue/s | Completion/s during input | End-to-end p99 ms | Drain s | Peak sampled backlog | Mean broker CPU % |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 400 | 373.07 | 261.83 | 33682.21 | 30.60 | 6666 | 85.7 |
| 500 | 500.00 | 499.93 | 176.41 | 0.05 | 127 | 66.0 |
| 600 | 343.37 | 186.97 | 61341.13 | 54.28 | 9379 | 99.3 |

All acknowledged jobs eventually completed with zero reported producer/worker errors or duplicates. PASS does not mean the offered-rate/latency target was achieved. CPU samples cover warmup and drain as well as the producer window; 100% means approximately one core.

## Evidence

The 400 control failed to reproduce the earlier 400/s steady state. Backlog was nearly zero through 20 seconds, then grew to roughly 6,666 by 60 seconds. The 500 run kept up, with p99 around 176ms. The 600 run overloaded similarly to the original 800 run. Therefore no simple stable capacity limit can be inferred. The initiating cause of the transient slowdown has not been isolated; host contention, cache/history conditions, and scheduler effects remain possible.

A synthetic in-memory SQL probe uses the project's bundled SQLite 3.45.0, the same relevant indexes and dequeue predicates, no rules, and unrestricted Waiting jobs. It shows the throttle-observation query scales with the entire backlog, even when no job can be rate limited:

| Waiting jobs | Current throttle probe VM steps | Candidate selection VM steps | No-rules guarded probe VM steps |
| ---: | ---: | ---: | ---: |
| 100 | 1,717 | 38 | 17 |
| 1,000 | 17,017 | 38 | 17 |
| 10,000 | 170,017 | 38 | 17 |
| 20,000 | 340,017 | 38 | 17 |

The query plan scans `idx_jobs_schedulable` and evaluates a correlated rule lookup per candidate. This diagnostic check runs inside the immediate claim transaction, holding the writer while scanning. This is a confirmed O(backlog) cost and a plausible positive feedback mechanism: backlog increases claim cost, which can increase backlog further. It is not proof that this alone explains the full Linux load-test collapse.

The guarded variant is an experiment only: `CASE WHEN EXISTS(SELECT 1 FROM rate_limit_rules) THEN <current probe> ELSE 0 END`. It removes the scan when the rule table is empty. Nonempty rules still need separate evaluation; a no-rules shortcut is not a general solution for all throttled workloads. No behavior has been changed in the broker yet.

## Next decision

Make the small no-rules fast path, preserve atomic enforcement and metrics semantics, add regression tests, and repeat identical 400/500/600/800 scenarios. Only then decide whether further query/index work is needed. This finding does not justify a storage-engine replacement, and Raft remains a separate availability decision.

## Reproduce

```powershell
./harness/sweep.ps1 -Rates 400,500,600 -Label followup
cargo run --example query_probe
```

Reports are `harness/artifacts/followup-*`; diagnostic output is `query-probe.txt`. Probe wall times are Windows debug/in-memory measurements and must not be compared directly to Linux release RPC latencies; VM instruction counts establish the scaling behavior. The 500 run emitted a single sub-microsecond negative-timer warning from the load generator's pacing check (Bun clamps this to 1ms); the run completed successfully. All other client/server parameters remained unchanged.
