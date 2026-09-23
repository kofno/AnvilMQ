# Initial local performance baseline — 2026-09-16

> **Superseded for capacity questions.** This is the original exploratory baseline. A rigorous,
> variable-isolated follow-up — clean database per scenario, separate concurrency and arrival-rate
> sweeps, a defined bounded-backlog criterion for maximum sustainable completion throughput, and
> repeated runs with reported spread — is in [BENCHMARKS.md](BENCHMARKS.md). This document is
> retained as the historical record.

Both scenarios passed: every acknowledged job completed, with zero producer/worker errors and zero duplicate deliveries. The unlimited single-worker run accumulated a substantial backlog; the controlled 200 jobs/s run kept up.

| Measurement | Unlimited, 1 producer / 1 worker | 200 jobs/s ceiling, 4 producers / 4 workers |
| --- | ---: | ---: |
| Measured duration | 10s | 15s |
| Warmup | 2s | 3s |
| Acknowledged/completed | 4,429 / 4,429 | 3,000 / 3,000 |
| Enqueue jobs/s | 442.90 | 200.00 |
| Completion jobs/s during window | 186.30 | 199.80 |
| Completion jobs/s including drain | 211.95 | 199.34 |
| Drain/shutdown | 10.90s | 0.05s |
| Enqueue p50 / p95 / p99 | 1.34 / 5.38 / 13.45 ms | 1.43 / 8.76 / 22.84 ms |
| End-to-end p50 / p95 / p99 | 8,237.56 / 10,988.13 / 11,006.01 ms | 6.25 / 18.02 / 30.55 ms |

End-to-end means submission through successful completion acknowledgment, not merely handler return. The first run's enqueue throughput is **not** sustainable completion throughput. Its long latency reflects backlog. These runs changed both concurrency and arrival pressure; they do not isolate the benefit of adding workers, and neither establishes maximum capacity.

## Environment and constraints

- Docker Desktop Linux engine 29.7.2, x86_64, Windows host.
- Docker VM: 4 CPUs and 8,332,894,208 bytes RAM; no per-container CPU/memory caps configured.
- Broker and runner share the VM. Bun 1.2.14; release Rust broker.
- Broker image ID: `sha256:30b42b3d639a9dfa7ec06f8753619f5407c86d86aba3180a9bb930b1d8c1b3cb`.
- SQLite WAL with synchronous=NORMAL, info-level JSON logging enabled.
- Named volume reused from smoke runs; history was not cleared between scenarios. Cache/history conditions therefore differ.
- 1,024 bytes of payload padding plus JSON envelope; zero simulated handler work; 10ms idle worker polling.
- Warmup drains before measurement. New client connections are created for the measured phase; their setup is included.
- No injected failures. This is an exploratory local baseline, not a production SLO or a durability guarantee.

## Reports and reproduction

Raw JSON, Markdown, and before/after Prometheus snapshots are under `harness/artifacts` (ignored by Git):

- `2026-09-16T11-55-30-905Z-218ec242.*`
- `2026-09-16T12-10-54-649Z-8ef0f446.*`

```powershell
./harness/load.ps1 -Producers 1 -Workers 1 -DurationSeconds 10 -WarmupSeconds 2
./harness/load.ps1 -Producers 4 -Workers 4 -DurationSeconds 15 -WarmupSeconds 3 -Rate 200
```

Next useful measurement: hold concurrency, payload, durability, and logging constant; sweep offered rates through 100/200/400 jobs/s with repeated longer runs to identify where backlog starts growing. Capture host contention and database size alongside those results.
