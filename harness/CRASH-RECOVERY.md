# Active-claim crash recovery — 2026-09-17

Two local Docker runs passed, including a repeat against the final harness after TypeScript checking. The release broker used the existing volume and WAL/NORMAL configuration. Four claims were deliberately abandoned while two concurrent producers continued submitting jobs; the host sent SIGKILL, waited for both producers to observe the outage, and restarted the broker.

| Observation | First run | Final-version run |
| --- | ---: | ---: |
| Acknowledged / completed jobs | 80 / 80 | 85 / 85 |
| Missing acknowledged jobs | 0 | 0 |
| Abandoned / recovered claims | 4 / 4 | 4 / 4 |
| Expected redeliveries | 4 | 4 |
| Stale completion rejections | 4 | 4 |
| Stale heartbeat rejections | 4 | 4 |
| Crash request to broker readiness | 7.76 s | 7.85 s |
| Observed outage to first recovered claim | 32.02 s | 32.09 s |
| Observed outage to completed verification | 33.08 s | 33.13 s |
| Ambiguous enqueue RPCs / observed committed | 2 / 0 | 2 / 0 |

Each recovered job returned as attempt two. Reusing the worker ID, the runner first tried attempt one's completion and heartbeat: both returned `FailedPrecondition`. Attempt two then completed successfully. Every acknowledged ID and payload sequence was reconciled. No unexpected retry or same-attempt duplicate was observed.

Broker readiness and abandoned-work recovery are distinct: the normal lease is 30 seconds, with expired leases swept every five seconds. These measurements include local Docker/Compose orchestration and health-check polling; they are not intrinsic startup latency or an availability SLO. Verification includes a one-second empty-queue observation after acknowledged jobs complete.

This establishes a process-crash baseline for controlled active claims on retained storage. It does not establish node/power-loss durability, uninterrupted availability, replicated failover, or safety of arbitrary external side effects. No completion request was deliberately raced against SIGKILL; ambiguous completion responses remain a separate test. The two failed enqueue requests per run were recorded without retry, since their commit outcome was initially unknown.

Reproduce with `./harness/crash-active.ps1`. Detailed ID ledgers, markers, JSON/Markdown reports, and logs are under these gitignored directories:

- `harness/artifacts/crash-20260917T132447-3dcbde59/`
- `harness/artifacts/crash-20260917T133445-91316f23/`

Validation: `bun run check` from `client`, PowerShell parser validation, two actual SIGKILL/restart scenarios, and `git diff --check`. No broker code changed for this harness increment.
