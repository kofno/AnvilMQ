# AnvilMQ local simulator

## Quickstart

From a checkout of this repository, with Docker Compose available:

```powershell
Set-Location examples\sim
docker compose up --build
```

This builds the broker and Bun workers, starts a synthetic workload, and provisions metrics,
logs, and four Grafana dashboards. The first broker build compiles Rust in release mode.
Use `docker compose up -d --build` to run detached, or `.\run.ps1` to also wait for readiness.
Allow a minute for rate-based charts to fill.

| Surface | Host address | Purpose |
|---|---|---|
| Grafana | http://127.0.0.1:3000 | Dashboards and log exploration |
| Prometheus | http://127.0.0.1:9490 | Metrics queries; scrapes the broker every 5 seconds |
| Broker gRPC | `127.0.0.1:50071` | Queue client and worker API |
| Broker HTTP | http://127.0.0.1:9092 | `/readyz`, `/metrics`, and read-only history APIs |
| Loki | http://127.0.0.1:3100 | Log ingestion and queries |

**Grafana credentials are `admin` / `admin`, with anonymous viewing enabled. Demo-only:**
all published ports bind to loopback. Do not expose this configuration publicly or reuse it in
production. Promtail reads this Compose project's container logs through the Docker socket;
a read-only socket mount is not a security boundary against Docker API access.

The example is self-contained: workers connect only to the local broker and observability
stack. Downstream service calls are simulated with bounded sleeps or CPU work. Data is held in
disposable Docker volumes, not external services. Image pulls and Grafana plugin installation
require network access. The broker ports differ from the repository's separate `harness/`
benchmark ports (`50061` / `9091`); this example does not include another benchmark harness.

### Dashboards and demos

| Dashboard | What it shows |
|---|---|
| **AnvilMQ queue health** | Throughput, terminal failures, retries, p95 latency, backlog, per-job overview, and retained history/receipt pruning |
| **AnvilMQ safety controls (ancestry & dispatch throttle)** | Ancestry-depth rejection, dispatch-side throttling, and structured enqueue-rejection logs |
| **AnvilMQ ingress velocity limits** | Admission-side rejections and task completion rate during bursts |
| **AnvilMQ recent failures** | Terminal failures with error messages, attempts, depth, trace IDs, and job IDs from the broker HTTP API |

The default producer exercises safety controls periodically. The standalone demos below cover
queue semantics, full-text search, tenant fairness, and ingress limits. Find provisioned
dashboards in Grafana's **AnvilMQ** folder; use **Explore** with Loki for logs.

## Feature demos

Run these commands from `examples\sim` while the cluster is running. They reuse the worker
image and its internal broker addresses, so no host Bun installation is required.

```powershell
docker compose run --rm workers /app/examples/sim/src/semantics-check.ts
docker compose run --rm workers /app/examples/sim/src/search-demo.ts
docker compose run --rm workers /app/examples/sim/src/fairness-demo.ts
docker compose run --rm workers /app/examples/sim/src/ingress-demo.ts
```

**Queue semantics:** checks priority ordering, delayed scheduling, retries with exponential
backoff, and idempotent enqueue using unique queue names.

**Full-text search:** seeds intentional failures carrying invented tokens, then checks token
retrieval, implicit AND, structured filtering, job-name matching, and token-vs-substring
behavior. `ANVILMQ_FTS_ENABLED=1` is enabled here; set it to `0` and recreate the broker to
use the default escaped-LIKE search instead (the FTS-specific demo expects FTS enabled).

**Tenant fairness:** a dedicated queue receives 60 jobs from a flood tenant, followed by 12
jobs each from three small tenants at equal priority. A serial worker reports claim order.
`ANVILMQ_FAIRNESS_ENABLED=true` enables equal round-robin turns by facet. To observe the
FIFO contrast, set it to `false`, run `docker compose up -d broker`, then rerun the probe;
its fairness assertions intentionally fail when fairness is disabled.

**Ingress velocity:** installs a temporary rule on a unique facet (40 jobs per 2 seconds by
default), submits about 200 jobs/s for 30 seconds, and checks admission/rejection counts.
The rule is removed on normal exit. Watch the ingress dashboard: rejections increase while
accepted tasks complete. The completion panel also includes other `ProcessTask` jobs; it is
not a per-facet measurement. Override `SIM_INGRESS_MAX`, `SIM_INGRESS_WINDOW_MS`,
`SIM_INGRESS_RATE`, `SIM_INGRESS_SECONDS`, or `SIM_INGRESS_LANES` using
`docker compose run -e NAME=value --rm workers ...`.

## Synthetic workload

AnvilMQ routes jobs by name. `src/queues.ts` defines the eight names used by the workers and
the broker's `ANVILMQ_METRICS_QUEUES` allowlist:

| Job name | Simulated behavior |
|---|---|
| `IngestEvent` | Creates two child tasks with inherited ancestry |
| `ProcessTask` | Event children create three subtasks each; direct tasks use `subtaskCount: 0` and do not fan out |
| `ProcessSubtask` | Creates one record-write child |
| `WriteRecord` | Simulates a downstream write |
| `Delay` | Bounded sleep |
| `Stress` | Bounded CPU work |
| `Throw` | Intentional failures to exercise retries, history, and search |
| `RecursiveProbe` | Self-recursion until the ancestry-depth circuit breaker rejects a child |

The default event chain is **1 event -> 2 tasks -> 6 subtasks -> 6 writes**, at depths 1
through 4. Payloads use synthetic tenant IDs, record IDs, and byte counts, not real data.
The shared `ProcessTask` handler keeps direct tasks lightweight without changing this chain.

The `prod` profile is a synthetic mixed demo, not a production trace: its relative weights
are 50 events, 35 direct tasks, 8 delays, 4 CPU jobs, 3 failures, and 1 recursive probe.
It targets 13 top-level submissions/s; fan-out adds internal jobs. The `soak` profile submits
only direct tasks with no fan-out.

The mixed profile also bursts two separate facets every 15 seconds: admission limits reject
surplus jobs on one facet, while dispatch limits admit jobs but slow claims on the other.
The recursive probe demonstrates a third, independent control. Set `SIM_LIMIT_DEMO=0` to
disable the periodic facet bursts; soak disables them by default.

### Configuration

Set environment variables before `docker compose up -d` to override Compose defaults.

| Variable | Default | Purpose |
|---|---|---|
| `SIM_PROFILE` | `prod` | Mixed demo or `soak` |
| `SIM_RATE` | `13` | Aggregate top-level jobs/s; `0` is unlimited |
| `SIM_WORKER_CONCURRENCY` | `2` | Workers per job name |
| `SIM_WORK_MS` | `15` | Jittered simulated downstream latency |
| `SIM_TASKS_PER_EVENT` | `2` | First fan-out multiplier |
| `SIM_SUBTASKS_PER_TASK` | `3` | Second fan-out multiplier |
| `SIM_MAX_JOBS` | `0` | Top-level submission cap; `0` is unlimited |
| `SIM_DURATION_SECONDS` | `0` | Producer time limit; `0` is unlimited |
| `SIM_LIMIT_DEMO` | Profile-dependent | Facet bursts on for `prod`, off for `soak` |
| `ANVILMQ_METRICS_MODE` | `all` | Auto-register names up to the broker cap; use `allowlist` for only the catalog |
| `ANVILMQ_FTS_ENABLED` | `1` | Full-text history search |
| `ANVILMQ_FAIRNESS_ENABLED` | `true` | Equal-priority tenant rotation |

The retention demo keeps completed history for 120 seconds and failed history for 300
seconds, with count limits disabled, a 15-second sweep interval, and a 1,000-row batch.
Override the `ANVILMQ_RETENTION_*` values in `compose.yaml` for longer history or larger
loads. Prune capacity must exceed completion throughput if history is to remain bounded.
This short history window also limits how far back HTTP drilldowns can retrieve jobs.

Per-name metrics are bounded by the broker's registration cap. Global counters still include
jobs beyond that cap. `anvilmq_job_duration_seconds` measures **enqueue to terminal**
latency, including queue wait, retries, and backoff; it is not processing-only time.

## Soak, logs, and cleanup

```powershell
.\soak.ps1 -MaxJobs 1000000 -Rate 2000 -Concurrency 8
docker compose logs -f workers producer
docker compose down
# Remove this example's volumes as well (deletes its job history and telemetry):
docker compose down -v
```

The soak script configures environment variables in the current PowerShell session.
Clear those overrides or open a fresh shell before returning to the mixed profile.

For host-side development, install dependencies in **both** `client` and `examples\sim`:
the example imports the repository client source, whose dependencies resolve from its own
directory. From the repository root, with Bun installed:

```powershell
Push-Location client
bun install --frozen-lockfile
Pop-Location
Push-Location examples\sim
bun install --frozen-lockfile
bun run typecheck
bun run semantics
Pop-Location
```

Host scripts default to `127.0.0.1:50071` and `http://127.0.0.1:9092`; override with
`ANVILMQ_ADDR` and `ANVILMQ_HTTP_URL` only when intentionally targeting another broker.
