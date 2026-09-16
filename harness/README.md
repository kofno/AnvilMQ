# Container reliability harness

Requires Docker with Linux containers, Compose v2+, and PowerShell (7 recommended). Run from the repository root:

```powershell
./harness/smoke.ps1
```

The script builds a release Rust broker and a separate Bun runner, then:

1. Starts the broker and waits for readiness.
2. Enqueues/processes successful, delayed, and retrying jobs. Checks IDs, retry attempt numbers, and committed completion counts.
3. Enqueues ten acknowledged jobs without a worker and saves their IDs to `artifacts/pending.json`.
4. Sends SIGKILL to **only this Compose project's broker**, then starts it with the same named volume.
5. Processes the exact saved IDs and verifies first-attempt delivery and committed completion counts.

This tests process-crash persistence of acknowledged waiting jobs. It does **not** simulate power loss, crashes during arbitrary transactions, or active-lease recovery. The existing client integration test covers killed-worker recovery. This smoke runner is not a throughput benchmark; reported elapsed times include polling and deliberate delays.

## Isolation and results

Default project: `anvilmq-harness`. Use `./harness/smoke.ps1 -Project another-name` for a separate volume/container namespace. Host ports still need separate configuration for simultaneous projects. Do not run concurrent harness scenarios against the same broker: completion gauge checks assume a dedicated instance. Queue names are unique per run, so sequential smoke runs are supported.

- gRPC: `127.0.0.1:50061` (override `ANVILMQ_HARNESS_GRPC_PORT`).
- Metrics/health: `http://127.0.0.1:9091` (override `ANVILMQ_HARNESS_HTTP_PORT`).
- Database: Compose named volume `broker-data`, mounted at `/data`.
- Reports: `harness/artifacts/smoke.json`, `seed.json`, `verify.json` (overwritten per scenario).
- Seed manifest: `harness/artifacts/pending.json`. Do not run `verify` twice against the same manifest; the jobs have already completed.

The script leaves the broker running and preserves data on success or failure for inspection. It never mounts the Docker socket into a container. The broker runs as UID 10001; the runner writes only to the reports bind mount. This is a local test deployment, with plaintext endpoints bound to host loopback.

```powershell
docker compose -p anvilmq-harness -f harness/compose.yaml logs broker
docker compose -p anvilmq-harness -f harness/compose.yaml ps
Invoke-WebRequest http://127.0.0.1:9091/metrics
# Stop/remove containers and network, preserving the database volume:
docker compose -p anvilmq-harness -f harness/compose.yaml down
# Explicitly discard this harness's database too (destructive):
docker compose -p anvilmq-harness -f harness/compose.yaml down -v
```

## Manual scenarios (any shell)

```sh
docker compose -p anvilmq-harness -f harness/compose.yaml build broker runner
docker compose -p anvilmq-harness -f harness/compose.yaml up -d --wait broker
docker compose -p anvilmq-harness -f harness/compose.yaml run --rm runner smoke
docker compose -p anvilmq-harness -f harness/compose.yaml run --rm runner seed
docker compose -p anvilmq-harness -f harness/compose.yaml kill -s SIGKILL broker
docker compose -p anvilmq-harness -f harness/compose.yaml up -d --wait broker
docker compose -p anvilmq-harness -f harness/compose.yaml run --rm runner verify
```

The image build uses `cargo --locked` and `bun --frozen-lockfile`. Rust and Bun image versions are pinned by tag, not digest; Debian packages can change. Record image IDs and host/Docker resource limits when collecting future performance baselines. First builds need network access and may take several minutes.

Next increments: configurable load generation and percentile reports; crashes during active processing; database/storage faults; and a documented NORMAL/FULL durability comparison.

## Configurable load and latency reports

```powershell
./harness/load.ps1 -Producers 4 -Workers 4 -DurationSeconds 30 -WarmupSeconds 3 -PayloadBytes 1024 -WorkMs 0 -Rate 0
# Capped load with simulated 5ms async handler work:
./harness/load.ps1 -Producers 4 -Workers 8 -DurationSeconds 30 -Rate 200 -WorkMs 5
```

`Rate=0` runs closed-loop producers without a rate ceiling. Each producer permits one in-flight enqueue. `Rate` is an aggregate pacing ceiling shared by all producers, not a promise of open-loop arrival pressure. Worker polling is 10ms in this scenario; handlers acknowledge successful work, with one attempt per job. Warmup uses separate jobs and fully drains before measurement. Client connections are new for the measured phase; connection startup is included. Run one load scenario per broker at a time.

Configuration (also usable via `docker compose run --rm -e NAME=value runner load`):

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| LOAD_PRODUCERS | 4 | Concurrent producers |
| LOAD_WORKERS | 4 | Concurrent single-job workers |
| LOAD_DURATION_SECONDS | 15 | Producer measurement window |
| LOAD_WARMUP_SECONDS | 3 | Unreported warmup window; zero disables |
| LOAD_PAYLOAD_BYTES | 1024 | String padding bytes, excluding JSON metadata |
| LOAD_WORK_MS | 0 | Simulated asynchronous handler delay |
| LOAD_RATE | 0 | Aggregate enqueue rate ceiling; zero unlimited |
| LOAD_POLL_MS | 10 | Idle worker poll interval |
| LOAD_DRAIN_SECONDS | 60 | Time allowed for acknowledged work to finish after producers stop |

Each run writes timestamped JSON and Markdown, `latest.json`/`latest.md`, and Prometheus snapshots before/after measurement into `harness/artifacts`. Warmup/setup errors write a timestamped error JSON. A scenario fails on producer/worker errors, duplicates, incomplete drainage, or mismatched acknowledged/completed counts. A timeout may leave jobs in the persisted database; reports do not silently discard that outcome. Counter snapshots can include activity from unrelated clients, so use the dedicated broker.

Latency distributions use a monotonic clock within the runner process:

- **enqueueRpc:** submission to successful AddJob response.
- **submissionToHandler:** submission to handler entry; includes enqueue, networking, and queue wait.
- **submissionToCompletionAck:** submission to successful CompleteJob response; includes handler time.

Reports include count, mean, p50/p95/p99, max, producer/worker errors, duplicate deliveries, acknowledged/completed counts, and drain duration. Percentiles use nearest-rank samples; beyond 200,000 observations per distribution a uniform reservoir is used. Enqueue latency includes only successful calls; errors remain explicit. Completion throughput is reported both during the producer window and over the full run including drain—do not confuse enqueue throughput with sustainable processing throughput. Failed enqueue RPCs can have ambiguous server outcomes; such a run is marked failed.

This measures the combined client/network/broker workload, not isolated storage latency. The runner shares Docker Desktop resources with the broker, info-level logging stays enabled, SQLite uses the current NORMAL durability mode, and history accumulates in the reused volume. Container-visible CPU/RAM and Bun version are recorded; they do not necessarily equal Docker cgroup limits. These short local runs are exploratory, not capacity guarantees or production SLO evidence.

Initial measured results are recorded in [BASELINE.md](BASELINE.md): an overload run and a controlled-rate run. They are exploratory observations, not maximum-capacity claims.
