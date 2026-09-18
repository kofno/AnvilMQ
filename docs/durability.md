# Durability contract

AnvilMQ acknowledges mutations after the SQLite transaction commits. Set `ANVILMQ_DURABILITY=NORMAL` (default, preserving existing behavior) or `FULL`. Values are case-insensitive; empty, misspelled, numeric, and other modes are rejected before opening the database. Restart to change modes. No schema migration or data reset is required.

```powershell
$env:ANVILMQ_DURABILITY = 'FULL'
cargo run
```

For the container harness, set the same variable before `docker compose -p anvilmq-harness -f harness/compose.yaml up -d --wait broker`. Compose recreates the broker when its configuration changes. Check the JSON startup event `database durability configured`: `durability`, `journal_mode`, and numeric `synchronous` are read back from the open connection. NORMAL is 1; FULL is 2. File-backed startup fails if WAL or the requested synchronous setting was not applied.

## What a successful response means

| Failure | WAL/NORMAL | WAL/FULL |
| --- | --- | --- |
| Broker process crash, storage retained | Committed mutations survive | Committed mutations survive |
| OS crash or power loss, storage retained | Recent acknowledged mutations may roll back | SQLite synchronizes each commit; durability depends on the storage stack honoring sync |
| Disk/volume loss | No replica to recover from | No replica to recover from |
| Broker/node unavailable | Service waits for recovery/replacement | Service waits for recovery/replacement |

SQLite documents FULL as durable in WAL mode and NORMAL as potentially losing committed transactions after system/power failure. NORMAL avoids syncing most individual commits; FULL adds a WAL sync per transaction. These guarantees assume a correctly functioning filesystem, VM/block-device stack, and hardware. See [SQLite synchronous documentation](https://www.sqlite.org/pragma.html#pragma_synchronous).

The contract covers enqueue, claims, lease renewals, completion receipts, retries, and rate-limit changes. FULL does not make external handler effects atomic with completion, provide replicas, or eliminate ambiguous RPC responses. A lost reply still requires the appropriate idempotency strategy. Completed-job receipts depend on retained history.

## Operating choice and validation limits

Use FULL when acknowledged writes must withstand OS/power failure on retained storage; choose NORMAL only when that loss window is acceptable. The default stays NORMAL for compatibility, so deployments requiring the stronger contract must configure FULL explicitly. Switching back to NORMAL weakens guarantees for subsequent commits. Apply the setting on every connection if a connection pool is introduced later.

Process SIGKILL tests validate retained-storage process recovery only. Container benchmarks measure performance and applied configuration, not power-loss durability. Neither confirms that a production storage stack honors sync. Validate storage behavior on the intended deployment independently. Raft is still required if we choose independently replicated brokers for node-loss availability; its persistent log and commit acknowledgment rules must meet the selected durability contract.

Run `./harness/durability.ps1` for matching NORMAL/FULL/FULL/NORMAL workloads. See the [measured comparison](../harness/DURABILITY.md) for methodology and results.
