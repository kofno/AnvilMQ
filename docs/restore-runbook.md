# Restore from snapshot runbook

AnvilMQ keeps all durable state in a single embedded SQLite database and produces consistent, self-contained snapshots of it on a schedule (see the [Backups](../README.md#backups) section and [`src/backup.rs`](../src/backup.rs)). This runbook is the operational counterpart: how to bring a broker back up from one of those snapshots after the live database is lost or corrupted.

Read the [durability contract](durability.md) first — it defines what "acknowledged" means and what a snapshot can and cannot recover.

## When to use this / failure model

Use this procedure when the live database at `ANVILMQ_DB_PATH` (`/data/anvil.db` in the chart) can no longer be opened cleanly, or its volume is gone. Two cases:

| Situation | PVC / volume | What you restore into |
| --- | --- | --- |
| Database corrupt, volume intact | Survives | The existing volume, overwriting `anvil.db` |
| Volume lost (node/disk failure, PVC deleted) | Gone | A freshly provisioned volume |

Recovery restores the **last completed snapshot**, so you lose at most one snapshot interval (`ANVILMQ_BACKUP_INTERVAL_MS`, default `120000` = 2 min) of accepted work — everything committed after the most recent snapshot. This is the recovery-point objective (RPO): **≤ one interval**.

Because AnvilMQ delivery is **at-least-once**, this loss window has two consequences:

- Workers may **reprocess** jobs that were in flight around the snapshot; handlers must stay idempotent, exactly as in normal operation.
- Producers that **cannot lose submissions** must keep an **outbox** and resubmit anything enqueued during the lost window. The broker cannot reconstruct work it never snapshotted.

## Critical safety rule: single writer

The broker is the **sole custodian** of accepted work and the **only** writer of its database — the chart runs `replicaCount: 1` for this reason. Two processes writing the same SQLite file corrupt it.

> **Never restore while a broker process is running against that database.** Always stop or scale the broker down to zero first, restore, then bring exactly one broker back up.

## Pick a snapshot

Snapshots are named `snapshot-<zero-padded-unix-ms>.db`. The timestamp is fixed-width, so the names **sort chronologically** — the lexicographically **last** name is the newest. Prefer the newest unless you are deliberately rolling back to an earlier point.

- **Local source:** `/data/backups/` on the broker's PVC. Local retention keeps only the newest `ANVILMQ_BACKUP_RETAIN` (default `3`).
- **Remote source:** the Blob container/prefix the [upload sidecar](../README.md#shipping-snapshots-to-azure-blob-storage-upload-sidecar) ships to: `https://<account>.blob.core.windows.net/<container>/<prefix>/snapshot-*.db`. Remote retention is an out-of-band Blob lifecycle rule, so older points may exist there than locally.

## Kubernetes procedure

Names below follow the chart: StatefulSet `<release>-anvilmq`, pod `<release>-anvilmq-0`, PVC `data-<release>-anvilmq-0`, data volume mounted at `/data`, `runAsUser`/`fsGroup` `10001`.

### 1. Stop the broker

```bash
kubectl scale statefulset <release>-anvilmq --replicas=0
kubectl wait --for=delete pod/<release>-anvilmq-0 --timeout=120s
```

Wait for the pod to be **gone**. With the broker pod deleted you cannot `kubectl exec` into it, so the restore runs from a short-lived maintenance pod that mounts the same PVC.

### 2. Start a maintenance pod on the PVC

Save this as `maint.yaml` and apply it in the release namespace. It mounts the existing claim `data-<release>-anvilmq-0` at `/data` as the same non-root user the broker runs as (`runAsUser: 10001`), so a copied-in file lands broker-owned with no `chown` needed. `mcr.microsoft.com/azure-cli` bundles a shell plus `az`; swap in any image that has a shell. Note the pod is `runAsNonRoot`/`runAsUser: 10001`, so it **cannot `apk add`/`apt-get install`** anything at runtime — if you need an in-pod `sqlite3` or in-pod Blob auth, see the notes in [step 3](#3-remote-only-download-the-snapshot) and [step 5](#5-validate-before-starting-the-broker).

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: anvil-restore
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 10001
    runAsGroup: 10001
    fsGroup: 10001
  containers:
    - name: maint
      image: mcr.microsoft.com/azure-cli
      command: ["sleep", "3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: data-<release>-anvilmq-0
```

```bash
kubectl apply -f maint.yaml
kubectl wait --for=condition=Ready pod/anvil-restore --timeout=120s
kubectl exec -it anvil-restore -- sh
```

If the PVC was **lost**, see [PVC lost](#pvc-lost) below before this step.

### 3. (Remote only) download the snapshot

Skip if you are restoring from `/data/backups/` already on the PVC. Reading from Blob needs an identity/role with **read** on the container (e.g. Storage Blob Data Reader; the broker's own upload identity already has Blob Data Contributor, which includes read).

**Primary path (break-glass, no in-pod auth).** During an incident the simplest approach is to download the snapshot from a workstation or bastion that is already signed in to Azure, then copy it into the maintenance pod. This keeps all credentials off the pod:

```bash
# On a workstation already `az login`'d with an identity that can read the container:
az storage blob download \
  --account-name <account> --container-name <container> \
  --name <prefix>/snapshot-<ts>.db --file ./snapshot-<ts>.db --auth-mode login

kubectl cp ./snapshot-<ts>.db anvil-restore:/data/backups/snapshot-<ts>.db
```

**Alternative: download inside the maintenance pod.** If you must pull from Blob in-cluster, the pod needs the broker's federated identity. Add the workload-identity label and ServiceAccount to `maint.yaml` (under `metadata.labels` and `spec` respectively):

```yaml
metadata:
  name: anvil-restore
  labels:
    azure.workload.identity/use: "true"
spec:
  serviceAccountName: <release>-anvilmq   # reuses the broker's federated identity
```

The Azure CLI does **not** auto-consume workload identity the way the SDK does, so you must run an explicit federated `az login` first (the same preamble the upload sidecar uses) before any `az storage` call:

```sh
az login --service-principal \
  -u "$AZURE_CLIENT_ID" -t "$AZURE_TENANT_ID" \
  --federated-token "$(cat "$AZURE_FEDERATED_TOKEN_FILE")" \
  --allow-no-subscriptions

# newest single blob, or mirror the whole prefix:
az storage blob download \
  --account-name <account> --container-name <container> \
  --name <prefix>/snapshot-<ts>.db --file /data/backups/snapshot-<ts>.db --auth-mode login
az storage blob download-batch \
  --account-name <account> --source <container> \
  --pattern '<prefix>/snapshot-*.db' --destination /data/backups --auth-mode login
```

### 4. Restore the file

Inside the maintenance pod. Pick the newest snapshot, copy it over `anvil.db`, and **delete the stale WAL sidecars** so SQLite cannot replay an old write-ahead log onto the restored file. The snapshot is a clean, non-WAL database; the broker recreates its own WAL when it opens the file.

```sh
cd /data
snap=$(ls -1 backups/snapshot-*.db | tail -n 1)
echo "restoring from $snap"

cp "$snap" anvil.db
rm -f anvil.db-wal anvil.db-shm      # drop any stale WAL/SHM; never replay an old WAL

chown 10001:10001 anvil.db
chmod 600 anvil.db
```

### 5. Validate before starting the broker

**Broker startup is the ultimate validation** — a clean start (step 6) proves the file opens, migrates, and recovers. Treat that as the primary check; the optional pre-start `PRAGMA integrity_check` below just catches a bad file sooner.

Running the integrity check in-pod needs a `sqlite3` binary, and the maintenance pod above is `runAsNonRoot`/`runAsUser: 10001`, so it **cannot install packages**. Two options: use a maintenance image that already bundles `sqlite3`, or, as a transient break-glass choice, start the maintenance pod as root (`runAsUser: 0`, drop `runAsNonRoot`) solely for this check. Either way:

```sh
sqlite3 anvil.db 'PRAGMA integrity_check;'          # expect: ok
sqlite3 anvil.db 'SELECT count(*) FROM jobs;'        # live jobs restored
sqlite3 anvil.db 'SELECT count(*) FROM job_history;' # completed-job receipts restored
```

A clean `integrity_check` and sane counts mean the file is good. If you skipped it, rely on broker startup instead. Then delete the maintenance pod so nothing else holds the volume:

```sh
exit
```
```bash
kubectl delete pod anvil-restore
```

### 6. Bring the broker back

```bash
kubectl scale statefulset <release>-anvilmq --replicas=1
kubectl logs -f statefulset/<release>-anvilmq
```

On startup the broker opens the restored database (recreating the WAL), runs migrations, and performs **lease recovery**: any previously-`Active` job whose lease has lapsed is requeued to `Waiting`/`Delayed` so a new worker can pick it up. Confirm via the startup logs (`AnvilMQ daemon listening`, `job transition ... reason="lease_expired"`) and `/metrics`.

### PVC lost

If the volume itself is gone, provision a fresh one before restoring:

- Delete the old PVC and pod so the StatefulSet's `volumeClaimTemplate` recreates `data-<release>-anvilmq-0` on the next scale-up, **or** pre-create the claim with the same name and storage class.
- Point the maintenance pod at the new (empty) claim, then restore into it from Blob following steps 2–5 (the `/data/backups/` copies are gone with the old volume, so the remote source is your only snapshot).

## Local / non-Kubernetes procedure

For local development and the [`harness/`](../harness) compose flow, the same three rules apply: stop the single writer, copy the snapshot over the DB path, delete the WAL sidecars, restart.

```powershell
# 1. Stop the broker (Ctrl-C the `cargo run` process, or stop the compose service).

# 2. Restore over the configured DB path.
$db = $env:ANVILMQ_DB_PATH        # e.g. .\anvil.db
$snap = Get-ChildItem .\backups\snapshot-*.db | Sort-Object Name | Select-Object -Last 1
Copy-Item $snap.FullName $db -Force

# 3. Delete the stale WAL sidecars so no old WAL is replayed.
Remove-Item "$db-wal","$db-shm" -Force -ErrorAction SilentlyContinue

# 4. (optional) sanity-check, then restart.
sqlite3 $db 'PRAGMA integrity_check;'
cargo run
```

For the container harness the database lives on the `broker-data` volume at `/data/anvil.db`. Stop the broker service, restore into that volume (via a throwaway container that mounts it, mirroring the maintenance-pod steps above), then bring the broker back with `docker compose -p anvilmq-harness -f harness/compose.yaml up -d --wait broker`.

## Post-restore verification & expectations

A restore is successful when:

- `PRAGMA integrity_check` returns `ok` on the restored file.
- The broker reaches the `AnvilMQ daemon listening` startup log with no errors, and `journal_mode` is back to `wal`.
- `jobs` and `job_history` counts reflect the restored state; previously in-flight (`Active`) work is requeued by lease recovery.

Restated expectations:

- **RPO ≤ one interval.** At most `ANVILMQ_BACKUP_INTERVAL_MS` of accepted work is lost — everything committed after the last snapshot. At-least-once delivery means workers may reprocess, and producers that cannot lose submissions must resubmit from an outbox.
- **RTO** = detection + (volume provisioning, if lost) + copy + broker startup recovery. It is not instant; size alerting and automation accordingly.
- **Remote retention is out of band.** Nothing in this procedure deletes remote blobs; prune them with an Azure Blob lifecycle-management rule scoped to the container/prefix.
