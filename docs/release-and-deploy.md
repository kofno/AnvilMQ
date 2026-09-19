# Evaluation releases and Azure deployment

This release packages a **single-node** broker. It provides no Raft replication, authentication, TLS, or rolling-upgrade availability. Use a dedicated evaluation namespace and PVC. The chart defaults to FULL durability, one replica, internal services, and ingress restricted to labelled clients. Helm rejects replica counts above one because independent broker databases would split the queue.

## GitHub release workflow

The repository is [kofno/AnvilMQ](https://github.com/kofno/AnvilMQ). Commit and push the intended changes to main before publishing a new version tag. No GitHub release is created by the local packaging script.

The included workflows validate pushes to `main` and pull requests. Pushing a version tag such as `v0.1.0-rc.2` triggers the release workflow:

1. Run Rust formatting/tests/build and TypeScript checks/integration tests.
2. Lint/package the Helm chart and package the Bun client with its protobuf.
3. Publish `ghcr.io/<owner>/<repository>:0.1.0-rc.2` for Linux amd64, labelled with source/version/revision.
4. Create a GitHub prerelease with chart, client, observability bundle, protobuf, AKS values, image digest, source metadata, and SHA-256 checksums.

GitHub Actions needs permission to write packages and releases; the workflow requests those permissions using `GITHUB_TOKEN`. Organization policy can still restrict them. GHCR package visibility/access must be configured for your cluster. The workflow does not deploy Kubernetes resources. Do not move/reuse published version tags; use a new version for changes. Source Cargo/package versions describe the development base; the release tag sets image, chart, and packaged client versions.

After reviewing and committing the intended source, publish the tag explicitly:

```powershell
git tag v0.1.0-rc.2
git push origin v0.1.0-rc.2
```

## Local packaging

```powershell
./scripts/release.ps1 -Version 0.1.0-rc.2
```

Requires Docker, Helm, PowerShell 7, Git, and tar. Builds `anvilmq:0.1.0-rc.2` locally and writes packages/checksums to `dist/0.1.0-rc.2/`; it does not push images or tags. The manifest records the commit and whether the working tree was dirty. Existing output directories are rejected to avoid accidentally replacing release artifacts. `-ImageRepository` changes the image name; `-SkipImageBuild` only packages files and marks that fact in `release.json`.

The client archive is npm-compatible and includes the canonical protobuf. It exports TypeScript and is intended for Bun applications; a compiled Node.js distribution is not included. No npm registry publication is required.

## Deploy the published release

Download the assets from the GitHub release and verify `SHA256SUMS`. Replace the repository placeholder below. Confirm your current Kubernetes context is the intended Azure cluster. Keep this release in its own namespace, separate from production Valkey.

```powershell
kubectl config current-context
helm upgrade --install eval ./anvilmq-0.1.0-rc.2.tgz `
  --namespace anvilmq-eval --create-namespace `
  -f ./aks-evaluation.yaml `
  --set image.repository=ghcr.io/kofno/anvilmq `
  --set-string image.tag=0.1.0-rc.2 `
  --wait --timeout 5m
```

Prefer `--set-string image.digest=sha256:...` from the release's `image.txt`; digest takes precedence over tag. Image repository paths must be lowercase. For private GHCR packages, provision a `kubernetes.io/dockerconfigjson` pull secret in `anvilmq-eval` using your secret-management workflow, then pass `--set imagePullSecrets[0].name=ghcr-pull`. Do not put credentials in Helm values or commit them.

AKS values request a new 10Gi `managed-csi` PVC, matching the configured storage-class name for Valkey. Actual disk SKU, topology, caching, and VM limits must be recorded for comparison; matching the class name alone does not establish identical storage performance. Change node selectors/tolerations for your pool if needed.

Validation:

```powershell
kubectl -n anvilmq-eval get pods,pvc,svc
kubectl -n anvilmq-eval logs eval-anvilmq-0
# Expect database durability configured: FULL, wal, synchronous=2.
kubectl -n anvilmq-eval port-forward svc/eval-anvilmq 50061:50051 9091:9090
# In a second terminal:
Invoke-WebRequest http://127.0.0.1:9091/readyz
Invoke-WebRequest http://127.0.0.1:9091/metrics
```

The NetworkPolicy requires a policy-enforcing CNI. Same-namespace client pods need `anvilmq-client: "true"` in their pod-template labels. For callers in another namespace, replace `networkPolicy.allowedFrom` with an explicit namespaceSelector AND podSelector in the same peer. Allow monitoring clients explicitly as well. Both gRPC and HTTP ports use these rules. No public LoadBalancer or Ingress is created.

## Point application code at it

Install the client asset in a Bun application:

```powershell
bun add ./anvilmq-client-0.1.0-rc.2.tgz
```

```typescript
import { Queue, Worker } from "@anvilmq/client";

const address = process.env.ANVILMQ_ADDR ?? "127.0.0.1:50061";
// In-cluster address: eval-anvilmq.anvilmq-eval.svc.cluster.local:50051
const name = "comparison-run-001";
const queue = new Queue<{ sourceJobId: string; payload: unknown }>(name, { address });
const worker = new Worker(name, async (job, signal) => {
  signal.throwIfAborted();
  // Invoke your evaluation handler here. Direct writes to isolated test destinations.
  console.log(job.id, job.data);
}, { address, onCompleted: id => console.log("completion acknowledged", id) });

await queue.add({ sourceJobId: "captured-job-123", payload: {} });
// Keep the worker alive while testing. On shutdown:
// await worker.close(); queue.close();
```

This is not a BullMQ wire/API replacement. Adapt producer/worker calls and explicitly map retries, delays, and facets. Use captured payloads with side effects disabled or redirected; replaying production jobs through both engines can execute effects twice. At-least-once handlers still need their own idempotency.

For a useful comparison, replay the same payload mix and handler work with the same concurrency, resources, and storage assumptions. Measure submission-to-completion acknowledgment, queue delay, throughput, errors, retries, and backlog. Start at 13/s, then 26/s and 50/s. Compare against Valkey's actual AOF/everysec configuration and label the durability difference. Synthetic zero-work numbers and real-handler timings answer different questions.

## Upgrade, rollback, and retained data

Single-pod updates interrupt service; clients must tolerate reconnects and ambiguous replies. The daemon currently terminates on SIGTERM without a graceful RPC drain, and unfinished claims recover after lease expiry. No PodDisruptionBudget can make this single broker highly available.

Use a new image version/digest for upgrades. `helm rollback eval <revision> -n anvilmq-eval --wait` restores chart/image configuration only; it does not roll back database contents or migrations. Verify schema compatibility and arrange a storage-consistent backup before upgrades involving schema changes. PVC template settings cannot generally be changed in-place through a StatefulSet upgrade; plan storage changes separately.

`helm uninstall eval -n anvilmq-eval` removes the workload/services while StatefulSet-created PVCs remain. Keep the namespace if retaining its PVC: deleting the namespace deletes its claims, and the storage reclaim policy may then delete the underlying disk. Never mount Valkey's volume into AnvilMQ. Kubernetes PVC retention behavior is documented in [StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/).
