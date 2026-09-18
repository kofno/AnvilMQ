# Availability requirements and Raft evaluation

Raft is being considered for redundancy and availability within a regional Kubernetes cluster, not as a throughput optimization. Single-node performance measurements establish a baseline and headroom; they do not decide whether redundancy is needed.

## Intended requirement

Continue queue service through the loss or recycling of one broker pod or its Kubernetes node, with a bounded failover interruption and no loss of acknowledged jobs within the stated failure model. The acceptable interruption (RTO) still needs a concrete target. Worker side effects remain at least once; broker replication cannot make arbitrary external effects exactly once.

## Alternatives to evaluate

- **One broker + persistent volume + Kubernetes replacement:** recovers a process and can retain its data, but the queue is unavailable until detection, scheduling, volume availability, database recovery, and startup finish. Adequate only if that interruption is acceptable and the volume survives the relevant failure.
- **Three broker replicas with Raft:** independently persisted replicated state, majority commits, leader election, and client failover can retain service after one replica/node loss. Place replicas on distinct nodes; three pods on one node do not meet the requirement. Quorum loss intentionally stops progress rather than permitting conflicting claims.
- **Broker with an external HA database or existing replicated queue service:** may meet the availability requirement without implementing Raft inside AnvilMQ, but changes the embedded/local operating model and adds an external dependency.

A StatefulSet provides stable identity/storage management, not application-level replication. Storage replication alone also does not establish a safe active writer or coordinate queue claims.

## What a Raft implementation must include

- Replicate all mutations: enqueue, claim/lease renewal, completion/failure, retries/recovery, and quota/rule changes. No local-only writes that bypass consensus.
- Deterministic commands: leader-chosen IDs/timestamps and replicated claim decisions; followers must not independently choose work or expire leases using their own clocks.
- Durable Raft log/state, snapshots, replay, member replacement, and tests for divergence and rejoining replicas.
- Quorum-backed acknowledgment semantics and explicit read consistency.
- Client discovery/routing to the leader, bounded retries, and request deduplication for ambiguous outcomes.
- Node placement, disruption budgets, persistent volume behavior, rolling upgrades, and operational visibility.

## Acceptance tests before calling it HA

Kill follower, kill leader, recycle a node, isolate a minority partition, lose quorum, and rejoin a stale member. Measure recovery time while producers/workers stay active. Verify acknowledged jobs are retained, stale owners cannot mutate newer claims, quotas stay consistent, and minority replicas cannot accept conflicting writes. State the storage and power-failure assumptions explicitly.

References: [Raft protocol and paper](https://raft.github.io/), [Kubernetes StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/).
