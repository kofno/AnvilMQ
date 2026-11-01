use super::*;
use queue::v1::*;

async fn flow_service() -> MyQueueService {
    flow_service_with_caps(DEFAULT_MAX_FLOW_DEPTH, DEFAULT_MAX_FLOW_NODES).await
}

async fn flow_service_with_caps(max_flow_depth: u32, max_flow_nodes: u32) -> MyQueueService {
    flow_service_full(max_flow_depth, max_flow_nodes, DEFAULT_MAX_FLOW_BULK).await
}

async fn flow_service_full(
    max_flow_depth: u32,
    max_flow_nodes: u32,
    max_flow_bulk: u32,
) -> MyQueueService {
    MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(":memory:").await.unwrap()),
        max_execution_depth: 10,
        max_chain_size: 0,
        fairness_enabled: false,
        lease_duration_ms: leases::LEASE_DURATION_MS,
        max_flow_depth,
        max_flow_nodes,
        max_flow_bulk,
    }
}

fn leaf(name: &str) -> FlowJob {
    FlowJob {
        name: name.into(),
        ..Default::default()
    }
}

fn parent(name: &str, children: Vec<FlowJob>) -> FlowJob {
    FlowJob {
        name: name.into(),
        children,
        ..Default::default()
    }
}

async fn add_flow(service: &MyQueueService, flow: FlowJob) -> Result<AddFlowResponse, Status> {
    service
        .add_flow(Request::new(AddFlowRequest { flow: Some(flow) }))
        .await
        .map(|r| r.into_inner())
}

async fn add_flow_bulk(
    service: &MyQueueService,
    flows: Vec<FlowJob>,
) -> Result<AddFlowBulkResponse, Status> {
    service
        .add_flow_bulk(Request::new(AddFlowBulkRequest { flows }))
        .await
        .map(|r| r.into_inner())
}

async fn add_flow_merge(
    service: &MyQueueService,
    flow: FlowJob,
) -> Result<AddFlowMergeResponse, Status> {
    service
        .add_flow_merge(Request::new(AddFlowMergeRequest { flow: Some(flow) }))
        .await
        .map(|r| r.into_inner())
}

/// A keyed root/leaf (name == idempotency_key) for building merge attach targets.
fn keyed(name: &str, key: &str) -> FlowJob {
    FlowJob {
        name: name.into(),
        idempotency_key: key.into(),
        ..Default::default()
    }
}

/// Current value of a single-line `gauge`/counter from the rendered metrics text.
fn metric_value(service: &MyQueueService, metric: &str) -> i64 {
    let rendered = service.db_manager.metrics.render();
    for line in rendered.lines() {
        if let Some(rest) = line.strip_prefix(metric) {
            if let Some(value) = rest.strip_prefix(' ') {
                return value.trim().parse().unwrap();
            }
        }
    }
    panic!("metric {metric} not found");
}

fn node_id(resp: &AddFlowResponse, name: &str) -> String {
    resp.nodes
        .iter()
        .find(|n| n.name == name)
        .unwrap_or_else(|| panic!("node {name} not in response"))
        .id
        .clone()
}

fn node_id_merge(resp: &AddFlowMergeResponse, name: &str) -> String {
    resp.attached
        .iter()
        .find(|n| n.name == name)
        .unwrap_or_else(|| panic!("node {name} not in merge response"))
        .id
        .clone()
}

/// First `id` column returned by a query (panics if no row).
async fn first_id(service: &MyQueueService, sql: &'static str) -> String {
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        conn.blocking_lock()
            .query_row(sql, [], |r| r.get::<_, String>(0))
            .unwrap()
    })
    .await
    .unwrap()
}

/// (state, pending_children, parent_id, tree_id, tree_depth) for a live job, or None if absent.
async fn job_row(
    service: &MyQueueService,
    id: &str,
) -> Option<(String, i64, Option<String>, Option<String>, i64)> {
    let conn = service.db_manager.get_shared_connection();
    let id = id.to_string();
    tokio::task::spawn_blocking(move || {
        conn.blocking_lock()
            .query_row(
                "SELECT state, pending_children, parent_id, tree_id, tree_depth FROM jobs WHERE id = ?1",
                [&id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()
            .unwrap()
    })
    .await
    .unwrap()
}

/// Terminal history state for a parked job, or None if not yet terminal.
async fn history_state(service: &MyQueueService, id: &str) -> Option<String> {
    let conn = service.db_manager.get_shared_connection();
    let id = id.to_string();
    tokio::task::spawn_blocking(move || {
        conn.blocking_lock()
            .query_row("SELECT state FROM job_history WHERE id = ?1", [&id], |r| {
                r.get::<_, String>(0)
            })
            .optional()
            .unwrap()
    })
    .await
    .unwrap()
}

async fn count(service: &MyQueueService, sql: &'static str) -> i64 {
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        conn.blocking_lock()
            .query_row(sql, [], |r| r.get(0))
            .unwrap()
    })
    .await
    .unwrap()
}

async fn claim(service: &MyQueueService) -> GetNextJobResponse {
    service
        .get_next_job(Request::new(GetNextJobRequest {
            worker_id: "worker".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn complete_with_result(service: &MyQueueService, id: &str, result: Vec<u8>) {
    service
        .complete_job(Request::new(CompleteJobRequest {
            id: id.into(),
            worker_id: "worker".into(),
            attempt: 1,
            result,
        }))
        .await
        .unwrap();
}

/// Claim one job, complete it, and return its id (None when nothing is claimable).
async fn claim_and_complete(service: &MyQueueService) -> Option<String> {
    let job = claim(service).await;
    if !job.found {
        return None;
    }
    complete_with_result(service, &job.id, Vec::new()).await;
    Some(job.id)
}

// ---- 1. Atomic creation: a 3-level tree inserts every node with correct linkage ----

#[tokio::test]
async fn add_flow_creates_the_whole_tree_atomically() {
    let service = flow_service().await;
    let flow = parent(
        "root",
        vec![
            parent("childA", vec![leaf("gcA1")]),
            parent("childB", vec![leaf("gcB1")]),
        ],
    );
    let resp = add_flow(&service, flow).await.unwrap();

    assert_eq!(resp.nodes.len(), 5);
    // Root-first topological ordering.
    assert_eq!(resp.nodes[0].name, "root");
    let root = node_id(&resp, "root");
    assert_eq!(resp.root_id, root);

    // Root: blocked on 2 children, depth 0, no parent, tree_id = own id.
    let (state, pending, parent_id, tree_id, depth) = job_row(&service, &root).await.unwrap();
    assert_eq!(state, "WaitingChildren");
    assert_eq!(pending, 2);
    assert_eq!(parent_id, None);
    assert_eq!(tree_id.as_deref(), Some(root.as_str()));
    assert_eq!(depth, 0);

    // Internal child: blocked on 1 grandchild, depth 1, parent = root.
    let child_a = node_id(&resp, "childA");
    let (state, pending, parent_id, tree_id, depth) = job_row(&service, &child_a).await.unwrap();
    assert_eq!(state, "WaitingChildren");
    assert_eq!(pending, 1);
    assert_eq!(parent_id.as_deref(), Some(root.as_str()));
    assert_eq!(tree_id.as_deref(), Some(root.as_str()));
    assert_eq!(depth, 1);

    // Leaf grandchild: immediately Waiting, depth 2, parent = childA.
    let gc_a1 = node_id(&resp, "gcA1");
    let (state, pending, parent_id, tree_id, depth) = job_row(&service, &gc_a1).await.unwrap();
    assert_eq!(state, "Waiting");
    assert_eq!(pending, 0);
    assert_eq!(parent_id.as_deref(), Some(child_a.as_str()));
    assert_eq!(tree_id.as_deref(), Some(root.as_str()));
    assert_eq!(depth, 2);

    // Every node shares the root as tree_id.
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 5);
    assert_eq!(
        count(
            &service,
            "SELECT COUNT(*) FROM jobs WHERE tree_id IS NOT NULL AND execution_depth = tree_depth"
        )
        .await,
        5
    );
}

// ---- 2. Dependency gate: only leaves are claimable; parents are never returned ----

#[tokio::test]
async fn waiting_children_parents_are_never_claimed() {
    let service = flow_service().await;
    let resp = add_flow(
        &service,
        parent(
            "root",
            vec![parent("childA", vec![leaf("gcA1")]), leaf("childB")],
        ),
    )
    .await
    .unwrap();
    let root = node_id(&resp, "root");
    let child_a = node_id(&resp, "childA");

    // Only the two leaves (gcA1, childB) are initially claimable.
    let first = claim(&service).await;
    assert!(first.found);
    let second = claim(&service).await;
    assert!(second.found);
    let third = claim(&service).await;
    assert!(
        !third.found,
        "root/childA must stay blocked behind children"
    );

    // Neither the root nor the internal child was ever handed out.
    for id in [first.id, second.id] {
        assert_ne!(id, root);
        assert_ne!(id, child_a);
    }
}

// ---- 3. Release ordering: children-first, grandchild releases child before root ----

#[tokio::test]
async fn completing_children_releases_parents_bottom_up() {
    let service = flow_service().await;
    let resp = add_flow(
        &service,
        parent(
            "root",
            vec![
                parent("childA", vec![leaf("gcA1")]),
                parent("childB", vec![leaf("gcB1")]),
            ],
        ),
    )
    .await
    .unwrap();
    let root = node_id(&resp, "root");
    let child_a = node_id(&resp, "childA");
    let gc_a1 = node_id(&resp, "gcA1");

    // Completing the grandchild releases its direct parent, but not yet the root.
    // Drain until childA is claimable; find gcA1 specifically.
    assert_eq!(
        job_row(&service, &child_a).await.unwrap().0,
        "WaitingChildren"
    );

    // Process the whole tree and record completion order.
    let mut order = Vec::new();
    while let Some(id) = claim_and_complete(&service).await {
        order.push(id);
    }
    assert_eq!(order.len(), 5);

    // The grandchild precedes its child; every child precedes the root.
    let pos = |id: &str| order.iter().position(|x| x == id).unwrap();
    assert!(pos(&gc_a1) < pos(&child_a));
    assert!(pos(&child_a) < pos(&root));
    assert_eq!(*order.last().unwrap(), root, "root completes last");

    // All nodes parked Completed; jobs table drained.
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 0);
    assert_eq!(
        history_state(&service, &root).await.as_deref(),
        Some("Completed")
    );
}

// ---- 4. Exactly-once: pending_children decrements once; replay does not double-release ----

#[tokio::test]
async fn pending_children_decrements_exactly_once() {
    let service = flow_service().await;
    let resp = add_flow(&service, parent("root", vec![leaf("child")]))
        .await
        .unwrap();
    let root = node_id(&resp, "root");
    let child = node_id(&resp, "child");

    let claimed = claim(&service).await;
    assert_eq!(claimed.id, child);
    complete_with_result(&service, &child, Vec::new()).await;

    // Parent released exactly once.
    let (state, pending, _, _, _) = job_row(&service, &root).await.unwrap();
    assert_eq!(state, "Waiting");
    assert_eq!(pending, 0);

    // A replayed completion of the already-parked child is a no-op: pending stays 0, not -1.
    service
        .complete_job(Request::new(CompleteJobRequest {
            id: child.clone(),
            worker_id: "worker".into(),
            attempt: 1,
            result: Vec::new(),
        }))
        .await
        .unwrap();
    let (state, pending, _, _, _) = job_row(&service, &root).await.unwrap();
    assert_eq!(state, "Waiting");
    assert_eq!(pending, 0);
}

// ---- 5. failParentOnFailure ----

async fn fail_terminal(service: &MyQueueService, id: &str) -> bool {
    service
        .fail_job(Request::new(FailJobRequest {
            id: id.into(),
            worker_id: "worker".into(),
            attempt: 1,
            error_message: "boom".into(),
            non_retryable: true,
        }))
        .await
        .unwrap()
        .into_inner()
        .moved_to_failed_state
}

#[tokio::test]
async fn fail_parent_on_failure_cascades_to_root_while_siblings_continue() {
    let service = flow_service().await;
    // root(unflagged) -> [ childFail(flagged, leaf), childOk(leaf) ]
    let mut child_fail = leaf("childFail");
    child_fail.fail_parent_on_failure = true;
    let resp = add_flow(&service, parent("root", vec![child_fail, leaf("childOk")]))
        .await
        .unwrap();
    let root = node_id(&resp, "root");
    let child_fail = node_id(&resp, "childFail");
    let child_ok = node_id(&resp, "childOk");

    // Fail the flagged child terminally -> root is failed even with a pending sibling.
    claim(&service).await;
    claim(&service).await;
    assert!(fail_terminal(&service, &child_fail).await);
    assert_eq!(
        history_state(&service, &child_fail).await.as_deref(),
        Some("Failed")
    );
    assert_eq!(
        history_state(&service, &root).await.as_deref(),
        Some("Failed")
    );
    assert!(job_row(&service, &root).await.is_none(), "root parked");

    // The surviving sibling still completes cleanly.
    complete_with_result(&service, &child_ok, Vec::new()).await;
    assert_eq!(
        history_state(&service, &child_ok).await.as_deref(),
        Some("Completed")
    );
}

#[tokio::test]
async fn fail_parent_cascades_through_flagged_ancestors_only() {
    let service = flow_service().await;
    // root(unflagged) -> child(flagged) -> grandchild(flagged, leaf)
    let mut grandchild = leaf("grandchild");
    grandchild.fail_parent_on_failure = true;
    let mut child = parent("child", vec![grandchild]);
    child.fail_parent_on_failure = true;
    let resp = add_flow(&service, parent("root", vec![child]))
        .await
        .unwrap();
    let root = node_id(&resp, "root");
    let child = node_id(&resp, "child");
    let grandchild = node_id(&resp, "grandchild");

    claim(&service).await;
    assert!(fail_terminal(&service, &grandchild).await);

    // Failure propagated up both flagged ancestors to the root.
    for id in [&grandchild, &child, &root] {
        assert_eq!(history_state(&service, id).await.as_deref(), Some("Failed"));
    }
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 0);
}

#[tokio::test]
async fn unflagged_failure_leaves_parent_blocked() {
    let service = flow_service().await;
    // child has no fail_parent flag: its terminal failure leaves the parent blocked (BullMQ default).
    let resp = add_flow(&service, parent("root", vec![leaf("child")]))
        .await
        .unwrap();
    let root = node_id(&resp, "root");
    let child = node_id(&resp, "child");

    claim(&service).await;
    assert!(fail_terminal(&service, &child).await);
    assert_eq!(
        history_state(&service, &child).await.as_deref(),
        Some("Failed")
    );

    // Parent stays WaitingChildren with its pending dependency unresolved; never auto-released.
    let (state, pending, _, _, _) = job_row(&service, &root).await.unwrap();
    assert_eq!(state, "WaitingChildren");
    assert_eq!(pending, 1);
}

// ---- 6. GetChildrenValues: completed children's result bytes keyed by id ----

#[tokio::test]
async fn get_children_values_returns_completed_child_results() {
    let service = flow_service().await;
    let resp = add_flow(
        &service,
        parent("root", vec![leaf("childA"), leaf("childB")]),
    )
    .await
    .unwrap();
    let root = node_id(&resp, "root");
    let child_a = node_id(&resp, "childA");
    let child_b = node_id(&resp, "childB");

    // Claim both leaves, then complete each with a distinct result payload.
    claim(&service).await;
    claim(&service).await;
    complete_with_result(&service, &child_a, vec![1, 2, 3]).await;
    complete_with_result(&service, &child_b, vec![4, 5]).await;

    let values = service
        .get_children_values(Request::new(GetChildrenValuesRequest {
            parent_id: root.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .values;
    assert_eq!(values.len(), 2);
    assert_eq!(values.get(&child_a).unwrap(), &vec![1, 2, 3]);
    assert_eq!(values.get(&child_b).unwrap(), &vec![4, 5]);
}

// ---- 7. Caps: oversized trees rejected with invalid_argument and nothing inserted ----

#[tokio::test]
async fn flow_depth_cap_rejects_and_rolls_back() {
    let service = flow_service_with_caps(1, 1000).await;
    // Height 2 (root -> child -> grandchild) exceeds max_flow_depth = 1.
    let err = add_flow(
        &service,
        parent("root", vec![parent("child", vec![leaf("grandchild")])]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 0);
}

#[tokio::test]
async fn flow_nodes_cap_rejects_and_rolls_back() {
    let service = flow_service_with_caps(10, 2).await;
    // 4 nodes exceeds max_flow_nodes = 2.
    let err = add_flow(
        &service,
        parent("root", vec![leaf("a"), leaf("b"), leaf("c")]),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 0);
}

// ---- Idempotency: a replayed node key within AddFlow is rejected, tree rolled back ----

#[tokio::test]
async fn add_flow_rejects_replayed_idempotency_key() {
    let service = flow_service().await;
    service
        .add_job(Request::new(AddJobRequest {
            name: "childA".into(),
            idempotency_key: "k1".into(),
            ..Default::default()
        }))
        .await
        .unwrap();

    let mut child = leaf("childA");
    child.idempotency_key = "k1".into();
    let err = add_flow(&service, parent("root", vec![child]))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    // Whole tree rolled back: only the standalone pre-seeded job remains.
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 1);
}

// ==== A6b: AddFlowBulk ====

// ---- 1. Happy path: 3 independent trees inserted atomically, results index-aligned ----

#[tokio::test]
async fn add_flow_bulk_creates_all_trees() {
    let service = flow_service().await;
    let before = metric_value(&service, "anvilmq_flows_created_total");
    let resp = add_flow_bulk(
        &service,
        vec![
            parent("r1", vec![leaf("a1")]),
            leaf("r2"),
            parent("r3", vec![leaf("a3"), leaf("b3")]),
        ],
    )
    .await
    .unwrap();

    assert_eq!(resp.results.len(), 3);
    // Index-aligned with the request: each result carries its own tree's root first.
    assert_eq!(resp.results[0].nodes[0].name, "r1");
    assert_eq!(resp.results[0].nodes.len(), 2);
    assert_eq!(resp.results[1].nodes[0].name, "r2");
    assert_eq!(resp.results[1].root_id, resp.results[1].nodes[0].id);
    assert_eq!(resp.results[2].nodes[0].name, "r3");
    assert_eq!(resp.results[2].nodes.len(), 3);

    // Every node of every tree persisted (2 + 1 + 3), each tree bumped flows_created once.
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 6);
    assert_eq!(
        metric_value(&service, "anvilmq_flows_created_total"),
        before + 3
    );
}

// ---- 2. Atomic rollback: one over-cap tree aborts the whole batch, nothing written ----

#[tokio::test]
async fn add_flow_bulk_rolls_back_whole_batch_on_cap_violation() {
    let service = flow_service_with_caps(10, 2).await;
    // Tree #2 has 4 nodes, exceeding max_flow_nodes = 2.
    let err = add_flow_bulk(
        &service,
        vec![
            parent("r1", vec![leaf("a1")]),
            parent("r2", vec![leaf("a2"), leaf("b2"), leaf("c2")]),
        ],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    // Not even tree #1 was written.
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 0);
}

// ---- 3. Cross-tree idempotency-key collision rejects the batch ----

#[tokio::test]
async fn add_flow_bulk_rejects_cross_tree_key_collision() {
    let service = flow_service().await;
    let mut n1 = leaf("x");
    n1.idempotency_key = "k".into();
    let mut n2 = leaf("x");
    n2.idempotency_key = "k".into();
    let err = add_flow_bulk(
        &service,
        vec![parent("r1", vec![n1]), parent("r2", vec![n2])],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 0);
}

// ---- 4. Batch-count cap: too many trees, and an empty batch, both rejected ----

#[tokio::test]
async fn add_flow_bulk_enforces_batch_count_cap() {
    let service = flow_service_full(10, 1000, 2).await;
    // 3 trees exceeds ANVILMQ_MAX_FLOW_BULK = 2.
    let err = add_flow_bulk(&service, vec![leaf("a"), leaf("b"), leaf("c")])
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 0);

    // Empty batch is rejected too.
    let err = add_flow_bulk(&service, vec![]).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ---- 5. Reused pre-existing key inside a batch tree rolls the whole batch back ----

#[tokio::test]
async fn add_flow_bulk_rejects_preexisting_key_and_rolls_back() {
    let service = flow_service().await;
    service
        .add_job(Request::new(AddJobRequest {
            name: "pre".into(),
            idempotency_key: "pk".into(),
            ..Default::default()
        }))
        .await
        .unwrap();

    let mut child = leaf("pre");
    child.idempotency_key = "pk".into();
    let err = add_flow_bulk(
        &service,
        vec![parent("r1", vec![leaf("ok")]), parent("r2", vec![child])],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    // Whole batch rolled back: only the standalone pre-seeded job remains.
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 1);
}

// ==== A6b: AddFlowMerge ====

// ---- 6. Happy path: attach 2 children, re-gate a runnable target, gauge +1 ----

#[tokio::test]
async fn add_flow_merge_attaches_children_and_regates_target() {
    let service = flow_service().await;
    // Keyed single-node flow: the target is runnable (Waiting) and locatable via its receipt.
    let created = add_flow(&service, keyed("target", "jobkey")).await.unwrap();
    let target = created.root_id.clone();
    assert_eq!(job_row(&service, &target).await.unwrap().0, "Waiting");
    let before_gauge = metric_value(&service, "anvilmq_flow_waiting_children");
    let before_merges = metric_value(&service, "anvilmq_flow_merges_total");

    let mut merge_root = keyed("target", "jobkey");
    merge_root.children = vec![leaf("newA"), leaf("newB")];
    let resp = add_flow_merge(&service, merge_root).await.unwrap();

    assert_eq!(resp.parent_id, target);
    assert_eq!(resp.attached.len(), 2);
    for node in &resp.attached {
        assert_eq!(node.parent_id, target);
        assert_eq!(node.depth, 1);
        assert_eq!(node.state, "Waiting");
    }

    // Target re-gated Waiting -> WaitingChildren, pending_children bumped by 2.
    let (state, pending, _, _, _) = job_row(&service, &target).await.unwrap();
    assert_eq!(state, "WaitingChildren");
    assert_eq!(pending, 2);
    assert_eq!(
        metric_value(&service, "anvilmq_flow_waiting_children"),
        before_gauge + 1
    );
    assert_eq!(
        metric_value(&service, "anvilmq_flow_merges_total"),
        before_merges + 1
    );
    assert_eq!(metric_value(&service, "anvilmq_flow_merge_nodes_total"), 2);
    // Both new children share the target's tree lineage.
    let (_, _, _, tree_id, _) = job_row(&service, &node_id_merge(&resp, "newA"))
        .await
        .unwrap();
    assert_eq!(tree_id.as_deref(), Some(target.as_str()));
}

// ---- 7. Attach to an already-WaitingChildren target: state kept, gauge unchanged ----

#[tokio::test]
async fn add_flow_merge_onto_waiting_children_target_keeps_state() {
    let service = flow_service().await;
    let mut root = keyed("target", "jk");
    root.children = vec![leaf("origChild")];
    let created = add_flow(&service, root).await.unwrap();
    let target = created.root_id.clone();
    assert_eq!(
        job_row(&service, &target).await.unwrap().0,
        "WaitingChildren"
    );
    let before_gauge = metric_value(&service, "anvilmq_flow_waiting_children");

    let mut merge_root = keyed("target", "jk");
    merge_root.children = vec![leaf("newA")];
    add_flow_merge(&service, merge_root).await.unwrap();

    let (state, pending, _, _, _) = job_row(&service, &target).await.unwrap();
    assert_eq!(state, "WaitingChildren");
    assert_eq!(pending, 2); // 1 original + 1 attached
    assert_eq!(
        metric_value(&service, "anvilmq_flow_waiting_children"),
        before_gauge
    );
}

// ---- 8. Completing all children (old + attached) releases the parent exactly once ----

#[tokio::test]
async fn add_flow_merge_parent_released_after_all_children_complete() {
    let service = flow_service().await;
    let mut root = keyed("target", "jk");
    root.children = vec![leaf("origChild")];
    let created = add_flow(&service, root).await.unwrap();
    let target = created.root_id.clone();

    let mut merge_root = keyed("target", "jk");
    merge_root.children = vec![leaf("newA")];
    add_flow_merge(&service, merge_root).await.unwrap();

    // Two leaves are claimable; the parent stays blocked until BOTH complete.
    let first = claim_and_complete(&service).await.unwrap();
    assert_ne!(first, target);
    assert_eq!(
        job_row(&service, &target).await.unwrap().0,
        "WaitingChildren"
    );
    let second = claim_and_complete(&service).await.unwrap();
    assert_ne!(second, target);

    // Parent released exactly once to Waiting with pending 0.
    let (state, pending, _, _, _) = job_row(&service, &target).await.unwrap();
    assert_eq!(state, "Waiting");
    assert_eq!(pending, 0);

    // The parent then runs and completes last; the tree fully drains.
    let third = claim_and_complete(&service).await.unwrap();
    assert_eq!(third, target);
    assert!(claim_and_complete(&service).await.is_none());
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 0);
    assert_eq!(
        history_state(&service, &target).await.as_deref(),
        Some("Completed")
    );
}

// ---- 9. Locate/validation failures: missing key, empty key, empty children ----

#[tokio::test]
async fn add_flow_merge_locate_and_validation_errors() {
    let service = flow_service().await;

    // No receipt matches -> not_found.
    let mut missing = keyed("nope", "missingkey");
    missing.children = vec![leaf("x")];
    let err = add_flow_merge(&service, missing).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    // Empty root key -> invalid_argument.
    let empty_key = parent("t", vec![leaf("x")]);
    let err = add_flow_merge(&service, empty_key).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // Empty root children -> invalid_argument.
    let err = add_flow_merge(&service, keyed("t", "k")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ---- 10. Attach-target state guards: Active and terminal both rejected ----

#[tokio::test]
async fn add_flow_merge_rejects_active_and_terminal_targets() {
    let service = flow_service().await;

    // Active target: claim the keyed job so a worker holds it.
    add_flow(&service, keyed("active", "ak")).await.unwrap();
    let claimed = claim(&service).await;
    assert!(claimed.found);
    let mut merge_active = keyed("active", "ak");
    merge_active.children = vec![leaf("x")];
    let err = add_flow_merge(&service, merge_active).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // Terminal target: a completed job drained to history (its receipt survives).
    add_flow(&service, keyed("done", "dk")).await.unwrap();
    let job = claim(&service).await;
    complete_with_result(&service, &job.id, Vec::new()).await;
    let mut merge_done = keyed("done", "dk");
    merge_done.children = vec![leaf("y")];
    let err = add_flow_merge(&service, merge_done).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

// ---- 11. Scope guard: a non-root new node referencing an existing job is rejected ----

#[tokio::test]
async fn add_flow_merge_rejects_nonroot_existing_key() {
    let service = flow_service().await;
    add_flow(&service, keyed("target", "tk")).await.unwrap();
    // Pre-seed a standalone job whose key a new merge node will illegally reference.
    service
        .add_job(Request::new(AddJobRequest {
            name: "childX".into(),
            idempotency_key: "ck".into(),
            ..Default::default()
        }))
        .await
        .unwrap();

    let mut child = leaf("childX");
    child.idempotency_key = "ck".into();
    let mut merge_root = keyed("target", "tk");
    merge_root.children = vec![child];
    let err = add_flow_merge(&service, merge_root).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // Rollback: the target keeps its original runnable state and pending count.
    let target = count(&service, "SELECT COUNT(*) FROM jobs WHERE name = 'target'").await;
    assert_eq!(target, 1);
    let (state, pending, _, _, _) = job_row(
        &service,
        &first_id(&service, "SELECT id FROM jobs WHERE name = 'target'").await,
    )
    .await
    .unwrap();
    assert_eq!(state, "Waiting");
    assert_eq!(pending, 0);
}

// ---- 12. Depth and size caps on the merged tree ----

#[tokio::test]
async fn add_flow_merge_enforces_depth_and_size_caps() {
    // Depth: target at depth 0, attaching a height-3 subtree exceeds max_flow_depth = 2.
    let service = flow_service_with_caps(2, 1000).await;
    add_flow(&service, keyed("t", "k")).await.unwrap();
    let mut merge_root = keyed("t", "k");
    merge_root.children = vec![parent("c1", vec![parent("c2", vec![leaf("c3")])])];
    let err = add_flow_merge(&service, merge_root).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    // Nothing attached; target stays runnable with no pending children.
    let (state, pending, _, _, _) = job_row(
        &service,
        &first_id(&service, "SELECT id FROM jobs WHERE name = 't'").await,
    )
    .await
    .unwrap();
    assert_eq!(state, "Waiting");
    assert_eq!(pending, 0);

    // Size: a 1-node tree plus 2 new nodes exceeds max_flow_nodes = 2.
    let service = flow_service_with_caps(10, 2).await;
    add_flow(&service, keyed("t", "k")).await.unwrap();
    let mut merge_root = keyed("t", "k");
    merge_root.children = vec![leaf("a"), leaf("b")];
    let err = add_flow_merge(&service, merge_root).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 1);
}

// ---- 13. Within-merge-request duplicate (name, key) among new nodes ----

#[tokio::test]
async fn add_flow_merge_rejects_within_request_duplicate_key() {
    let service = flow_service().await;
    add_flow(&service, keyed("t", "k")).await.unwrap();
    let mut c1 = leaf("dup");
    c1.idempotency_key = "d".into();
    let mut c2 = leaf("dup");
    c2.idempotency_key = "d".into();
    let mut merge_root = keyed("t", "k");
    merge_root.children = vec![c1, c2];
    let err = add_flow_merge(&service, merge_root).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    // Nothing attached.
    assert_eq!(count(&service, "SELECT COUNT(*) FROM jobs").await, 1);
}
