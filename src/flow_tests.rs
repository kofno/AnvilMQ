use super::*;
use queue::v1::*;

async fn flow_service() -> MyQueueService {
    flow_service_with_caps(DEFAULT_MAX_FLOW_DEPTH, DEFAULT_MAX_FLOW_NODES).await
}

async fn flow_service_with_caps(max_flow_depth: u32, max_flow_nodes: u32) -> MyQueueService {
    MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(":memory:").await.unwrap()),
        max_execution_depth: 10,
        max_chain_size: 0,
        fairness_enabled: false,
        lease_duration_ms: leases::LEASE_DURATION_MS,
        max_flow_depth,
        max_flow_nodes,
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

fn node_id(resp: &AddFlowResponse, name: &str) -> String {
    resp.nodes
        .iter()
        .find(|n| n.name == name)
        .unwrap_or_else(|| panic!("node {name} not in response"))
        .id
        .clone()
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
