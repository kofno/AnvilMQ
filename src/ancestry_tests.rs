use super::*;

async fn service_with(path: &str, max_chain_size: u64) -> MyQueueService {
    MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(path).await.unwrap()),
        max_execution_depth: 10,
        max_chain_size,
        fairness_enabled: false,
        lease_duration_ms: leases::LEASE_DURATION_MS,
    }
}

async fn add(service: &MyQueueService, req: AddJobRequest) -> Result<AddJobResponse, Status> {
    service
        .add_job(Request::new(req))
        .await
        .map(Response::into_inner)
}

fn job(name: &str) -> AddJobRequest {
    AddJobRequest {
        name: name.into(),
        payload: b"x".to_vec(),
        ..Default::default()
    }
}

fn meta(parent_id: &str, trace_id: &str, depth: u32) -> Option<queue::v1::JobMetadata> {
    Some(queue::v1::JobMetadata {
        parent_id: parent_id.into(),
        trace_id: trace_id.into(),
        execution_depth: depth,
    })
}

async fn read_trace(service: &MyQueueService, id: &str) -> (String, i64) {
    let conn = service.db_manager.get_shared_connection();
    let id = id.to_string();
    tokio::task::spawn_blocking(move || {
        let conn = conn.blocking_lock();
        conn.query_row(
            "SELECT trace_id, execution_depth FROM jobs WHERE id = ?1",
            [&id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
        )
        .unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn valid_parent_with_correct_depth_succeeds_and_inherits_trace() {
    let service = service_with(":memory:", 0).await;
    let mut root = job("emails");
    root.metadata = meta("", "", 0);
    let root = add(&service, root).await.unwrap();
    let (root_trace, root_depth) = read_trace(&service, &root.id).await;
    assert_eq!(root_depth, 0);
    assert!(!root_trace.is_empty());

    // A spoofed/different trace_id is overridden with the parent's lineage id.
    let mut child = job("emails");
    child.metadata = meta(&root.id, "spoofed-trace", 1);
    let child = add(&service, child).await.unwrap();
    let (child_trace, child_depth) = read_trace(&service, &child.id).await;
    assert_eq!(child_depth, 1);
    assert_eq!(child_trace, root_trace);
    assert!(service
        .db_manager
        .metrics
        .render()
        .contains("anvilmq_ancestry_rejections_total 0\n"));
}

#[tokio::test]
async fn inconsistent_depth_is_rejected() {
    let service = service_with(":memory:", 0).await;
    let mut root = job("emails");
    root.metadata = meta("", "", 0);
    let root = add(&service, root).await.unwrap();
    // 0 now derives (parent_depth + 1); only wrong NON-ZERO depths are rejected.
    for bad_depth in [2u32, 7] {
        let mut child = job("emails");
        child.metadata = meta(&root.id, "", bad_depth);
        let err = add(&service, child).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("execution_depth"));
    }
    assert!(service
        .db_manager
        .metrics
        .render()
        .contains("anvilmq_ancestry_rejections_total 2\n"));
}

#[tokio::test]
async fn missing_parent_is_rejected() {
    let service = service_with(":memory:", 0).await;
    let mut child = job("emails");
    child.metadata = meta("does-not-exist", "", 1);
    let err = add(&service, child).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("parent"));
    assert!(service
        .db_manager
        .metrics
        .render()
        .contains("anvilmq_ancestry_rejections_total 1\n"));
}

#[tokio::test]
async fn parentless_enqueue_is_unchanged() {
    let service = service_with(":memory:", 0).await;
    let resp = add(&service, job("emails")).await.unwrap();
    let (trace, depth) = read_trace(&service, &resp.id).await;
    assert_eq!(depth, 0);
    assert!(!trace.is_empty());
    assert!(service
        .db_manager
        .metrics
        .render()
        .contains("anvilmq_ancestry_rejections_total 0\n"));
}

#[tokio::test]
async fn parent_resolved_from_history_validates() {
    let service = service_with(":memory:", 0).await;
    let mut root = job("emails");
    root.metadata = meta("", "hist-trace", 0);
    let root = add(&service, root).await.unwrap();
    let claim = service
        .get_next_job(Request::new(GetNextJobRequest {
            worker_id: "w".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    service
        .complete_job(Request::new(CompleteJobRequest {
            id: claim.id,
            worker_id: "w".into(),
            attempt: 1,
        }))
        .await
        .unwrap();
    // Parent now lives only in job_history; the UNION lookup still resolves it.
    let mut child = job("emails");
    child.metadata = meta(&root.id, "", 1);
    let child = add(&service, child).await.unwrap();
    let (child_trace, child_depth) = read_trace(&service, &child.id).await;
    assert_eq!(child_depth, 1);
    assert_eq!(child_trace, "hist-trace");
}

#[tokio::test]
async fn chain_quarantine_bounds_a_lineage() {
    let service = service_with(":memory:", 3).await;
    let mut root = job("fanout");
    root.metadata = meta("", "", 0);
    let root = add(&service, root).await.unwrap(); // lineage count = 1

    let mut c1 = job("fanout");
    c1.metadata = meta(&root.id, "", 1);
    let c1 = add(&service, c1).await.unwrap(); // 2

    let mut c2 = job("fanout");
    c2.metadata = meta(&c1.id, "", 2);
    add(&service, c2).await.unwrap(); // 3

    // The 4th job in the same lineage is quarantined.
    let mut c3 = job("fanout");
    c3.metadata = meta(&c1.id, "", 2);
    let err = add(&service, c3).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert!(err.message().contains("chain quarantined"));

    // A separate root lineage is unaffected.
    let mut other = job("fanout");
    other.metadata = meta("", "", 0);
    add(&service, other).await.unwrap();

    assert!(service
        .db_manager
        .metrics
        .render()
        .contains("anvilmq_chain_quarantines_total 1\n"));
}

#[tokio::test]
async fn chain_cap_zero_disables_quarantine() {
    let service = service_with(":memory:", 0).await;
    for _ in 0..50 {
        let mut j = job("fanout");
        j.metadata = meta("", "shared", 0);
        add(&service, j).await.unwrap();
    }
    assert!(service
        .db_manager
        .metrics
        .render()
        .contains("anvilmq_chain_quarantines_total 0\n"));
}

#[tokio::test]
async fn omitted_depth_is_derived_from_parent_and_inherits_trace() {
    let service = service_with(":memory:", 0).await;
    let mut root = job("emails");
    root.metadata = meta("", "", 0);
    let root = add(&service, root).await.unwrap();
    let (root_trace, _) = read_trace(&service, &root.id).await;

    // Child declares only parent_id (depth left at the 0 sentinel): the server derives.
    let mut child = job("emails");
    child.metadata = meta(&root.id, "", 0);
    let child = add(&service, child).await.unwrap();
    let (child_trace, child_depth) = read_trace(&service, &child.id).await;
    assert_eq!(child_depth, 1);
    assert_eq!(child_trace, root_trace);
    assert!(service
        .db_manager
        .metrics
        .render()
        .contains("anvilmq_ancestry_rejections_total 0\n"));
}

#[tokio::test]
async fn derived_depth_trips_circuit_breaker() {
    let service = service_with(":memory:", 0).await;
    // Build a chain up to the max allowed depth (10) using explicit correct depths.
    let mut root = job("chain");
    root.metadata = meta("", "", 0);
    let mut parent = add(&service, root).await.unwrap();
    for depth in 1..=10u32 {
        let mut child = job("chain");
        child.metadata = meta(&parent.id, "", depth);
        parent = add(&service, child).await.unwrap();
    }
    let (_, parent_depth) = read_trace(&service, &parent.id).await;
    assert_eq!(parent_depth, 10);

    // Supplied 0 passes the cheap pre-tx check, but the derived depth (11) trips the
    // authoritative breaker inside the transaction.
    let mut child = job("chain");
    child.metadata = meta(&parent.id, "", 0);
    let err = add(&service, child).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert!(err.message().contains("Circuit breaker tripped"));
}

#[tokio::test]
async fn multi_level_derivation_increments_and_shares_trace() {
    let service = service_with(":memory:", 0).await;
    let mut root = job("chain");
    root.metadata = meta("", "", 0);
    let root = add(&service, root).await.unwrap();
    let (root_trace, _) = read_trace(&service, &root.id).await;

    // Each level declares only its parent; the server derives +1 per hop and every
    // job shares the root lineage id.
    let mut parent = root;
    for expected_depth in 1..=4i64 {
        let mut child = job("chain");
        child.metadata = meta(&parent.id, "", 0);
        let child = add(&service, child).await.unwrap();
        let (trace, depth) = read_trace(&service, &child.id).await;
        assert_eq!(depth, expected_depth);
        assert_eq!(trace, root_trace);
        parent = child;
    }
}

async fn read_rejections(
    service: &MyQueueService,
) -> Vec<(String, String, Option<String>, Option<String>)> {
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let conn = conn.blocking_lock();
        let mut stmt = conn
            .prepare("SELECT kind, name, trace_id, parent_id FROM enqueue_rejections ORDER BY id")
            .unwrap();
        stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
    })
    .await
    .unwrap()
}

async fn count_jobs(service: &MyQueueService) -> i64 {
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let conn = conn.blocking_lock();
        conn.query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get(0))
            .unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn circuit_breaker_rejection_is_durably_recorded_with_lineage() {
    let service = service_with(":memory:", 0).await;
    // Build a lineage up to the max allowed depth (10), each hop deriving parent_depth + 1.
    let mut root = job("chain");
    root.metadata = meta("", "", 0);
    let mut parent = add(&service, root).await.unwrap();
    for _ in 0..service.max_execution_depth {
        let mut child = job("chain");
        child.metadata = meta(&parent.id, "", 0);
        parent = add(&service, child).await.unwrap();
    }
    let jobs_before = count_jobs(&service).await;

    // The next child derives depth 11 (> 10) and trips the circuit breaker at admission.
    let mut over = job("chain");
    over.metadata = meta(&parent.id, "", 0);
    let err = add(&service, over).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert!(err.message().contains("Circuit breaker tripped"));

    // The rejected child persisted no job row (the admission tx rolled back)...
    assert_eq!(count_jobs(&service).await, jobs_before);
    // ...but the rejection is durably captured exactly once, with full lineage.
    let rows = read_rejections(&service).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "circuit_breaker");
    assert_eq!(rows[0].1, "chain");
    assert!(rows[0].2.is_some(), "trace_id recorded");
    assert_eq!(rows[0].3.as_deref(), Some(parent.id.as_str()));
    assert!(service
        .db_manager
        .metrics
        .render()
        .contains("anvilmq_enqueue_rejections_total{kind=\"circuit_breaker\"} 1\n"));
}

#[tokio::test]
async fn missing_parent_rejection_is_durably_recorded() {
    let service = service_with(":memory:", 0).await;
    let mut child = job("emails");
    child.metadata = meta("does-not-exist", "", 0);
    let err = add(&service, child).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    assert_eq!(count_jobs(&service).await, 0);
    let rows = read_rejections(&service).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "parent_not_found");
    assert_eq!(rows[0].1, "emails");
    assert_eq!(rows[0].3.as_deref(), Some("does-not-exist"));
}
