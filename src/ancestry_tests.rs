use super::*;

async fn service_with(path: &str, max_chain_size: u64) -> MyQueueService {
    MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(path).await.unwrap()),
        max_execution_depth: 10,
        max_chain_size,
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
    for bad_depth in [0u32, 2, 7] {
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
        .contains("anvilmq_ancestry_rejections_total 3\n"));
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
