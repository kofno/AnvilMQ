use super::*;

fn input() -> AddJobRequest {
    AddJobRequest {
        name: "invoices".into(),
        payload: b"invoice-123".to_vec(),
        idempotency_key: "generate:123:v1".into(),
        ..Default::default()
    }
}
async fn service(path: &str) -> MyQueueService {
    MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(path).await.unwrap()),
        max_execution_depth: 10,
    }
}
async fn add(service: &MyQueueService, req: AddJobRequest) -> Result<AddJobResponse, Status> {
    service
        .add_job(Request::new(req))
        .await
        .map(Response::into_inner)
}

#[tokio::test]
async fn concurrent_enqueues_create_one_job_and_receipt() {
    let service = Arc::new(service(":memory:").await);
    let mut calls = Vec::new();
    for _ in 0..16 {
        let service = service.clone();
        calls.push(tokio::spawn(async move {
            add(&service, input()).await.unwrap()
        }));
    }
    let mut ids = std::collections::HashSet::new();
    let mut originals = 0;
    for call in calls {
        let response = call.await.unwrap();
        ids.insert(response.id);
        originals += usize::from(!response.replayed);
    }
    assert_eq!(ids.len(), 1);
    assert_eq!(originals, 1);
    let metrics = service.db_manager.metrics.render();
    assert!(metrics.contains("anvilmq_jobs{state=\"Waiting\"} 1\n"));
    assert!(metrics.contains("anvilmq_transitions_total{event=\"enqueued\"} 1\n"));
    assert!(metrics.contains("anvilmq_enqueue_replays_total 15\n"));
    assert!(metrics.contains("anvilmq_enqueue_receipts 1\n"));
}

#[tokio::test]
async fn conflicts_cover_payload_and_options_but_defaults_are_normalized() {
    let service = service(":memory:").await;
    let original = add(&service, input()).await.unwrap();
    let mut normalized = input();
    normalized.max_attempts = 3;
    normalized.retry_backoff_max_ms = 60000;
    normalized.metadata = Some(Default::default());
    assert_eq!(add(&service, normalized).await.unwrap().id, original.id);
    for change in 0..9 {
        let mut req = input();
        match change {
            0 => req.payload.push(0),
            1 => req.priority = 1,
            2 => req.delay_ms = 1,
            3 => req.max_attempts = 4,
            4 => req.retry_backoff_ms = 1,
            5 => req.retry_backoff_max_ms = 60001,
            6 => req.rate_limit_facet = "tenant".into(),
            7 => {
                req.metadata = Some(queue::v1::JobMetadata {
                    trace_id: "trace".into(),
                    ..Default::default()
                })
            }
            _ => {
                req.metadata = Some(queue::v1::JobMetadata {
                    execution_depth: 1,
                    ..Default::default()
                })
            }
        }
        assert_eq!(
            add(&service, req).await.unwrap_err().code(),
            tonic::Code::AlreadyExists
        );
    }
    let mut other_queue = input();
    other_queue.name = "other".into();
    assert_ne!(add(&service, other_queue).await.unwrap().id, original.id);
    let mut no_key = input();
    no_key.idempotency_key.clear();
    assert_ne!(
        add(&service, no_key.clone()).await.unwrap().id,
        add(&service, no_key).await.unwrap().id
    );
    for key in [" ".into(), "x".repeat(257), "é".repeat(129)] {
        let mut req = input();
        req.idempotency_key = key;
        assert_eq!(
            add(&service, req).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    assert!(service
        .db_manager
        .metrics
        .render()
        .contains("anvilmq_enqueue_conflicts_total 9\n"));
}

#[tokio::test]
async fn receipt_survives_completion_and_restart_without_rescheduling() {
    let path = std::env::temp_dir().join(format!("anvil-enqueue-{}.db", uuid::Uuid::new_v4()));
    let original;
    {
        let service = service(path.to_str().unwrap()).await;
        original = add(&service, input()).await.unwrap();
        let claim = service
            .get_next_job(Request::new(GetNextJobRequest {
                worker_id: "worker".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        service
            .complete_job(Request::new(CompleteJobRequest {
                id: claim.id,
                worker_id: "worker".into(),
                attempt: 1,
            }))
            .await
            .unwrap();
    }
    {
        let service = service(path.to_str().unwrap()).await;
        let replay = add(&service, input()).await.unwrap();
        assert_eq!(replay.id, original.id);
        assert_eq!(replay.state, "Waiting");
        assert!(replay.replayed);
        let next = service
            .get_next_job(Request::new(GetNextJobRequest {
                worker_id: "worker".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!next.found);
        assert!(service
            .db_manager
            .metrics
            .render()
            .contains("anvilmq_enqueue_receipts 1\n"));
    }
    tokio::task::spawn_blocking(move || std::fs::remove_file(path).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn receipt_insert_failure_rolls_back_job_and_metrics() {
    let service = service(":memory:").await;
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || conn.blocking_lock().execute_batch("CREATE TRIGGER reject_receipt BEFORE INSERT ON enqueue_receipts BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap()).await.unwrap();
    assert_eq!(
        add(&service, input()).await.unwrap_err().code(),
        tonic::Code::Internal
    );
    assert!(service
        .db_manager
        .metrics
        .render()
        .contains("anvilmq_jobs{state=\"Waiting\"} 0\n"));
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let conn = conn.blocking_lock();
        assert_eq!(
            conn.query_row(
                "SELECT (SELECT COUNT(*) FROM jobs) + (SELECT COUNT(*) FROM enqueue_receipts)",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        conn.execute_batch("DROP TRIGGER reject_receipt").unwrap();
    })
    .await
    .unwrap();
    assert!(!add(&service, input()).await.unwrap().replayed);
}
