use super::*;

#[tokio::test]
async fn completion_replay_requires_the_successful_claim_and_is_mutation_free() {
    let (service, id) = setup(2).await;
    claim(&service).await;
    fail(&service, &id, "worker", 1).await.unwrap();
    claim(&service).await;
    let (a, b) = tokio::join!(
        complete(&service, &id, "worker", 2),
        complete(&service, &id, "worker", 2)
    );
    a.unwrap();
    b.unwrap();
    let before = snapshot(&service).await;
    complete(&service, &id, "worker", 2).await.unwrap();
    assert_eq!(snapshot(&service).await, before);
    assert!(!claim(&service).await.found);
    for (worker, attempt) in [("other", 2), ("worker", 0), ("worker", 1), ("worker", 3)] {
        assert_eq!(
            complete(&service, &id, worker, attempt)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
    assert_eq!(
        fail(&service, &id, "worker", 2).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let (service, id) = setup(1).await;
    claim(&service).await;
    fail(&service, &id, "worker", 1).await.unwrap();
    assert_eq!(
        complete(&service, &id, "worker", 1)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
async fn completion_receipt_survives_reopening_database() {
    let path = std::env::temp_dir().join(format!("anvil-completion-{}.db", uuid::Uuid::new_v4()));
    let id;
    {
        let service = MyQueueService {
            db_manager: Arc::new(DatabaseManager::new(path.to_str().unwrap()).await.unwrap()),
            max_execution_depth: 10,
            max_chain_size: 0,
            fairness_enabled: false,
        };
        id = service
            .add_job(Request::new(AddJobRequest {
                name: "receipt".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .id;
        claim(&service).await;
        complete(&service, &id, "worker", 0).await.unwrap();
    }
    {
        let service = MyQueueService {
            db_manager: Arc::new(DatabaseManager::new(path.to_str().unwrap()).await.unwrap()),
            max_execution_depth: 10,
            max_chain_size: 0,
            fairness_enabled: false,
        };
        complete(&service, &id, "worker", 1).await.unwrap();
        complete(&service, &id, "worker", 0).await.unwrap();
        assert!(!claim(&service).await.found);
    }
    tokio::task::spawn_blocking(move || std::fs::remove_file(path).unwrap())
        .await
        .unwrap();
}

async fn heartbeat(
    service: &MyQueueService,
    id: &str,
    worker: &str,
    attempt: u32,
) -> Result<Response<queue::v1::HeartbeatResponse>, Status> {
    service
        .heartbeat(Request::new(queue::v1::HeartbeatRequest {
            id: id.into(),
            worker_id: worker.into(),
            attempt,
        }))
        .await
}

#[tokio::test]
async fn heartbeat_renews_only_valid_claims() {
    let (service, id) = setup(3).await;
    let job = claim(&service).await;
    assert!(job.lease_expires_at_ms > 0);
    assert_eq!(
        heartbeat(&service, &id, "worker", 0)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        heartbeat(&service, &id, "other", 1)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        heartbeat(&service, &id, "worker", 2)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    sql(
        &service,
        "UPDATE jobs SET lease_expires_at_ms = lease_expires_at_ms - 10000",
    )
    .await;
    let renewed = heartbeat(&service, &id, "worker", 1)
        .await
        .unwrap()
        .into_inner();
    assert!(renewed.lease_expires_at_ms >= job.lease_expires_at_ms);
    assert_eq!(
        leases::recover_expired(service.db_manager.clone())
            .await
            .unwrap(),
        0
    );
    complete(&service, &id, "worker", 1).await.unwrap();
    assert_eq!(
        heartbeat(&service, &id, "worker", 1)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
async fn expiration_requeues_and_fences_previous_attempt() {
    let (service, id) = setup(2).await;
    claim(&service).await;
    sql(&service, "UPDATE jobs SET lease_expires_at_ms = 0").await;
    assert_eq!(
        heartbeat(&service, &id, "worker", 1)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        complete(&service, &id, "worker", 1)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        fail(&service, &id, "worker", 1).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let (recovered, late) = tokio::join!(
        leases::recover_expired(service.db_manager.clone()),
        complete(&service, &id, "worker", 1)
    );
    assert_eq!(recovered.unwrap(), 1);
    assert_eq!(late.unwrap_err().code(), tonic::Code::FailedPrecondition);
    let stored = snapshot(&service).await;
    assert_eq!(
        (stored.0.as_str(), stored.1, stored.2),
        ("Waiting", None, 1)
    );
    assert_eq!(claim(&service).await.attempts, 2);
    assert_eq!(
        heartbeat(&service, &id, "worker", 1)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        complete(&service, &id, "worker", 1)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    sql(&service, "UPDATE jobs SET lease_expires_at_ms = 0").await;
    assert_eq!(
        leases::recover_expired(service.db_manager.clone())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        leases::recover_expired(service.db_manager.clone())
            .await
            .unwrap(),
        0
    );
    let stored = snapshot(&service).await;
    assert_eq!(
        (stored.0.as_str(), stored.2, stored.3.as_deref()),
        ("Failed", 2, Some("Worker lease expired"))
    );
}

#[tokio::test]
async fn recovery_and_heartbeat_serialize_without_losing_renewal() {
    let (service, id) = setup(2).await;
    claim(&service).await;
    let (renewed, recovered) = tokio::join!(
        heartbeat(&service, &id, "worker", 1),
        leases::recover_expired(service.db_manager.clone())
    );
    renewed.unwrap();
    assert_eq!(recovered.unwrap(), 0);
    assert_eq!(snapshot(&service).await.0, "Active");
}

#[tokio::test]
async fn recovery_transfer_rolls_back_on_failure() {
    let (service, _) = setup(1).await;
    claim(&service).await;
    sql(&service, "UPDATE jobs SET lease_expires_at_ms = 0; CREATE TRIGGER reject_recovery BEFORE DELETE ON jobs BEGIN SELECT RAISE(ABORT, 'injected'); END;").await;
    assert_eq!(
        leases::recover_expired(service.db_manager.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Internal
    );
    assert_eq!(snapshot(&service).await.0, "Active");
    sql(&service, "DROP TRIGGER reject_recovery").await;
    // A leaked history insert would make this retry fail its unique constraint.
    assert_eq!(
        leases::recover_expired(service.db_manager.clone())
            .await
            .unwrap(),
        1
    );
    assert_eq!(snapshot(&service).await.0, "Failed");
}

#[tokio::test]
async fn persisted_leases_survive_reopen_and_legacy_claims_recover() {
    let path = format!("target/lease-test-{}.db", uuid::Uuid::new_v4());
    let service = MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(&path).await.unwrap()),
        max_execution_depth: 10,
        max_chain_size: 0,
        fairness_enabled: false,
    };
    let id = service
        .add_job(Request::new(AddJobRequest {
            name: "email".into(),
            max_attempts: 2,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .id;
    let job = claim(&service).await;
    drop(service);
    let service = MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(&path).await.unwrap()),
        max_execution_depth: 10,
        max_chain_size: 0,
        fairness_enabled: false,
    };
    assert_eq!(
        leases::recover_expired(service.db_manager.clone())
            .await
            .unwrap(),
        0
    );
    assert!(
        heartbeat(&service, &id, "worker", 1)
            .await
            .unwrap()
            .into_inner()
            .lease_expires_at_ms
            >= job.lease_expires_at_ms
    );
    // Simulate an old database whose Active claim has no persisted lease.
    sql(&service, "UPDATE jobs SET lease_expires_at_ms = NULL").await;
    drop(service);
    let service = MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(&path).await.unwrap()),
        max_execution_depth: 10,
        max_chain_size: 0,
        fairness_enabled: false,
    };
    assert_eq!(
        leases::recover_expired(service.db_manager.clone())
            .await
            .unwrap(),
        1
    );
    assert_eq!(claim(&service).await.attempts, 2);
    complete(&service, &id, "worker", 2).await.unwrap();
    drop(service);
    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn concurrent_completion_and_failure_commit_only_one_outcome() {
    let (service, id) = setup(1).await;
    claim(&service).await;
    let (completed, failed) = tokio::join!(
        complete(&service, &id, "worker", 1),
        fail(&service, &id, "worker", 1),
    );
    assert_ne!(completed.is_ok(), failed.is_ok());
    if let Err(error) = completed {
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
    if let Err(error) = failed {
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
    let conn = service.db_manager.get_shared_connection();
    let counts = tokio::task::spawn_blocking(move || {
        conn.blocking_lock()
            .query_row(
                "SELECT (SELECT COUNT(*) FROM jobs), (SELECT COUNT(*) FROM job_history)",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(counts, (0, 1));
}

async fn setup(max_attempts: u32) -> (MyQueueService, String) {
    let service = MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(":memory:").await.unwrap()),
        max_execution_depth: 10,
        max_chain_size: 0,
        fairness_enabled: false,
    };
    let id = service
        .add_job(Request::new(AddJobRequest {
            name: "email".into(),
            payload: vec![1, 2],
            max_attempts,
            rate_limit_facet: "tenant:a".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .id;
    (service, id)
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

async fn complete(
    service: &MyQueueService,
    id: &str,
    worker: &str,
    attempt: u32,
) -> Result<Response<CompleteJobResponse>, Status> {
    service
        .complete_job(Request::new(CompleteJobRequest {
            id: id.into(),
            worker_id: worker.into(),
            attempt,
        }))
        .await
}

async fn fail(
    service: &MyQueueService,
    id: &str,
    worker: &str,
    attempt: u32,
) -> Result<Response<FailJobResponse>, Status> {
    service
        .fail_job(Request::new(FailJobRequest {
            id: id.into(),
            worker_id: worker.into(),
            attempt,
            error_message: format!("failure {attempt}"),
        }))
        .await
}

async fn sql(service: &MyQueueService, sql: &'static str) {
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || conn.blocking_lock().execute_batch(sql).unwrap())
        .await
        .unwrap();
}

// Read both tables so tests assert durable state rather than only RPC responses.
async fn snapshot(
    service: &MyQueueService,
) -> (String, Option<String>, u32, Option<String>, String, i64) {
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        conn.blocking_lock().query_row(
            "SELECT state, worker_id, attempts, last_error, rate_limit_facet, updated_at FROM jobs
             UNION ALL SELECT state, worker_id, attempts, last_error, rate_limit_facet, finished_at FROM job_history",
            [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
        ).unwrap()
    }).await.unwrap()
}

#[tokio::test]
async fn completion_archives_replays_duplicate_and_rejects_conflicting_ack() {
    let (service, id) = setup(3).await;
    claim(&service).await;
    assert!(
        complete(&service, &id, "worker", 0)
            .await
            .unwrap()
            .into_inner()
            .success
    );
    let stored = snapshot(&service).await;
    assert_eq!(
        (
            stored.0.as_str(),
            stored.1.as_deref(),
            stored.2,
            stored.4.as_str()
        ),
        ("Completed", Some("worker"), 1, "tenant:a")
    );
    assert!(stored.5 > 0);
    complete(&service, &id, "worker", 1).await.unwrap();
    assert_eq!(
        fail(&service, &id, "worker", 1).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert!(!claim(&service).await.found);
}

#[tokio::test]
async fn retries_exhaust_limit_and_preserve_error() {
    let (service, id) = setup(2).await;
    assert_eq!(claim(&service).await.attempts, 1);
    assert!(
        !fail(&service, &id, "worker", 1)
            .await
            .unwrap()
            .into_inner()
            .moved_to_failed_state
    );
    let stored = snapshot(&service).await;
    assert_eq!(
        (stored.0.as_str(), stored.1, stored.2, stored.3.as_deref()),
        ("Waiting", None, 1, Some("failure 1"))
    );
    assert_eq!(
        fail(&service, &id, "worker", 1).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(claim(&service).await.attempts, 2);
    for stale in [0, 1] {
        assert_eq!(
            complete(&service, &id, "worker", stale)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            fail(&service, &id, "worker", stale)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
    assert!(
        fail(&service, &id, "worker", 2)
            .await
            .unwrap()
            .into_inner()
            .moved_to_failed_state
    );
    let stored = snapshot(&service).await;
    assert_eq!(
        (stored.0.as_str(), stored.2, stored.3.as_deref()),
        ("Failed", 2, Some("failure 2"))
    );
    assert!(!claim(&service).await.found);
    assert_eq!(
        fail(&service, &id, "worker", 2).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test]
async fn validates_input_state_and_ownership_for_both_handlers() {
    let (service, id) = setup(1).await;
    for (job, worker, code) in [
        ("", "worker", tonic::Code::InvalidArgument),
        (id.as_str(), " ", tonic::Code::InvalidArgument),
        ("missing", "worker", tonic::Code::NotFound),
        (id.as_str(), "worker", tonic::Code::FailedPrecondition),
    ] {
        assert_eq!(
            complete(&service, job, worker, 1).await.unwrap_err().code(),
            code
        );
        assert_eq!(
            fail(&service, job, worker, 1).await.unwrap_err().code(),
            code
        );
    }
    claim(&service).await;
    assert_eq!(
        complete(&service, &id, "other", 1)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        fail(&service, &id, "other", 1).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(snapshot(&service).await.0, "Active");
    assert!(
        fail(&service, &id, "worker", 1)
            .await
            .unwrap()
            .into_inner()
            .moved_to_failed_state
    );
}

#[tokio::test]
async fn terminal_transfer_rolls_back_history_insert_when_delete_fails() {
    for failure in [false, true] {
        let (service, id) = setup(1).await;
        claim(&service).await;
        sql(&service, "CREATE TRIGGER reject_delete BEFORE DELETE ON jobs BEGIN SELECT RAISE(ABORT, 'injected'); END;").await;
        let code = if failure {
            fail(&service, &id, "worker", 1).await.unwrap_err().code()
        } else {
            complete(&service, &id, "worker", 1)
                .await
                .unwrap_err()
                .code()
        };
        assert_eq!(code, tonic::Code::Internal);
        assert_eq!(snapshot(&service).await.0, "Active");
        let conn = service.db_manager.get_shared_connection();
        let count: i64 = tokio::task::spawn_blocking(move || {
            conn.blocking_lock()
                .query_row("SELECT COUNT(*) FROM job_history", [], |r| r.get(0))
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(count, 0);
        sql(&service, "DROP TRIGGER reject_delete").await;
        if failure {
            fail(&service, &id, "worker", 1).await.unwrap();
        } else {
            complete(&service, &id, "worker", 1).await.unwrap();
        }
    }
}

#[tokio::test]
async fn failed_retry_update_rolls_back_and_completion_preserves_prior_error() {
    let (service, id) = setup(0).await; // Default limit is three.
    claim(&service).await;
    sql(&service, "CREATE TRIGGER reject_retry AFTER UPDATE ON jobs BEGIN SELECT RAISE(ABORT, 'injected'); END;").await;
    assert_eq!(
        fail(&service, &id, "worker", 1).await.unwrap_err().code(),
        tonic::Code::Internal
    );
    let stored = snapshot(&service).await;
    assert_eq!(
        (stored.0.as_str(), stored.1.as_deref(), stored.3),
        ("Active", Some("worker"), None)
    );
    sql(&service, "DROP TRIGGER reject_retry").await;
    fail(&service, &id, "worker", 1).await.unwrap();
    assert_eq!(claim(&service).await.attempts, 2);
    complete(&service, &id, "worker", 2).await.unwrap();
    let stored = snapshot(&service).await;
    assert_eq!(
        (stored.0.as_str(), stored.2, stored.3.as_deref()),
        ("Completed", 2, Some("failure 1"))
    );
}

#[tokio::test]
async fn delayed_jobs_and_backoff_wait_until_due() {
    let (service, _) = setup(1).await;
    claim(&service).await;
    let id = service
        .add_job(Request::new(AddJobRequest {
            name: "scheduled".into(),
            delay_ms: 60000,
            max_attempts: 3,
            retry_backoff_ms: 10000,
            retry_backoff_max_ms: 15000,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .id;
    assert!(!claim(&service).await.found);
    sql(
        &service,
        "UPDATE jobs SET available_at = 0 WHERE state = 'Delayed'",
    )
    .await;
    let job = claim(&service).await;
    assert_eq!(job.id, id);
    fail(&service, &id, "worker", 1).await.unwrap();
    assert!(!claim(&service).await.found);
    let conn = service.db_manager.get_shared_connection();
    let delay: i64 = tokio::task::spawn_blocking(move || {
        conn.blocking_lock()
            .query_row(
                "SELECT available_at - updated_at FROM jobs WHERE state = 'Delayed'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    })
    .await
    .unwrap();
    assert!((9900..=10000).contains(&delay));
    sql(
        &service,
        "UPDATE jobs SET available_at = 0 WHERE state = 'Delayed'",
    )
    .await;
    assert_eq!(claim(&service).await.attempts, 2);
    sql(
        &service,
        "UPDATE jobs SET lease_expires_at_ms = 0 WHERE name = 'scheduled'",
    )
    .await;
    leases::recover_expired(service.db_manager.clone())
        .await
        .unwrap();
    assert!(!claim(&service).await.found);
    let conn = service.db_manager.get_shared_connection();
    let delay: i64 = tokio::task::spawn_blocking(move || {
        conn.blocking_lock()
            .query_row(
                "SELECT available_at - updated_at FROM jobs WHERE state = 'Delayed'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(delay, 15000);
    sql(
        &service,
        "UPDATE jobs SET available_at = 0 WHERE state = 'Delayed'",
    )
    .await;
    assert_eq!(claim(&service).await.attempts, 3);
    assert!(
        fail(&service, &id, "worker", 3)
            .await
            .unwrap()
            .into_inner()
            .moved_to_failed_state
    );
}

#[tokio::test]
async fn invalid_schedules_are_rejected() {
    let (service, _) = setup(1).await;
    for (delay, base, cap) in [
        (-1, 0, 0),
        (0, -1, 0),
        (0, 0, -1),
        (0, 100, 50),
        (i64::MAX, 0, 0),
    ] {
        assert_eq!(
            service
                .add_job(Request::new(AddJobRequest {
                    delay_ms: delay,
                    retry_backoff_ms: base,
                    retry_backoff_max_ms: cap,
                    ..Default::default()
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[tokio::test]
async fn schedule_and_policy_survive_restart() {
    let path = format!("target/schedule-test-{}.db", uuid::Uuid::new_v4());
    let service = MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(&path).await.unwrap()),
        max_execution_depth: 10,
        max_chain_size: 0,
        fairness_enabled: false,
    };
    service
        .add_job(Request::new(AddJobRequest {
            name: "later".into(),
            delay_ms: 60000,
            retry_backoff_ms: 1000,
            retry_backoff_max_ms: 5000,
            ..Default::default()
        }))
        .await
        .unwrap();
    drop(service);
    let service = MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(&path).await.unwrap()),
        max_execution_depth: 10,
        max_chain_size: 0,
        fairness_enabled: false,
    };
    assert!(!claim(&service).await.found);
    let conn = service.db_manager.get_shared_connection();
    let policy = tokio::task::spawn_blocking(move || conn.blocking_lock().query_row("SELECT retry_backoff_ms, retry_backoff_max_ms, available_at - created_at FROM jobs", [], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?))).unwrap()).await.unwrap();
    assert_eq!(policy, (1000, 5000, 60000));
    sql(&service, "UPDATE jobs SET available_at = 0").await;
    assert!(claim(&service).await.found);
    drop(service);
    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn metrics_follow_commits_not_rollbacks() {
    let (service, id) = setup(2).await;
    let metrics = service.db_manager.metrics.clone();
    assert!(metrics
        .render()
        .contains("anvilmq_jobs{state=\"Waiting\"} 1"));
    claim(&service).await;
    let before = metrics.render();
    sql(&service, "CREATE TRIGGER reject_metrics BEFORE DELETE ON jobs BEGIN SELECT RAISE(ABORT, 'injected'); END;").await;
    complete(&service, &id, "worker", 1).await.unwrap_err();
    let after = metrics.render();
    for line in before
        .lines()
        .filter(|l| l.starts_with("anvilmq_jobs") || l.starts_with("anvilmq_transitions"))
    {
        assert!(after.lines().any(|v| v == line));
    }
    sql(&service, "DROP TRIGGER reject_metrics").await;
    fail(&service, &id, "worker", 1).await.unwrap();
    assert!(metrics
        .render()
        .contains("anvilmq_transitions_total{event=\"retried\"} 1"));
    claim(&service).await;
    complete(&service, &id, "worker", 2).await.unwrap();
    let output = metrics.render();
    assert!(output.contains("anvilmq_jobs{state=\"Completed\"} 1"));
    assert!(output.contains("anvilmq_jobs{state=\"Active\"} 0"));
    assert!(output.contains("anvilmq_rpc_duration_seconds_count{method=\"CompleteJob\"} 2"));
    assert!(!output.contains(&id));
}

#[tokio::test]
async fn rate_limits_skip_blocked_facets_and_reset_windows() {
    use queue::v1::*;
    let (service, _) = setup(1).await;
    claim(&service).await;
    service
        .upsert_rate_limit_rule(Request::new(UpsertRateLimitRuleRequest {
            facet_pattern: "tenant:a".into(),
            max_jobs: 1,
            window_duration_ms: 60000,
        }))
        .await
        .unwrap();
    for (name, facet, priority) in [
        ("limited", "tenant:a", 0),
        ("limited", "tenant:a", 0),
        ("other", "tenant:b", 10),
    ] {
        service
            .add_job(Request::new(AddJobRequest {
                name: name.into(),
                rate_limit_facet: facet.into(),
                priority,
                ..Default::default()
            }))
            .await
            .unwrap();
    }
    assert_eq!(claim(&service).await.name, "limited");
    assert_eq!(claim(&service).await.name, "other");
    assert!(!claim(&service).await.found);
    let status = service
        .get_rate_limit_status(Request::new(GetRateLimitStatusRequest {
            facet_key: "tenant:a".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.current_count, 1);
    assert!(status.is_throttled);
    sql(
        &service,
        "UPDATE rate_limit_counters SET window_expires_at = 0",
    )
    .await;
    let status = service
        .get_rate_limit_status(Request::new(GetRateLimitStatusRequest {
            facet_key: "tenant:a".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.current_count, 0);
    assert!(!status.is_throttled);
    assert_eq!(claim(&service).await.name, "limited");
    assert!(service
        .db_manager
        .metrics
        .render()
        .contains("anvilmq_throttled_polls_total 2"));
}

#[tokio::test]
async fn concurrent_claims_do_not_exceed_quota_and_failed_claim_refunds() {
    use queue::v1::*;
    let (service, _) = setup(1).await;
    claim(&service).await;
    service
        .upsert_rate_limit_rule(Request::new(UpsertRateLimitRuleRequest {
            facet_pattern: "limited".into(),
            max_jobs: 2,
            window_duration_ms: 60000,
        }))
        .await
        .unwrap();
    for _ in 0..8 {
        service
            .add_job(Request::new(AddJobRequest {
                name: "limited".into(),
                rate_limit_facet: "limited".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
    }
    sql(&service, "CREATE TRIGGER reject_limited BEFORE UPDATE ON jobs BEGIN SELECT RAISE(ABORT, 'injected'); END;").await;
    service
        .get_next_job(Request::new(GetNextJobRequest {
            worker_id: "worker".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    let status = service
        .get_rate_limit_status(Request::new(GetRateLimitStatusRequest {
            facet_key: "limited".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.current_count, 0);
    sql(&service, "DROP TRIGGER reject_limited").await;
    let service = Arc::new(service);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let service = service.clone();
        tasks.spawn(async move { claim(&service).await.found });
    }
    let mut claimed = 0;
    while let Some(result) = tasks.join_next().await {
        if result.unwrap() {
            claimed += 1;
        }
    }
    assert_eq!(claimed, 2);
}

#[tokio::test]
async fn rate_rule_validation_pause_update_and_delete() {
    use queue::v1::*;
    let (service, _) = setup(1).await;
    for (key, duration) in [
        ("", 1000),
        ("a*", 1000),
        (" a", 1000),
        ("a", 0),
        ("a", i64::MAX),
    ] {
        assert_eq!(
            service
                .upsert_rate_limit_rule(Request::new(UpsertRateLimitRuleRequest {
                    facet_pattern: key.into(),
                    max_jobs: 1,
                    window_duration_ms: duration
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    service
        .upsert_rate_limit_rule(Request::new(UpsertRateLimitRuleRequest {
            facet_pattern: "tenant:a".into(),
            max_jobs: 0,
            window_duration_ms: 1000,
        }))
        .await
        .unwrap();
    assert!(!claim(&service).await.found);
    service
        .upsert_rate_limit_rule(Request::new(UpsertRateLimitRuleRequest {
            facet_pattern: "tenant:a".into(),
            max_jobs: 1,
            window_duration_ms: 60000,
        }))
        .await
        .unwrap();
    assert!(claim(&service).await.found);
    for expected in [true, false] {
        assert_eq!(
            service
                .delete_rate_limit_rule(Request::new(DeleteRateLimitRuleRequest {
                    facet_key: "tenant:a".into()
                }))
                .await
                .unwrap()
                .into_inner()
                .deleted,
            expected
        );
    }
    assert!(
        !service
            .get_rate_limit_status(Request::new(GetRateLimitStatusRequest {
                facet_key: "tenant:a".into()
            }))
            .await
            .unwrap()
            .into_inner()
            .rule_exists
    );
}

#[tokio::test]
async fn rate_limit_usage_survives_restart_and_rule_update_resets_it() {
    use queue::v1::*;
    let path = format!("target/rate-test-{}.db", uuid::Uuid::new_v4());
    let service = MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(&path).await.unwrap()),
        max_execution_depth: 10,
        max_chain_size: 0,
        fairness_enabled: false,
    };
    service
        .upsert_rate_limit_rule(Request::new(UpsertRateLimitRuleRequest {
            facet_pattern: "a".into(),
            max_jobs: 1,
            window_duration_ms: 60000,
        }))
        .await
        .unwrap();
    for name in ["one", "two"] {
        service
            .add_job(Request::new(AddJobRequest {
                name: name.into(),
                rate_limit_facet: "a".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
    }
    claim(&service).await;
    drop(service);
    let service = MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(&path).await.unwrap()),
        max_execution_depth: 10,
        max_chain_size: 0,
        fairness_enabled: false,
    };
    assert!(!claim(&service).await.found);
    let status = service
        .get_rate_limit_status(Request::new(GetRateLimitStatusRequest {
            facet_key: "a".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.current_count, 1);
    service
        .upsert_rate_limit_rule(Request::new(UpsertRateLimitRuleRequest {
            facet_pattern: "a".into(),
            max_jobs: 1,
            window_duration_ms: 60000,
        }))
        .await
        .unwrap();
    assert!(claim(&service).await.found);
    drop(service);
    tokio::fs::remove_file(path).await.unwrap();
}

// ---- Tenant fairness (equal round-robin) ----

async fn fairness_service(enabled: bool) -> MyQueueService {
    MyQueueService {
        db_manager: Arc::new(DatabaseManager::new(":memory:").await.unwrap()),
        max_execution_depth: 10,
        max_chain_size: 0,
        fairness_enabled: enabled,
    }
}

async fn add(service: &MyQueueService, name: &str, facet: &str, priority: i32) {
    service
        .add_job(Request::new(AddJobRequest {
            name: name.into(),
            rate_limit_facet: facet.into(),
            priority,
            ..Default::default()
        }))
        .await
        .unwrap();
}

async fn claim_name(service: &MyQueueService) -> String {
    claim(service).await.name
}

async fn facet_dispatch_keys(service: &MyQueueService) -> Vec<String> {
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let guard = conn.blocking_lock();
        let mut stmt = guard
            .prepare("SELECT facet_key FROM facet_dispatch ORDER BY facet_key")
            .unwrap();
        let keys = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        keys
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn fairness_rotates_equal_priority_jobs_across_facets() {
    let service = fairness_service(true).await;
    for _ in 0..3 {
        add(&service, "a", "tenant:a", 0).await;
    }
    for _ in 0..3 {
        add(&service, "b", "tenant:b", 0).await;
    }
    // Make facet a's jobs strictly older so the first-claim tie resolves to a deterministically.
    sql(
        &service,
        "UPDATE jobs SET created_at = CASE WHEN name = 'a' THEN 1 ELSE 2 END",
    )
    .await;
    let mut order = Vec::new();
    for _ in 0..6 {
        order.push(claim_name(&service).await);
    }
    assert_eq!(order, vec!["a", "b", "a", "b", "a", "b"]);
}

#[tokio::test]
async fn priority_is_a_hard_tier_over_fairness() {
    let service = fairness_service(true).await;
    // Serve tenant:a once so it is the most-recently-served facet.
    add(&service, "a-seed", "tenant:a", 5).await;
    assert_eq!(claim_name(&service).await, "a-seed");
    // A higher-priority job of the just-served facet must beat a starved facet's lower-priority job.
    add(&service, "a-hi", "tenant:a", 0).await;
    add(&service, "b-lo", "tenant:b", 5).await;
    assert_eq!(claim_name(&service).await, "a-hi");
    assert_eq!(claim_name(&service).await, "b-lo");
}

#[tokio::test]
async fn unfaceted_jobs_rotate_as_a_single_tenant() {
    let service = fairness_service(true).await;
    for _ in 0..2 {
        add(&service, "u", "", 0).await;
    }
    for _ in 0..2 {
        add(&service, "a", "tenant:a", 0).await;
    }
    sql(
        &service,
        "UPDATE jobs SET created_at = CASE WHEN name = 'u' THEN 1 ELSE 2 END",
    )
    .await;
    let mut order = Vec::new();
    for _ in 0..4 {
        order.push(claim_name(&service).await);
    }
    // Unfaceted jobs share one rotation slot instead of monopolizing back-to-back.
    assert_eq!(order, vec!["u", "a", "u", "a"]);
}

#[tokio::test]
async fn never_served_facet_is_picked_promptly() {
    let service = fairness_service(true).await;
    add(&service, "a-seed", "tenant:a", 0).await;
    assert_eq!(claim_name(&service).await, "a-seed");
    // An older tenant:a job and a brand-new facet's job: the never-served facet sorts first.
    add(&service, "a-old", "tenant:a", 0).await;
    add(&service, "new", "tenant:new", 0).await;
    sql(
        &service,
        "UPDATE jobs SET created_at = CASE WHEN name = 'a-old' THEN 1 ELSE 2 END",
    )
    .await;
    assert_eq!(claim_name(&service).await, "new");
}

#[tokio::test]
async fn rate_limit_blocked_facet_is_skipped_and_takes_no_turn() {
    use queue::v1::*;
    let service = fairness_service(true).await;
    service
        .upsert_rate_limit_rule(Request::new(UpsertRateLimitRuleRequest {
            facet_pattern: "tenant:a".into(),
            max_jobs: 0,
            window_duration_ms: 60000,
        }))
        .await
        .unwrap();
    add(&service, "a", "tenant:a", 0).await;
    add(&service, "b", "tenant:b", 0).await;
    assert_eq!(claim_name(&service).await, "b");
    // The blocked facet was never selected, so it consumed no rotation slot.
    assert_eq!(facet_dispatch_keys(&service).await, vec!["tenant:b"]);
}

#[tokio::test]
async fn fairness_disabled_preserves_priority_fifo_and_writes_nothing() {
    let service = fairness_service(false).await;
    for _ in 0..3 {
        add(&service, "a", "tenant:a", 0).await;
    }
    for _ in 0..3 {
        add(&service, "b", "tenant:b", 0).await;
    }
    sql(
        &service,
        "UPDATE jobs SET created_at = CASE WHEN name = 'a' THEN 1 ELSE 2 END",
    )
    .await;
    let mut order = Vec::new();
    for _ in 0..6 {
        order.push(claim_name(&service).await);
    }
    // Legacy behavior: one facet's backlog drains fully before the other (no rotation).
    assert_eq!(order, vec!["a", "a", "a", "b", "b", "b"]);
    // Disabled path must not write the fairness table at all.
    assert!(facet_dispatch_keys(&service).await.is_empty());
}
