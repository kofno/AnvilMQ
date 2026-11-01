use crate::{
    leases,
    queue::v1::{
        AddFlowRequest, AddFlowResponse, AddJobRequest, FlowJob, FlowNode,
        GetChildrenValuesRequest, GetChildrenValuesResponse, JobMetadata,
    },
    MyQueueService,
};
use prost::Message;
use rusqlite::{OptionalExtension, TransactionBehavior};
use std::collections::{HashMap, HashSet};
use tonic::Status;

/// A single flow-tree node after validation and id/linkage assignment, ready to insert in one
/// transaction. Scheduling/retry options are normalized exactly as `validate_and_prepare` does for
/// a standalone `AddJobRequest` so a flow node is admitted with identical semantics.
struct PreparedNode {
    id: String,
    parent_id: Option<String>,
    name: String,
    payload: Vec<u8>,
    priority: i32,
    max_attempts: u32,
    delay_ms: i64,
    rate_limit_facet: String,
    retry_backoff_ms: i64,
    retry_backoff_max_ms: i64,
    retry_jitter: f64,
    idempotency_key: String,
    fail_parent_on_failure: bool,
    depth: u32,
    pending_children: i64,
    // Resting state at insert: `WaitingChildren` (has children), `Delayed` (leaf with delay),
    // otherwise `Waiting`.
    state: &'static str,
}

impl PreparedNode {
    fn has_children(&self) -> bool {
        self.state == "WaitingChildren"
    }
}

/// Recursively validate one `FlowJob`, normalize its options, assign a fresh id, and flatten the
/// subtree into `out` (root-first, topological). Enforces the per-tree height cap and total node
/// cap in memory, before any transaction opens, so a rejected tree writes nothing. `seen_keys`
/// detects a duplicate idempotency key reused by two nodes of the same tree.
#[allow(clippy::too_many_arguments)]
fn prepare_node(
    node: FlowJob,
    parent_id: Option<String>,
    depth: u32,
    max_flow_depth: u32,
    max_flow_nodes: u32,
    seen_keys: &mut HashSet<(String, String)>,
    out: &mut Vec<PreparedNode>,
) -> Result<(), Status> {
    if depth > max_flow_depth {
        return Err(Status::invalid_argument(format!(
            "flow tree height exceeds max allowed {max_flow_depth}"
        )));
    }
    if out.len() as u32 >= max_flow_nodes {
        return Err(Status::invalid_argument(format!(
            "flow tree node count exceeds max allowed {max_flow_nodes}"
        )));
    }
    // Field validation mirrors enqueue's `validate_and_prepare` so a flow node and a standalone job
    // are admitted under the same rules.
    if !node.idempotency_key.is_empty()
        && (node.idempotency_key.trim().is_empty() || node.idempotency_key.len() > 256)
    {
        return Err(Status::invalid_argument(
            "idempotency_key must be nonblank and at most 256 UTF-8 bytes",
        ));
    }
    if node.delay_ms < 0 || node.retry_backoff_ms < 0 || node.retry_backoff_max_ms < 0 {
        return Err(Status::invalid_argument("delays must not be negative"));
    }
    let retry_backoff_max_ms = if node.retry_backoff_max_ms == 0 {
        60_000.max(node.retry_backoff_ms)
    } else {
        node.retry_backoff_max_ms
    };
    if retry_backoff_max_ms < node.retry_backoff_ms {
        return Err(Status::invalid_argument(
            "backoff cap must be at least the base",
        ));
    }
    if node.retry_jitter.is_nan() || !(0.0..=1.0).contains(&node.retry_jitter) {
        return Err(Status::invalid_argument(
            "retry_jitter must be a fraction in [0, 1]",
        ));
    }
    let max_attempts = if node.max_attempts == 0 {
        3
    } else {
        node.max_attempts
    };
    if !node.idempotency_key.is_empty()
        && !seen_keys.insert((node.name.clone(), node.idempotency_key.clone()))
    {
        return Err(Status::invalid_argument(
            "duplicate idempotency_key within flow",
        ));
    }
    let id = uuid::Uuid::new_v4().to_string();
    let child_count = node.children.len();
    let state = if child_count > 0 {
        "WaitingChildren"
    } else if node.delay_ms > 0 {
        "Delayed"
    } else {
        "Waiting"
    };
    out.push(PreparedNode {
        id: id.clone(),
        parent_id,
        name: node.name,
        payload: node.payload,
        priority: node.priority,
        max_attempts,
        delay_ms: node.delay_ms,
        rate_limit_facet: node.rate_limit_facet,
        retry_backoff_ms: node.retry_backoff_ms,
        retry_backoff_max_ms,
        retry_jitter: node.retry_jitter,
        idempotency_key: node.idempotency_key,
        fail_parent_on_failure: node.fail_parent_on_failure,
        depth,
        pending_children: child_count as i64,
        state,
    });
    for child in node.children {
        prepare_node(
            child,
            Some(id.clone()),
            depth + 1,
            max_flow_depth,
            max_flow_nodes,
            seen_keys,
            out,
        )?;
    }
    Ok(())
}

/// Build the serialized idempotency receipt request for a node, matching the exact-bytes scheme
/// `validate_and_prepare` uses for a standalone `AddJobRequest`, so a later `AddJob`/`AddFlow`
/// reusing the key replays on a byte-identical request and conflicts otherwise.
fn receipt_request(node: &PreparedNode, tree_id: &str) -> Vec<u8> {
    AddJobRequest {
        name: node.name.clone(),
        payload: node.payload.clone(),
        priority: node.priority,
        metadata: Some(JobMetadata {
            parent_id: node.parent_id.clone().unwrap_or_default(),
            trace_id: tree_id.to_string(),
            execution_depth: node.depth,
        }),
        max_attempts: node.max_attempts,
        delay_ms: node.delay_ms,
        rate_limit_facet: node.rate_limit_facet.clone(),
        retry_backoff_ms: node.retry_backoff_ms,
        retry_backoff_max_ms: node.retry_backoff_max_ms,
        idempotency_key: node.idempotency_key.clone(),
        retry_jitter: node.retry_jitter,
        release_on_finish: false,
    }
    .encode_to_vec()
}

pub async fn add_flow(
    service: &MyQueueService,
    req: AddFlowRequest,
) -> Result<AddFlowResponse, Status> {
    let Some(flow) = req.flow else {
        service.db_manager.metrics.flow_rejected();
        return Err(Status::invalid_argument("flow is required"));
    };
    let max_flow_depth = service.max_flow_depth;
    let max_flow_nodes = service.max_flow_nodes;
    // Validate the whole tree (caps + per-node fields) in memory before opening any transaction.
    let mut nodes = Vec::new();
    let mut seen_keys = HashSet::new();
    if let Err(status) = prepare_node(
        flow,
        None,
        0,
        max_flow_depth,
        max_flow_nodes,
        &mut seen_keys,
        &mut nodes,
    ) {
        service.db_manager.metrics.flow_rejected();
        return Err(status);
    }
    let metrics = service.db_manager.metrics.clone();
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let internal = |e: rusqlite::Error| Status::internal(e.to_string());
        let mut conn = conn.blocking_lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(internal)?;
        let now = leases::now_ms(&tx).map_err(internal)?;
        // The root id is the tree id and the shared lineage (trace_id) for every node.
        let tree_id = nodes[0].id.clone();
        let mut waiting_children = 0u64;
        let mut receipts_created = 0u64;
        for node in &nodes {
            let available_at = now.checked_add(node.delay_ms).ok_or_else(|| {
                Status::invalid_argument("delay timestamp overflows")
            })?;
            // Per-node idempotency: a key already registered means a replay. A6a creates fresh,
            // self-contained trees only (no cross-flow merge/attach — that is A6b), so a replay is
            // rejected with failed_precondition and the whole tree rolls back.
            if !node.idempotency_key.is_empty() {
                let existing: Option<String> = tx
                    .query_row(
                        "SELECT job_id FROM enqueue_receipts WHERE queue_name = ?1 AND idempotency_key = ?2",
                        rusqlite::params![node.name, node.idempotency_key],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(internal)?;
                if existing.is_some() {
                    return Err(Status::failed_precondition(
                        "flow node idempotency_key already used; AddFlow creates fresh trees only",
                    ));
                }
            }
            tx.execute(
                "INSERT INTO jobs (id, name, state, priority, payload, parent_id, trace_id,
                 execution_depth, max_attempts, created_at, updated_at, rate_limit_facet, available_at,
                 retry_backoff_ms, retry_backoff_max_ms, retry_jitter, pending_children,
                 fail_parent_on_failure, tree_id, tree_depth)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
                rusqlite::params![
                    node.id,
                    node.name,
                    node.state,
                    node.priority,
                    node.payload,
                    node.parent_id,
                    tree_id,
                    node.depth,
                    node.max_attempts,
                    now,
                    if node.rate_limit_facet.is_empty() {
                        None
                    } else {
                        Some(&node.rate_limit_facet)
                    },
                    available_at,
                    node.retry_backoff_ms,
                    node.retry_backoff_max_ms,
                    node.retry_jitter,
                    node.pending_children,
                    node.fail_parent_on_failure as i64,
                    tree_id,
                    node.depth,
                ],
            )
            .map_err(internal)?;
            if node.has_children() {
                waiting_children += 1;
            }
            if !node.idempotency_key.is_empty() {
                tx.execute(
                    "INSERT INTO enqueue_receipts(queue_name, idempotency_key, request, job_id, initial_state, created_at, release_on_finish) VALUES(?1, ?2, ?3, ?4, ?5, ?6, 0)",
                    rusqlite::params![
                        node.name,
                        node.idempotency_key,
                        receipt_request(node, &tree_id),
                        node.id,
                        node.state,
                        now
                    ],
                )
                .map_err(internal)?;
                receipts_created += 1;
            }
        }
        tx.commit().map_err(internal)?;
        // Flush metrics only after the commit succeeds so a rolled-back tree never inflates them.
        for node in &nodes {
            // `transition` bumps the per-state gauge for Waiting/Delayed leaves and is a safe
            // no-op for WaitingChildren (not a tracked state — counted by the dedicated gauge
            // below), while always firing the `enqueued` event and named series.
            metrics.transition(None, node.state, "enqueued");
            metrics.named.event(&node.name, "enqueued");
        }
        metrics.flow_waiting_children_inc(waiting_children);
        for _ in 0..receipts_created {
            metrics.enqueue_receipt_created();
        }
        metrics.flow_created();
        metrics.flow_nodes_created(nodes.len() as u64);
        let response_nodes = nodes
            .iter()
            .map(|node| FlowNode {
                id: node.id.clone(),
                parent_id: node.parent_id.clone().unwrap_or_default(),
                name: node.name.clone(),
                depth: node.depth,
                state: node.state.to_string(),
            })
            .collect();
        tracing::info!(root_id = %tree_id, nodes = nodes.len(), "flow created");
        Ok(AddFlowResponse {
            root_id: tree_id,
            nodes: response_nodes,
        })
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
}

pub async fn get_children_values(
    service: &MyQueueService,
    req: GetChildrenValuesRequest,
) -> Result<GetChildrenValuesResponse, Status> {
    if req.parent_id.trim().is_empty() {
        return Err(Status::invalid_argument("parent_id must not be blank"));
    }
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || -> rusqlite::Result<GetChildrenValuesResponse> {
        let conn = conn.blocking_lock();
        let mut statement = conn.prepare(
            "SELECT id, result FROM job_history WHERE parent_id = ?1 AND state = 'Completed'",
        )?;
        let rows = statement.query_map([&req.parent_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<Vec<u8>>>(1)?))
        })?;
        let mut values = HashMap::new();
        for row in rows {
            let (id, result) = row?;
            // Omit children with a NULL (no) result; only completed children with stored bytes appear.
            if let Some(result) = result {
                values.insert(id, result);
            }
        }
        Ok(GetChildrenValuesResponse { values })
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
    .map_err(|e| Status::internal(e.to_string()))
}
