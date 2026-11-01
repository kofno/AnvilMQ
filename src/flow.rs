use crate::{
    leases,
    queue::v1::{
        AddFlowBulkRequest, AddFlowBulkResponse, AddFlowMergeRequest, AddFlowMergeResponse,
        AddFlowRequest, AddFlowResponse, AddJobRequest, FlowJob, FlowNode,
        GetChildrenValuesRequest, GetChildrenValuesResponse, JobMetadata,
    },
    telemetry::Metrics,
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
/// reusing the key replays on a byte-identical request and conflicts otherwise. `parent_id` and
/// `depth` are passed explicitly because a merged node's resolved parent/depth differ from the
/// values captured during in-memory preparation.
fn receipt_request(node: &PreparedNode, tree_id: &str, parent_id: &str, depth: u32) -> Vec<u8> {
    AddJobRequest {
        name: node.name.clone(),
        payload: node.payload.clone(),
        priority: node.priority,
        metadata: Some(JobMetadata {
            parent_id: parent_id.to_string(),
            trace_id: tree_id.to_string(),
            execution_depth: depth,
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

/// Insert one fully-prepared flow tree (root-first topological `nodes`) into the open transaction.
/// Mirrors A6a's single-tree insert exactly so `add_flow` and `add_flow_bulk` share identical
/// per-tree semantics: a node whose idempotency key already exists rolls the whole tree (and, for
/// bulk, the whole batch) back with `failed_precondition`. Returns the WaitingChildren count and
/// receipts written (for the post-commit metric flush) plus the response descriptor.
fn insert_prepared_tree(
    tx: &rusqlite::Transaction,
    nodes: &[PreparedNode],
    now: i64,
) -> Result<(u64, u64, AddFlowResponse), Status> {
    let internal = |e: rusqlite::Error| Status::internal(e.to_string());
    // The root id is the tree id and the shared lineage (trace_id) for every node.
    let tree_id = nodes[0].id.clone();
    let mut waiting_children = 0u64;
    let mut receipts_created = 0u64;
    for node in nodes {
        let available_at = now
            .checked_add(node.delay_ms)
            .ok_or_else(|| Status::invalid_argument("delay timestamp overflows"))?;
        // Per-node idempotency: a key already registered means a replay. AddFlow/AddFlowBulk create
        // fresh, self-contained trees only (cross-flow merge/attach is AddFlowMerge), so a replay is
        // rejected with failed_precondition and the whole tree (and batch) rolls back.
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
                    receipt_request(node, &tree_id, node.parent_id.as_deref().unwrap_or(""), node.depth),
                    node.id,
                    node.state,
                    now
                ],
            )
            .map_err(internal)?;
            receipts_created += 1;
        }
    }
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
    Ok((
        waiting_children,
        receipts_created,
        AddFlowResponse {
            root_id: tree_id,
            nodes: response_nodes,
        },
    ))
}

/// Post-commit metric flush for one created tree, identical to A6a's inline flush. Called only
/// after the transaction commits so a rolled-back tree never inflates counters/gauges.
fn flush_tree_metrics(
    metrics: &Metrics,
    nodes: &[PreparedNode],
    waiting_children: u64,
    receipts_created: u64,
) {
    for node in nodes {
        // `transition` bumps the per-state gauge for Waiting/Delayed leaves and is a safe no-op for
        // WaitingChildren (counted by the dedicated gauge below), while always firing the named
        // series `enqueued` event.
        metrics.transition(None, node.state, "enqueued");
        metrics.named.event(&node.name, "enqueued");
    }
    metrics.flow_waiting_children_inc(waiting_children);
    for _ in 0..receipts_created {
        metrics.enqueue_receipt_created();
    }
    metrics.flow_created();
    metrics.flow_nodes_created(nodes.len() as u64);
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
        let (waiting_children, receipts_created, response) =
            insert_prepared_tree(&tx, &nodes, now)?;
        tx.commit().map_err(internal)?;
        // Flush metrics only after the commit succeeds so a rolled-back tree never inflates them.
        flush_tree_metrics(&metrics, &nodes, waiting_children, receipts_created);
        tracing::info!(root_id = %response.root_id, nodes = nodes.len(), "flow created");
        Ok(response)
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
}

/// A6b: create multiple independent flow trees in ONE atomic transaction (FlowProducer.addBulk).
/// All-or-nothing — if any tree fails validation or insert, the whole batch rolls back and nothing
/// is written. Validates every tree in memory first (A6a `prepare_node` caps/field/dup checks, per
/// tree), with the idempotency-key dedup set spanning the WHOLE batch so a `(name, key)` collision
/// between or within trees is rejected `invalid_argument` before any transaction opens.
pub async fn add_flow_bulk(
    service: &MyQueueService,
    req: AddFlowBulkRequest,
) -> Result<AddFlowBulkResponse, Status> {
    if req.flows.is_empty() {
        service.db_manager.metrics.flow_rejected();
        return Err(Status::invalid_argument(
            "AddFlowBulk requires at least one flow",
        ));
    }
    if req.flows.len() as u32 > service.max_flow_bulk {
        service.db_manager.metrics.flow_rejected();
        return Err(Status::invalid_argument(format!(
            "AddFlowBulk exceeds max batch size {}",
            service.max_flow_bulk
        )));
    }
    let max_flow_depth = service.max_flow_depth;
    let max_flow_nodes = service.max_flow_nodes;
    // Validate+flatten every tree up front. A single dedup set spans the batch so a reused
    // (name, idempotency_key) within OR across trees is a cheap `invalid_argument` reject before
    // any transaction opens; each tree's depth/node caps are still enforced independently.
    let mut trees: Vec<Vec<PreparedNode>> = Vec::with_capacity(req.flows.len());
    let mut seen_keys = HashSet::new();
    for flow in req.flows {
        let mut nodes = Vec::new();
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
        trees.push(nodes);
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
        // Insert every tree in the single immediate transaction; the first failure aborts the whole
        // batch (tx drops -> rollback), so nothing is written.
        let mut results = Vec::with_capacity(trees.len());
        let mut flushes = Vec::with_capacity(trees.len());
        for nodes in &trees {
            let (waiting_children, receipts_created, response) =
                insert_prepared_tree(&tx, nodes, now)?;
            flushes.push((waiting_children, receipts_created));
            results.push(response);
        }
        tx.commit().map_err(internal)?;
        // Flush metrics for every tree only after the single commit succeeds.
        for (nodes, (waiting_children, receipts_created)) in trees.iter().zip(flushes) {
            flush_tree_metrics(&metrics, nodes, waiting_children, receipts_created);
        }
        tracing::info!(trees = results.len(), "flow bulk created");
        Ok(AddFlowBulkResponse { results })
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
}

/// A6b: attach new child subtree(s) under an EXISTING flow node (FlowProducer merge-by-jobId). The
/// request root's `name` + `idempotency_key` locate the already-existing attach target (AnvilMQ's
/// native jobId); the root's descendants are created fresh and linked under it. The root itself is
/// never recreated — only its identity is used to resolve the target. All work is one atomic
/// immediate transaction.
pub async fn add_flow_merge(
    service: &MyQueueService,
    req: AddFlowMergeRequest,
) -> Result<AddFlowMergeResponse, Status> {
    let Some(flow) = req.flow else {
        service.db_manager.metrics.flow_rejected();
        return Err(Status::invalid_argument("flow is required"));
    };
    if flow.idempotency_key.is_empty() {
        service.db_manager.metrics.flow_rejected();
        return Err(Status::invalid_argument(
            "AddFlowMerge requires an idempotency_key on the root to identify the attach target",
        ));
    }
    if flow.children.is_empty() {
        service.db_manager.metrics.flow_rejected();
        return Err(Status::invalid_argument(
            "AddFlowMerge root must have at least one child to attach",
        ));
    }
    let root_name = flow.name.clone();
    let root_key = flow.idempotency_key.clone();
    let max_flow_depth = service.max_flow_depth;
    let max_flow_nodes = service.max_flow_nodes;
    // Validate+flatten the new child subtrees in memory (per-node field checks + within-request
    // dedup). The real depth/size caps depend on the resolved target and are enforced inside the
    // transaction, so the in-prepare caps are disabled here (u32::MAX). Direct children start at
    // relative depth 1; `PreparedNode::depth` therefore holds the depth RELATIVE to the target.
    let mut new_nodes = Vec::new();
    let mut seen_keys = HashSet::new();
    for child in flow.children {
        if let Err(status) = prepare_node(
            child,
            None,
            1,
            u32::MAX,
            u32::MAX,
            &mut seen_keys,
            &mut new_nodes,
        ) {
            service.db_manager.metrics.flow_rejected();
            return Err(status);
        }
    }
    // Height of the attached subtree (deepest relative level) and the number of new DIRECT children
    // (the only new nodes with no in-subtree parent) for the target re-gate.
    let subtree_height = new_nodes.iter().map(|n| n.depth).max().unwrap_or(1) as i64;
    let new_direct_count = new_nodes.iter().filter(|n| n.parent_id.is_none()).count() as i64;
    let metrics = service.db_manager.metrics.clone();
    let conn = service.db_manager.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let internal = |e: rusqlite::Error| Status::internal(e.to_string());
        let mut conn = conn.blocking_lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(internal)?;
        let now = leases::now_ms(&tx).map_err(internal)?;
        // Locate the attach target by name + idempotency_key.
        let target_id: Option<String> = tx
            .query_row(
                "SELECT job_id FROM enqueue_receipts WHERE queue_name = ?1 AND idempotency_key = ?2",
                rusqlite::params![root_name, root_key],
                |r| r.get(0),
            )
            .optional()
            .map_err(internal)?;
        let Some(target_id) = target_id else {
            metrics.flow_rejected();
            return Err(Status::not_found(
                "no existing job matches the merge root's name/idempotency_key",
            ));
        };
        // Load the live target row. Absence means the job already drained to history (terminal).
        let target: Option<(String, Option<String>, i64)> = tx
            .query_row(
                "SELECT state, tree_id, tree_depth FROM jobs WHERE id = ?1",
                [&target_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(internal)?;
        let Some((target_state, target_tree_id, target_tree_depth)) = target else {
            metrics.flow_rejected();
            return Err(Status::failed_precondition(
                "cannot attach children to a finished job",
            ));
        };
        // The target must be live and quiescent: a worker holding an Active job must not be
        // re-gated mid-flight.
        match target_state.as_str() {
            "Waiting" | "Delayed" | "WaitingChildren" => {}
            "Active" => {
                metrics.flow_rejected();
                return Err(Status::failed_precondition(
                    "cannot attach children to an in-flight (Active) job",
                ));
            }
            other => {
                metrics.flow_rejected();
                return Err(Status::failed_precondition(format!(
                    "cannot attach children to a job in state {other}"
                )));
            }
        }
        // The new nodes share the target's existing tree lineage. A standalone target (no tree_id)
        // becomes the root of a new tree and adopts its own id so the size query and child lineage
        // stay consistent.
        let effective_tree_id = target_tree_id.clone().unwrap_or_else(|| target_id.clone());
        if target_tree_id.is_none() {
            tx.execute(
                "UPDATE jobs SET tree_id = ?1 WHERE id = ?2",
                rusqlite::params![effective_tree_id, target_id],
            )
            .map_err(internal)?;
        }
        // Per-tree caps on the MERGED tree. Depth: target depth + attached height.
        if target_tree_depth + subtree_height > max_flow_depth as i64 {
            metrics.flow_rejected();
            return Err(Status::invalid_argument(format!(
                "merged flow tree height exceeds max allowed {max_flow_depth}"
            )));
        }
        // Size: count LIVE jobs rows for the tree (terminal nodes already drained to history are not
        // counted) plus the new nodes being added.
        let live_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE tree_id = ?1",
                [&effective_tree_id],
                |r| r.get(0),
            )
            .map_err(internal)?;
        if live_count + new_nodes.len() as i64 > max_flow_nodes as i64 {
            metrics.flow_rejected();
            return Err(Status::invalid_argument(format!(
                "merged flow tree node count exceeds max allowed {max_flow_nodes}"
            )));
        }
        // Insert the new subtree(s). Direct children link to the target; deeper nodes keep their
        // in-subtree parent. Absolute depth = target depth + relative depth.
        let mut waiting_children = 0u64;
        let mut receipts_created = 0u64;
        let mut attached = Vec::with_capacity(new_nodes.len());
        for node in &new_nodes {
            let resolved_parent = node.parent_id.clone().unwrap_or_else(|| target_id.clone());
            let actual_depth = target_tree_depth as u32 + node.depth;
            let available_at = now
                .checked_add(node.delay_ms)
                .ok_or_else(|| Status::invalid_argument("delay timestamp overflows"))?;
            // Scope guard + per-node idempotency: only the merge root may reference an existing job,
            // so any NEW node whose key already resolves to a receipt is rejected (whole merge rolls
            // back). A general mixed tree with interior existing nodes is out of scope for A6b.
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
                    metrics.flow_rejected();
                    return Err(Status::failed_precondition(
                        "only the merge root may reference an existing job",
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
                    resolved_parent,
                    effective_tree_id,
                    actual_depth,
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
                    effective_tree_id,
                    actual_depth,
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
                        receipt_request(node, &effective_tree_id, &resolved_parent, actual_depth),
                        node.id,
                        node.state,
                        now
                    ],
                )
                .map_err(internal)?;
                receipts_created += 1;
            }
            attached.push(FlowNode {
                id: node.id.clone(),
                parent_id: resolved_parent,
                name: node.name.clone(),
                depth: actual_depth,
                state: node.state.to_string(),
            });
        }
        // Re-gate the target: it now waits for the newly attached direct children.
        tx.execute(
            "UPDATE jobs SET pending_children = pending_children + ?2,
             updated_at = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) WHERE id = ?1",
            rusqlite::params![target_id, new_direct_count],
        )
        .map_err(internal)?;
        // A runnable target (Waiting/Delayed) must now block on its new children; an already-blocked
        // (WaitingChildren) target keeps its state and gauge, only its pending count grows.
        let target_regated = matches!(target_state.as_str(), "Waiting" | "Delayed");
        if target_regated {
            tx.execute(
                "UPDATE jobs SET state = 'WaitingChildren',
                 updated_at = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER) WHERE id = ?1",
                [&target_id],
            )
            .map_err(internal)?;
        }
        tx.commit().map_err(internal)?;
        // Flush metrics only after the commit succeeds.
        for node in &new_nodes {
            metrics.transition(None, node.state, "enqueued");
            metrics.named.event(&node.name, "enqueued");
        }
        metrics.flow_waiting_children_inc(waiting_children);
        if target_regated {
            // The target left a tracked runnable state for WaitingChildren (untracked by the per-
            // state gauge, counted by the dedicated gauge).
            metrics.transition(Some(target_state.as_str()), "WaitingChildren", "flow_merge_regate");
            metrics.flow_waiting_children_inc(1);
        }
        for _ in 0..receipts_created {
            metrics.enqueue_receipt_created();
        }
        metrics.flow_nodes_created(new_nodes.len() as u64);
        metrics.flow_merge();
        metrics.flow_merge_nodes(new_nodes.len() as u64);
        tracing::info!(parent_id = %target_id, attached = new_nodes.len(), "flow merge");
        Ok(AddFlowMergeResponse {
            parent_id: target_id,
            attached,
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
