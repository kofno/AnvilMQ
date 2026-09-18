use crate::{db::DatabaseManager, leases::now_ms, queue::v1::*};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior};
use std::sync::Arc;
use tonic::Status;

fn validate(key: &str) -> Result<(), Status> {
    if key.trim().is_empty() || key != key.trim() || key.contains(['*', '?']) {
        return Err(Status::invalid_argument(
            "facet must be an exact nonblank key without surrounding whitespace or wildcards",
        ));
    }
    Ok(())
}
fn internal(e: rusqlite::Error) -> Status {
    Status::internal(e.to_string())
}

pub async fn upsert(
    db: Arc<DatabaseManager>,
    req: UpsertRateLimitRuleRequest,
) -> Result<UpsertRateLimitRuleResponse, Status> {
    validate(&req.facet_pattern)?;
    if req.window_duration_ms <= 0 {
        return Err(Status::invalid_argument(
            "window_duration_ms must be positive",
        ));
    }
    let conn = db.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let mut conn = conn.blocking_lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(internal)?;
        now_ms(&tx).map_err(internal)?.checked_add(req.window_duration_ms).ok_or_else(|| Status::invalid_argument("window timestamp overflows"))?;
        tx.execute("INSERT INTO rate_limit_rules(facet_pattern, max_jobs, window_duration_ms) VALUES (?1, ?2, ?3) ON CONFLICT(facet_pattern) DO UPDATE SET max_jobs = excluded.max_jobs, window_duration_ms = excluded.window_duration_ms", rusqlite::params![req.facet_pattern, req.max_jobs, req.window_duration_ms]).map_err(internal)?;
        tx.execute("DELETE FROM rate_limit_counters WHERE facet_key = ?1", [&req.facet_pattern]).map_err(internal)?;
        tx.commit().map_err(internal)?;
        tracing::info!(max_jobs = req.max_jobs, window_duration_ms = req.window_duration_ms, "rate limit rule upserted; window reset");
        Ok(UpsertRateLimitRuleResponse { success: true })
    }).await.map_err(|e| Status::internal(e.to_string()))?
}

pub async fn delete(
    db: Arc<DatabaseManager>,
    req: DeleteRateLimitRuleRequest,
) -> Result<DeleteRateLimitRuleResponse, Status> {
    validate(&req.facet_key)?;
    let conn = db.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let mut conn = conn.blocking_lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(internal)?;
        let deleted = tx
            .execute(
                "DELETE FROM rate_limit_rules WHERE facet_pattern = ?1",
                [&req.facet_key],
            )
            .map_err(internal)?
            > 0;
        tx.execute(
            "DELETE FROM rate_limit_counters WHERE facet_key = ?1",
            [&req.facet_key],
        )
        .map_err(internal)?;
        tx.commit().map_err(internal)?;
        Ok(DeleteRateLimitRuleResponse { deleted })
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
}

pub async fn status(
    db: Arc<DatabaseManager>,
    req: GetRateLimitStatusRequest,
) -> Result<GetRateLimitStatusResponse, Status> {
    validate(&req.facet_key)?;
    let conn = db.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let conn = conn.blocking_lock();
        let now = now_ms(&conn).map_err(internal)?;
        let rule = conn.query_row("SELECT r.max_jobs, r.window_duration_ms, COALESCE(c.current_count, 0), COALESCE(c.window_expires_at, 0) FROM rate_limit_rules r LEFT JOIN rate_limit_counters c ON c.facet_key = r.facet_pattern WHERE r.facet_pattern = ?1", [&req.facet_key], |r| Ok((r.get::<_, u32>(0)?, r.get::<_, i64>(1)?, r.get::<_, u32>(2)?, r.get::<_, i64>(3)?))).optional().map_err(internal)?;
        let Some((max_jobs, duration, count, expires)) = rule else {
            return Ok(GetRateLimitStatusResponse { facet_key: req.facet_key, ..Default::default() });
        };
        let count = if expires > now { count } else { 0 };
        Ok(GetRateLimitStatusResponse { facet_key: req.facet_key, current_count: count, max_jobs, window_expires_at: if expires > now { expires } else { 0 }, is_throttled: max_jobs == 0 || count >= max_jobs, rule_exists: true, window_duration_ms: duration })
    }).await.map_err(|e| Status::internal(e.to_string()))?
}

// Called only after eligibility selection in the same immediate transaction as the claim.
pub fn consume(tx: &Transaction<'_>, id: &str, now: i64) -> rusqlite::Result<()> {
    let rule = tx.query_row("SELECT r.facet_pattern, r.window_duration_ms FROM jobs j JOIN rate_limit_rules r ON r.facet_pattern = j.rate_limit_facet WHERE j.id = ?1", [id], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))).optional()?;
    if let Some((facet, duration)) = rule {
        tx.execute("INSERT INTO rate_limit_counters(facet_key, current_count, window_expires_at) VALUES (?1, 1, ?2)
            ON CONFLICT(facet_key) DO UPDATE SET
            current_count = CASE WHEN window_expires_at <= ?3 THEN 1 ELSE current_count + 1 END,
            window_expires_at = CASE WHEN window_expires_at <= ?3 THEN excluded.window_expires_at ELSE window_expires_at END", rusqlite::params![facet, now.saturating_add(duration), now])?;
    }
    Ok(())
}

// SQLite evaluates CASE branches lazily; an empty rule table must not scan queued jobs.
pub fn throttled_poll_sql(candidates: &str, blocked: &str) -> String {
    format!("SELECT CASE WHEN EXISTS(SELECT 1 FROM rate_limit_rules) THEN EXISTS({candidates} AND {blocked}) ELSE 0 END")
}

#[cfg(test)]
mod query_tests {
    use super::*;
    use rusqlite::{Connection, StatementStatus};

    #[test]
    fn no_rules_probe_cost_does_not_grow_with_backlog() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE rate_limit_rules(facet_pattern TEXT PRIMARY KEY); CREATE TABLE jobs(facet TEXT);
            INSERT INTO jobs VALUES ('limited');").unwrap();
        let sql = throttled_poll_sql(
            "SELECT 1 FROM jobs WHERE 1=1",
            "EXISTS(SELECT 1 FROM rate_limit_rules WHERE facet_pattern = jobs.facet)",
        );
        fn run(conn: &Connection, sql: &str) -> (bool, i32) {
            let mut stmt = conn.prepare(sql).unwrap();
            let result = stmt.query_row([], |r| r.get(0)).unwrap();
            (result, stmt.get_status(StatementStatus::VmStep))
        }
        let small = run(&conn, &sql);
        conn.execute_batch("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i < 10000) INSERT INTO jobs SELECT 'limited' FROM n;").unwrap();
        let large = run(&conn, &sql);
        assert!(!small.0 && !large.0);
        assert_eq!(
            small.1, large.1,
            "empty-rule probe must not visit backlog rows"
        );
        conn.execute("INSERT INTO rate_limit_rules VALUES ('limited')", [])
            .unwrap();
        assert!(
            run(&conn, &sql).0,
            "adding a rule must enable the full probe"
        );
        conn.execute("DELETE FROM rate_limit_rules", []).unwrap();
        assert_eq!(run(&conn, &sql), small);
    }
}
