//! Read-only query-cost experiment on a synthetic in-memory database; never opens anvil.db.
use rusqlite::{Connection, StatementStatus};

fn measure(conn: &Connection, label: &str, sql: &str, count: usize) -> rusqlite::Result<()> {
    let mut statement = conn.prepare(sql)?;
    statement.reset_status(StatementStatus::VmStep);
    let begin = std::time::Instant::now();
    for _ in 0..20 {
        let mut rows = statement.query(rusqlite::params![1000_i64, "load"])?;
        while rows.next()?.is_some() {}
    }
    println!(
        "{label},{count},{:.3},{}",
        begin.elapsed().as_secs_f64() * 1000.0 / 20.0,
        statement.get_status(StatementStatus::VmStep) / 20
    );
    Ok(())
}
fn main() -> rusqlite::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE jobs(id TEXT PRIMARY KEY, name TEXT, state TEXT, priority INTEGER, payload BLOB, parent_id TEXT, trace_id TEXT, execution_depth INTEGER, attempts INTEGER, created_at INTEGER, available_at INTEGER, rate_limit_facet TEXT);
        CREATE TABLE rate_limit_rules(facet_pattern TEXT PRIMARY KEY, max_jobs INTEGER, window_duration_ms INTEGER);
        CREATE TABLE rate_limit_counters(facet_key TEXT PRIMARY KEY, current_count INTEGER, window_expires_at INTEGER);
        CREATE INDEX idx_jobs_waiting_queue ON jobs(state, priority ASC, created_at ASC) WHERE state = 'Waiting';")?;
    conn.execute_batch("CREATE INDEX idx_jobs_schedulable ON jobs(priority, created_at, id, available_at) WHERE state IN ('Waiting','Delayed');")?;
    let source = include_str!("../src/main.rs");
    let blocked = source
        .split("let blocked = \"")
        .nth(1)
        .expect("dequeue predicate")
        .split('"')
        .next()
        .unwrap();
    let base = "SELECT id, name, payload, parent_id, trace_id, execution_depth, attempts, state FROM jobs WHERE state IN ('Waiting','Delayed') AND available_at <= ?1 AND name IN (?2)";
    let probe = format!("SELECT EXISTS({base} AND {blocked})");
    let guarded = format!("SELECT CASE WHEN EXISTS(SELECT 1 FROM rate_limit_rules) THEN EXISTS({base} AND {blocked}) ELSE 0 END");
    let select =
        format!("{base} AND NOT {blocked} ORDER BY priority ASC, created_at ASC, id ASC LIMIT 1");
    println!("SQLite {}", rusqlite::version());
    for (label, sql) in [("throttle_probe", &probe), ("candidate_select", &select)] {
        let mut plan = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        let rows = plan.query_map(rusqlite::params![1000_i64, "load"], |r| {
            r.get::<_, String>(3)
        })?;
        for row in rows {
            println!("PLAN {label}: {}", row?);
        }
    }
    println!("query,waiting_jobs,mean_ms,vm_steps_per_query");
    for count in [0, 100, 1000, 10000, 20000] {
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM jobs", [])?;
        for id in 0..count {
            tx.execute("INSERT INTO jobs(id,name,state,priority,payload,trace_id,execution_depth,attempts,available_at,created_at) VALUES (?1,'load','Waiting',0,X'00','trace',0,0,0,?2)", rusqlite::params![id.to_string(), id])?;
        }
        tx.commit()?;
        measure(&conn, "throttle_probe", &probe, count)?;
        measure(&conn, "candidate_select", &select, count)?;
        measure(&conn, "guarded_probe_no_rules", &guarded, count)?;
    }
    Ok(())
}
