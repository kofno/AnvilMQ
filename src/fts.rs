//! Opt-in full-text search index over `job_history` metadata (SQLite FTS5).
//!
//! AnvilMQ's `/v1/search` defaults to an escaped-LIKE scan over `job_history`. When
//! `ANVILMQ_FTS_ENABLED` is truthy, the broker additionally maintains a standalone FTS5 virtual
//! table (`job_history_fts`) over a bounded set of metadata columns and resolves free-text `q`
//! queries with `MATCH` instead of LIKE.
//!
//! Design notes:
//!   * **Off by default, zero write cost when disabled.** The virtual table and triggers only
//!     exist when the flag is set; enqueue never touches `job_history`, so the hot path is never
//!     affected either way. All index cost falls on terminal writes (complete/fail) and is carried
//!     by SQLite triggers, not per-call-site Rust.
//!   * **Trigger-maintained.** `job_history` rows are insert-then-delete only (never updated in
//!     place), so an `AFTER INSERT` and an `AFTER DELETE` trigger keep the index exactly in sync
//!     across every write site (lifecycle RPCs, lease-recovery failure) and every delete
//!     (retention sweeps) — no `AFTER UPDATE` is required.
//!   * **Keyed by `rowid`.** `job_history.id` is a TEXT primary key, so the table keeps a stable
//!     implicit integer `rowid`; retention relies on page reuse rather than VACUUM, so rowids are
//!     stable. Mirroring `rowid` into the FTS table makes delete propagation a plain
//!     delete-by-rowid.
//!   * **Metadata only (v1).** Indexed columns are `id` (UNINDEXED — retrieval only), `name`,
//!     `trace_id`, and `last_error`. The payload BLOB is intentionally not indexed (a future
//!     opt-in).

use rusqlite::Connection;

/// Environment flag that turns the FTS index and query path on. Accepts `1`, `true`, or `yes`
/// (ASCII case-insensitive, surrounding whitespace ignored); anything else — including unset —
/// leaves FTS disabled.
pub const ENABLE_ENV: &str = "ANVILMQ_FTS_ENABLED";

/// Default backfill batch size: how many existing `job_history` rows are copied into the index per
/// statement at startup. Bounds the writer hold time of each backfill step.
pub const DEFAULT_BACKFILL_BATCH: i64 = 1_000;

/// Standalone FTS5 table plus the two triggers that keep it in sync. All `IF NOT EXISTS`, so
/// running this on every startup is idempotent.
const SETUP_SQL: &str = "
    CREATE VIRTUAL TABLE IF NOT EXISTS job_history_fts USING fts5(
        id UNINDEXED,
        name,
        trace_id,
        last_error
    );

    CREATE TRIGGER IF NOT EXISTS job_history_fts_ai AFTER INSERT ON job_history BEGIN
        INSERT INTO job_history_fts(rowid, id, name, trace_id, last_error)
        VALUES (new.rowid, new.id, new.name, new.trace_id, new.last_error);
    END;

    CREATE TRIGGER IF NOT EXISTS job_history_fts_ad AFTER DELETE ON job_history BEGIN
        DELETE FROM job_history_fts WHERE rowid = old.rowid;
    END;
";

/// Returns true when `ANVILMQ_FTS_ENABLED` is set to a recognized truthy value.
pub fn enabled_from_env() -> bool {
    std::env::var(ENABLE_ENV)
        .map(|value| is_truthy(&value))
        .unwrap_or(false)
}

fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes"
    )
}

/// Creates the FTS5 virtual table and its insert/delete triggers if they do not already exist.
/// Idempotent; safe to run on every startup.
pub fn setup_blocking(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(SETUP_SQL)
}

/// Copies existing `job_history` rows into the index in bounded batches until none remain, then
/// returns the total number of rows backfilled. Idempotent: rows already present (matched by
/// `rowid`) are skipped, so a partial or repeated run cannot double-insert. Intended to run once at
/// startup, off the hot path.
pub fn backfill_blocking(conn: &Connection, batch: i64) -> rusqlite::Result<usize> {
    let batch = batch.max(1);
    let mut total = 0usize;
    loop {
        let inserted = conn.execute(
            "INSERT INTO job_history_fts(rowid, id, name, trace_id, last_error)
             SELECT h.rowid, h.id, h.name, h.trace_id, h.last_error
             FROM job_history h
             WHERE NOT EXISTS (SELECT 1 FROM job_history_fts f WHERE f.rowid = h.rowid)
             LIMIT ?1",
            [batch],
        )?;
        total += inserted;
        if inserted == 0 {
            break;
        }
    }
    Ok(total)
}

/// Turns raw user text into a safe FTS5 `MATCH` expression, or `None` when it carries no usable
/// tokens. Each whitespace-separated token is wrapped in double quotes (with embedded quotes
/// doubled), so every FTS5 operator character (`* : ( ) ^ - AND OR NOT`) is treated as a literal
/// string term. This guarantees a pathological `q` can never raise an FTS5 syntax error; it either
/// matches literally or matches nothing.
///
/// **Prefix wildcard (opt-in via an explicit trailing `*`).** A token that ends in one or more `*`
/// (e.g. `upstrea*`) becomes a whole-token FTS5 prefix query: the trailing stars are stripped, the
/// remaining stem is quoted/escaped, and a single bare `*` is appended *outside* the closing quote
/// (`"upstrea"*`) — valid FTS5 prefix syntax that matches `upstream`, `upstream-svc`, etc. Only a
/// *trailing* `*` triggers this; a `*` anywhere else (`foo*bar`, `*word`) stays inside the quoted
/// phrase as a literal. A token that is only stars (or stars after quotes, e.g. `*`, `**`, `"*`)
/// has an empty/contentless stem and is skipped like a quote-only token. Tokens without a trailing
/// `*` are emitted byte-for-byte as before.
pub fn sanitize_match(query: &str) -> Option<String> {
    let mut terms: Vec<String> = Vec::new();
    for token in query.split_whitespace() {
        // A trailing run of `*` requests a whole-token prefix query; strip it to find the stem.
        let stem = token.trim_end_matches('*');
        let is_prefix = stem.len() != token.len();

        // A stem made up entirely of quote characters (or emptied by stripping stars) carries no
        // searchable content; skip it so it never contributes an empty phrase to the MATCH
        // expression.
        if stem.chars().all(|c| c == '"') {
            continue;
        }
        let escaped = stem.replace('"', "\"\"");
        if is_prefix {
            // The bare `*` goes OUTSIDE the closing quote — FTS5 prefix syntax on a quoted phrase.
            terms.push(format!("\"{escaped}\"*"));
        } else {
            terms.push(format!("\"{escaped}\""));
        }
    }
    if terms.is_empty() {
        None
    } else {
        Some(terms.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE job_history (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL,
                 trace_id TEXT,
                 last_error TEXT
             );",
        )
        .unwrap();
        setup_blocking(conn).unwrap();
    }

    fn insert(conn: &Connection, id: &str, name: &str, trace: &str, err: Option<&str>) {
        conn.execute(
            "INSERT INTO job_history (id, name, trace_id, last_error) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![id, name, trace, err],
        )
        .unwrap();
    }

    fn match_ids(conn: &Connection, expr: &str) -> Vec<String> {
        let mut stmt = conn
            .prepare(
                "SELECT h.id FROM job_history_fts f JOIN job_history h ON h.rowid = f.rowid
                 WHERE job_history_fts MATCH ?1 ORDER BY h.id",
            )
            .unwrap();
        let rows = stmt
            .query_map([expr], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        rows
    }

    #[test]
    fn is_truthy_accepts_only_recognized_values() {
        for yes in ["1", "true", "TRUE", "Yes", " yes ", "tRuE"] {
            assert!(is_truthy(yes), "{yes:?} should be truthy");
        }
        for no in ["", "0", "false", "no", "off", "2", "enabled", "y"] {
            assert!(!is_truthy(no), "{no:?} should not be truthy");
        }
    }

    #[test]
    fn insert_trigger_indexes_tokens_in_columns() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        insert(
            &conn,
            "job-1",
            "email",
            "trace-a",
            Some("timeout talking to smtp"),
        );
        insert(&conn, "job-2", "sms", "trace-b", None);

        // A word inside last_error is matchable.
        assert_eq!(
            match_ids(&conn, &sanitize_match("smtp").unwrap()),
            vec!["job-1"]
        );
        // name column is indexed.
        assert_eq!(
            match_ids(&conn, &sanitize_match("sms").unwrap()),
            vec!["job-2"]
        );
        // trace_id column is indexed.
        assert_eq!(
            match_ids(&conn, &sanitize_match("trace-a").unwrap()),
            vec!["job-1"]
        );
    }

    #[test]
    fn delete_trigger_removes_row_from_index() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        insert(&conn, "job-1", "email", "trace-a", Some("boom"));
        assert_eq!(
            match_ids(&conn, &sanitize_match("boom").unwrap()),
            vec!["job-1"]
        );
        conn.execute("DELETE FROM job_history WHERE id = 'job-1'", [])
            .unwrap();
        assert!(match_ids(&conn, &sanitize_match("boom").unwrap()).is_empty());
    }

    #[test]
    fn backfill_is_idempotent_and_bounded() {
        let conn = Connection::open_in_memory().unwrap();
        // Create the base table WITHOUT the triggers, seed it, then set up FTS and backfill.
        conn.execute_batch(
            "CREATE TABLE job_history (
                 id TEXT PRIMARY KEY, name TEXT NOT NULL, trace_id TEXT, last_error TEXT
             );",
        )
        .unwrap();
        for i in 0..5 {
            insert(
                &conn,
                &format!("job-{i}"),
                "email",
                "trace",
                Some("preexisting failure"),
            );
        }
        setup_blocking(&conn).unwrap();

        // First backfill copies all 5 rows (batch smaller than the set to exercise looping).
        assert_eq!(backfill_blocking(&conn, 2).unwrap(), 5);
        assert_eq!(
            match_ids(&conn, &sanitize_match("preexisting").unwrap()).len(),
            5
        );
        // Re-running is a no-op — rows already present are skipped, never double-inserted.
        assert_eq!(backfill_blocking(&conn, 2).unwrap(), 0);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM job_history_fts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 5);
    }

    #[test]
    fn sanitize_neutralizes_operators_and_never_errors() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        insert(&conn, "job-1", "email", "trace", Some("connection refused"));

        // Operator-laden and quote-only inputs must produce a MATCH that runs without error.
        for hostile in ["a AND ( OR *:^", "\"\"\"", "NOT refused -foo", "((("] {
            let result = sanitize_match(hostile)
                .map(|expr| {
                    conn.prepare("SELECT rowid FROM job_history_fts WHERE job_history_fts MATCH ?1")
                        .unwrap()
                        .query_map([expr], |r| r.get::<_, i64>(0))
                        .unwrap()
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .transpose();
            assert!(
                result.is_ok(),
                "hostile query {hostile:?} must not raise an FTS5 error"
            );
        }

        // A quote-only string carries no usable token.
        assert_eq!(sanitize_match("\"\""), None);
        assert_eq!(sanitize_match("   "), None);

        // Operators are matched literally, so a bare operator word finds nothing here.
        let expr = sanitize_match("refused").unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM job_history_fts WHERE job_history_fts MATCH ?1",
                [expr],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
    }

    #[test]
    fn sanitize_prefix_wildcard_string_forms() {
        // A trailing `*` becomes a quoted-phrase prefix query (`*` outside the closing quote).
        assert_eq!(sanitize_match("upstrea*").as_deref(), Some("\"upstrea\"*"));
        // No trailing `*` is byte-for-byte unchanged.
        assert_eq!(sanitize_match("foo").as_deref(), Some("\"foo\""));
        // A token that is only stars has an empty stem and is skipped.
        assert_eq!(sanitize_match("*"), None);
        assert_eq!(sanitize_match("**"), None);
        // A `*` that is not trailing stays a literal inside the quoted phrase, not a prefix.
        assert_eq!(sanitize_match("foo*bar").as_deref(), Some("\"foo*bar\""));
        // Multiple trailing stars collapse to a single prefix query.
        assert_eq!(sanitize_match("word**").as_deref(), Some("\"word\"*"));
        // Mixed tokens: exact phrase AND prefix phrase.
        assert_eq!(
            sanitize_match("connection upstrea*").as_deref(),
            Some("\"connection\" \"upstrea\"*")
        );
    }

    #[test]
    fn sanitize_prefix_wildcard_live_matches() {
        let conn = Connection::open_in_memory().unwrap();
        setup(&conn);
        insert(&conn, "job-1", "upstream", "trace-a", Some("boom"));
        insert(&conn, "job-2", "upstreamer", "trace-b", None);
        insert(&conn, "job-3", "downstream", "trace-c", None);

        // A trailing-`*` prefix query matches every token that begins with the stem.
        assert_eq!(
            match_ids(&conn, &sanitize_match("upstrea*").unwrap()),
            vec!["job-1", "job-2"]
        );
        // The exact (no-star) form still matches the whole token only.
        assert_eq!(
            match_ids(&conn, &sanitize_match("upstream").unwrap()),
            vec!["job-1"]
        );
        // Regression guard: a partial stem WITHOUT a `*` is a literal token and matches nothing —
        // we did not silently turn every query into a prefix search.
        assert!(match_ids(&conn, &sanitize_match("upstrea").unwrap()).is_empty());

        // Implicit-AND across an exact token and a prefix token narrows to rows carrying both.
        insert(
            &conn,
            "job-4",
            "email",
            "trace-d",
            Some("connection to upstream"),
        );
        assert_eq!(
            match_ids(&conn, &sanitize_match("connection upstrea*").unwrap()),
            vec!["job-4"]
        );

        // Hostile wildcard-adjacent inputs never build invalid MATCH SQL.
        for hostile in ["*", "\"*", "up*stream"] {
            let ran = sanitize_match(hostile)
                .map(|expr| {
                    conn.prepare("SELECT rowid FROM job_history_fts WHERE job_history_fts MATCH ?1")
                        .unwrap()
                        .query_map([expr], |r| r.get::<_, i64>(0))
                        .unwrap()
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .map(|rows| rows.len())
                })
                .transpose()
                .expect("hostile prefix input must not raise an FTS5 error");
            // `up*stream` is a literal token that matches nothing; `*`/`"*` sanitize to None.
            assert_eq!(
                ran.unwrap_or(0),
                0,
                "hostile {hostile:?} should match nothing"
            );
        }
    }
}
