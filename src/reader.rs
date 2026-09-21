//! Read-only WAL replica connection primitive for observability/search reads.
//!
//! SQLite in WAL mode permits one writer plus many concurrent readers. The broker holds a
//! single writer connection behind `Arc<Mutex<Connection>>` (see `db::DatabaseManager`); this
//! `Reader` opens SEPARATE read-only connections so heavy observability reads never contend on
//! the writer mutex nor stall enqueue/claim/complete. It mirrors the proven pattern in
//! `pressure.rs`: `OpenFlags::SQLITE_OPEN_READ_ONLY`, a tight `busy_timeout`, a
//! `progress_handler` that interrupts long-running queries, and execution on
//! `tokio::task::spawn_blocking`.
//!
//! Config (module consts document the defaults):
//!   * `ANVILMQ_READER_MAX_CONCURRENCY` — bound on concurrent reader connections (default 4,
//!     minimum 1; invalid or zero values normalize to the minimum).
//!   * `ANVILMQ_READER_TIMEOUT_MS` — per-query wall-clock budget (default 500).
use rusqlite::{Connection, OpenFlags};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Default bound on concurrent read-only connections. Keeps observability reads from opening
/// an unbounded number of SQLite connections under load.
const DEFAULT_MAX_CONCURRENCY: usize = 4;
/// Default per-query wall-clock budget in milliseconds.
const DEFAULT_TIMEOUT_MS: u64 = 500;
/// How often (in VDBE steps) the progress handler polls the elapsed clock.
const PROGRESS_STEPS: std::os::raw::c_int = 1000;
/// Busy timeout for a reader connection. Readers should never wait long on WAL locks.
const READER_BUSY_TIMEOUT: Duration = Duration::from_millis(25);

/// Failure modes surfaced by [`Reader::query`]. Callers map these to HTTP status codes
/// (Busy/Timeout/Unavailable → 503, Db → 500).
#[derive(Debug)]
pub enum ReaderError {
    /// The database was locked longer than the reader's busy timeout allowed.
    Busy,
    /// The query exceeded the configured wall-clock budget and was interrupted.
    Timeout,
    /// The reader connection could not be opened, or the blocking task was cancelled.
    Unavailable,
    /// Any other rusqlite error raised while running the caller's closure.
    Db(rusqlite::Error),
}

impl std::fmt::Display for ReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReaderError::Busy => write!(f, "reader database busy"),
            ReaderError::Timeout => write!(f, "reader query timed out"),
            ReaderError::Unavailable => write!(f, "reader unavailable"),
            ReaderError::Db(error) => write!(f, "reader query failed: {error}"),
        }
    }
}

impl std::error::Error for ReaderError {}

impl From<rusqlite::Error> for ReaderError {
    fn from(error: rusqlite::Error) -> Self {
        // SQLITE_BUSY / SQLITE_LOCKED surface as Busy so callers can shed load with 503.
        if let rusqlite::Error::SqliteFailure(inner, _) = &error {
            match inner.code {
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked => {
                    return ReaderError::Busy
                }
                // A progress-handler interrupt aborts the statement with SQLITE_INTERRUPT.
                rusqlite::ErrorCode::OperationInterrupted => return ReaderError::Timeout,
                _ => {}
            }
        }
        ReaderError::Db(error)
    }
}

/// Bounded, isolated read-only accessor over the WAL replica connection.
///
/// A fresh connection is opened per query (as in `pressure.rs`) which is simple and avoids any
/// shared mutable state; a connection pool is a possible later optimization if per-query open
/// cost ever matters.
#[derive(Clone)]
pub struct Reader {
    path: String,
    permits: Arc<Semaphore>,
    timeout: Duration,
}

impl Reader {
    /// Builds a reader from environment configuration. Invalid or zero concurrency normalizes to
    /// the minimum of 1; an unparsable timeout falls back to the default.
    pub fn from_env(path: String) -> Reader {
        let max_concurrency = std::env::var("ANVILMQ_READER_MAX_CONCURRENCY")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value >= 1)
            .unwrap_or(DEFAULT_MAX_CONCURRENCY);
        let timeout_ms = std::env::var("ANVILMQ_READER_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_TIMEOUT_MS);
        Reader {
            path,
            permits: Arc::new(Semaphore::new(max_concurrency)),
            timeout: Duration::from_millis(timeout_ms),
        }
    }

    /// Runs `f` against a fresh read-only connection under bounded concurrency and a per-query
    /// timeout. The connection is opened `SQLITE_OPEN_READ_ONLY` with `query_only=true`, so `f`
    /// can never mutate the database, and it NEVER acquires the broker's writer mutex.
    pub async fn query<T, F>(&self, f: F) -> Result<T, ReaderError>
    where
        F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        // Bound the number of concurrent reader connections. A closed semaphore is unreachable
        // here (we never close it), but treat acquisition failure as unavailability.
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| ReaderError::Unavailable)?;
        let path = self.path.clone();
        let timeout = self.timeout;
        let blocking = tokio::task::spawn_blocking(move || -> Result<T, ReaderError> {
            let start = Instant::now();
            let conn = Connection::open_with_flags(
                &path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
            )
            .map_err(|_| ReaderError::Unavailable)?;
            // Defense in depth: reject any accidental write from the caller's closure.
            conn.pragma_update(None, "query_only", true)
                .map_err(ReaderError::from)?;
            conn.busy_timeout(READER_BUSY_TIMEOUT)
                .map_err(ReaderError::from)?;
            // Interrupt the statement once the wall-clock budget is exhausted (mirrors pressure.rs).
            conn.progress_handler(PROGRESS_STEPS, Some(move || start.elapsed() > timeout));
            f(&conn).map_err(ReaderError::from)
        });
        // Belt-and-suspenders: bound the whole operation even if the progress handler is slow to
        // fire (e.g. a query blocked before executing any VDBE steps). Add slack so the in-query
        // interrupt normally wins and produces the more precise Timeout mapping.
        match tokio::time::timeout(
            timeout + READER_BUSY_TIMEOUT + Duration::from_millis(50),
            blocking,
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(_join_error)) => Err(ReaderError::Unavailable),
            Err(_elapsed) => Err(ReaderError::Timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{DatabaseManager, Durability};

    fn temp_db_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("anvil-reader-{}.db", uuid::Uuid::new_v4()))
    }

    async fn seed_history(db: &DatabaseManager, rows: &[(&str, &str, &str, i64, i64)]) {
        // rows: (id, name, state, attempts, finished_at)
        let conn = db.get_shared_connection();
        let rows: Vec<_> = rows
            .iter()
            .map(|(id, name, state, attempts, finished)| {
                (
                    id.to_string(),
                    name.to_string(),
                    state.to_string(),
                    *attempts,
                    *finished,
                )
            })
            .collect();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            for (id, name, state, attempts, finished) in rows {
                conn.execute(
                    "INSERT INTO job_history (id, name, state, priority, payload, parent_id, trace_id, execution_depth, attempts, max_attempts, created_at, finished_at, last_error) \
                     VALUES (?1, ?2, ?3, 0, X'', NULL, 'trace', 0, ?4, 3, 0, ?5, 'boom')",
                    rusqlite::params![id, name, state, attempts, finished],
                )
                .unwrap();
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn reads_seeded_rows_read_only() {
        let path = temp_db_path();
        let db = DatabaseManager::with_durability(path.to_str().unwrap(), Durability::Normal)
            .await
            .unwrap();
        seed_history(&db, &[("a", "email", "Failed", 3, 100)]).await;
        let reader = Reader::from_env(path.to_str().unwrap().to_string());
        let count: i64 = reader
            .query(|conn| conn.query_row("SELECT COUNT(*) FROM job_history", [], |r| r.get(0)))
            .await
            .unwrap();
        assert_eq!(count, 1);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn reads_do_not_block_on_held_writer_mutex() {
        let path = temp_db_path();
        let db = DatabaseManager::with_durability(path.to_str().unwrap(), Durability::Normal)
            .await
            .unwrap();
        seed_history(&db, &[("a", "email", "Failed", 3, 100)]).await;
        let reader = Reader::from_env(path.to_str().unwrap().to_string());
        // Hold the writer mutex for the whole read to prove readers are isolated from it.
        let conn = db.get_shared_connection();
        let guard = conn.lock().await;
        let count: i64 = reader
            .query(|conn| conn.query_row("SELECT COUNT(*) FROM job_history", [], |r| r.get(0)))
            .await
            .unwrap();
        assert_eq!(count, 1);
        drop(guard);
        drop(conn);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn query_only_rejects_writes() {
        let path = temp_db_path();
        let db = DatabaseManager::with_durability(path.to_str().unwrap(), Durability::Normal)
            .await
            .unwrap();
        let reader = Reader::from_env(path.to_str().unwrap().to_string());
        let result = reader
            .query(|conn| {
                conn.execute(
                    "INSERT INTO job_history (id, name, state, priority, payload, trace_id, execution_depth, attempts, max_attempts, created_at, finished_at) \
                     VALUES ('x','n','Failed',0,X'','t',0,0,3,0,0)",
                    [],
                )
            })
            .await;
        assert!(matches!(
            result,
            Err(ReaderError::Db(_)) | Err(ReaderError::Busy)
        ));
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn from_env_normalizes_invalid_concurrency() {
        // No env set: defaults apply and a query still works.
        let path = temp_db_path();
        let db = DatabaseManager::with_durability(path.to_str().unwrap(), Durability::Normal)
            .await
            .unwrap();
        let reader = Reader::from_env(path.to_str().unwrap().to_string());
        assert!(reader.permits.available_permits() >= 1);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}
