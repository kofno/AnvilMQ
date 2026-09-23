use rusqlite::{Connection, Result as SqlResult};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    Normal,
    Full,
}

impl Durability {
    pub fn parse(value: &str) -> Result<Self, std::io::Error> {
        match value.to_ascii_uppercase().as_str() {
            "NORMAL" => Ok(Self::Normal),
            "FULL" => Ok(Self::Full),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ANVILMQ_DURABILITY must be NORMAL or FULL",
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Full => "FULL",
        }
    }

    fn synchronous(self) -> i64 {
        match self {
            Self::Normal => 1,
            Self::Full => 2,
        }
    }
}

pub struct DatabaseManager {
    // Wrap connection in an Arc<Mutex> for safe concurrent async access
    conn: Arc<Mutex<Connection>>,
    pub metrics: Arc<crate::telemetry::Metrics>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn durability_is_validated_and_applied_on_each_open() {
        assert_eq!(Durability::parse("full").unwrap(), Durability::Full);
        assert_eq!(Durability::parse("NORMAL").unwrap(), Durability::Normal);
        for invalid in ["", "OFF", "EXTRA", "1", "typo"] {
            assert!(Durability::parse(invalid).is_err());
        }
        let path =
            std::env::temp_dir().join(format!("anvil-durability-{}.db", uuid::Uuid::new_v4()));
        for durability in [Durability::Normal, Durability::Full, Durability::Normal] {
            let db = DatabaseManager::with_durability(path.to_str().unwrap(), durability)
                .await
                .unwrap();
            let conn = db.get_shared_connection();
            tokio::task::spawn_blocking(move || {
                let conn = conn.blocking_lock();
                let mode: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0)).unwrap();
                let sync: i64 = conn.pragma_query_value(None, "synchronous", |r| r.get(0)).unwrap();
                assert_eq!(mode, "wal");
                assert_eq!(sync, durability.synchronous());
                conn.execute_batch("CREATE TABLE IF NOT EXISTS durability_probe(id INTEGER PRIMARY KEY); INSERT OR IGNORE INTO durability_probe VALUES(1);").unwrap();
                assert_eq!(conn.query_row("SELECT COUNT(*) FROM durability_probe", [], |r| r.get::<_, i64>(0)).unwrap(), 1);
            }).await.unwrap();
        }
        tokio::task::spawn_blocking(move || std::fs::remove_file(path).unwrap())
            .await
            .unwrap();
    }

    #[test]
    fn migration_preserves_legacy_jobs_and_is_repeatable() {
        let mut conn = Connection::open_in_memory().unwrap();
        DatabaseManager::run_migrations(&mut conn).unwrap();
        conn.execute("ALTER TABLE jobs DROP COLUMN worker_id", [])
            .unwrap();
        conn.execute_batch(
            "DROP INDEX idx_jobs_pressure;
            DROP INDEX idx_jobs_active_lease;
            DROP INDEX idx_chain_counters_updated;
            DROP TABLE chain_counters;
            ALTER TABLE jobs DROP COLUMN lease_expires_at_ms;
            ALTER TABLE jobs DROP COLUMN last_error;
            ALTER TABLE job_history DROP COLUMN worker_id;
            ALTER TABLE job_history DROP COLUMN last_error;
            ALTER TABLE job_history DROP COLUMN rate_limit_facet;",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO jobs (id, name, state, priority, payload, trace_id, execution_depth, created_at, updated_at)
             VALUES ('existing', 'email', 'Waiting', 0, X'01', 'trace', 0, 1, 1)", [],
        ).unwrap();
        DatabaseManager::run_migrations(&mut conn).unwrap();
        DatabaseManager::run_migrations(&mut conn).unwrap();
        let row = conn
            .query_row(
                "SELECT state, worker_id FROM jobs WHERE id = 'existing'",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .unwrap();
        assert_eq!(row, ("Waiting".into(), None));
        let error: Option<String> = conn
            .query_row(
                "SELECT last_error FROM jobs WHERE id = 'existing'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(error, None);
        conn.prepare("SELECT worker_id, last_error, rate_limit_facet FROM job_history")
            .unwrap();
        conn.prepare("SELECT trace_id, job_count, updated_at FROM chain_counters")
            .unwrap();
    }
}

impl DatabaseManager {
    #[cfg(test)]
    pub async fn new(path: &str) -> SqlResult<Self> {
        Self::with_durability(path, Durability::Normal).await
    }

    pub async fn with_durability(path: &str, durability: Durability) -> SqlResult<Self> {
        let path = path.to_string();

        // SQLite operations are blocking, so we initialize and migrate on a blocking thread
        let (conn, metrics) = tokio::task::spawn_blocking(move || {
            let mut conn = Connection::open(&path)?;
            conn.busy_timeout(std::time::Duration::from_secs(5))?;

            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", durability.synchronous())?;
            let journal: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
            let synchronous: i64 = conn.pragma_query_value(None, "synchronous", |r| r.get(0))?;
            // In-memory unit tests use MEMORY journaling. File-backed brokers require WAL.
            if synchronous != durability.synchronous() || (path != ":memory:" && journal != "wal") {
                return Err(rusqlite::Error::InvalidQuery);
            }
            tracing::info!(durability = durability.name(), journal_mode = %journal, synchronous, "database durability configured");

            Self::run_migrations(&mut conn)?;
            let metrics = crate::telemetry::Metrics::initialize(&conn)?;
            Ok::<_, rusqlite::Error>((conn, metrics))
        })
        .await
        .unwrap()?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            metrics: Arc::new(metrics),
        })
    }

    pub fn get_shared_connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.conn)
    }

    /// Sets up the opt-in FTS5 index over `job_history` and backfills existing rows. Creates the
    /// virtual table plus insert/delete triggers (idempotent) and copies any not-yet-indexed
    /// history rows in bounded batches, returning the number backfilled. Runs on a blocking thread
    /// against the shared writer connection; intended for startup only, off the hot path.
    pub async fn enable_fts(&self, batch: i64) -> SqlResult<usize> {
        let conn = self.get_shared_connection();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            crate::fts::setup_blocking(&conn)?;
            crate::fts::backfill_blocking(&conn, batch)
        })
        .await
        .unwrap()
    }

    fn run_migrations(conn: &mut Connection) -> SqlResult<()> {
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                state TEXT NOT NULL,
                priority INTEGER NOT NULL,
                payload BLOB NOT NULL,
                parent_id TEXT,
                trace_id TEXT NOT NULL,
                execution_depth INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                rate_limit_facet TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_waiting_queue 
            ON jobs (state, priority ASC, created_at ASC) 
            WHERE state = 'Waiting';

            CREATE TABLE IF NOT EXISTS job_history (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                state TEXT NOT NULL,
                priority INTEGER NOT NULL,
                payload BLOB NOT NULL,
                parent_id TEXT,
                trace_id TEXT NOT NULL,
                execution_depth INTEGER NOT NULL,
                attempts INTEGER NOT NULL,
                max_attempts INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                finished_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rate_limit_rules (
                facet_pattern TEXT PRIMARY KEY,
                max_jobs INTEGER NOT NULL,
                window_duration_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS enqueue_receipts (
                queue_name TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                request BLOB NOT NULL,
                job_id TEXT NOT NULL,
                initial_state TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (queue_name, idempotency_key)
            );

            CREATE TABLE IF NOT EXISTS rate_limit_counters (
                facet_key TEXT PRIMARY KEY,
                current_count INTEGER NOT NULL,
                window_expires_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ingress_limit_rules (
                facet_pattern TEXT PRIMARY KEY,
                max_jobs INTEGER NOT NULL,
                window_duration_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ingress_limit_counters (
                facet_key TEXT PRIMARY KEY,
                current_window_start INTEGER NOT NULL,
                current_count INTEGER NOT NULL,
                previous_count INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS chain_counters (
                trace_id TEXT PRIMARY KEY,
                job_count INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_chain_counters_updated
            ON chain_counters (updated_at);
            ",
        )?;

        // Preserve existing databases created before worker ownership was introduced.
        let has_worker_id = {
            let mut statement = tx.prepare("PRAGMA table_info(jobs)")?;
            let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
            columns
                .collect::<SqlResult<Vec<_>>>()?
                .iter()
                .any(|name| name == "worker_id")
        };
        if !has_worker_id {
            tx.execute("ALTER TABLE jobs ADD COLUMN worker_id TEXT", [])?;
        }
        // These identifiers are fixed migration definitions, never request input.
        for (table, column) in [
            ("jobs", "last_error"),
            ("job_history", "worker_id"),
            ("job_history", "last_error"),
            ("job_history", "rate_limit_facet"),
        ] {
            let exists = {
                let mut statement = tx.prepare(&format!("PRAGMA table_info({table})"))?;
                let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
                columns
                    .collect::<SqlResult<Vec<_>>>()?
                    .iter()
                    .any(|name| name == column)
            };
            if !exists {
                tx.execute(&format!("ALTER TABLE {table} ADD COLUMN {column} TEXT"), [])?;
            }
        }
        let has_lease = {
            let mut statement = tx.prepare("PRAGMA table_info(jobs)")?;
            let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
            columns
                .collect::<SqlResult<Vec<_>>>()?
                .iter()
                .any(|name| name == "lease_expires_at_ms")
        };
        if !has_lease {
            tx.execute(
                "ALTER TABLE jobs ADD COLUMN lease_expires_at_ms INTEGER",
                [],
            )?;
        }
        // Legacy Active jobs with no lease are eligible for recovery immediately.
        tx.execute("UPDATE jobs SET lease_expires_at_ms = 0 WHERE state = 'Active' AND lease_expires_at_ms IS NULL", [])?;
        tx.execute("CREATE INDEX IF NOT EXISTS idx_jobs_active_lease ON jobs(lease_expires_at_ms) WHERE state = 'Active'", [])?;
        for (column, definition) in [
            ("available_at", "INTEGER"),
            ("retry_backoff_ms", "INTEGER NOT NULL DEFAULT 0"),
            ("retry_backoff_max_ms", "INTEGER NOT NULL DEFAULT 60000"),
        ] {
            let exists = {
                let mut statement = tx.prepare("PRAGMA table_info(jobs)")?;
                let columns = statement.query_map([], |r| r.get::<_, String>(1))?;
                columns
                    .collect::<SqlResult<Vec<_>>>()?
                    .iter()
                    .any(|name| name == column)
            };
            if !exists {
                tx.execute(
                    &format!("ALTER TABLE jobs ADD COLUMN {column} {definition}"),
                    [],
                )?;
            }
        }
        // Old Delayed jobs have no recoverable deadline; leave them unscheduled.
        tx.execute("UPDATE jobs SET available_at = created_at WHERE available_at IS NULL AND state != 'Delayed'", [])?;
        tx.execute("CREATE INDEX IF NOT EXISTS idx_jobs_schedulable ON jobs(priority, created_at, id, available_at) WHERE state IN ('Waiting', 'Delayed')", [])?;
        // Keep periodic pressure scans away from payload pages; adds write/storage cost.
        tx.execute("CREATE INDEX IF NOT EXISTS idx_jobs_pressure ON jobs(name, state, available_at, created_at, lease_expires_at_ms)", [])?;
        // Retention sweeps: age prune scans (state, finished_at) oldest-first; per-name count
        // prune partitions by name and orders finished_at DESC. Both avoid payload pages.
        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_history_state_finished ON job_history(state, finished_at)",
            [],
        )?;
        tx.execute(
            "CREATE INDEX IF NOT EXISTS idx_history_name_state_finished ON job_history(name, state, finished_at DESC)",
            [],
        )?;
        tx.commit()?;
        tracing::info!("Database migrations completed successfully");
        Ok(())
    }
}
