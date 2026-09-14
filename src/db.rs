use rusqlite::{Connection, Result as SqlResult};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct DatabaseManager {
    // Wrap connection in an Arc<Mutex> for safe concurrent async access
    conn: Arc<Mutex<Connection>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_preserves_legacy_jobs_and_is_repeatable() {
        let mut conn = Connection::open_in_memory().unwrap();
        DatabaseManager::run_migrations(&mut conn).unwrap();
        conn.execute("ALTER TABLE jobs DROP COLUMN worker_id", [])
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
    }
}

impl DatabaseManager {
    pub async fn new(path: &str) -> SqlResult<Self> {
        let path = path.to_string();

        // SQLite operations are blocking, so we initialize and migrate on a blocking thread
        let conn = tokio::task::spawn_blocking(move || {
            let mut conn = Connection::open(&path)?;
            conn.busy_timeout(std::time::Duration::from_secs(5))?;

            // Enable WAL mode for high-concurrency read/write performance
            conn.execute_batch(
                "
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
            ",
            )?;

            Self::run_migrations(&mut conn)?;
            Ok::<Connection, rusqlite::Error>(conn)
        })
        .await
        .unwrap()?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn get_shared_connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.conn)
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

            CREATE TABLE IF NOT EXISTS rate_limit_counters (
                facet_key TEXT PRIMARY KEY,
                current_count INTEGER NOT NULL,
                window_expires_at INTEGER NOT NULL
            );
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
        tx.commit()?;
        println!("Database migrations completed successfully.");
        Ok(())
    }
}
