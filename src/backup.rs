//! Broker-side local snapshot writer.
//!
//! AnvilMQ keeps all durable state in a single embedded SQLite database. This module produces
//! consistent, self-contained copies of that database on a schedule so operators have a recent
//! recovery point if a pod (and its local volume) is lost. Each snapshot is produced with
//! `VACUUM INTO`, which writes a fully committed, non-WAL copy of the last committed state — the
//! ideal single shippable artifact.
//!
//! Throughput matters: the snapshot runs on a DEDICATED `rusqlite::Connection` opened to the same
//! database file, not on the shared writer connection. WAL mode lets this second reader observe a
//! consistent committed snapshot without holding the writer mutex, so backups never stall
//! enqueues, recovery, or retention.
//!
//! Scope: this is LOCAL snapshot production only. Shipping snapshots to object storage (a
//! swappable upload sidecar) and the restore runbook are follow-up slices in this phase and are
//! intentionally not implemented here — no object-store dependencies enter the core binary.

use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Prefix of a complete snapshot file. Temp files use `.` + this prefix so a partially written
/// snapshot never matches the completed-file glob (and a future uploader never sees it).
const SNAPSHOT_PREFIX: &str = "snapshot-";
const SNAPSHOT_SUFFIX: &str = ".db";
/// Zero-pad unix-ms timestamps to a fixed width so `snapshot-*.db` names sort lexicographically in
/// chronological order. 13 digits covers unix ms through the year 2286.
const TS_WIDTH: usize = 13;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupConfig {
    /// Master switch. Opt-in (default false) so existing and CI behavior is unchanged.
    pub enabled: bool,
    /// Snapshot cadence. Must be > 0. Default 2 min, within the agreed 1–5 min RPO band.
    pub interval_ms: u64,
    /// Directory for local snapshots. Must be non-empty when enabled; created if missing.
    pub dir: String,
    /// Number of most-recent local snapshots to keep. Must be >= 1.
    pub retain: usize,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_ms: 120_000,
            dir: "backups".to_string(),
            retain: 3,
        }
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        ),
        Err(_) => default,
    }
}

fn env_u64(key: &str, default: u64) -> Result<u64, String> {
    match std::env::var(key) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("{key} must be a non-negative integer, got {value:?}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("{key}: {error}")),
    }
}

fn env_usize(key: &str, default: usize) -> Result<usize, String> {
    match std::env::var(key) {
        Ok(value) => value
            .trim()
            .parse::<usize>()
            .map_err(|_| format!("{key} must be a non-negative integer, got {value:?}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("{key}: {error}")),
    }
}

fn env_string(key: &str, default: &str) -> Result<String, String> {
    match std::env::var(key) {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok(default.to_string()),
        Err(error) => Err(format!("{key}: {error}")),
    }
}

impl BackupConfig {
    /// Build config from `ANVILMQ_BACKUP_*` env vars. A present-but-invalid value is a hard error.
    pub fn from_env() -> Result<Self, String> {
        let d = Self::default();
        let cfg = Self {
            enabled: env_bool("ANVILMQ_BACKUP_ENABLED", d.enabled),
            interval_ms: env_u64("ANVILMQ_BACKUP_INTERVAL_MS", d.interval_ms)?,
            dir: env_string("ANVILMQ_BACKUP_DIR", &d.dir)?,
            retain: env_usize("ANVILMQ_BACKUP_RETAIN", d.retain)?,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), String> {
        if self.interval_ms == 0 {
            return Err("ANVILMQ_BACKUP_INTERVAL_MS must be positive".to_string());
        }
        if self.retain < 1 {
            return Err(format!(
                "ANVILMQ_BACKUP_RETAIN={} must be >= 1",
                self.retain
            ));
        }
        if self.enabled && self.dir.trim().is_empty() {
            return Err(
                "ANVILMQ_BACKUP_DIR must be non-empty when backups are enabled".to_string(),
            );
        }
        Ok(())
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }
}

/// Outcome of a single successful snapshot run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupOutcome {
    pub path: PathBuf,
    pub timestamp_ms: i64,
    pub duration_ms: i64,
    pub size_bytes: i64,
    pub pruned: usize,
}

/// In-memory databases (the unit-test databases) are not file-backed and have no on-disk state to
/// snapshot. Detect them by path and no-op, mirroring how `DatabaseManager::checkpoint()` no-ops on
/// a non-WAL journal. Opening a fresh `:memory:` connection here would snapshot an empty database,
/// so we must decide from the path rather than by opening it.
fn is_memory_path(path: &str) -> bool {
    let trimmed = path.trim();
    trimmed.is_empty() || trimmed.contains(":memory:") || trimmed.contains("mode=memory")
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Run a single backup cycle. Returns `Ok(None)` when the database is not file-backed (a safe
/// no-op), or `Ok(Some(outcome))` after a snapshot is written and old snapshots pruned. Runs the
/// blocking SQLite/filesystem work on a blocking thread so it never stalls the async runtime.
pub async fn run_once(db_path: String, cfg: BackupConfig) -> Result<Option<BackupOutcome>, String> {
    tokio::task::spawn_blocking(move || run_blocking(&db_path, &cfg))
        .await
        .map_err(|e| format!("backup task join error: {e}"))?
}

fn run_blocking(db_path: &str, cfg: &BackupConfig) -> Result<Option<BackupOutcome>, String> {
    if is_memory_path(db_path) {
        tracing::info!(db_path, "backup skipped; database is not file-backed");
        return Ok(None);
    }

    let dir = Path::new(cfg.dir.trim());
    std::fs::create_dir_all(dir)
        .map_err(|e| format!("failed to create backup dir {}: {e}", dir.display()))?;

    let started = SystemTime::now();
    let timestamp_ms = unix_ms();
    let ts = format!("{:0width$}", timestamp_ms, width = TS_WIDTH);
    let base = format!("{SNAPSHOT_PREFIX}{ts}{SNAPSHOT_SUFFIX}");
    let final_path = dir.join(&base);
    // Leading dot keeps the in-progress file out of the completed-snapshot glob.
    let temp_path = dir.join(format!(".{base}.tmp"));

    // A stale temp from a previously crashed run would make VACUUM INTO fail (it refuses to
    // overwrite). Remove it best-effort first.
    let _ = std::fs::remove_file(&temp_path);

    if let Err(error) = write_snapshot(db_path, &temp_path) {
        let _ = std::fs::remove_file(&temp_path);
        return Err(error);
    }

    // Atomic rename publishes the complete file under its final name; a future uploader only ever
    // sees fully written snapshots.
    if let Err(error) = std::fs::rename(&temp_path, &final_path) {
        let _ = std::fs::remove_file(&temp_path);
        return Err(format!(
            "failed to publish snapshot {}: {error}",
            final_path.display()
        ));
    }

    let size_bytes = std::fs::metadata(&final_path)
        .map(|m| m.len() as i64)
        .unwrap_or(0);
    let pruned = prune(dir, cfg.retain)?;
    let duration_ms = started.elapsed().map(|d| d.as_millis() as i64).unwrap_or(0);

    Ok(Some(BackupOutcome {
        path: final_path,
        timestamp_ms,
        duration_ms,
        size_bytes,
        pruned,
    }))
}

/// Snapshot the database into `temp_path` using a dedicated read connection.
///
/// `VACUUM INTO` writes a fully self-contained, consistent, non-WAL copy of the last committed
/// state. Running it on a second connection (not the shared writer) means WAL readers never take
/// the writer mutex, so this never stalls enqueue/recovery/retention.
fn write_snapshot(db_path: &str, temp_path: &Path) -> Result<(), String> {
    let conn = Connection::open(db_path).map_err(|e| format!("open {db_path} for backup: {e}"))?;
    conn.busy_timeout(Duration::from_secs(5))
        .map_err(|e| format!("set busy_timeout for backup: {e}"))?;
    let target = temp_path.to_string_lossy().to_string();
    conn.execute("VACUUM INTO ?1", [&target])
        .map_err(|e| format!("VACUUM INTO {target}: {e}"))?;
    Ok(())
}

/// Keep only the `retain` most-recent complete `snapshot-*.db` files in `dir`, deleting older ones.
/// Names are fixed-width timestamps, so a reverse lexicographic sort is newest-first. Returns the
/// number of files removed.
fn prune(dir: &Path, retain: usize) -> Result<usize, String> {
    let mut snapshots: Vec<PathBuf> = Vec::new();
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("read backup dir {}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("read backup dir entry: {e}"))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(SNAPSHOT_PREFIX) && name.ends_with(SNAPSHOT_SUFFIX) {
            snapshots.push(entry.path());
        }
    }
    snapshots.sort();
    let mut removed = 0;
    if snapshots.len() > retain {
        let excess = snapshots.len() - retain;
        for path in snapshots.into_iter().take(excess) {
            std::fs::remove_file(&path)
                .map_err(|e| format!("prune snapshot {}: {e}", path.display()))?;
            removed += 1;
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    // The `ANVILMQ_BACKUP_*` env vars are process-global; Rust runs tests in parallel threads, so
    // all env-mutating assertions live in one serial test to avoid cross-thread races.
    #[test]
    fn env_driven_config() {
        let keys = [
            "ANVILMQ_BACKUP_ENABLED",
            "ANVILMQ_BACKUP_INTERVAL_MS",
            "ANVILMQ_BACKUP_DIR",
            "ANVILMQ_BACKUP_RETAIN",
        ];
        let clear = || {
            for key in keys {
                std::env::remove_var(key);
            }
        };

        // Disabled by default when nothing is set.
        clear();
        let cfg = BackupConfig::from_env().unwrap();
        assert!(!cfg.enabled());
        assert_eq!(cfg, BackupConfig::default());

        // Truthy parsing mirrors ANVILMQ_FAIRNESS_ENABLED (1|true|yes).
        for value in ["1", "true", "TRUE", "Yes", " yes "] {
            std::env::set_var("ANVILMQ_BACKUP_ENABLED", value);
            assert!(env_bool("ANVILMQ_BACKUP_ENABLED", false), "value={value:?}");
        }
        for value in ["0", "false", "no", "", "maybe"] {
            std::env::set_var("ANVILMQ_BACKUP_ENABLED", value);
            assert!(!env_bool("ANVILMQ_BACKUP_ENABLED", true), "value={value:?}");
        }

        // Present-but-invalid numeric values are hard errors.
        clear();
        std::env::set_var("ANVILMQ_BACKUP_INTERVAL_MS", "notanumber");
        assert!(BackupConfig::from_env().is_err());
        clear();
        std::env::set_var("ANVILMQ_BACKUP_RETAIN", "-1");
        assert!(BackupConfig::from_env().is_err());
        clear();
    }

    #[test]
    fn interval_must_be_positive() {
        let cfg = BackupConfig {
            interval_ms: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
        let cfg = BackupConfig {
            interval_ms: 1,
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn retain_must_be_at_least_one() {
        let cfg = BackupConfig {
            retain: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
        let cfg = BackupConfig {
            retain: 1,
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn dir_must_be_non_empty_when_enabled() {
        let cfg = BackupConfig {
            enabled: true,
            dir: "   ".to_string(),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
        // Empty dir is tolerated while disabled (the default-off path).
        let cfg = BackupConfig {
            enabled: false,
            dir: String::new(),
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    fn temp_db() -> PathBuf {
        std::env::temp_dir().join(format!("anvil-backup-{}.db", uuid::Uuid::new_v4()))
    }

    fn seed_file_db(path: &Path) {
        let conn = Connection::open(path).unwrap();
        conn.busy_timeout(Duration::from_secs(5)).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.execute_batch(
            "CREATE TABLE probe(id INTEGER PRIMARY KEY, note TEXT);
             INSERT INTO probe(id, note) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');",
        )
        .unwrap();
    }

    fn cleanup_db(path: &Path) {
        for extra in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{}", path.display(), extra));
        }
    }

    #[tokio::test]
    async fn snapshot_writes_valid_copy_and_prunes() {
        let db_path = temp_db();
        seed_file_db(&db_path);
        let dir = std::env::temp_dir().join(format!("anvil-backups-{}", uuid::Uuid::new_v4()));
        let cfg = BackupConfig {
            enabled: true,
            interval_ms: 1_000,
            dir: dir.to_string_lossy().to_string(),
            retain: 3,
        };

        // First run: one snapshot exists, contains the seeded rows, no leftover temp file.
        let outcome = run_once(db_path.to_string_lossy().to_string(), cfg.clone())
            .await
            .unwrap()
            .expect("expected a snapshot outcome");

        let snapshots = list_snapshots(&dir);
        assert_eq!(snapshots.len(), 1, "exactly one snapshot after first run");
        assert!(outcome.size_bytes > 0);

        let snap = Connection::open(&snapshots[0]).unwrap();
        let rows: i64 = snap
            .query_row("SELECT COUNT(*) FROM probe", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 3, "snapshot must contain the seeded rows");
        let note: String = snap
            .query_row("SELECT note FROM probe WHERE id = 2", [], |r| r.get(0))
            .unwrap();
        assert_eq!(note, "beta");
        drop(snap);

        assert!(no_temp_files(&dir), "no leftover .tmp file after success");

        // Run enough additional cycles that pruning must engage; assert exactly `retain` remain.
        for _ in 0..5 {
            // Distinct, monotonically increasing timestamps keep names unique and sortable.
            tokio::time::sleep(Duration::from_millis(2)).await;
            run_once(db_path.to_string_lossy().to_string(), cfg.clone())
                .await
                .unwrap()
                .expect("expected a snapshot outcome");
        }
        let snapshots = list_snapshots(&dir);
        assert_eq!(
            snapshots.len(),
            cfg.retain,
            "pruning keeps exactly retain snapshots"
        );
        // The kept snapshots must be the newest ones (largest timestamps).
        let mut sorted = snapshots.clone();
        sorted.sort();
        assert_eq!(sorted, snapshots, "kept snapshots are the newest by name");
        assert!(no_temp_files(&dir), "no leftover .tmp files after pruning");

        cleanup_db(&db_path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn in_memory_database_is_a_no_op() {
        let outcome = run_once(":memory:".to_string(), BackupConfig::default())
            .await
            .unwrap();
        assert!(outcome.is_none(), "in-memory backup must be a no-op");
    }

    fn list_snapshots(dir: &Path) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                let name = p.file_name().unwrap().to_string_lossy();
                name.starts_with(SNAPSHOT_PREFIX) && name.ends_with(SNAPSHOT_SUFFIX)
            })
            .collect();
        out.sort();
        out
    }

    fn no_temp_files(dir: &Path) -> bool {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .all(|e| !e.file_name().to_string_lossy().ends_with(".tmp"))
    }
}
