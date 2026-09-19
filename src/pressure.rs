//! Bounded-cardinality, cached pressure metrics. No database access during scrapes.
use rusqlite::{Connection, OpenFlags};
use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering::Relaxed},
    Arc,
};
use std::time::{Duration, Instant};

const BOUNDS_MS: [i64; 10] = [1, 10, 100, 500, 1000, 5000, 15000, 60000, 300000, 3600000];
const KINDS: [&str; 6] = [
    "waiting",
    "delayed",
    "active",
    "due",
    "unscheduled",
    "expired_leases",
];

#[derive(Default)]
struct Backlog {
    counts: [AtomicI64; 6],
    oldest_due_ms: AtomicI64,
}
#[derive(Default)]
struct Histogram {
    buckets: [AtomicU64; 10],
    count: AtomicU64,
    millis: AtomicU64,
}
impl Histogram {
    fn observe(&self, ms: i64) {
        let ms = ms.max(0);
        for (i, bound) in BOUNDS_MS.iter().enumerate() {
            if ms <= *bound {
                self.buckets[i].fetch_add(1, Relaxed);
            }
        }
        self.millis.fetch_add(ms as u64, Relaxed);
        self.count.fetch_add(1, Relaxed);
    }
    fn render(&self, name: &str, out: &mut String) {
        for (i, bound) in BOUNDS_MS.iter().enumerate() {
            *out += &format!(
                "{name}_bucket{{le=\"{}\"}} {}\n",
                *bound as f64 / 1000.,
                self.buckets[i].load(Relaxed)
            );
        }
        *out += &format!(
            "{name}_bucket{{le=\"+Inf\"}} {}\n{name}_count {}\n{name}_sum {}\n",
            self.count.load(Relaxed),
            self.count.load(Relaxed),
            self.millis.load(Relaxed) as f64 / 1000.
        );
    }
}
#[derive(Default)]
pub struct Pressure {
    all: Backlog,
    queues: Vec<(String, Backlog)>,
    sampled_at_ms: AtomicI64,
    errors: AtomicU64,
    duration_us: AtomicU64,
    first_claim: Histogram,
    due_wait: Histogram,
}
impl Pressure {
    pub fn configured(value: &str) -> Result<Self, std::io::Error> {
        let names: Vec<_> = if value.is_empty() {
            vec![]
        } else {
            value.split(',').collect()
        };
        if names.len() > 32
            || names
                .iter()
                .any(|n| n.trim().is_empty() || n.trim() != *n || n.len() > 256)
            || names
                .iter()
                .enumerate()
                .any(|(i, n)| names[..i].contains(n))
        {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "ANVILMQ_METRICS_QUEUES requires up to 32 unique comma-separated names, each 1..256 UTF-8 bytes, without surrounding whitespace"));
        }
        Ok(Self {
            queues: names
                .into_iter()
                .map(|n| (n.into(), Backlog::default()))
                .collect(),
            ..Self::default()
        })
    }
    pub fn claimed(&self, created: i64, available: i64, now: i64, attempt: u32) {
        if attempt == 1 {
            self.first_claim.observe(now.saturating_sub(created));
        }
        self.due_wait
            .observe(now.saturating_sub(available.max(created)));
    }
    fn sample(&self, conn: &Connection, now: i64) -> rusqlite::Result<()> {
        // Only configured queue names become groups. Arbitrary producer names cannot
        // create unbounded metric series or intermediate aggregation groups.
        let placeholders = (2..self.queues.len() + 2)
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT CASE WHEN name IN ({placeholders}) THEN name ELSE NULL END AS queue_group,
            SUM(state = 'Waiting'), SUM(state = 'Delayed'), SUM(state = 'Active'),
            COALESCE(SUM(state IN ('Waiting','Delayed') AND available_at <= ?1), 0),
            SUM(state IN ('Waiting','Delayed') AND available_at IS NULL),
            SUM(state = 'Active' AND (lease_expires_at_ms IS NULL OR lease_expires_at_ms <= ?1)),
            MIN(CASE WHEN state IN ('Waiting','Delayed') AND available_at <= ?1 THEN MAX(created_at, available_at) END)
            FROM jobs GROUP BY queue_group");
        let parameters = std::iter::once(rusqlite::types::Value::Integer(now)).chain(
            self.queues
                .iter()
                .map(|(n, _)| rusqlite::types::Value::Text(n.clone())),
        );
        let mut statement = conn.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(parameters), |r| {
            let mut counts = [0i64; 6];
            for (i, count) in counts.iter_mut().enumerate() {
                *count = r.get(i + 1)?;
            }
            Ok((
                r.get::<_, Option<String>>(0)?,
                counts,
                r.get::<_, Option<i64>>(7)?,
            ))
        })?;
        let mut all = ([0i64; 6], None::<i64>);
        let mut queues = vec![([0i64; 6], None::<i64>); self.queues.len()];
        for row in rows {
            let (name, counts, oldest) = row?;
            for (i, value) in counts.iter().enumerate() {
                all.0[i] += value;
            }
            if let Some(oldest) = oldest {
                all.1 = Some(all.1.map_or(oldest, |v| v.min(oldest)));
            }
            if let Some(i) = name.and_then(|n| self.queues.iter().position(|(q, _)| *q == n)) {
                queues[i] = (counts, oldest);
            }
        }
        // Publish only complete samples; an interrupted query retains the previous sample.
        let publish = |target: &Backlog, value: ([i64; 6], Option<i64>)| {
            for (i, count) in value.0.iter().enumerate() {
                target.counts[i].store(*count, Relaxed);
            }
            target
                .oldest_due_ms
                .store(value.1.map_or(0, |t| now.saturating_sub(t).max(0)), Relaxed);
        };
        publish(&self.all, all);
        for (i, (_, target)) in self.queues.iter().enumerate() {
            publish(target, queues[i]);
        }
        self.sampled_at_ms.store(now, Relaxed);
        Ok(())
    }
    pub fn render(&self, out: &mut String) {
        *out += "# HELP anvilmq_backlog_jobs Sampled live jobs; due and expired_leases overlap state counts.\n# TYPE anvilmq_backlog_jobs gauge\n# HELP anvilmq_oldest_due_age_seconds Age since oldest waiting job became due at sample time.\n# TYPE anvilmq_oldest_due_age_seconds gauge\n";
        let mut render = |value: &Backlog, labels: String| {
            for (i, kind) in KINDS.iter().enumerate() {
                *out += &format!(
                    "anvilmq_backlog_jobs{{{labels},kind=\"{kind}\"}} {}\n",
                    value.counts[i].load(Relaxed)
                );
            }
            *out += &format!(
                "anvilmq_oldest_due_age_seconds{{{labels}}} {}\n",
                value.oldest_due_ms.load(Relaxed) as f64 / 1000.
            );
        };
        render(&self.all, "scope=\"all\",queue=\"\"".into());
        for (name, value) in &self.queues {
            let name = name
                .replace('\\', "\\\\")
                .replace('\n', "\\n")
                .replace('"', "\\\"");
            render(value, format!("scope=\"queue\",queue=\"{name}\""));
        }
        for (name, help, kind, value) in [
            (
                "anvilmq_pressure_sample_timestamp_seconds",
                "Last successful pressure sample Unix timestamp; zero before first sample.",
                "gauge",
                self.sampled_at_ms.load(Relaxed) as f64 / 1000.,
            ),
            (
                "anvilmq_pressure_sample_errors_total",
                "Failed or timed-out pressure samples.",
                "counter",
                self.errors.load(Relaxed) as f64,
            ),
            (
                "anvilmq_pressure_sample_duration_seconds",
                "Duration of last sampling attempt.",
                "gauge",
                self.duration_us.load(Relaxed) as f64 / 1e6,
            ),
        ] {
            *out += &format!("# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n");
        }
        for (name, help, histogram) in [
            ("anvilmq_first_claim_delay_seconds", "Enqueue to first committed claim including intentional initial delay.", &self.first_claim),
            ("anvilmq_due_wait_seconds", "Time past due until committed claim, every attempt, excluding intentional delay/backoff.", &self.due_wait),
        ] { *out += &format!("# HELP {name} {help}\n# TYPE {name} histogram\n"); histogram.render(name, out); }
    }
}

pub fn spawn(path: String, metrics: Arc<crate::telemetry::Metrics>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let path = path.clone();
            let metrics = metrics.clone();
            // Separate short-lived reader never acquires the broker's writer mutex.
            let result = tokio::task::spawn_blocking(move || {
                let start = Instant::now();
                let result = (|| {
                    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
                    conn.busy_timeout(Duration::from_millis(25))?;
                    conn.progress_handler(
                        1000,
                        Some(move || start.elapsed() > Duration::from_millis(100)),
                    );
                    let now = conn.query_row(
                        "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)",
                        [],
                        |r| r.get(0),
                    )?;
                    metrics.pressure.sample(&conn, now)
                })();
                metrics
                    .pressure
                    .duration_us
                    .store(start.elapsed().as_micros() as u64, Relaxed);
                if result.is_err() {
                    metrics.pressure.errors.fetch_add(1, Relaxed);
                }
                result
            })
            .await;
            match result {
                Ok(Ok(())) => {}
                error => {
                    tracing::warn!(?error, "pressure sample failed; retaining previous sample")
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backlog_separates_due_future_unknown_and_expired_and_resets_empty_queues() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE jobs(name TEXT, state TEXT, available_at INTEGER, created_at INTEGER, lease_expires_at_ms INTEGER);
            INSERT INTO jobs VALUES ('email','Waiting',1000,500,NULL), ('email','Delayed',9000,500,NULL),
            ('email','Delayed',NULL,500,NULL), ('other','Delayed',2000,500,NULL),
            ('other','Active',0,500,4000), ('other','Active',0,500,9000);").unwrap();
        let pressure = Pressure::configured("email,empty").unwrap();
        pressure.sample(&conn, 5000).unwrap();
        assert_eq!(
            pressure
                .all
                .counts
                .iter()
                .map(|v| v.load(Relaxed))
                .collect::<Vec<_>>(),
            [1, 3, 2, 2, 1, 1]
        );
        assert_eq!(pressure.all.oldest_due_ms.load(Relaxed), 4000);
        assert_eq!(pressure.queues[0].1.counts[3].load(Relaxed), 1);
        assert_eq!(pressure.queues[1].1.counts[3].load(Relaxed), 0);
        let mut text = String::new();
        pressure.render(&mut text);
        assert!(!text.contains("queue=\"other\""));
        conn.execute("DELETE FROM jobs", []).unwrap();
        pressure.sample(&conn, 6000).unwrap();
        assert_eq!(pressure.all.counts[3].load(Relaxed), 0);
        assert_eq!(pressure.queues[0].1.oldest_due_ms.load(Relaxed), 0);
        // Query failure must preserve the last successful snapshot and timestamp.
        conn.execute("DROP TABLE jobs", []).unwrap();
        assert!(pressure.sample(&conn, 7000).is_err());
        assert_eq!(pressure.sampled_at_ms.load(Relaxed), 6000);
    }

    #[test]
    fn histograms_exclude_retry_from_first_claim_and_backoff_from_due_wait() {
        let pressure = Pressure::default();
        pressure.claimed(1000, 5000, 6000, 1);
        pressure.claimed(1000, 10000, 10500, 2);
        pressure.claimed(12000, 12000, 11000, 1); // Clock regression clamps to zero.
        assert_eq!(pressure.first_claim.count.load(Relaxed), 2);
        assert_eq!(pressure.first_claim.millis.load(Relaxed), 5000);
        assert_eq!(pressure.due_wait.count.load(Relaxed), 3);
        assert_eq!(pressure.due_wait.millis.load(Relaxed), 1500);
        assert_eq!(pressure.due_wait.buckets[4].load(Relaxed), 3);
    }

    #[test]
    fn bounded_labels_are_validated_and_escaped() {
        for invalid in ["a,a", "a,", " a", "a, b"] {
            assert!(Pressure::configured(invalid).is_err());
        }
        assert!(Pressure::configured(
            &(0..33).map(|i| i.to_string()).collect::<Vec<_>>().join(",")
        )
        .is_err());
        let pressure = Pressure::configured("a\"b\\c\nd").unwrap();
        let mut text = String::new();
        pressure.render(&mut text);
        assert!(text.contains("queue=\"a\\\"b\\\\c\\nd\""));
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE jobs(name TEXT, state TEXT, available_at INTEGER, created_at INTEGER, lease_expires_at_ms INTEGER);").unwrap();
        Pressure::default().sample(&conn, 1000).unwrap(); // Empty allowlist is valid SQL.
    }

    #[tokio::test]
    async fn background_reader_samples_persisted_data_without_writer_mutex() {
        let path = std::env::temp_dir().join(format!("anvil-pressure-{}.db", uuid::Uuid::new_v4()));
        let db = crate::db::DatabaseManager::new(path.to_str().unwrap())
            .await
            .unwrap();
        let conn = db.get_shared_connection();
        let _writer = conn.lock().await;
        let sampler = spawn(path.to_str().unwrap().into(), db.metrics.clone());
        tokio::time::timeout(Duration::from_secs(5), async {
            while db.metrics.pressure.sampled_at_ms.load(Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        sampler.abort();
        let _ = sampler.await;
        drop(_writer);
        drop(conn);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}
