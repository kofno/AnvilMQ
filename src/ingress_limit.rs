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

/// Two adjacent fixed-window counter buckets used for the sliding-window approximation.
/// `start` is the current window's start timestamp (ms); `current` counts admissions in
/// that window; `previous` retains the immediately preceding window's total for weighting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Bucket {
    start: i64,
    current: i64,
    previous: i64,
}

/// Advances the bucket pair so `now` falls inside the current window. A single-window step
/// carries the current total into `previous`; a gap of two or more windows fully resets and
/// realigns the window start. `w` must be positive. Pure and side-effect free.
fn roll(mut b: Bucket, now: i64, w: i64) -> Bucket {
    let elapsed = now - b.start;
    if elapsed >= w {
        // w > 0 and elapsed >= w >= 1, so the quotient is >= 1.
        if elapsed / w == 1 {
            b.previous = b.current;
            b.current = 0;
            b.start += w;
        } else {
            b.previous = 0;
            b.current = 0;
            b.start = now - elapsed % w;
        }
    }
    b
}

/// Weighted admission estimate for a rolled bucket at `now`: the previous window contributes
/// its full count at the current window's start and decays linearly to zero at its end.
fn estimate(b: &Bucket, now: i64, w: i64) -> f64 {
    let elapsed_in_current = (now - b.start) as f64;
    let weight = ((w as f64 - elapsed_in_current) / w as f64).clamp(0.0, 1.0);
    b.previous as f64 * weight + b.current as f64
}

pub async fn upsert(
    db: Arc<DatabaseManager>,
    req: UpsertIngressLimitRuleRequest,
) -> Result<UpsertIngressLimitRuleResponse, Status> {
    validate(&req.facet_pattern)?;
    if req.window_duration_ms <= 0 {
        return Err(Status::invalid_argument(
            "window_duration_ms must be positive",
        ));
    }
    let conn = db.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let mut conn = conn.blocking_lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(internal)?;
        now_ms(&tx)
            .map_err(internal)?
            .checked_add(req.window_duration_ms)
            .ok_or_else(|| Status::invalid_argument("window timestamp overflows"))?;
        tx.execute(
            "INSERT INTO ingress_limit_rules(facet_pattern, max_jobs, window_duration_ms) VALUES (?1, ?2, ?3) ON CONFLICT(facet_pattern) DO UPDATE SET max_jobs = excluded.max_jobs, window_duration_ms = excluded.window_duration_ms",
            rusqlite::params![req.facet_pattern, req.max_jobs, req.window_duration_ms],
        )
        .map_err(internal)?;
        tx.execute(
            "DELETE FROM ingress_limit_counters WHERE facet_key = ?1",
            [&req.facet_pattern],
        )
        .map_err(internal)?;
        tx.commit().map_err(internal)?;
        tracing::info!(max_jobs = req.max_jobs, window_duration_ms = req.window_duration_ms, "ingress limit rule upserted; window reset");
        Ok(UpsertIngressLimitRuleResponse { success: true })
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
}

pub async fn delete(
    db: Arc<DatabaseManager>,
    req: DeleteIngressLimitRuleRequest,
) -> Result<DeleteIngressLimitRuleResponse, Status> {
    validate(&req.facet_key)?;
    let conn = db.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let mut conn = conn.blocking_lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(internal)?;
        let deleted = tx
            .execute(
                "DELETE FROM ingress_limit_rules WHERE facet_pattern = ?1",
                [&req.facet_key],
            )
            .map_err(internal)?
            > 0;
        tx.execute(
            "DELETE FROM ingress_limit_counters WHERE facet_key = ?1",
            [&req.facet_key],
        )
        .map_err(internal)?;
        tx.commit().map_err(internal)?;
        Ok(DeleteIngressLimitRuleResponse { deleted })
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
}

pub async fn status(
    db: Arc<DatabaseManager>,
    req: GetIngressLimitStatusRequest,
) -> Result<GetIngressLimitStatusResponse, Status> {
    validate(&req.facet_key)?;
    let conn = db.get_shared_connection();
    tokio::task::spawn_blocking(move || {
        let conn = conn.blocking_lock();
        let now = now_ms(&conn).map_err(internal)?;
        let rule = conn
            .query_row(
                "SELECT r.max_jobs, r.window_duration_ms, c.current_window_start, c.current_count, c.previous_count FROM ingress_limit_rules r LEFT JOIN ingress_limit_counters c ON c.facet_key = r.facet_pattern WHERE r.facet_pattern = ?1",
                [&req.facet_key],
                |r| {
                    Ok((
                        r.get::<_, u32>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, Option<i64>>(2)?,
                        r.get::<_, Option<i64>>(3)?,
                        r.get::<_, Option<i64>>(4)?,
                    ))
                },
            )
            .optional()
            .map_err(internal)?;
        let Some((max_jobs, duration, start, current, previous)) = rule else {
            return Ok(GetIngressLimitStatusResponse {
                facet_key: req.facet_key,
                ..Default::default()
            });
        };
        // Read-only: roll a copy in memory to project the estimate at `now` without persisting.
        let (estimated, window_started_at) = match start {
            Some(start) => {
                let rolled = roll(
                    Bucket {
                        start,
                        current: current.unwrap_or(0),
                        previous: previous.unwrap_or(0),
                    },
                    now,
                    duration,
                );
                (estimate(&rolled, now, duration), rolled.start)
            }
            None => (0.0, 0),
        };
        let estimated_count = estimated.round().clamp(0.0, u32::MAX as f64) as u32;
        Ok(GetIngressLimitStatusResponse {
            facet_key: req.facet_key,
            estimated_count,
            max_jobs,
            window_duration_ms: duration,
            window_started_at,
            is_throttled: max_jobs == 0 || estimated >= max_jobs as f64,
            rule_exists: true,
        })
    })
    .await
    .map_err(|e| Status::internal(e.to_string()))?
}

/// Admission check + consume, run inside the enqueue transaction after `now` is computed and
/// the idempotency replay early-return, before the job insert. Returns `Ok(true)` when the
/// enqueue may proceed (a missing rule means unrestricted ingress) and `Ok(false)` when the
/// facet is throttled or paused. On admit it advances the sliding-window counter and persists
/// it, so consumption commits atomically with the job insert; on reject it writes nothing.
/// Callers must only invoke this when `facet` is non-empty.
pub fn admit(tx: &Transaction<'_>, facet: &str, now: i64) -> rusqlite::Result<bool> {
    let rule = tx
        .query_row(
            "SELECT max_jobs, window_duration_ms FROM ingress_limit_rules WHERE facet_pattern = ?1",
            [facet],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )
        .optional()?;
    let Some((max_jobs, window)) = rule else {
        return Ok(true);
    };
    if max_jobs == 0 {
        return Ok(false);
    }
    let existing = tx
        .query_row(
            "SELECT current_window_start, current_count, previous_count FROM ingress_limit_counters WHERE facet_key = ?1",
            [facet],
            |r| {
                Ok(Bucket {
                    start: r.get(0)?,
                    current: r.get(1)?,
                    previous: r.get(2)?,
                })
            },
        )
        .optional()?;
    let mut bucket = roll(
        existing.unwrap_or(Bucket {
            start: now,
            current: 0,
            previous: 0,
        }),
        now,
        window,
    );
    if estimate(&bucket, now, window) >= max_jobs as f64 {
        return Ok(false);
    }
    bucket.current += 1;
    tx.execute(
        "INSERT INTO ingress_limit_counters(facet_key, current_window_start, current_count, previous_count) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(facet_key) DO UPDATE SET current_window_start = excluded.current_window_start, current_count = excluded.current_count, previous_count = excluded.previous_count",
        rusqlite::params![facet, bucket.start, bucket.current, bucket.previous],
    )?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{Connection, TransactionBehavior};

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE ingress_limit_rules(facet_pattern TEXT PRIMARY KEY, max_jobs INTEGER NOT NULL, window_duration_ms INTEGER NOT NULL);
             CREATE TABLE ingress_limit_counters(facet_key TEXT PRIMARY KEY, current_window_start INTEGER NOT NULL, current_count INTEGER NOT NULL, previous_count INTEGER NOT NULL);",
        )
        .unwrap();
        conn
    }

    fn admit_now(conn: &mut Connection, facet: &str, now: i64) -> bool {
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let admitted = admit(&tx, facet, now).unwrap();
        tx.commit().unwrap();
        admitted
    }

    #[test]
    fn validate_rejects_wildcards_blank_and_whitespace() {
        assert!(validate("tenant:1").is_ok());
        for bad in ["", "   ", " tenant", "tenant ", "ten*", "ten?ant"] {
            assert!(validate(bad).is_err(), "expected {bad:?} to be rejected");
        }
    }

    #[test]
    fn roll_keeps_bucket_within_same_window() {
        let b = Bucket {
            start: 100,
            current: 3,
            previous: 5,
        };
        // Anywhere inside [start, start + w) leaves the bucket untouched.
        assert_eq!(roll(b, 100, 1000), b);
        assert_eq!(roll(b, 999 + 100, 1000), b);
    }

    #[test]
    fn roll_single_step_carries_weighted_previous() {
        let b = Bucket {
            start: 0,
            current: 7,
            previous: 42,
        };
        let rolled = roll(b, 1500, 1000);
        assert_eq!(
            rolled,
            Bucket {
                start: 1000,
                current: 0,
                previous: 7,
            }
        );
        // At the fresh window start the previous count contributes fully.
        assert_eq!(estimate(&rolled, 1000, 1000), 7.0);
    }

    #[test]
    fn roll_two_window_gap_fully_resets_and_realigns() {
        let b = Bucket {
            start: 0,
            current: 9,
            previous: 9,
        };
        let rolled = roll(b, 2500, 1000);
        assert_eq!(
            rolled,
            Bucket {
                start: 2000,
                current: 0,
                previous: 0,
            }
        );
        assert_eq!(estimate(&rolled, 2500, 1000), 0.0);
    }

    #[test]
    fn weight_is_full_at_window_start_and_approaches_zero_at_end() {
        let b = Bucket {
            start: 0,
            current: 0,
            previous: 100,
        };
        assert_eq!(estimate(&b, 0, 1000), 100.0);
        assert!((estimate(&b, 500, 1000) - 50.0).abs() < 1e-9);
        assert!(estimate(&b, 999, 1000) < 1.0);
    }

    #[test]
    fn admits_up_to_limit_then_rejects_within_window() {
        let mut conn = setup();
        conn.execute("INSERT INTO ingress_limit_rules VALUES ('t', 3, 1000)", [])
            .unwrap();
        assert!(admit_now(&mut conn, "t", 0));
        assert!(admit_now(&mut conn, "t", 10));
        assert!(admit_now(&mut conn, "t", 20));
        // Fourth within the same window exceeds max_jobs = 3.
        assert!(!admit_now(&mut conn, "t", 30));
    }

    #[test]
    fn missing_rule_is_unrestricted() {
        let mut conn = setup();
        for i in 0..100 {
            assert!(admit_now(&mut conn, "no-rule", i));
        }
        // A rejected/absent admission must not create a counter row.
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM ingress_limit_counters", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn max_jobs_zero_pauses_ingress() {
        let mut conn = setup();
        conn.execute("INSERT INTO ingress_limit_rules VALUES ('t', 0, 1000)", [])
            .unwrap();
        assert!(!admit_now(&mut conn, "t", 0));
        // Paused rejection writes nothing.
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM ingress_limit_counters", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn admissions_resume_after_window_elapses() {
        let mut conn = setup();
        conn.execute("INSERT INTO ingress_limit_rules VALUES ('t', 1, 1000)", [])
            .unwrap();
        assert!(admit_now(&mut conn, "t", 0));
        assert!(!admit_now(&mut conn, "t", 500));
        // Two full windows later the previous bucket has fully decayed.
        assert!(admit_now(&mut conn, "t", 2000));
    }

    #[test]
    fn rejected_admission_does_not_consume_quota() {
        let mut conn = setup();
        conn.execute("INSERT INTO ingress_limit_rules VALUES ('t', 1, 1000)", [])
            .unwrap();
        assert!(admit_now(&mut conn, "t", 0));
        assert!(!admit_now(&mut conn, "t", 100));
        let current: i64 = conn
            .query_row(
                "SELECT current_count FROM ingress_limit_counters WHERE facet_key = 't'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            current, 1,
            "rejected admissions must not increment the counter"
        );
    }
}
