// Attempt one waits base, attempt two waits 2 * base, etc. Saturate before capping.
pub fn retry_delay(base: i64, cap: i64, attempt: u32) -> i64 {
    if base == 0 {
        return 0;
    }
    let multiplier = 1_i64
        .checked_shl(attempt.saturating_sub(1))
        .filter(|v| *v > 0)
        .unwrap_or(i64::MAX);
    base.saturating_mul(multiplier).min(cap)
}

/// Apply BullMQ-style jitter to an already-capped exponential backoff `delay`.
///
/// `jitter` is a fraction clamped to [0, 1]; `rand_unit` is a uniform sample in [0, 1).
/// Returns `round((1 - jitter) * delay + jitter * rand_unit * delay)` clamped to [0, delay].
/// Jitter of 0 (or a non-positive delay) returns `delay` unchanged, preserving exact backoff.
pub fn apply_jitter(delay: i64, jitter: f64, rand_unit: f64) -> i64 {
    if jitter <= 0.0 || delay <= 0 {
        return delay;
    }
    let jitter = jitter.clamp(0.0, 1.0);
    let delay_f = delay as f64;
    let jittered = ((1.0 - jitter) * delay_f + jitter * rand_unit * delay_f).round();
    (jittered as i64).clamp(0, delay)
}

use chrono::{TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use std::str::FromStr;

/// Errors from tz-aware cron scheduling. Distinguishes an unparseable cron pattern from an
/// unknown timezone so RPC callers can surface a precise `invalid_argument`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SchedulerError {
    #[error("invalid cron pattern: {0}")]
    InvalidCron(String),
    #[error("invalid timezone: {0}")]
    InvalidTimezone(String),
    #[error("could not compute next run: {0}")]
    Compute(String),
}

/// Resolve an IANA timezone name. An empty or blank name selects UTC.
fn resolve_tz(tz: &str) -> Result<Tz, SchedulerError> {
    let trimmed = tz.trim();
    if trimmed.is_empty() {
        return Ok(Tz::UTC);
    }
    Tz::from_str(trimmed).map_err(|_| SchedulerError::InvalidTimezone(tz.to_string()))
}

/// Parse a standard (5-field) cron pattern. Seconds are optional per croner, so a 6-field
/// pattern is also accepted and simply schedules at second granularity.
fn parse_cron(cron: &str) -> Result<Cron, SchedulerError> {
    if cron.trim().is_empty() {
        return Err(SchedulerError::InvalidCron("pattern is empty".to_string()));
    }
    Cron::from_str(&normalize_cron(cron)).map_err(|e| SchedulerError::InvalidCron(e.to_string()))
}

/// croner 4.x rejects single-number step components such as `0/20`, requiring `*/20` or a ranged
/// `X-Y/20`. Standard/Vixie cron (and BullMQ) accept `N/M` as "from N, every M to the field max".
/// Rewrite each such component to the equivalent `N-{max}/M` so those real-world patterns parse
/// with identical semantics. Only 5- and 6-field patterns are touched; anything else is left for
/// croner to reject.
fn normalize_cron(cron: &str) -> String {
    let fields: Vec<&str> = cron.split_whitespace().collect();
    // Upper bound per field: 5-field is min/hour/dom/month/dow; 6-field prepends seconds.
    let maxes: &[u32] = match fields.len() {
        5 => &[59, 23, 31, 12, 7],
        6 => &[59, 59, 23, 31, 12, 7],
        _ => return cron.to_string(),
    };
    fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let max = maxes[i];
            field
                .split(',')
                .map(|part| normalize_step_component(part, max))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_step_component(part: &str, max: u32) -> String {
    if let Some((start, step)) = part.split_once('/') {
        let bare_number = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit());
        // Only rewrite `N/M`; leave `*/M` and already-ranged `X-Y/M` untouched.
        if bare_number(start) && bare_number(step) {
            return format!("{start}-{max}/{step}");
        }
    }
    part.to_string()
}

/// Validate that `cron` parses and `tz` resolves, without computing an occurrence. Reused by the
/// upsert RPC to reject bad input before any persistence.
pub fn validate_cron(cron: &str, tz: &str) -> Result<(), SchedulerError> {
    parse_cron(cron)?;
    resolve_tz(tz)?;
    Ok(())
}

/// First occurrence of `cron` strictly after `now_ms` (epoch millis), evaluated in `tz`
/// (empty => UTC), returned as epoch millis. DST-correct: nonexistent spring-forward local times
/// are skipped and fall-back duplicated hours fire once. Skip-missed semantics — always the next
/// future occurrence relative to `now_ms`, never a backfill of past fires.
pub fn next_cron_after(now_ms: i64, cron: &str, tz: &str) -> Result<i64, SchedulerError> {
    let cron = parse_cron(cron)?;
    let tz = resolve_tz(tz)?;
    let now_utc = Utc
        .timestamp_millis_opt(now_ms)
        .single()
        .ok_or_else(|| SchedulerError::Compute(format!("invalid now_ms: {now_ms}")))?;
    let now_local = now_utc.with_timezone(&tz);
    let next = cron
        .find_next_occurrence(&now_local, false)
        .map_err(|e| SchedulerError::Compute(e.to_string()))?;
    Ok(next.timestamp_millis())
}

#[cfg(test)]
mod cron_tests {
    use super::*;
    use chrono::{Datelike, Timelike};

    // Epoch millis for a wall-clock time in a given IANA zone.
    fn ms(tz: &str, y: i32, mo: u32, d: u32, h: u32, mi: u32) -> i64 {
        let zone: Tz = tz.parse().unwrap();
        zone.with_ymd_and_hms(y, mo, d, h, mi, 0)
            .single()
            .unwrap()
            .timestamp_millis()
    }

    // Local (tz) wall-clock components of an epoch-millis instant.
    fn local(tz: &str, at_ms: i64) -> (i32, u32, u32, u32, u32) {
        let zone: Tz = tz.parse().unwrap();
        let dt = Utc
            .timestamp_millis_opt(at_ms)
            .single()
            .unwrap()
            .with_timezone(&zone);
        (dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute())
    }

    #[test]
    fn spring_forward_skips_nonexistent_local_hour() {
        // America/Denver 2024-03-10: clocks jump 01:59 MST -> 03:00 MDT, so 02:00-02:59 local
        // does not exist. A 02:30 daily fire cannot occur at its nominal wall-clock time, so
        // croner rolls it forward to the first instant that exists after the gap: 03:00 MDT.
        let now = ms("America/Denver", 2024, 3, 10, 0, 0);
        let next = next_cron_after(now, "30 2 * * *", "America/Denver").unwrap();
        assert!(next > now);
        // The result must be a real (unambiguous, existing) local instant on the same day.
        let zone: Tz = "America/Denver".parse().unwrap();
        assert!(
            Utc.timestamp_millis_opt(next)
                .single()
                .unwrap()
                .with_timezone(&zone)
                .timestamp_millis()
                == next
        );
        let (y, mo, d, h, mi) = local("America/Denver", next);
        assert_eq!((y, mo, d, h, mi), (2024, 3, 10, 3, 0));
    }

    #[test]
    fn fall_back_duplicated_hour_fires_once() {
        // America/Denver 2024-11-03: clocks fall 01:59 MDT -> 01:00 MST, so 01:00-01:59
        // local occurs twice. A 01:30 daily fire resolves to that day at the first instance.
        let now = ms("America/Denver", 2024, 11, 3, 0, 0);
        let next = next_cron_after(now, "30 1 * * *", "America/Denver").unwrap();
        assert!(next > now);
        let (y, mo, d, h, mi) = local("America/Denver", next);
        assert_eq!((y, mo, d, h), (2024, 11, 3, 1));
        assert_eq!(mi, 30);
    }

    #[test]
    fn real_stat_patterns_compute_matching_next_occurrence() {
        let tz = "America/Denver";
        let now = ms(tz, 2024, 6, 15, 12, 34);
        for pattern in [
            "* * * * *",
            "*/5 * * * *",
            "0/20 7-19 * * *",
            "0 3 * * *",
            "57 */2 * * *",
            "21,51 * * * *",
            "0 14 */10 * 1-5",
        ] {
            let next = next_cron_after(now, pattern, tz).unwrap();
            assert!(next > now, "pattern {pattern} must fire after now");
            // The returned instant must itself satisfy the pattern in the same zone.
            let zone: Tz = tz.parse().unwrap();
            let dt = Utc
                .timestamp_millis_opt(next)
                .single()
                .unwrap()
                .with_timezone(&zone);
            let cron = parse_cron(pattern).unwrap();
            assert!(
                cron.is_time_matching(&dt).unwrap(),
                "pattern {pattern} did not match its own next occurrence {dt}"
            );
        }
    }

    #[test]
    fn far_past_now_jumps_to_next_occurrence_without_backfill() {
        // now far in the past: the result is the immediate next fire after that instant, not a
        // skip-ahead to the present. "0 3 * * *" next after 1970-01-01T00:00Z is the SAME day at
        // 03:00 (03:00 == 3h), proving it returns the next occurrence rather than backfilling.
        let now = 0_i64;
        let next = next_cron_after(now, "0 3 * * *", "UTC").unwrap();
        assert!(next > now);
        assert_eq!(next, 3 * 3_600_000);
        // Strictly the next occurrence, so well within two days of the (past) now.
        assert!(next < now + 2 * 86_400_000);
    }

    #[test]
    fn empty_timezone_defaults_to_utc() {
        let now = ms("UTC", 2024, 6, 15, 12, 0);
        assert_eq!(
            next_cron_after(now, "0 3 * * *", ""),
            next_cron_after(now, "0 3 * * *", "UTC"),
        );
        assert_eq!(
            next_cron_after(now, "0 3 * * *", "   "),
            next_cron_after(now, "0 3 * * *", "UTC"),
        );
    }

    #[test]
    fn invalid_cron_and_invalid_tz_error_distinctly() {
        let now = ms("UTC", 2024, 6, 15, 12, 0);
        assert!(matches!(
            next_cron_after(now, "not a cron", "UTC"),
            Err(SchedulerError::InvalidCron(_))
        ));
        assert!(matches!(
            next_cron_after(now, "", "UTC"),
            Err(SchedulerError::InvalidCron(_))
        ));
        assert!(matches!(
            next_cron_after(now, "0 3 * * *", "Mars/Olympus_Mons"),
            Err(SchedulerError::InvalidTimezone(_))
        ));
        assert!(validate_cron("0 3 * * *", "America/Denver").is_ok());
        assert!(matches!(
            validate_cron("nope", "UTC"),
            Err(SchedulerError::InvalidCron(_))
        ));
        assert!(matches!(
            validate_cron("0 3 * * *", "Nowhere/Land"),
            Err(SchedulerError::InvalidTimezone(_))
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exponential_delay_caps_without_overflow() {
        assert_eq!(
            (1..=4)
                .map(|n| retry_delay(100, 500, n))
                .collect::<Vec<_>>(),
            vec![100, 200, 400, 500]
        );
        assert_eq!(retry_delay(0, 500, u32::MAX), 0);
        assert_eq!(retry_delay(i64::MAX, i64::MAX, u32::MAX), i64::MAX);
    }

    #[test]
    fn jitter_zero_leaves_delay_unchanged() {
        assert_eq!(apply_jitter(1000, 0.0, 0.5), 1000);
        assert_eq!(apply_jitter(1000, -0.1, 0.9), 1000);
    }

    #[test]
    fn jitter_full_spans_zero_to_delay() {
        assert_eq!(apply_jitter(1000, 1.0, 0.0), 0);
        let near_full = apply_jitter(1000, 1.0, 0.999);
        assert!(near_full <= 1000);
        assert!(near_full >= 998, "expected near delay, got {near_full}");
    }

    #[test]
    fn jitter_half_stays_within_band() {
        for &rand_unit in &[0.0, 0.25, 0.5, 0.75, 0.999] {
            let d = apply_jitter(1000, 0.5, rand_unit);
            assert!((500..=1000).contains(&d), "got {d} for rand {rand_unit}");
        }
    }

    #[test]
    fn jitter_clamps_and_handles_zero_delay() {
        assert_eq!(apply_jitter(0, 1.0, 0.5), 0);
        // jitter above 1.0 is clamped to 1.0.
        assert_eq!(apply_jitter(1000, 2.0, 0.0), 0);
    }
}
