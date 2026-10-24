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
