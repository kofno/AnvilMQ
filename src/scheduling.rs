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
}
