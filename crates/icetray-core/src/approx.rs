//! Integer-to-float conversions for reporting, and the conversions between
//! the `usize` of an in-memory length and the `u64` of a byte or row total.
//! Ratios and gauge values are approximate by nature, and this module is the
//! one place that conversion happens, so a call site says what the float is
//! for. Identifiers never become floats: a 63-bit id loses its low digits in
//! an `f64`, so ids go in metric labels and log fields instead.
//!
//! The crate builds only for targets whose `usize` is 64 bits, which the
//! assertion below enforces at compile time, so `count` and `length` are
//! exact both ways and neither saturates nor fails.

#![allow(clippy::cast_precision_loss)]

const _: () = assert!(
    usize::BITS == u64::BITS,
    "icetray-core supports only targets with a 64-bit usize"
);

/// `numerator / denominator` as a float, or zero when the denominator is
/// zero. For bytes-per-key and similar reporting ratios.
pub fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// A signed count as a metric gauge value: a snapshot's sequence number,
/// which counts the table's commits.
pub fn signed_gauge(value: i64) -> f64 {
    value as f64
}

/// A count or byte total as a metric gauge value.
pub fn gauge(value: u64) -> f64 {
    value as f64
}

/// A length or byte total held in memory as a gauge value.
pub fn gauge_len(value: usize) -> f64 {
    gauge(count(value))
}

/// A count or length held in memory as the `u64` the byte and row totals
/// take. Exact: the supported targets' `usize` is 64 bits. A `const fn`, so
/// a constant derived from a `usize` one widens through it too.
pub const fn count(value: usize) -> u64 {
    // The module's compile-time assertion makes this cast exact.
    value as u64
}

/// A byte count or length held as a `u64` or `u32` (a file's length, a
/// span, a configured bound, a prefix read off a file) as the `usize` a
/// buffer or an index takes. Exact: the supported targets' `usize` is 64
/// bits, so the `expect` cannot fire. A caller that must not allocate what
/// a file claims bounds the value before it calls this; the conversion
/// never bounds it.
pub fn length(value: impl Into<u64>) -> usize {
    usize::try_from(value.into()).expect("a u64 fits usize on the supported 64-bit targets")
}

/// A footer number as a measurement: a chunk size, a value count, a filter
/// length, a row count, or a column width the doctor and the resident-bytes
/// prediction sum, in the `i32` or `i64` the footer holds it in. A negative
/// one, which no writer produces and which `FileIndex::build` refuses
/// wherever a decision rests on it, measures as zero, so an estimate over
/// any footer is a number and never a fault.
pub(crate) fn measured(value: impl Into<i64>) -> u64 {
    u64::try_from(value.into()).unwrap_or(0)
}

/// Byte units for the tests' budgets and limits.
#[cfg(test)]
pub(crate) mod units {
    /// A kibibyte.
    pub(crate) const KIB: u64 = 1 << 10;
    /// A mebibyte.
    pub(crate) const MIB: u64 = 1 << 20;
    /// A gibibyte.
    pub(crate) const GIB: u64 = 1 << 30;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ratio_zero_denominator_is_zero() {
        assert_eq!(ratio(5, 0).to_bits(), 0.0f64.to_bits());
    }

    #[test]
    fn test_ratio_divides() {
        assert_eq!(ratio(3, 4).to_bits(), 0.75f64.to_bits());
    }

    #[test]
    fn test_gauge_is_the_value() {
        assert_eq!(gauge(7).to_bits(), 7.0f64.to_bits());
    }

    #[test]
    fn test_gauge_len_is_the_value() {
        assert_eq!(gauge_len(7).to_bits(), 7.0f64.to_bits());
    }

    #[test]
    fn test_count_is_the_value() {
        assert_eq!(count(7), 7);
        assert_eq!(count(usize::MAX), u64::MAX);
    }

    #[test]
    fn test_length_is_the_value() {
        assert_eq!(length(7u64), 7);
        assert_eq!(count(length(u32::MAX)), u64::from(u32::MAX));
        assert_eq!(length(u64::MAX), usize::MAX);
    }

    #[test]
    fn test_measured_negative_is_zero() {
        assert_eq!(measured(7), 7);
        assert_eq!(measured(0), 0);
        assert_eq!(measured(-1), 0);
        assert_eq!(measured(i64::MIN), 0);
    }
}
