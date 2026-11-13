//! Time conversions. Elapsed time is a `Duration` and a wall-clock instant
//! a `jiff::Timestamp` everywhere else in the crate; this is the one module
//! where either becomes text or a number and back, so no call site carries
//! its own unit arithmetic.

use std::time::{Duration, SystemTime};

/// A duration as a float of seconds, the unit a Prometheus histogram takes
/// and the unit its bucket bounds are declared in. The one place a
/// `Duration` becomes a float for a metric.
pub(crate) fn seconds(duration: Duration) -> f64 {
    duration.as_secs_f64()
}

/// An elapsed time as whole microseconds, for a one-shot subcommand's
/// output and a counter of microseconds. Saturates at `u64::MAX`, which is
/// over five hundred thousand years; neither reaches it.
pub fn micros(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
}

/// Whole microseconds as an elapsed time: the inverse of `micros`, for a
/// caller that stores a duration as that count and reads it back.
pub fn from_micros(micros: u64) -> Duration {
    Duration::from_micros(micros)
}

/// The wall clock now as a timestamp, for every caller that keeps time in
/// `jiff`; a caller that needs "now" never names the clock itself. The one
/// other read of the clock is `system_now`, for the SDK boundary.
pub fn now() -> jiff::Timestamp {
    jiff::Timestamp::now()
}

/// The wall clock now as a `SystemTime`, for the one boundary that speaks
/// it: the AWS SDK hands a credential's expiry back as a `SystemTime`, and
/// the credential cache compares and offsets it with `SystemTime`'s own
/// methods rather than converting a value that can fall outside `jiff`'s
/// range. Every other caller uses `now`.
pub(crate) fn system_now() -> SystemTime {
    SystemTime::now()
}

/// Seconds from `since` to now: the `generation_age_seconds` gauge, from a
/// served snapshot's own timestamp. A `since` in the future gives a negative
/// age, kept as measured rather than clamped, so a writer's clock running
/// ahead shows as such.
pub fn age_seconds(since: jiff::Timestamp) -> f64 {
    now().duration_since(since).as_secs_f64()
}

/// A wall-clock timestamp as seconds since the Unix epoch, with the
/// fraction: the `generation_swapped_at_seconds` gauge, the unit Prometheus
/// reads a time in. The one place a timestamp becomes epoch seconds.
pub(crate) fn unix_seconds(at: jiff::Timestamp) -> f64 {
    at.as_duration().as_secs_f64()
}

/// A wall-clock timestamp as log text: RFC 3339 in UTC with a `Z` offset,
/// through jiff's own printer, with the fraction of a second only when it
/// is non-zero. The one place a timestamp becomes text.
pub fn log_timestamp(at: jiff::Timestamp) -> String {
    at.to_string()
}

/// A count of milliseconds since the Unix epoch, as Iceberg records a
/// snapshot's timestamp, as a `jiff::Timestamp`. The one place an epoch
/// millisecond becomes a timestamp. `Err` for a value outside the range jiff
/// represents, which spans years -9999 to 9999.
pub(crate) fn timestamp_from_millis(millis: i64) -> Result<jiff::Timestamp, jiff::Error> {
    jiff::Timestamp::from_millisecond(millis)
}

/// The epoch-millisecond count `step` after `base`, an epoch-millisecond
/// count as Iceberg records a snapshot's timestamp and the metadata's last
/// update: how a fixture writer, the testkit's or core's tests', stamps a
/// commit later than the metadata's last update, which Iceberg requires.
/// No serving path writes a commit; this is public because the testkit is
/// another crate. `step` counts in whole milliseconds, the unit Iceberg
/// stamps in, and its remainder is dropped. Saturates at `i64::MAX`,
/// outside the range `timestamp_from_millis` admits, so an overflow reads
/// back as `SnapshotTimestampOutOfRange` and never as another time.
pub fn millis_after(base: i64, step: Duration) -> i64 {
    base.saturating_add(i64::try_from(step.as_millis()).unwrap_or(i64::MAX))
}

/// The unit an Iceberg date or timestamp counts from the Unix epoch in,
/// as its order values carry it: days for `date`, microseconds for
/// `timestamp` and `timestamptz`, nanoseconds for the two `_ns` types.
/// Display gives the unit's plural name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EpochUnit {
    /// Days, for `date`.
    Days,
    /// Microseconds, for `timestamp` and `timestamptz`.
    Microseconds,
    /// Nanoseconds, for `timestamp_ns` and `timestamptz_ns`.
    Nanoseconds,
}

impl std::fmt::Display for EpochUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Days => "days",
            Self::Microseconds => "microseconds",
            Self::Nanoseconds => "nanoseconds",
        })
    }
}

/// Why text is not a time `epoch_count` turns into a count of its unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EpochTextFault {
    /// The text is not in the one form the unit takes: `YYYY-MM-DD` for
    /// days, `YYYY-MM-DDTHH:MM:SS[.fraction](Z|+HH:MM|-HH:MM)` for the
    /// others, with an uppercase `T` and `Z`, a four-digit year, at most
    /// nine fraction digits, and an offset hour of at most 23.
    #[error("the text is not in the unit's ISO 8601 form")]
    NotIso,
    /// The text has the form and names no date or instant jiff holds: a
    /// field out of range such as a February 30, or an instant past
    /// 9999-12-30T22:00:00.999999999Z, where jiff's instants end.
    #[error("the text names no date or instant jiff holds")]
    NoSuchTime,
    /// The instant's seconds are 60, a leap second, which jiff reads as
    /// the second before; refused, as a bound one second off moves the
    /// window.
    #[error("the instant is a leap second, which no epoch count holds")]
    LeapSecond,
    /// The instant has digits below a microsecond that are not zero, which
    /// a count of microseconds cannot hold.
    #[error("the instant has digits below a microsecond")]
    BelowTheUnit,
    /// The instant's count of nanoseconds does not fit a signed 64-bit
    /// integer, which spans 1677-09-21T00:12:43.145224192Z to
    /// 2262-04-11T23:47:16.854775807Z.
    #[error("the instant is outside the range a long of nanoseconds holds")]
    OutOfRange,
}

/// The shape of a calendar date: `D` stands for an ASCII digit, and every
/// other byte for itself.
const DATE_SHAPE: &[u8] = b"DDDD-DD-DD";

/// The shape of an instant up to its fraction and offset.
const DATETIME_SHAPE: &[u8] = b"DDDD-DD-DDTDD:DD:DD";

/// The shape of a numeric offset after its sign.
const OFFSET_SHAPE: &[u8] = b"DD:DD";

/// The byte a shape holds where the text holds an ASCII digit.
const SHAPE_DIGIT: u8 = b'D';

/// What separates an instant's seconds from its fraction.
const FRACTION_POINT: char = '.';

/// The most fraction digits an instant takes: nanoseconds.
const MAX_FRACTION_DIGITS: usize = 9;

/// The offset of UTC.
const ZULU: &str = "Z";

/// The greatest offset hour, as its two digits; ASCII digits of one width
/// order as their numbers do.
const MAX_OFFSET_HOUR: &str = "23";

/// How an instant whose seconds are a leap second ends, up to its
/// fraction.
const LEAP_SECOND: &str = ":60";

/// Whether `text` has `shape` byte for byte.
fn has_shape(text: &str, shape: &[u8]) -> bool {
    text.len() == shape.len()
        && text.bytes().zip(shape).all(|(byte, &want)| {
            if want == SHAPE_DIGIT {
                byte.is_ascii_digit()
            } else {
                byte == want
            }
        })
}

/// Whether `offset` is `Z` or a sign and `HH:MM` with an hour of at most
/// 23. jiff checks the minutes.
fn is_offset(offset: &str) -> bool {
    offset == ZULU
        || offset.strip_prefix(['+', '-']).is_some_and(|numeric| {
            has_shape(numeric, OFFSET_SHAPE)
                && numeric
                    .get(..MAX_OFFSET_HOUR.len())
                    .is_some_and(|hour| hour <= MAX_OFFSET_HOUR)
        })
}

/// `Ok` when `text` has the one shape an instant takes (`NotIso`) and its
/// seconds are no leap second (`LeapSecond`), which jiff would read as the
/// second before.
fn instant_shape(text: &str) -> Result<(), EpochTextFault> {
    let (datetime, rest) = text
        .split_at_checked(DATETIME_SHAPE.len())
        .ok_or(EpochTextFault::NotIso)?;
    if !has_shape(datetime, DATETIME_SHAPE) {
        return Err(EpochTextFault::NotIso);
    }
    let offset = match rest.strip_prefix(FRACTION_POINT) {
        Some(fraction) => {
            let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
            if !(1..=MAX_FRACTION_DIGITS).contains(&digits) {
                return Err(EpochTextFault::NotIso);
            }
            fraction.get(digits..).ok_or(EpochTextFault::NotIso)?
        }
        None => rest,
    };
    if !is_offset(offset) {
        return Err(EpochTextFault::NotIso);
    }
    if datetime.ends_with(LEAP_SECOND) {
        return Err(EpochTextFault::LeapSecond);
    }
    Ok(())
}

/// The first day of the Unix epoch, which Iceberg counts dates from.
fn epoch_date() -> jiff::civil::Date {
    jiff::tz::Offset::UTC
        .to_datetime(jiff::Timestamp::UNIX_EPOCH)
        .date()
}

/// The count of `unit` since the Unix epoch that ISO 8601 `text` names,
/// as an Iceberg `date` or timestamp stores it: the one place such text
/// becomes an epoch count. Each unit takes exactly one form, checked
/// before jiff parses it, so no form jiff also reads, such as a space
/// for `T`, an hour-only offset, or a bracketed time zone, passes for
/// another time. Days take a calendar date, `2026-01-01`. The other
/// units take an instant, `2026-01-01T00:00:00.5Z` or
/// `2026-01-01T09:00:00+09:00`, whose offset is required, so no local
/// time zone is ever consulted, a `timestamp`'s included, though that
/// type records none: its count is the instant's UTC wall-clock reading.
/// An instant is never rounded: a leap second is `LeapSecond`, digits
/// below a microsecond that are not zero are `BelowTheUnit` for
/// microseconds, and an instant whose nanoseconds a long does not hold
/// is `OutOfRange`.
pub fn epoch_count(text: &str, unit: EpochUnit) -> Result<i64, EpochTextFault> {
    match unit {
        EpochUnit::Days => {
            if !has_shape(text, DATE_SHAPE) {
                return Err(EpochTextFault::NotIso);
            }
            let date: jiff::civil::Date = text.parse().map_err(|_| EpochTextFault::NoSuchTime)?;
            let days = date
                .since(epoch_date())
                .expect("a span in days holds the distance between any two dates jiff holds")
                .get_days();
            Ok(i64::from(days))
        }
        EpochUnit::Microseconds => {
            let at = instant(text)?;
            let micros = at.as_microsecond();
            if jiff::SignedDuration::from_micros(micros) == at.as_duration() {
                Ok(micros)
            } else {
                Err(EpochTextFault::BelowTheUnit)
            }
        }
        EpochUnit::Nanoseconds => {
            let at = instant(text)?;
            i64::try_from(at.as_nanosecond()).map_err(|_| EpochTextFault::OutOfRange)
        }
    }
}

/// `text` as an instant, when it has the one shape an instant takes and
/// names an instant jiff holds.
fn instant(text: &str) -> Result<jiff::Timestamp, EpochTextFault> {
    instant_shape(text)?;
    text.parse().map_err(|_| EpochTextFault::NoSuchTime)
}

/// A duration as configuration text, in the friendly form
/// `friendly_duration` reads: `"0s"`, `"250ms"`, `"1m 30s"`. What a
/// message prints a configured duration as, so it reads as the file wrote
/// it; text that spelled the same duration in other units reads back in
/// the largest units that hold it.
pub fn friendly(duration: Duration) -> String {
    jiff::fmt::friendly::SpanPrinter::new().unsigned_duration_to_string(&duration)
}

/// Serde `with` module for a `std::time::Duration` field written in jiff's
/// friendly form: `"10s"`, `"250ms"`, `"1m 30s"`. Only that grammar is
/// accepted: a negative duration (`"-1s"`, `"1s ago"`) and text without a
/// unit designator (`"10"`) are errors.
pub mod friendly_duration {
    use std::time::Duration;

    use jiff::fmt::friendly::SpanParser;
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer};

    /// The parser of the friendly grammar, in its default configuration,
    /// which accepts every unit designator and no bare number.
    static PARSER: SpanParser = SpanParser::new();

    /// Parses the friendly form; anything else is a deserialization error.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let text = String::deserialize(deserializer)?;
        PARSER
            .parse_unsigned_duration(&text)
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde::Deserialize;

    /// A field carrying the `with` module, so the tests go through serde
    /// exactly as a configuration field does.
    #[derive(Debug, PartialEq, Eq, Deserialize)]
    struct Holder {
        #[serde(with = "super::friendly_duration")]
        elapsed: Duration,
    }

    /// The value `"10s"` denotes.
    const TEN_SECONDS: Duration = Duration::from_secs(10);
    /// The value `"1m"` denotes.
    const ONE_MINUTE: Duration = Duration::from_secs(60);
    /// The value `"250ms"` denotes.
    const QUARTER_SECOND: Duration = Duration::from_millis(250);
    /// The value `"1m 30s"` denotes, which spans two units.
    const NINETY_SECONDS: Duration = Duration::from_secs(90);
    /// A duration with a fractional second, so the float carries it.
    const ONE_AND_A_HALF_SECONDS: Duration = Duration::from_millis(1500);
    /// The float `ONE_AND_A_HALF_SECONDS` becomes; exactly representable.
    const ONE_AND_A_HALF: f64 = 1.5;

    fn parse(text: &str) -> Result<Holder, serde_json::Error> {
        serde_json::from_str(&format!("{{\"elapsed\": \"{text}\"}}"))
    }

    #[test]
    fn test_friendly_round_trips_through_the_parser() {
        for duration in [
            Duration::ZERO,
            TEN_SECONDS,
            ONE_MINUTE,
            QUARTER_SECOND,
            NINETY_SECONDS,
        ] {
            let text = super::friendly(duration);
            assert_eq!(parse(&text).unwrap().elapsed, duration, "{text}");
        }
        assert_eq!(super::friendly(Duration::ZERO), "0s");
        assert_eq!(super::friendly(NINETY_SECONDS), "1m 30s");
    }

    #[test]
    fn test_seconds_carries_the_fraction() {
        assert_eq!(
            super::seconds(ONE_AND_A_HALF_SECONDS).to_bits(),
            ONE_AND_A_HALF.to_bits()
        );
    }

    #[test]
    fn test_seconds_zero_is_zero() {
        assert_eq!(super::seconds(Duration::ZERO).to_bits(), 0.0f64.to_bits());
    }

    /// The microseconds `ONE_AND_A_HALF_SECONDS` holds.
    const ONE_AND_A_HALF_SECONDS_MICROS: u64 = 1_500_000;

    #[test]
    fn test_micros_is_the_whole_microseconds() {
        assert_eq!(
            super::micros(ONE_AND_A_HALF_SECONDS),
            ONE_AND_A_HALF_SECONDS_MICROS
        );
        assert_eq!(super::micros(Duration::ZERO), 0);
    }

    #[test]
    fn test_from_micros_reads_back_what_micros_gives() {
        assert_eq!(
            super::from_micros(ONE_AND_A_HALF_SECONDS_MICROS),
            ONE_AND_A_HALF_SECONDS
        );
        assert_eq!(
            super::from_micros(super::micros(ONE_AND_A_HALF_SECONDS)),
            ONE_AND_A_HALF_SECONDS
        );
    }

    /// One nanosecond short of a microsecond; truncates to 0.
    const JUST_UNDER_A_MICRO: Duration = Duration::from_nanos(999);
    /// One nanosecond short of two microseconds; truncates to 1.
    const JUST_UNDER_TWO_MICROS: Duration = Duration::from_nanos(1_999);

    #[test]
    fn test_micros_truncates_below_a_microsecond() {
        assert_eq!(super::micros(JUST_UNDER_A_MICRO), 0);
        assert_eq!(super::micros(JUST_UNDER_TWO_MICROS), 1);
    }

    #[test]
    fn test_micros_saturates_at_u64_max() {
        assert_eq!(super::micros(Duration::MAX), u64::MAX);
    }

    /// An age far enough in the past that clock skew between two reads of
    /// `now` cannot bring it below `AGE_LOWER_BOUND`.
    const AGE_OFFSET: Duration = Duration::from_secs(3_600);
    /// Half of `AGE_OFFSET`; the measured age is at least this.
    const AGE_LOWER_BOUND: f64 = 1_800.0;
    /// Twice `AGE_OFFSET`; the measured age is at most this.
    const AGE_UPPER_BOUND: f64 = 7_200.0;

    #[test]
    fn test_age_seconds_of_the_past_is_positive() {
        let since = super::now()
            .checked_sub(AGE_OFFSET)
            .expect("an hour ago is representable");
        let age = super::age_seconds(since);
        assert!(age > AGE_LOWER_BOUND && age < AGE_UPPER_BOUND, "{age}");
    }

    #[test]
    fn test_age_seconds_of_the_future_is_negative() {
        let since = super::now()
            .checked_add(AGE_OFFSET)
            .expect("an hour ahead is representable");
        let age = super::age_seconds(since);
        assert!(age < -AGE_LOWER_BOUND && age > -AGE_UPPER_BOUND, "{age}");
    }

    #[test]
    fn test_log_timestamp_is_rfc3339_in_utc() {
        assert_eq!(
            super::log_timestamp(jiff::Timestamp::UNIX_EPOCH),
            "1970-01-01T00:00:00Z"
        );
        let with_millis = super::timestamp_from_millis(EPOCH_MILLIS).unwrap();
        assert_eq!(
            super::log_timestamp(with_millis),
            "2023-11-14T22:13:20.123Z"
        );
    }

    #[test]
    fn test_log_timestamp_round_trips_through_jiff() {
        let at = super::timestamp_from_millis(EPOCH_MILLIS).unwrap();
        let back: jiff::Timestamp = super::log_timestamp(at).parse().unwrap();
        assert_eq!(back, at);
    }

    /// Milliseconds since the epoch with a fraction of a second, so the
    /// round trip carries the milliseconds.
    const EPOCH_MILLIS: i64 = 1_700_000_000_123;

    /// `EPOCH_MILLIS` in seconds, the fraction kept.
    const EPOCH_SECONDS: f64 = 1_700_000_000.123;

    #[test]
    fn test_unix_seconds_counts_from_the_epoch_with_the_fraction() {
        assert_eq!(
            super::unix_seconds(jiff::Timestamp::UNIX_EPOCH).to_bits(),
            0.0f64.to_bits()
        );
        let at = super::timestamp_from_millis(EPOCH_MILLIS).unwrap();
        assert_eq!(super::unix_seconds(at).to_bits(), EPOCH_SECONDS.to_bits());
    }

    #[test]
    fn test_timestamp_from_millis_zero_is_the_epoch() {
        assert_eq!(
            super::timestamp_from_millis(0).unwrap(),
            jiff::Timestamp::UNIX_EPOCH
        );
    }

    #[test]
    fn test_timestamp_from_millis_round_trips() {
        assert_eq!(
            super::timestamp_from_millis(EPOCH_MILLIS)
                .unwrap()
                .as_millisecond(),
            EPOCH_MILLIS
        );
    }

    #[test]
    fn test_timestamp_from_millis_rejects_out_of_range() {
        assert!(super::timestamp_from_millis(i64::MAX).is_err());
        assert!(super::timestamp_from_millis(i64::MIN).is_err());
    }

    /// The step a fixture commit is stamped after the metadata's last update,
    /// and the same step in the epoch milliseconds it adds.
    const COMMIT_STEP: Duration = Duration::from_millis(3);
    const COMMIT_STEP_MILLIS: i64 = 3;
    /// Below a millisecond: dropped, as Iceberg's timestamps cannot hold it.
    const SUB_MILLISECOND: Duration = Duration::from_micros(999);

    #[test]
    fn test_millis_after_adds_the_step() {
        assert_eq!(
            super::millis_after(EPOCH_MILLIS, COMMIT_STEP),
            EPOCH_MILLIS + COMMIT_STEP_MILLIS
        );
        assert_eq!(
            super::millis_after(EPOCH_MILLIS, COMMIT_STEP + SUB_MILLISECOND),
            EPOCH_MILLIS + COMMIT_STEP_MILLIS
        );
    }

    #[test]
    fn test_millis_after_saturates_outside_i64() {
        assert_eq!(super::millis_after(i64::MAX, COMMIT_STEP), i64::MAX);
        assert_eq!(super::millis_after(EPOCH_MILLIS, Duration::MAX), i64::MAX);
    }

    use super::{EpochTextFault, EpochUnit, epoch_count};

    #[test]
    fn test_epoch_count_days_counts_calendar_days_from_the_epoch() {
        let days = |text| epoch_count(text, EpochUnit::Days);
        assert_eq!(days("1970-01-01"), Ok(0));
        assert_eq!(days("1970-01-02"), Ok(1));
        assert_eq!(days("1969-12-31"), Ok(-1));
        assert_eq!(days("1972-03-01"), Ok(790), "1972 is a leap year");
        assert_eq!(days("2024-02-29"), Ok(19_782), "2024 is a leap year");
        assert_eq!(days("0000-01-01"), Ok(-719_528));
        assert_eq!(days("9999-12-31"), Ok(2_932_896));
    }

    #[test]
    fn test_epoch_count_days_refuses_anything_but_yyyy_mm_dd() {
        for text in [
            "+010000-01-01",
            "-000001-01-01",
            "+002026-01-01",
            "10000-01-01",
            "2026-01-01T00:00:00",
            "2026-01-01T00:00:00+00:00",
            "20260101",
            "2026-1-1",
            "2026-01-01 ",
            "",
        ] {
            assert_eq!(
                epoch_count(text, EpochUnit::Days),
                Err(EpochTextFault::NotIso),
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_epoch_count_days_refuses_a_day_no_calendar_holds() {
        for text in ["2026-02-30", "2023-02-29", "2026-13-01", "2026-00-10"] {
            assert_eq!(
                epoch_count(text, EpochUnit::Days),
                Err(EpochTextFault::NoSuchTime),
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_epoch_count_microseconds_reads_an_instant_with_its_offset() {
        let micros = |text| epoch_count(text, EpochUnit::Microseconds);
        assert_eq!(micros("1970-01-01T00:00:00Z"), Ok(0));
        assert_eq!(micros("1970-01-01T00:00:00.000001Z"), Ok(1));
        assert_eq!(micros("1970-01-01T00:00:00.000001000Z"), Ok(1));
        assert_eq!(micros("1969-12-31T23:59:59.999999Z"), Ok(-1));
        assert_eq!(micros("1970-01-01T01:00:00+01:00"), Ok(0));
        assert_eq!(micros("1970-01-01T00:00:00-00:00"), Ok(0));
        assert_eq!(
            micros("1970-01-01T23:59:00+23:59"),
            Ok(0),
            "the greatest offset"
        );
        assert_eq!(
            micros("1969-12-31T00:01:00-23:59"),
            Ok(0),
            "the least offset"
        );
        assert_eq!(micros("1969-12-31T23:00:00-01:00"), Ok(0));
        assert_eq!(
            micros("2023-11-14T22:13:20.123Z"),
            Ok(1_700_000_000_123_000)
        );
        assert_eq!(micros("2024-02-29T00:00:00Z"), Ok(1_709_164_800_000_000));
        assert_eq!(
            micros("9999-12-30T22:00:00.999999Z"),
            Ok(253_402_207_200_999_999),
            "the last microsecond jiff holds"
        );
    }

    #[test]
    fn test_epoch_count_microseconds_refuses_digits_below_a_microsecond() {
        for text in [
            "1970-01-01T00:00:00.0000001Z",
            "1970-01-01T00:00:00.000001001Z",
            "1969-12-31T23:59:59.9999999Z",
            "1969-12-31T23:59:59.9999995Z",
            "9999-12-30T22:00:00.999999999Z",
        ] {
            assert_eq!(
                epoch_count(text, EpochUnit::Microseconds),
                Err(EpochTextFault::BelowTheUnit),
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_epoch_count_instants_refuse_every_form_but_the_one() {
        let forms = [
            "2026-01-01T00:00:00",
            "2026-01-01",
            "1700000000",
            "now",
            "2026-01-01 00:00:00Z",
            "2026-01-01t00:00:00z",
            "2026-01-01T00:00:00z",
            "2026-01-01T00:00:00+05",
            "2026-01-01T00:00:00+0500",
            "2026-01-01T00:00:00+05:30:15",
            "2026-01-01T00:00:00+24:00",
            "2026-01-01T00Z",
            "2026-01-01T00:00Z",
            "20260101T000000Z",
            "2026-01-01T00:00:00,5Z",
            "2026-01-01T00:00:00.Z",
            "2026-01-01T00:00:00.0000000001Z",
            "+002026-01-01T00:00:00Z",
            "-000001-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z[America/New_York]",
            "2026-01-01T00:00:00+09:00[America/New_York]",
            "2026-01-01T00:00:00Z[u-ca=hebrew]",
            "2026-01-01T00:00:00[America/New_York]",
            "2026-01-01T00:00:00Z ",
        ];
        for unit in [EpochUnit::Microseconds, EpochUnit::Nanoseconds] {
            for text in forms {
                assert_eq!(
                    epoch_count(text, unit),
                    Err(EpochTextFault::NotIso),
                    "{text:?} as {unit}"
                );
            }
        }
    }

    #[test]
    fn test_epoch_count_instants_refuse_a_time_no_calendar_holds() {
        for text in [
            "2023-02-29T00:00:00Z",
            "2026-01-01T24:00:00Z",
            "2026-01-01T00:00:00+05:60",
            "9999-12-31T00:00:00Z",
        ] {
            assert_eq!(
                epoch_count(text, EpochUnit::Microseconds),
                Err(EpochTextFault::NoSuchTime),
                "{text:?}"
            );
        }
    }

    #[test]
    fn test_epoch_count_instants_refuse_a_leap_second() {
        for unit in [EpochUnit::Microseconds, EpochUnit::Nanoseconds] {
            for text in ["2016-12-31T23:59:60Z", "2016-12-31T23:59:60.5Z"] {
                assert_eq!(
                    epoch_count(text, unit),
                    Err(EpochTextFault::LeapSecond),
                    "{text:?} as {unit}"
                );
            }
        }
    }

    #[test]
    fn test_epoch_count_nanoseconds_spans_exactly_a_long() {
        let nanos = |text| epoch_count(text, EpochUnit::Nanoseconds);
        assert_eq!(nanos("1970-01-01T00:00:00.000000001Z"), Ok(1));
        assert_eq!(nanos("2262-04-11T23:47:16.854775807Z"), Ok(i64::MAX));
        assert_eq!(nanos("1677-09-21T00:12:43.145224192Z"), Ok(i64::MIN));
        for text in ["2262-04-11T23:47:16.854775808Z", "1677-09-21T00:00:00Z"] {
            assert_eq!(nanos(text), Err(EpochTextFault::OutOfRange), "{text:?}");
        }
    }

    #[test]
    fn test_deserialize_parses_seconds() {
        assert_eq!(parse("10s").unwrap().elapsed, TEN_SECONDS);
    }

    #[test]
    fn test_deserialize_parses_minutes() {
        assert_eq!(parse("1m").unwrap().elapsed, ONE_MINUTE);
    }

    #[test]
    fn test_deserialize_parses_milliseconds() {
        assert_eq!(parse("250ms").unwrap().elapsed, QUARTER_SECOND);
    }

    #[test]
    fn test_deserialize_rejects_negative() {
        assert!(parse("-1s").is_err());
        assert!(parse("1s ago").is_err());
    }

    #[test]
    fn test_deserialize_rejects_bare_number() {
        assert!(parse("10").is_err());
    }

    #[test]
    fn test_deserialize_parses_two_units() {
        assert_eq!(parse("1m 30s").unwrap().elapsed, NINETY_SECONDS);
    }
}
