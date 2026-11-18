use crate::shared::validation::IntegrityCategory;
use crate::{
    shared::domain::canonical::TimestampNs,
    shared::error::{MarketForgeError, Result},
    shared::normalization::TimestampEncoding,
};
use rust_decimal::prelude::ToPrimitive;

pub fn parse_timestamp_ns(value: &[u8], encoding: TimestampEncoding) -> Result<TimestampNs> {
    let value = std::str::from_utf8(value).map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid UTF-8 timestamp: {error}",))
    })?;

    match encoding {
        TimestampEncoding::Seconds => parse_seconds(value),

        TimestampEncoding::Milliseconds => scale_integer_timestamp(value, 1_000_000),

        TimestampEncoding::Microseconds => scale_integer_timestamp(value, 1_000),

        TimestampEncoding::Nanoseconds => scale_integer_timestamp(value, 1),
        TimestampEncoding::Iso8601Nanoseconds => parse_iso8601(value),
    }
}

fn parse_iso8601(value: &str) -> Result<TimestampNs> {
    let invalid =
        || MarketForgeError::InvalidCanonical("invalid ISO 8601 nanosecond timestamp".into());
    // RFC 3339 dates with an explicit offset, at most nine fractional digits.
    if value.as_bytes().get(10) != Some(&b'T')
        || value.split_once('.').is_some_and(|(_, fraction)| {
            fraction.bytes().take_while(u8::is_ascii_digit).count() > 9
        })
    {
        return Err(invalid());
    }
    let timestamp = chrono::DateTime::parse_from_rfc3339(value).map_err(|_| invalid())?;
    if timestamp.timestamp_subsec_nanos() >= 1_000_000_000 {
        return Err(invalid());
    }
    timestamp.timestamp_nanos_opt().ok_or_else(invalid)
}

fn scale_integer_timestamp(value: &str, multiplier: i64) -> Result<TimestampNs> {
    let timestamp = value.parse::<i64>().map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid integer timestamp {value}: {error}",))
    })?;

    timestamp
        .checked_mul(multiplier)
        .ok_or_else(|| MarketForgeError::InvalidCanonical(format!("timestamp overflow: {value}",)))
}

fn parse_seconds(value: &str) -> Result<TimestampNs> {
    let seconds = super::parse_decimal(value.as_bytes(), "seconds timestamp").map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid seconds timestamp {value}: {error}",))
    })?;

    let nanoseconds = seconds
        .checked_mul(rust_decimal::Decimal::from(1_000_000_000_i64))
        .ok_or_else(|| MarketForgeError::RecordIntegrity {
            category: IntegrityCategory::InvalidRecord,
            message: format!("timestamp overflow: {value}"),
        })?;

    if !nanoseconds.fract().is_zero() {
        return Err(MarketForgeError::InvalidCanonical(format!(
            "timestamp cannot be represented exactly in nanoseconds: {value}"
        )));
    }
    nanoseconds.to_i64().ok_or_else(|| {
        MarketForgeError::InvalidCanonical(format!(
            "timestamp cannot be represented as nanoseconds: {value}",
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_milliseconds_to_nanoseconds() {
        let timestamp =
            parse_timestamp_ns(b"1000", TimestampEncoding::Milliseconds).expect("parse timestamp");

        assert_eq!(timestamp, 1_000_000_000,);
    }

    #[test]
    fn converts_microseconds_to_nanoseconds() {
        let timestamp = parse_timestamp_ns(b"1000000", TimestampEncoding::Microseconds)
            .expect("parse timestamp");

        assert_eq!(timestamp, 1_000_000_000,);
    }

    #[test]
    fn converts_fractional_seconds_to_nanoseconds() {
        let timestamp =
            parse_timestamp_ns(b"1.5", TimestampEncoding::Seconds).expect("parse timestamp");

        assert_eq!(timestamp, 1_500_000_000,);
    }
}

#[cfg(test)]
mod exact_timestamp_tests {
    use super::*;

    #[test]
    fn iso8601_preserves_nanoseconds_and_offsets() {
        for input in [
            "2024-10-28T14:30:00.510827641Z",
            "2024-10-28T15:30:00.510827641+01:00",
        ] {
            assert_eq!(
                parse_timestamp_ns(input.as_bytes(), TimestampEncoding::Iso8601Nanoseconds)
                    .unwrap(),
                1_730_125_800_510_827_641
            );
        }
    }

    #[test]
    fn iso8601_rejects_invalid_or_lossy_values() {
        for input in [
            "1730125800510827641",
            "2024-10-28T14:30:00.510827641",
            "2024-10-28T14:30:00.5108276411Z",
            "2024-02-30T14:30:00Z",
            "2024-10-28T14:30:60Z",
            "2500-01-01T00:00:00Z",
        ] {
            assert!(
                parse_timestamp_ns(input.as_bytes(), TimestampEncoding::Iso8601Nanoseconds)
                    .is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn rejects_subnanosecond_seconds_instead_of_truncating() {
        assert!(parse_timestamp_ns(b"1.0000000001", TimestampEncoding::Seconds).is_err());
        assert!(parse_timestamp_ns(b"-1.0000000001", TimestampEncoding::Seconds).is_err());
        assert_eq!(
            parse_timestamp_ns(b"1.000000001", TimestampEncoding::Seconds).unwrap(),
            1_000_000_001
        );
        assert_eq!(
            parse_timestamp_ns(b"1.5e0", TimestampEncoding::Seconds).unwrap(),
            1_500_000_000
        );
    }
}
