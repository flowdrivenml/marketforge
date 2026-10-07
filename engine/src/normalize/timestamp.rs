use crate::{
    canonical::TimestampNs,
    error::{MarketForgeError, Result},
    job::TimestampEncoding,
};

pub fn parse_timestamp_ns(value: &[u8], encoding: TimestampEncoding) -> Result<TimestampNs> {
    let value = std::str::from_utf8(value).map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid UTF-8 timestamp: {error}",))
    })?;

    match encoding {
        TimestampEncoding::Seconds => parse_seconds(value),

        TimestampEncoding::Milliseconds => scale_integer_timestamp(value, 1_000_000),

        TimestampEncoding::Microseconds => scale_integer_timestamp(value, 1_000),

        TimestampEncoding::Nanoseconds => scale_integer_timestamp(value, 1),
    }
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
    let seconds = value.parse::<rust_decimal::Decimal>().map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid seconds timestamp {value}: {error}",))
    })?;

    let nanoseconds = seconds * rust_decimal::Decimal::from(1_000_000_000_i64);

    nanoseconds
        .trunc()
        .to_string()
        .parse::<i64>()
        .map_err(|error| {
            MarketForgeError::InvalidCanonical(format!(
                "timestamp cannot be represented as nanoseconds: {value}: {error}",
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
