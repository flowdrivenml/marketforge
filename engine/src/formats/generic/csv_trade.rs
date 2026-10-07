use csv::ByteRecord;

use crate::{
    canonical::{EventEnvelope, Exchange, Trade},
    error::{MarketForgeError, Result},
    job::{NormalizationConfig, QuantityEncoding},
    normalize::{parse_buy_sell, parse_decimal, parse_timestamp_ns},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvTradeSpec {
    pub timestamp: &'static str,
    pub price: &'static str,
    pub quantity: &'static str,

    pub side: Option<&'static str>,
    pub trade_id: Option<&'static str>,
    pub sequence: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCsvTradeSpec {
    pub timestamp_idx: usize,
    pub price_idx: usize,
    pub quantity_idx: usize,

    pub side_idx: Option<usize>,
    pub trade_id_idx: Option<usize>,
    pub sequence_idx: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeContext {
    pub exchange: Exchange,
    pub instrument_id: i64,
    pub symbol: String,
    pub stream_id: String,
}

pub struct GenericCsvTradeProcessor {
    spec: ResolvedCsvTradeSpec,
    normalization: NormalizationConfig,
    context: TradeContext,
}

impl CsvTradeSpec {
    pub fn resolve(&self, headers: &ByteRecord) -> Result<ResolvedCsvTradeSpec> {
        Ok(ResolvedCsvTradeSpec {
            timestamp_idx: resolve_required(headers, self.timestamp)?,

            price_idx: resolve_required(headers, self.price)?,

            quantity_idx: resolve_required(headers, self.quantity)?,

            side_idx: resolve_optional(headers, self.side)?,

            trade_id_idx: resolve_optional(headers, self.trade_id)?,

            sequence_idx: resolve_optional(headers, self.sequence)?,
        })
    }
}

impl GenericCsvTradeProcessor {
    pub fn new(
        spec: ResolvedCsvTradeSpec,
        normalization: NormalizationConfig,
        context: TradeContext,
    ) -> Self {
        Self {
            spec,
            normalization,
            context,
        }
    }

    pub fn process_record(&self, record: &ByteRecord) -> Result<Trade> {
        let timestamp = required_field(record, self.spec.timestamp_idx, "timestamp")?;

        let price = required_field(record, self.spec.price_idx, "price")?;

        let quantity = required_field(record, self.spec.quantity_idx, "quantity")?;

        let side_idx = self.spec.side_idx.ok_or_else(|| {
            MarketForgeError::InvalidCanonical(
                "trade format does not define a side column".to_owned(),
            )
        })?;

        let side = required_field(record, side_idx, "side")?;

        let event_timestamp_ns =
            parse_timestamp_ns(timestamp, self.normalization.timestamp_encoding)?;

        let price = parse_decimal(price, "price")?;

        let quantity = parse_decimal(quantity, "quantity")?;

        let side = parse_buy_sell(side)?;

        let trade_id = optional_string_field(record, self.spec.trade_id_idx, "trade_id")?;

        let sequence = optional_u64_field(record, self.spec.sequence_idx, "sequence")?;

        let (quantity_base, quantity_quote, quantity_contracts) =
            match self.normalization.quantity_encoding {
                QuantityEncoding::Base => (Some(quantity), None, None),

                QuantityEncoding::Quote => (None, Some(quantity), None),

                QuantityEncoding::Contracts => (None, None, Some(quantity)),
            };

        Ok(Trade {
            envelope: EventEnvelope {
                event_timestamp_ns,
                system_timestamp_ns: None,

                exchange: self.context.exchange,
                instrument_id: self.context.instrument_id,
                symbol: self.context.symbol.clone(),
                stream_id: self.context.stream_id.clone(),
            },

            trade_id,
            sequence,

            side,
            price,

            quantity_base,
            quantity_quote,
            quantity_contracts,

            is_rpi: None,

            trade_iv: None,
            mark_iv: None,

            index_price: None,
            mark_price: None,
        })
    }
}

fn resolve_required(headers: &ByteRecord, name: &str) -> Result<usize> {
    find_header(headers, name).ok_or_else(|| {
        MarketForgeError::InvalidConfiguration(format!("required CSV column not found: {name}",))
    })
}

fn resolve_optional(headers: &ByteRecord, name: Option<&str>) -> Result<Option<usize>> {
    match name {
        Some(name) => {
            let index = find_header(headers, name).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(format!(
                    "configured CSV column not found: {name}",
                ))
            })?;

            Ok(Some(index))
        }

        None => Ok(None),
    }
}

fn find_header(headers: &ByteRecord, name: &str) -> Option<usize> {
    headers.iter().position(|header| header == name.as_bytes())
}

fn required_field<'a>(record: &'a ByteRecord, index: usize, field: &str) -> Result<&'a [u8]> {
    record
        .get(index)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            MarketForgeError::InvalidCanonical(format!("missing required CSV field: {field}",))
        })
}

fn optional_string_field(
    record: &ByteRecord,
    index: Option<usize>,
    field: &str,
) -> Result<Option<String>> {
    let Some(index) = index else {
        return Ok(None);
    };

    let Some(value) = record.get(index) else {
        return Ok(None);
    };

    if value.is_empty() {
        return Ok(None);
    }

    let value = std::str::from_utf8(value).map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid UTF-8 in {field}: {error}",))
    })?;

    Ok(Some(value.to_owned()))
}

fn optional_u64_field(
    record: &ByteRecord,
    index: Option<usize>,
    field: &str,
) -> Result<Option<u64>> {
    let Some(index) = index else {
        return Ok(None);
    };

    let Some(value) = record.get(index) else {
        return Ok(None);
    };

    if value.is_empty() {
        return Ok(None);
    }

    let value = std::str::from_utf8(value).map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid UTF-8 in {field}: {error}",))
    })?;

    let value = value.parse::<u64>().map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid integer in {field}: {value}: {error}",))
    })?;

    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::{canonical::TradeSide, job::TimestampEncoding};

    fn test_spec() -> CsvTradeSpec {
        CsvTradeSpec {
            timestamp: "timestamp",
            price: "price",
            quantity: "size",

            side: Some("side"),
            trade_id: Some("trade_id"),
            sequence: Some("sequence"),
        }
    }

    #[test]
    fn resolves_trade_columns() {
        let headers = ByteRecord::from(vec![
            "timestamp",
            "symbol",
            "side",
            "size",
            "price",
            "trade_id",
            "sequence",
        ]);

        let resolved = test_spec()
            .resolve(&headers)
            .expect("resolve trade specification");

        assert_eq!(resolved.timestamp_idx, 0);
        assert_eq!(resolved.price_idx, 4);
        assert_eq!(resolved.quantity_idx, 3);

        assert_eq!(resolved.side_idx, Some(2));
        assert_eq!(resolved.trade_id_idx, Some(5));
        assert_eq!(resolved.sequence_idx, Some(6));
    }

    #[test]
    fn rejects_missing_required_column() {
        let headers = ByteRecord::from(vec!["timestamp", "size"]);

        let spec = CsvTradeSpec {
            timestamp: "timestamp",
            price: "price",
            quantity: "size",

            side: None,
            trade_id: None,
            sequence: None,
        };

        assert!(spec.resolve(&headers).is_err());
    }

    #[test]
    fn rejects_missing_configured_optional_column() {
        let headers = ByteRecord::from(vec!["timestamp", "price", "size"]);

        let spec = CsvTradeSpec {
            timestamp: "timestamp",
            price: "price",
            quantity: "size",

            side: Some("side"),
            trade_id: None,
            sequence: None,
        };

        assert!(spec.resolve(&headers).is_err());
    }

    #[test]
    fn converts_record_to_canonical_trade() {
        let headers = ByteRecord::from(vec![
            "timestamp",
            "symbol",
            "side",
            "size",
            "price",
            "trade_id",
            "sequence",
        ]);

        let resolved = test_spec()
            .resolve(&headers)
            .expect("resolve trade specification");

        let processor = GenericCsvTradeProcessor::new(
            resolved,
            NormalizationConfig {
                timestamp_encoding: TimestampEncoding::Milliseconds,

                quantity_encoding: QuantityEncoding::Base,
            },
            TradeContext {
                exchange: Exchange::Bybit,
                instrument_id: 1,
                symbol: "BTCUSDT".to_owned(),
                stream_id: "bybit:BTCUSDT:trades".to_owned(),
            },
        );

        let record = ByteRecord::from(vec![
            "1000", "BTCUSDT", "Buy", "2.5", "84500.25", "trade-42", "123",
        ]);

        let trade = processor.process_record(&record).expect("process trade");

        assert_eq!(trade.envelope.event_timestamp_ns, 1_000_000_000,);

        assert_eq!(trade.envelope.exchange, Exchange::Bybit,);

        assert_eq!(trade.side, TradeSide::Buy,);

        assert_eq!(trade.price, Decimal::new(8_450_025, 2),);

        assert_eq!(trade.quantity_base, Some(Decimal::new(25, 1)),);

        assert_eq!(trade.quantity_quote, None,);

        assert_eq!(trade.quantity_contracts, None,);

        assert_eq!(trade.trade_id.as_deref(), Some("trade-42"),);

        assert_eq!(trade.sequence, Some(123),);
    }
}
