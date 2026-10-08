use csv::ByteRecord;

use crate::{
    canonical::{EventEnvelope, Exchange, Trade},
    error::{MarketForgeError, Result},
    job::{InstrumentSpec, NormalizationConfig, TargetSchema},
    normalize::{normalize_quantity, parse_decimal, parse_timestamp_ns},
    validate::validate_trade,
};

use super::{
    spec::{FieldSpec, QuantitySpec, SideSpec, TradeSpec},
    transforms::{transform_bool, transform_quantity, transform_side},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeContext {
    pub exchange: Exchange,
    pub instrument_id: i64,
    pub symbol: String,
    pub stream_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResolvedField {
    index: usize,
}

#[derive(Debug, Clone)]
struct ResolvedTradeSpec {
    timestamp: ResolvedField,
    price: ResolvedField,
    quantity: ResolvedField,
    side: ResolvedField,
    trade_id: Option<ResolvedField>,
    sequence: Option<ResolvedField>,
    is_rpi: Option<ResolvedField>,
}

pub struct TradeProcessor {
    spec: TradeSpec,
    resolved: ResolvedTradeSpec,
    normalization: NormalizationConfig,
    instrument: InstrumentSpec,
    context: TradeContext,
}

impl TradeProcessor {
    pub fn new(
        headers: &ByteRecord,
        normalization: NormalizationConfig,
        instrument: InstrumentSpec,
        context: TradeContext,
    ) -> Result<Self> {
        if normalization.target_schema != TargetSchema::Trade {
            return Err(MarketForgeError::InvalidConfiguration(
                "trade processor requires target_schema=trade".to_owned(),
            ));
        }

        let spec = TradeSpec::from_normalization(&normalization)?;

        let resolved = ResolvedTradeSpec {
            timestamp: resolve_required(headers, &spec.event_timestamp)?,
            price: resolve_required(headers, &spec.price)?,
            quantity: resolve_quantity(headers, &spec.quantity)?,
            side: resolve_side(headers, &spec.side)?,

            trade_id: resolve_optional(headers, spec.trade_id.as_ref())?,
            sequence: resolve_optional(headers, spec.sequence.as_ref())?,

            is_rpi: match spec.optional.as_ref().and_then(|o| o.is_rpi.as_ref()) {
                Some(rule) => resolve_optional_source(headers, &rule.source)?,
                None => None,
            },
        };

        Ok(Self {
            spec,
            resolved,
            normalization,
            instrument,
            context,
        })
    }

    pub fn process_record(&self, record: &ByteRecord) -> Result<Trade> {
        let timestamp_raw = required(record, self.resolved.timestamp, "timestamp")?;
        let price_raw = required(record, self.resolved.price, "price")?;
        let quantity_raw = required(record, self.resolved.quantity, "quantity")?;
        let side_raw = required(record, self.resolved.side, "side")?;

        let event_timestamp_ns =
            parse_timestamp_ns(timestamp_raw, self.normalization.timestamp_encoding)?;

        let price = parse_decimal(price_raw, "price")?;

        let raw_quantity = transform_quantity(quantity_raw, &self.spec.quantity)?;

        let quantity = normalize_quantity(
            raw_quantity,
            price,
            self.normalization.quantity_encoding,
            &self.instrument,
        )?;

        let side = transform_side(side_raw, &self.spec.side)?;

        let trade_id = optional_string(record, self.resolved.trade_id)?;

        let sequence = optional_u64(record, self.resolved.sequence)?;

        let is_rpi = match (
            self.spec.optional.as_ref().and_then(|o| o.is_rpi.as_ref()),
            self.resolved.is_rpi,
        ) {
            (Some(rule), Some(field)) => {
                let raw = required(record, field, "is_rpi")?;
                Some(transform_bool(raw, rule)?)
            }

            _ => None,
        };

        let trade = Trade {
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

            quantity_base: quantity.base,
            quantity_quote: quantity.quote,
            quantity_contracts: quantity.contracts,

            is_rpi,

            trade_iv: None,
            mark_iv: None,
            index_price: None,
            mark_price: None,
        };

        validate_trade(&trade)?;

        Ok(trade)
    }
}

fn resolve_required(headers: &ByteRecord, field: &FieldSpec) -> Result<ResolvedField> {
    resolve_name(headers, &field.source)
}

fn resolve_quantity(headers: &ByteRecord, field: &QuantitySpec) -> Result<ResolvedField> {
    resolve_name(headers, &field.source)
}

fn resolve_side(headers: &ByteRecord, field: &SideSpec) -> Result<ResolvedField> {
    resolve_name(headers, &field.source)
}

fn resolve_optional(
    headers: &ByteRecord,
    field: Option<&FieldSpec>,
) -> Result<Option<ResolvedField>> {
    field.map(|f| resolve_name(headers, &f.source)).transpose()
}

fn resolve_optional_source(headers: &ByteRecord, source: &str) -> Result<Option<ResolvedField>> {
    Ok(headers
        .iter()
        .position(|header| header == source.as_bytes())
        .map(|index| ResolvedField { index }))
}

fn resolve_name(headers: &ByteRecord, name: &str) -> Result<ResolvedField> {
    headers
        .iter()
        .position(|header| header == name.as_bytes())
        .map(|index| ResolvedField { index })
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(format!(
                "required trade CSV column not found: {name}"
            ))
        })
}

fn required<'a>(record: &'a ByteRecord, field: ResolvedField, name: &str) -> Result<&'a [u8]> {
    record
        .get(field.index)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            MarketForgeError::InvalidCanonical(format!("missing required trade field: {name}"))
        })
}

fn optional_string(record: &ByteRecord, field: Option<ResolvedField>) -> Result<Option<String>> {
    let Some(field) = field else {
        return Ok(None);
    };

    let Some(value) = record.get(field.index) else {
        return Ok(None);
    };

    if value.is_empty() {
        return Ok(None);
    }

    let value = std::str::from_utf8(value).map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid UTF-8 in trade identifier: {error}"))
    })?;

    Ok(Some(value.to_owned()))
}

fn optional_u64(record: &ByteRecord, field: Option<ResolvedField>) -> Result<Option<u64>> {
    let Some(value) = field.and_then(|field| record.get(field.index)) else {
        return Ok(None);
    };

    if value.is_empty() {
        return Ok(None);
    }

    let value = std::str::from_utf8(value).map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid UTF-8 in trade sequence: {error}"))
    })?;

    value.parse::<u64>().map(Some).map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid trade sequence: {value}: {error}"))
    })
}
