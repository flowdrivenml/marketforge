//! Cold-path, read-only catalog metadata contract. Database hydration belongs to
//! a downstream adapter; no lookup belongs on the market-event path.
use super::{
    codec::ExactDecimal,
    identity::{ChannelSpec, ExchangeId, MarketKind, SourceStreamKey},
};
use crate::{
    instrument::{Currency, InstrumentIdentity},
    job::{ContractKind, InstrumentKind, InstrumentSpec, QuantityEncoding},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

/// Exact catalog tuple. Symbols are opaque: no suffix inference, case folding,
/// exchange aliases or symbol-only fallback is performed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogLookup {
    pub exchange: String,
    pub symbol: String,
    pub instrument_type: String,
    pub market_category: String,
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetadataError {
    #[error("required metadata field missing: {0}")]
    Missing(&'static str),
    #[error("invalid metadata field: {0}")]
    Invalid(&'static str),
    #[error("unsupported metadata classification: {0}")]
    Unsupported(&'static str),
    #[error("metadata classification conflicts: {0}")]
    Conflict(&'static str),
    #[error("instrument not found for complete catalog tuple")]
    NotFound,
    #[error("multiple instruments found for complete catalog tuple")]
    Ambiguous,
    #[error("metadata resource limit exceeded")]
    Limit,
    #[error("instrument classification must be resolved before activation")]
    UnresolvedClassification,
    #[error("selected quantity normalization is unsupported: {0}")]
    Normalization(&'static str),
    #[error("metadata is older than the caller's freshness bound")]
    Stale,
    #[error("metadata timestamp is later than the caller's reference time")]
    FutureTimestamp,
}
impl CatalogLookup {
    fn classification(
        &self,
    ) -> Result<(ExchangeId, InstrumentKind, MarketKind, Option<ContractKind>), MetadataError> {
        for (field, value) in [
            ("exchange", &self.exchange),
            ("symbol", &self.symbol),
            ("instrument_type", &self.instrument_type),
            ("market_category", &self.market_category),
        ] {
            if value.is_empty() {
                return Err(MetadataError::Missing(field));
            }
            if value.trim() != value {
                return Err(MetadataError::Invalid(field));
            }
        }
        let exchange = serde_json::from_value(serde_json::Value::String(self.exchange.clone()))
            .map_err(|_| MetadataError::Unsupported("exchange"))?;
        let kind = match self.instrument_type.as_str() {
            "spot" => InstrumentKind::Spot,
            "perpetual" => InstrumentKind::Perpetual,
            "future" => InstrumentKind::Future,
            _ => return Err(MetadataError::Unsupported("instrument_type")),
        };
        let market = match self.market_category.as_str() {
            "spot" => MarketKind::Spot,
            "linear" => MarketKind::Linear,
            "inverse" => MarketKind::Inverse,
            _ => return Err(MetadataError::Unsupported("market_category")),
        };
        let contract = match (kind, market) {
            (InstrumentKind::Spot, MarketKind::Spot) => None,
            (InstrumentKind::Perpetual | InstrumentKind::Future, MarketKind::Linear) => {
                Some(ContractKind::Linear)
            }
            (InstrumentKind::Perpetual | InstrumentKind::Future, MarketKind::Inverse) => {
                Some(ContractKind::Inverse)
            }
            _ => return Err(MetadataError::Conflict("instrument_type/market_category")),
        };
        Ok((exchange, kind, market, contract))
    }
}
/// Read-only projection of catalog.instrument_specs, not a new financial model.
/// ExactDecimal is the existing strict decimal-text DTO, converted to the shared
/// InstrumentSpec only after validation. None is never replaced with a multiplier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSpecifications {
    pub quantity_type: QuantityEncoding,
    pub contract_value: Option<ExactDecimal>,
    pub contract_value_asset: Option<String>,
    pub tick_size: Option<ExactDecimal>,
    pub qty_step: Option<ExactDecimal>,
    pub min_qty: Option<ExactDecimal>,
    pub max_qty: Option<ExactDecimal>,
    pub min_notional: Option<ExactDecimal>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogProvenance {
    pub exchange_id: i64,
    pub instrument_id: i64,
    /// catalog.instrument_specs.fetched_at converted without loss to nanoseconds.
    pub fetched_at_ns: Option<i64>,
    pub instrument_updated_at_ns: Option<i64>,
    /// Only populated when an actual catalog snapshot/version is available.
    pub catalog_version: Option<String>,
    pub raw_reference: Option<CatalogRawReference>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogRawReference {
    pub id: i64,
    pub fetched_at_ns: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogRecord {
    pub lookup: CatalogLookup,
    pub base_asset: Option<String>,
    pub quote_asset: Option<String>,
    pub settlement_asset: Option<String>,
    pub specifications: Option<CatalogSpecifications>,
    pub provenance: CatalogProvenance,
}
/// Immutable and not deserializable: validation cannot be bypassed by a DTO.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedInstrumentMetadata {
    lookup: CatalogLookup,
    identity: InstrumentIdentity,
    exchange: ExchangeId,
    market: MarketKind,
    instrument: InstrumentSpec,
    quantity_type: QuantityEncoding,
    base: Currency,
    quote: Currency,
    settlement: Option<Currency>,
    qty_step: Option<Decimal>,
    min_qty: Option<Decimal>,
    max_qty: Option<Decimal>,
    min_notional: Option<Decimal>,
    provenance: CatalogProvenance,
}
fn asset(value: Option<&str>, field: &'static str) -> Result<Currency, MetadataError> {
    Currency::new(value.ok_or(MetadataError::Missing(field))?)
        .map_err(|_| MetadataError::Invalid(field))
}
fn positive(
    value: Option<ExactDecimal>,
    field: &'static str,
) -> Result<Option<Decimal>, MetadataError> {
    value
        .map(|v| {
            if v.0 > Decimal::ZERO {
                Ok(v.0)
            } else {
                Err(MetadataError::Invalid(field))
            }
        })
        .transpose()
}
fn nonnegative(
    value: Option<ExactDecimal>,
    field: &'static str,
) -> Result<Option<Decimal>, MetadataError> {
    value
        .map(|v| {
            if v.0 >= Decimal::ZERO {
                Ok(v.0)
            } else {
                Err(MetadataError::Invalid(field))
            }
        })
        .transpose()
}
impl ValidatedInstrumentMetadata {
    pub fn validate(record: &CatalogRecord) -> Result<Self, MetadataError> {
        let (exchange, kind, market, contract) = record.lookup.classification()?;
        let specs = record
            .specifications
            .as_ref()
            .ok_or(MetadataError::Missing("specifications"))?;
        let tick = positive(specs.tick_size.clone(), "tick_size")?
            .ok_or(MetadataError::Missing("tick_size"))?;
        let contract_value = positive(specs.contract_value.clone(), "contract_value")?;
        if let Some(value) = &specs.contract_value_asset {
            asset(Some(value), "contract_value_asset")?;
        }
        let base = asset(record.base_asset.as_deref(), "base_asset")?;
        let quote = asset(record.quote_asset.as_deref(), "quote_asset")?;
        if base == quote {
            return Err(MetadataError::Conflict("base_asset/quote_asset"));
        }
        let settlement = record
            .settlement_asset
            .as_deref()
            .map(|s| asset(Some(s), "settlement_asset"))
            .transpose()?;
        let qty_step = positive(specs.qty_step.clone(), "qty_step")?;
        let min_qty = nonnegative(specs.min_qty.clone(), "min_qty")?;
        let max_qty = positive(specs.max_qty.clone(), "max_qty")?;
        let min_notional = nonnegative(specs.min_notional.clone(), "min_notional")?;
        if min_qty.zip(max_qty).is_some_and(|(min, max)| min > max) {
            return Err(MetadataError::Conflict("min_qty/max_qty"));
        }
        let p = &record.provenance;
        if p.exchange_id <= 0 || p.instrument_id <= 0 {
            return Err(MetadataError::Invalid("catalog_identity"));
        }
        if p.fetched_at_ns.is_none() {
            return Err(MetadataError::Missing("fetched_at"));
        }
        if p.catalog_version
            .as_ref()
            .is_some_and(|v| v.trim().is_empty())
            || p.raw_reference.as_ref().is_some_and(|r| r.id <= 0)
        {
            return Err(MetadataError::Invalid("provenance"));
        }
        Ok(Self {
            lookup: record.lookup.clone(),
            identity: InstrumentIdentity {
                exchange: record.lookup.exchange.clone(),
                symbol: record.lookup.symbol.clone(),
                instrument_kind: kind,
                contract_kind: contract,
            },
            exchange,
            market,
            instrument: InstrumentSpec {
                instrument_kind: kind,
                contract_kind: contract,
                tick_size: tick,
                contract_value,
                contract_value_asset: specs.contract_value_asset.clone(),
            },
            quantity_type: specs.quantity_type,
            base,
            quote,
            settlement,
            qty_step,
            min_qty,
            max_qty,
            min_notional,
            provenance: p.clone(),
        })
    }
    pub fn lookup(&self) -> &CatalogLookup {
        &self.lookup
    }
    pub fn identity(&self) -> &InstrumentIdentity {
        &self.identity
    }
    pub fn instrument(&self) -> &InstrumentSpec {
        &self.instrument
    }
    pub fn market(&self) -> MarketKind {
        self.market
    }
    pub fn quantity_type(&self) -> QuantityEncoding {
        self.quantity_type
    }
    pub fn base(&self) -> &Currency {
        &self.base
    }
    pub fn quote(&self) -> &Currency {
        &self.quote
    }
    pub fn settlement(&self) -> Option<&Currency> {
        self.settlement.as_ref()
    }
    pub fn qty_step(&self) -> Option<Decimal> {
        self.qty_step
    }
    pub fn min_qty(&self) -> Option<Decimal> {
        self.min_qty
    }
    pub fn max_qty(&self) -> Option<Decimal> {
        self.max_qty
    }
    pub fn min_notional(&self) -> Option<Decimal> {
        self.min_notional
    }
    pub fn provenance(&self) -> &CatalogProvenance {
        &self.provenance
    }
    pub fn validate_identity(&self, identity: &InstrumentIdentity) -> Result<(), MetadataError> {
        if identity.instrument_kind != InstrumentKind::Spot && identity.contract_kind.is_none() {
            return Err(MetadataError::UnresolvedClassification);
        }
        if identity != &self.identity {
            return Err(MetadataError::Conflict("instrument_identity"));
        }
        Ok(())
    }
    pub fn source_key(&self, channel: ChannelSpec) -> Result<SourceStreamKey, MetadataError> {
        let key = SourceStreamKey::new(self.exchange, self.market, self.identity.clone(), channel);
        if !key.validate().issues.is_empty() {
            return Err(MetadataError::Invalid("channel"));
        }
        Ok(key)
    }
    /// Economic preconditions only. WP-12 must separately prove the selected
    /// native field's unit/encoding and channel capability. This does not assume
    /// that every native field has the catalog's default quantity_type.
    pub fn check_normalization(&self, encoding: QuantityEncoding) -> Result<(), MetadataError> {
        if encoding != QuantityEncoding::Contracts {
            return Ok(());
        }
        let kind = self
            .instrument
            .contract_kind
            .ok_or(MetadataError::Normalization("contract_kind"))?;
        self.instrument
            .contract_value
            .ok_or(MetadataError::Normalization("contract_value"))?;
        let asset = self
            .instrument
            .contract_value_asset
            .as_deref()
            .ok_or(MetadataError::Normalization("contract_value_asset"))?;
        let expected = match kind {
            ContractKind::Linear => self.base.as_str(),
            ContractKind::Inverse => self.quote.as_str(),
        };
        if asset != expected {
            return Err(MetadataError::Normalization(
                "contract_value_asset requires an explicit conversion rule",
            ));
        }
        Ok(())
    }
    /// Freshness is an explicit compilation/refresh policy, not a universal TTL.
    pub fn check_freshness(&self, as_of_ns: i64, max_age_ns: u64) -> Result<(), MetadataError> {
        let fetched = self.provenance.fetched_at_ns.expect("validated timestamp");
        let age = i128::from(as_of_ns) - i128::from(fetched);
        if age < 0 {
            return Err(MetadataError::FutureTimestamp);
        }
        if age > i128::from(max_age_ns) {
            return Err(MetadataError::Stale);
        }
        Ok(())
    }
}
pub trait MetadataResolver: Send + Sync {
    fn resolve(
        &self,
        lookup: &CatalogLookup,
    ) -> Result<Arc<ValidatedInstrumentMetadata>, MetadataError>;
}
/// Deterministic read-only snapshot implementation, also usable after downstream
/// controlled catalog hydration. Multiple exact matches fail closed, not first-win.
pub struct InMemoryMetadataResolver {
    records: HashMap<CatalogLookup, Vec<CatalogRecord>>,
}
impl InMemoryMetadataResolver {
    pub fn new(
        records: Vec<CatalogRecord>,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Self, MetadataError> {
        if max_records == 0 || max_bytes == 0 || records.len() > max_records {
            return Err(MetadataError::Limit);
        }
        // A bounded writer prevents an oversized fixture/snapshot from allocating
        // an unbounded encoded copy just to measure it.
        struct Counter {
            bytes: usize,
            max: usize,
        }
        impl std::io::Write for Counter {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.bytes = self
                    .bytes
                    .checked_add(b.len())
                    .filter(|v| *v <= self.max)
                    .ok_or_else(|| std::io::Error::other("metadata limit"))?;
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        serde_json::to_writer(
            Counter {
                bytes: 0,
                max: max_bytes,
            },
            &records,
        )
        .map_err(|_| MetadataError::Limit)?;
        let mut indexed: HashMap<_, Vec<_>> = HashMap::new();
        for record in records {
            indexed
                .entry(record.lookup.clone())
                .or_default()
                .push(record);
        }
        Ok(Self { records: indexed })
    }
}
impl MetadataResolver for InMemoryMetadataResolver {
    fn resolve(
        &self,
        lookup: &CatalogLookup,
    ) -> Result<Arc<ValidatedInstrumentMetadata>, MetadataError> {
        lookup.classification()?;
        let rows = self.records.get(lookup).ok_or(MetadataError::NotFound)?;
        if rows.len() != 1 {
            return Err(MetadataError::Ambiguous);
        }
        ValidatedInstrumentMetadata::validate(&rows[0]).map(Arc::new)
    }
}
