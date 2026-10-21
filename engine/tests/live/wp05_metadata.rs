//! Deterministic catalog contract fixtures. No database or exchange access.
use marketforge_engine::{
    job::{ContractKind, InstrumentKind, QuantityEncoding},
    live::model::{
        codec::ExactDecimal,
        identity::{ChannelKind, ChannelSpec, MarketKind},
        metadata::*,
    },
};
use rust_decimal::Decimal;
use std::sync::Arc;
fn d(text: &str) -> ExactDecimal {
    ExactDecimal(Decimal::from_str_exact(text).unwrap())
}
fn row(kind: &str, market: &str) -> CatalogRecord {
    CatalogRecord {
        lookup: CatalogLookup {
            exchange: "bybit".into(),
            symbol: "BTCUSDT".into(),
            instrument_type: kind.into(),
            market_category: market.into(),
        },
        base_asset: Some("BTC".into()),
        quote_asset: Some("USDT".into()),
        settlement_asset: None,
        specifications: Some(CatalogSpecifications {
            quantity_type: QuantityEncoding::Base,
            contract_value: None,
            contract_value_asset: None,
            tick_size: Some(d("0.0000000000000000000000000001")),
            qty_step: Some(d("0.0001")),
            min_qty: Some(d("0.0010")),
            max_qty: Some(d("10000000000.000000000000000001")),
            min_notional: Some(d("5.0000")),
        }),
        provenance: CatalogProvenance {
            exchange_id: 2,
            instrument_id: 7,
            fetched_at_ns: Some(123456789),
            instrument_updated_at_ns: Some(123),
            catalog_version: Some("fixture-snapshot-17".into()),
            raw_reference: Some(CatalogRawReference {
                id: 88,
                fetched_at_ns: 123456788,
            }),
        },
    }
}
fn resolver(rows: Vec<CatalogRecord>) -> InMemoryMetadataResolver {
    InMemoryMetadataResolver::new(rows, 100, 100000).unwrap()
}
fn channel() -> ChannelSpec {
    ChannelSpec {
        kind: ChannelKind::Trades,
        depth: None,
        interval_ms: None,
    }
}
#[test]
fn wp05_metadata_classifies_all_approved_catalog_tuples_and_fences_identity() {
    for (kind, market, expected_kind, expected_market, contract) in [
        ("spot", "spot", InstrumentKind::Spot, MarketKind::Spot, None),
        (
            "perpetual",
            "linear",
            InstrumentKind::Perpetual,
            MarketKind::Linear,
            Some(ContractKind::Linear),
        ),
        (
            "perpetual",
            "inverse",
            InstrumentKind::Perpetual,
            MarketKind::Inverse,
            Some(ContractKind::Inverse),
        ),
        (
            "future",
            "linear",
            InstrumentKind::Future,
            MarketKind::Linear,
            Some(ContractKind::Linear),
        ),
        (
            "future",
            "inverse",
            InstrumentKind::Future,
            MarketKind::Inverse,
            Some(ContractKind::Inverse),
        ),
    ] {
        let record = row(kind, market);
        let lookup = record.lookup.clone();
        let metadata = resolver(vec![record]).resolve(&lookup).unwrap();
        assert_eq!(metadata.identity().instrument_kind, expected_kind);
        assert_eq!(metadata.identity().contract_kind, contract);
        assert_eq!(metadata.market(), expected_market);
        assert_eq!(metadata.instrument().contract_kind, contract);
        let source = metadata.source_key(channel()).unwrap();
        assert!(source.validate().issues.is_empty());
        assert_eq!(source.canonicalized().unwrap(), source);
        metadata.validate_identity(&source.instrument).unwrap();
        let mut wrong = source.instrument.clone();
        wrong.symbol.push('X');
        assert_eq!(
            metadata.validate_identity(&wrong),
            Err(MetadataError::Conflict("instrument_identity"))
        );
        if contract.is_some() {
            let mut missing = source.instrument.clone();
            missing.contract_kind = None;
            assert_eq!(
                metadata.validate_identity(&missing),
                Err(MetadataError::UnresolvedClassification)
            );
            let mut conflicting = source.clone();
            conflicting.market = if expected_market == MarketKind::Linear {
                MarketKind::Inverse
            } else {
                MarketKind::Linear
            };
            assert!(conflicting.validate().has_definite_failures());
            assert!(conflicting.canonicalized().is_err());
        }
    }
}
#[test]
fn wp05_metadata_complete_lookup_prevents_symbol_only_or_family_fallback() {
    let mut rows = vec![];
    for (i, (kind, market)) in [
        ("spot", "spot"),
        ("perpetual", "linear"),
        ("perpetual", "inverse"),
        ("future", "linear"),
        ("future", "inverse"),
    ]
    .into_iter()
    .enumerate()
    {
        let mut record = row(kind, market);
        record.provenance.instrument_id = i as i64 + 1;
        rows.push(record);
    }
    let resolver = resolver(rows.clone());
    for (i, record) in rows.iter().enumerate() {
        assert_eq!(
            resolver
                .resolve(&record.lookup)
                .unwrap()
                .provenance()
                .instrument_id,
            i as i64 + 1
        );
    }
    let mut unknown = rows[0].lookup.clone();
    unknown.symbol = "btcusdt".into();
    assert_eq!(
        resolver.resolve(&unknown).unwrap_err(),
        MetadataError::NotFound
    );
    unknown = rows[0].lookup.clone();
    unknown.exchange = "binance".into();
    assert_eq!(
        resolver.resolve(&unknown).unwrap_err(),
        MetadataError::NotFound
    );
    let duplicate = rows[0].clone();
    assert_eq!(
        self::resolver(vec![duplicate.clone(), duplicate.clone()])
            .resolve(&duplicate.lookup)
            .unwrap_err(),
        MetadataError::Ambiguous
    );
}
#[test]
fn wp05_metadata_rejects_missing_and_conflicting_classifications_without_inference() {
    for (kind, market, error) in [
        ("perpetual", "", MetadataError::Missing("market_category")),
        (
            "future",
            "spot",
            MetadataError::Conflict("instrument_type/market_category"),
        ),
        (
            "perpetual",
            "spot",
            MetadataError::Conflict("instrument_type/market_category"),
        ),
        (
            "spot",
            "linear",
            MetadataError::Conflict("instrument_type/market_category"),
        ),
        (
            "spot",
            "inverse",
            MetadataError::Conflict("instrument_type/market_category"),
        ),
        (
            "option",
            "linear",
            MetadataError::Unsupported("instrument_type"),
        ),
        (
            "unrecognized",
            "inverse",
            MetadataError::Unsupported("instrument_type"),
        ),
        (
            "perpetual",
            "mystery",
            MetadataError::Unsupported("market_category"),
        ),
    ] {
        let record = row(kind, market);
        assert_eq!(
            resolver(vec![record.clone()])
                .resolve(&record.lookup)
                .unwrap_err(),
            error
        );
    }
    let mut record = row("spot", "spot");
    record.lookup.symbol = "BTCUSD_INVERSE".into();
    let metadata = resolver(vec![record.clone()])
        .resolve(&record.lookup)
        .unwrap();
    assert_eq!(metadata.identity().contract_kind, None);
    assert_eq!(metadata.market(), MarketKind::Spot);
}
#[test]
fn wp05_metadata_missing_spec_tick_economics_and_provenance_are_actionable() {
    let original = row("future", "linear");
    let mut cases = vec![];
    let mut r = original.clone();
    r.specifications = None;
    cases.push((r, MetadataError::Missing("specifications")));
    let mut r = original.clone();
    r.specifications.as_mut().unwrap().tick_size = None;
    cases.push((r, MetadataError::Missing("tick_size")));
    for tick in ["0", "-0.01"] {
        let mut r = original.clone();
        r.specifications.as_mut().unwrap().tick_size = Some(d(tick));
        cases.push((r, MetadataError::Invalid("tick_size")));
    }
    let mut r = original.clone();
    r.base_asset = None;
    cases.push((r, MetadataError::Missing("base_asset")));
    let mut r = original.clone();
    r.quote_asset = r.base_asset.clone();
    cases.push((r, MetadataError::Conflict("base_asset/quote_asset")));
    let mut r = original.clone();
    r.settlement_asset = Some("bad asset".into());
    cases.push((r, MetadataError::Invalid("settlement_asset")));
    let mut r = original.clone();
    r.provenance.fetched_at_ns = None;
    cases.push((r, MetadataError::Missing("fetched_at")));
    let mut r = original.clone();
    r.provenance.instrument_id = 0;
    cases.push((r, MetadataError::Invalid("catalog_identity")));
    let mut r = original.clone();
    r.specifications.as_mut().unwrap().contract_value = Some(d("-1"));
    cases.push((r, MetadataError::Invalid("contract_value")));
    let mut r = original.clone();
    r.specifications.as_mut().unwrap().qty_step = Some(d("0"));
    cases.push((r, MetadataError::Invalid("qty_step")));
    let mut r = original.clone();
    r.specifications.as_mut().unwrap().max_qty = Some(d("0.0001"));
    cases.push((r, MetadataError::Conflict("min_qty/max_qty")));
    for (record, error) in cases {
        assert_eq!(
            resolver(vec![record.clone()])
                .resolve(&record.lookup)
                .unwrap_err(),
            error
        );
    }
}
#[test]
fn wp05_metadata_exact_decimals_limits_and_serialization_preserve_all_fields() {
    let mut record = row("perpetual", "linear");
    record.settlement_asset = Some("USDT".into());
    record.specifications.as_mut().unwrap().contract_value =
        Some(d("0.0000000000000000000000000001"));
    record.specifications.as_mut().unwrap().contract_value_asset = Some("BTC".into());
    let bytes = serde_json::to_vec(&record).unwrap();
    let restored: CatalogRecord = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(restored, record);
    let metadata = resolver(vec![restored]).resolve(&record.lookup).unwrap();
    assert_eq!(
        metadata.instrument().tick_size,
        d("0.0000000000000000000000000001").0
    );
    assert_eq!(
        metadata.instrument().contract_value,
        record
            .specifications
            .as_ref()
            .unwrap()
            .contract_value
            .clone()
            .map(|v| v.0)
    );
    assert_eq!(metadata.qty_step(), Some(d("0.0001").0));
    assert_eq!(metadata.min_qty(), Some(d("0.0010").0));
    assert_eq!(
        metadata.max_qty(),
        Some(d("10000000000.000000000000000001").0)
    );
    assert_eq!(metadata.min_notional(), Some(d("5.0000").0));
    assert_eq!(metadata.settlement().unwrap().as_str(), "USDT");
    assert_eq!(metadata.provenance(), &record.provenance);
    let mut value = serde_json::to_value(&record).unwrap();
    value["specifications"]["tick_size"] = serde_json::json!(0.1);
    assert!(serde_json::from_value::<CatalogRecord>(value).is_err());
    for invalid in ["NaN", "1e-3", " 0.1", "0.00000000000000000000000000001"] {
        let mut value = serde_json::to_value(&record).unwrap();
        value["specifications"]["tick_size"] = invalid.into();
        assert!(
            serde_json::from_value::<CatalogRecord>(value).is_err(),
            "{invalid}"
        );
    }
    assert!(matches!(
        InMemoryMetadataResolver::new(vec![record.clone()], 1, 1),
        Err(MetadataError::Limit)
    ));
    assert!(matches!(
        InMemoryMetadataResolver::new(vec![record.clone(), record], 1, 100000),
        Err(MetadataError::Limit)
    ));
}
#[test]
fn wp05_metadata_normalization_requires_multipliers_only_for_contract_encodings() {
    for market in ["linear", "inverse"] {
        for quantity_type in [
            QuantityEncoding::Base,
            QuantityEncoding::Quote,
            QuantityEncoding::Contracts,
        ] {
            let mut record = row("perpetual", market);
            record.specifications.as_mut().unwrap().quantity_type = quantity_type;
            let metadata = ValidatedInstrumentMetadata::validate(&record).unwrap();
            assert_eq!(metadata.quantity_type(), quantity_type);
            assert_eq!(metadata.instrument().contract_value, None);
            metadata
                .check_normalization(QuantityEncoding::Base)
                .unwrap();
            metadata
                .check_normalization(QuantityEncoding::Quote)
                .unwrap();
            assert_eq!(
                metadata.check_normalization(QuantityEncoding::Contracts),
                Err(MetadataError::Normalization("contract_value"))
            );
            record.specifications.as_mut().unwrap().contract_value = Some(d("0.01"));
            let metadata = ValidatedInstrumentMetadata::validate(&record).unwrap();
            assert_eq!(
                metadata.check_normalization(QuantityEncoding::Contracts),
                Err(MetadataError::Normalization("contract_value_asset"))
            );
            record.specifications.as_mut().unwrap().contract_value_asset =
                Some(if market == "linear" { "BTC" } else { "USDT" }.into());
            let metadata = ValidatedInstrumentMetadata::validate(&record).unwrap();
            metadata
                .check_normalization(QuantityEncoding::Contracts)
                .unwrap();
            let normalized = marketforge_engine::normalize::normalize_quantity(
                d("3").0,
                d("2").0,
                QuantityEncoding::Contracts,
                metadata.instrument(),
            )
            .unwrap();
            assert_eq!(normalized.contracts, Some(d("3").0));
            record.specifications.as_mut().unwrap().contract_value_asset = Some("ETH".into());
            assert!(matches!(
                ValidatedInstrumentMetadata::validate(&record)
                    .unwrap()
                    .check_normalization(QuantityEncoding::Contracts),
                Err(MetadataError::Normalization(_))
            ));
        }
    }
    assert_eq!(
        ValidatedInstrumentMetadata::validate(&row("spot", "spot"))
            .unwrap()
            .check_normalization(QuantityEncoding::Contracts),
        Err(MetadataError::Normalization("contract_kind"))
    );
}
#[test]
fn wp05_metadata_freshness_and_concurrent_model_only_consumers() {
    fn model_consumer<R: MetadataResolver + ?Sized>(
        resolver: &R,
        lookup: &CatalogLookup,
    ) -> Arc<ValidatedInstrumentMetadata> {
        resolver.resolve(lookup).unwrap()
    }
    let record = row("spot", "spot");
    let lookup = record.lookup.clone();
    let resolver: Arc<dyn MetadataResolver> = Arc::new(resolver(vec![record]));
    let metadata = model_consumer(resolver.as_ref(), &lookup);
    let fetched = metadata.provenance().fetched_at_ns.unwrap();
    metadata.check_freshness(fetched, 0).unwrap();
    metadata.check_freshness(fetched + 100, 100).unwrap();
    assert_eq!(
        metadata.check_freshness(fetched + 101, 100),
        Err(MetadataError::Stale)
    );
    assert_eq!(
        metadata.check_freshness(fetched - 1, 100),
        Err(MetadataError::FutureTimestamp)
    );
    assert_eq!(metadata.check_freshness(i64::MAX, u64::MAX), Ok(()));
    let threads = (0..4)
        .map(|_| {
            let resolver = resolver.clone();
            let lookup = lookup.clone();
            std::thread::spawn(move || model_consumer(resolver.as_ref(), &lookup))
        })
        .collect::<Vec<_>>();
    for thread in threads {
        assert_eq!(*thread.join().unwrap(), *metadata);
    }
}

#[test]
fn wp05_metadata_optional_absence_and_zero_limits_remain_explicit() {
    let mut record = row("future", "linear");
    record.provenance.catalog_version = None;
    record.provenance.raw_reference = None;
    let specs = record.specifications.as_mut().unwrap();
    specs.min_qty = Some(d("0"));
    specs.min_notional = Some(d("0"));
    specs.qty_step = None;
    specs.max_qty = None;
    let metadata = ValidatedInstrumentMetadata::validate(&record).unwrap();
    assert_eq!(metadata.min_qty(), Some(Decimal::ZERO));
    assert_eq!(metadata.min_notional(), Some(Decimal::ZERO));
    assert_eq!(metadata.qty_step(), None);
    assert_eq!(metadata.max_qty(), None);
    assert_eq!(metadata.settlement(), None);
    assert_eq!(metadata.provenance().catalog_version, None);
    assert_eq!(metadata.provenance().raw_reference, None);
    for encoding in [QuantityEncoding::Base, QuantityEncoding::Quote] {
        metadata.check_normalization(encoding).unwrap();
        let quantity = marketforge_engine::normalize::normalize_quantity(
            d("3").0,
            d("2").0,
            encoding,
            metadata.instrument(),
        )
        .unwrap();
        let (base, quote) = match encoding {
            QuantityEncoding::Base => ("3", "6"),
            QuantityEncoding::Quote => ("1.5", "3"),
            _ => unreachable!(),
        };
        assert_eq!(quantity.base, Some(d(base).0));
        assert_eq!(quantity.quote, Some(d(quote).0));
        assert_eq!(quantity.contracts, None);
    }
    for (field, value, error) in [
        ("exchange", "", MetadataError::Missing("exchange")),
        ("symbol", "", MetadataError::Missing("symbol")),
        (
            "exchange",
            "unknown",
            MetadataError::Unsupported("exchange"),
        ),
        ("exchange", "BYBIT", MetadataError::Unsupported("exchange")),
        ("symbol", " BTCUSDT", MetadataError::Invalid("symbol")),
    ] {
        let mut changed = record.clone();
        match field {
            "exchange" => changed.lookup.exchange = value.into(),
            _ => changed.lookup.symbol = value.into(),
        };
        assert_eq!(
            ValidatedInstrumentMetadata::validate(&changed).unwrap_err(),
            error
        );
    }
}
