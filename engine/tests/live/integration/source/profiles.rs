use marketforge_engine::live::{
    config::{ExchangeConfigStore, FormatConfigStore},
    infrastructure::normalization::{NormalizerFactory, NormalizerLimits},
    stream::{
        request::{CreateSourceStreamRequest, SourceChannel},
        source::SourceStreamFactory,
    },
};
use std::{path::Path, sync::Arc};

pub fn requests() -> Vec<CreateSourceStreamRequest> {
    let data: serde_json::Value = serde_json::from_str(include_str!(
        "../../support/data/instruments/btc_sources.json"
    ))
    .unwrap();
    data["instruments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|profile| {
            let mut value = profile.clone();
            for key in [
                "exchange_id",
                "tick_size",
                "qty_step",
                "min_qty",
                "max_qty",
                "min_notional",
            ] {
                value.as_object_mut().unwrap().remove(key);
            }
            value["channel"] = "trades".into();
            serde_json::from_value(value).unwrap()
        })
        .collect()
}

#[test]
fn all_btc_profiles_construct_both_source_channels() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live");
    let exchanges = Arc::new(ExchangeConfigStore::load_dir(root.join("exchanges")).unwrap());
    let formats = Arc::new(FormatConfigStore::load_dir(root.join("formats")).unwrap());
    let factory = SourceStreamFactory::new(Arc::new(
        NormalizerFactory::new(exchanges, formats, NormalizerLimits::default()).unwrap(),
    ));
    let profiles = requests();
    assert_eq!(profiles.len(), 14);
    for request in profiles {
        for channel in [SourceChannel::Trades, SourceChannel::Depth] {
            let mut request = request.clone();
            request.channel = channel;
            let id = format!("btc-{}-{channel:?}", request.instrument_id);
            let source = factory.create(&id, request.clone()).unwrap();
            assert_eq!(source.stream_id(), id);
            assert_eq!(source.request(), &request);
            println!(
                "[{:?}/{:?}/{channel:?}] Source initialized instrument={}",
                request.exchange, request.market_category, request.instrument_id
            );
        }
    }
}

pub fn definition(
    source: CreateSourceStreamRequest,
    id: &str,
) -> marketforge_engine::live::stream::persistence::PersistedStreamDefinition {
    use marketforge_engine::live::stream::persistence::*;
    PersistedStreamDefinition {
        stream_id: id.into(),
        recording_stream_key: Some(1),
        definition: StreamDefinition::Source(source),
        desired: DesiredStreamState {
            enabled: true,
            activation_policy: ActivationPolicy::AlwaysOn,
        },
        revision: 1,
    }
}

#[test]
fn all_btc_profiles_prepare_fresh_integrity() {
    use marketforge_engine::live::stream::source::{
        SourceInitializationLimits, SourceReadiness, SourceRuntimeBuilder,
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live");
    let builder = SourceRuntimeBuilder::new(
        Arc::new(ExchangeConfigStore::load_dir(root.join("exchanges")).unwrap()),
        Arc::new(FormatConfigStore::load_dir(root.join("formats")).unwrap()),
        SourceInitializationLimits::default(),
    )
    .unwrap();
    for request in requests() {
        for channel in [SourceChannel::Trades, SourceChannel::Depth] {
            let mut request = request.clone();
            request.channel = channel;
            let id = format!("prepared-{}-{channel:?}", request.instrument_id);
            let prepared = builder.prepare(definition(request.clone(), &id)).unwrap();
            assert_eq!(prepared.runner().stream_id(), id);
            assert_ne!(prepared.runner().readiness(), SourceReadiness::Ready);
            assert_eq!(prepared.stream_request().stream_id(), Some(id.as_str()));
            println!(
                "[{:?}/{:?}/{channel:?}] fresh integrity prepared",
                request.exchange, request.market_category
            );
        }
    }
}

#[test]
fn fourteen_native_trade_profiles_preserve_exact_quantities() {
    use marketforge_engine::live::infrastructure::normalization::NormalizedMessage;
    use marketforge_engine::live::stream::request::QuantityType;
    use rust_decimal::Decimal;
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live");
    let factory = SourceStreamFactory::new(Arc::new(
        NormalizerFactory::new(
            Arc::new(ExchangeConfigStore::load_dir(root.join("exchanges")).unwrap()),
            Arc::new(FormatConfigStore::load_dir(root.join("formats")).unwrap()),
            NormalizerLimits::default(),
        )
        .unwrap(),
    ));
    for request in requests() {
        let profile = format!(
            "{}_{}",
            request.exchange.as_str(),
            request.market_category.as_str()
        );
        let source = factory.create(&profile, request.clone()).unwrap();
        let native = super::pipeline::native(&profile);
        let normalized = source.normalizer().normalize_ws(&native).unwrap();
        let NormalizedMessage::Trades(trades) = normalized else {
            panic!("wrong channel")
        };
        assert_eq!(trades.record_count(), 1);
        let trade = &trades.trades()[0];
        let record = if let Some(records) = native["data"].as_array() {
            &records[0]
        } else if let Some(records) = native["result"].as_array() {
            &records[0]
        } else if native.get("result").is_some() {
            &native["result"]
        } else {
            &native
        };
        let quantity = ["q", "v", "sz", "size", "amount"]
            .iter()
            .find_map(|key| record.get(*key))
            .unwrap();
        let amount = quantity
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| quantity.to_string())
            .parse::<Decimal>()
            .unwrap()
            .abs();
        let actual = match request.quantity_type {
            QuantityType::Base => trade.quantity_base,
            QuantityType::Quote => trade.quantity_quote,
            QuantityType::Contracts => trade.quantity_contracts,
        };
        assert_eq!(actual, Some(amount), "{profile}: native quantity altered");
        assert_eq!(trade.price, Decimal::from(80000));
        println!("[{profile}/normalization] original economic quantity={amount} preserved exactly");
    }
}
