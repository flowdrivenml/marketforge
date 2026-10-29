use std::{path::Path, sync::Arc};
use marketforge_engine::live::{
    config::{ExchangeConfigStore, FormatConfigStore},
    infrastructure::normalization::{NormalizerFactory, NormalizerLimits},
    stream::{request::{CreateSourceStreamRequest, SourceChannel}, source::SourceStreamFactory},
};

pub fn requests() -> Vec<CreateSourceStreamRequest> {
    let data: serde_json::Value = serde_json::from_str(include_str!("../../support/data/instruments/btc_sources.json")).unwrap();
    data["instruments"].as_array().unwrap().iter().map(|profile| {
        let mut value = profile.clone();
        for key in ["exchange_id", "tick_size", "qty_step", "min_qty", "max_qty", "min_notional"] {
            value.as_object_mut().unwrap().remove(key);
        }
        value["channel"] = "trades".into();
        serde_json::from_value(value).unwrap()
    }).collect()
}

#[test]
fn all_btc_profiles_construct_both_source_channels() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live");
    let exchanges = Arc::new(ExchangeConfigStore::load_dir(root.join("exchanges")).unwrap());
    let formats = Arc::new(FormatConfigStore::load_dir(root.join("formats")).unwrap());
    let factory = SourceStreamFactory::new(Arc::new(NormalizerFactory::new(exchanges, formats, NormalizerLimits::default()).unwrap()));
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
            println!("[{:?}/{:?}/{channel:?}] Source initialized instrument={}", request.exchange, request.market_category, request.instrument_id);
        }
    }
}

pub fn definition(source: CreateSourceStreamRequest, id: &str) -> marketforge_engine::live::stream::persistence::PersistedStreamDefinition {
    use marketforge_engine::live::stream::persistence::*;
    PersistedStreamDefinition { stream_id: id.into(), recording_stream_key: Some(1), definition: StreamDefinition::Source(source),
        desired: DesiredStreamState { enabled: true, activation_policy: ActivationPolicy::AlwaysOn }, revision: 1 }
}

#[test]
fn all_btc_profiles_prepare_fresh_integrity() {
    use marketforge_engine::live::stream::source::{SourceInitializationLimits, SourceRuntimeBuilder, SourceReadiness};
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live");
    let builder = SourceRuntimeBuilder::new(
        Arc::new(ExchangeConfigStore::load_dir(root.join("exchanges")).unwrap()),
        Arc::new(FormatConfigStore::load_dir(root.join("formats")).unwrap()), SourceInitializationLimits::default()).unwrap();
    for request in requests() {
        for channel in [SourceChannel::Trades, SourceChannel::Depth] {
            let mut request = request.clone(); request.channel = channel;
            let id = format!("prepared-{}-{channel:?}", request.instrument_id);
            let prepared = builder.prepare(definition(request.clone(), &id)).unwrap();
            assert_eq!(prepared.runner().stream_id(), id);
            assert_ne!(prepared.runner().readiness(), SourceReadiness::Ready);
            assert_eq!(prepared.stream_request().stream_id(), Some(id.as_str()));
            println!("[{:?}/{:?}/{channel:?}] fresh integrity prepared", request.exchange, request.market_category);
        }
    }
}
