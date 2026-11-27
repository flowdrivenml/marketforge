use super::*;
use marketforge_engine::live::{
    application::{compile_limiters, compile_websocket},
    config::{
        ExchangeConfigStore, ExchangeId, FormatConfigStore, MarketType,
        general::RecoveryDefaultsConfig,
    },
    infrastructure::websocket::{StreamInput, WebSocketManager},
    stream::{
        persistence::{
            ActivationPolicy, DesiredStreamState, PersistedStreamDefinition, StreamDefinition,
        },
        request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
        source::{SourceInitializationLimits, SourceRuntimeBuilder},
    },
};
use serde_json::Value;

#[derive(Clone, Copy)]
struct Variant {
    profile: &'static str,
    exchange: ExchangeId,
    settlement: &'static str,
    metadata_file_env: &'static str,
}

const VARIANTS: [Variant; 2] = [
    Variant {
        profile: "gateio_linear",
        exchange: ExchangeId::Gateio,
        settlement: "USD1",
        metadata_file_env: "MF_GATE_USD1_METADATA",
    },
    Variant {
        profile: "bitget_linear",
        exchange: ExchangeId::Bitget,
        settlement: "USDC",
        metadata_file_env: "MF_BITGET_USDC_METADATA",
    },
];

fn field<'a>(row: &'a Value, name: &str) -> Result<&'a str, String> {
    row[name]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("instrument metadata missing {name}"))
}

fn source_from_metadata(
    variant: Variant,
    metadata: &Value,
) -> Result<CreateSourceStreamRequest, String> {
    let gate = variant.exchange == ExchangeId::Gateio;
    let rows = if gate {
        metadata.as_array()
    } else if metadata["code"] == "00000" {
        metadata["data"].as_array()
    } else {
        return Err("Bitget instrument catalogue did not return success".into());
    }
    .ok_or("instrument catalogue is not a list")?;

    let eligible = |row: &&Value| {
        if gate {
            row["type"] == "direct"
                && row["status"] == "trading"
                && row["in_delisting"] == false
                && row["name"]
                    .as_str()
                    .and_then(|name| name.rsplit_once('_'))
                    .is_some_and(|(_, quote)| quote == variant.settlement)
        } else {
            row["category"] == "USDC-FUTURES"
                && row["type"] == "perpetual"
                && row["status"] == "online"
                && row["quoteCoin"] == variant.settlement
                && row
                    .get("settleCoin")
                    .is_none_or(|asset| asset == variant.settlement)
        }
    };
    let row = rows
        .iter()
        .filter(eligible)
        .min_by_key(|row| {
            let btc = if gate {
                row["name"].as_str().is_some_and(|s| s.starts_with("BTC_"))
            } else {
                row["baseCoin"] == "BTC"
            };
            (
                !btc,
                row[if gate { "name" } else { "symbol" }]
                    .as_str()
                    .unwrap_or(""),
            )
        })
        .ok_or_else(|| {
            format!(
                "no active {} linear perpetual in authoritative catalogue",
                variant.settlement
            )
        })?;

    let (symbol, base, quote, quantity_type, contract_value) = if gate {
        let symbol = field(row, "name")?;
        // Same direct-contract metadata convention as Forge's Gate catalogue provider.
        let (base, quote) = symbol
            .rsplit_once('_')
            .ok_or("invalid Gate contract identity")?;
        (
            symbol,
            base,
            quote,
            QuantityType::Contracts,
            Some(field(row, "quanto_multiplier")?.to_owned()),
        )
    } else {
        // The USDC product uses base asset size in the checked-in native format.
        (
            field(row, "symbol")?,
            field(row, "baseCoin")?,
            field(row, "quoteCoin")?,
            QuantityType::Base,
            None,
        )
    };
    Ok(CreateSourceStreamRequest {
        // Local verification identity. The exchange catalogue does not assign Forge IDs.
        instrument_id: 1,
        exchange: variant.exchange,
        symbol: symbol.into(),
        market_category: MarketType::Linear,
        instrument_type: "perpetual".into(),
        base_asset: Some(base.into()),
        quote_asset: Some(quote.into()),
        contract_value_asset: contract_value.as_ref().map(|_| base.into()),
        contract_value,
        quantity_type,
        channel: SourceChannel::Trades,
        settlement_asset: Some(variant.settlement.into()),
    })
}

fn load_metadata(variant: Variant) -> Result<CreateSourceStreamRequest, String> {
    let file = std::env::var_os(variant.metadata_file_env).ok_or_else(|| {
        format!(
            "{} must point to a fresh public instrument catalogue response",
            variant.metadata_file_env
        )
    })?;
    let data = std::fs::read(file).map_err(|e| format!("cannot read instrument catalogue: {e}"))?;
    let metadata = serde_json::from_slice(&data)
        .map_err(|e| format!("invalid instrument catalogue JSON: {e}"))?;
    source_from_metadata(variant, &metadata)
}

async fn verify_variant(
    variant: Variant,
    source: CreateSourceStreamRequest,
    builder: &SourceRuntimeBuilder,
    configs: &ExchangeConfigStore,
    formats: &FormatConfigStore,
    limiters: &marketforge_engine::live::infrastructure::limiter::factory::ExchangeLimiters,
) -> Result<Verification, String> {
    let config = configs.get(variant.profile).ok_or("missing profile")?;
    let channel = source.channel.as_str();
    let stream_id = format!("public-{}-{channel}", variant.profile);
    let prepared = builder
        .prepare(PersistedStreamDefinition {
            stream_id,
            recording_stream_key: Some(1),
            revision: 1,
            desired: DesiredStreamState {
                enabled: true,
                activation_policy: ActivationPolicy::AlwaysOn,
            },
            definition: StreamDefinition::Source(source.clone()),
        })
        .map_err(|error| error.to_string())?;
    prepared
        .validate_subscription(configs)
        .map_err(|error| error.to_string())?;
    let request = prepared.stream_request();
    let mut runtime = compile_websocket(
        configs,
        limiters,
        &RecoveryDefaultsConfig {
            initial_backoff_ms: 200,
            max_backoff_seconds: 1,
            jitter: false,
        },
        &BTreeMap::new(),
    )?;
    runtime.reconnect_policy = reconnect_policy();
    let profile = runtime
        .profiles
        .iter_mut()
        .find(|profile| profile.exchange.profile() == variant.profile)
        .ok_or("missing compiled profile")?;
    let native_ws_inst_type = if variant.exchange == ExchangeId::Gateio {
        let route = Some("usd1".to_owned());
        if profile.urls.get(&route).map(String::as_str) != Some("wss://fx-ws.gateio.ws/v4/ws/usd1")
            || profile
                .exchange
                .websocket
                .headers
                .get("X-Gate-Size-Decimal")
                .map(String::as_str)
                != Some("1")
        {
            return Err(
                "USD1 endpoint or decimal-size header differs from the expected protocol".into(),
            );
        }
        // Only permit the configured USD1 URL. The manager still has to select the
        // route carried by the prepared Source request.
        profile.urls.retain(|key, _| key == &route);
        println!(
            "{} {channel} {} expects usd1 with X-Gate-Size-Decimal=1",
            variant.profile, source.symbol
        );
        None
    } else {
        if request
            .parameters()
            .get("native_ws_inst_type")
            .map(String::as_str)
            != Some("usdc-futures")
            || profile.urls.get(&None).map(String::as_str)
                != Some("wss://ws.bitget.com/v3/ws/public")
        {
            return Err("Bitget Source did not prepare the expected product and endpoint".into());
        }
        println!(
            "{} {channel} {} resolved usdc-futures",
            variant.profile, source.symbol
        );
        Some("usdc-futures")
    };
    runtime
        .profiles
        .retain(|profile| profile.exchange.profile() == variant.profile);
    let manager = WebSocketManager::from_runtime(runtime).map_err(|error| error.to_string())?;
    let (sender, mut receiver) = mpsc::channel(64);
    let mut evidence = Evidence::default();
    let case = ProfileCase {
        name: variant.profile,
        symbol: &source.symbol,
        route: None,
        native_ws_inst_type,
    };
    let confirmation = time::timeout(ACK_TIMEOUT, async {
        manager
            .attach_request_with_formats(request.clone(), sender, formats)
            .await
            .map_err(|error| error.to_string())?;
        match receiver.recv().await {
            Some(StreamInput::Connected(acquisition)) => Ok(acquisition),
            event => Err(format!(
                "expected internal native confirmation, received {event:?}"
            )),
        }
    })
    .await;
    let mut outcome = match confirmation {
        Ok(Ok(acquisition)) => {
            evidence.connected = true;
            evidence.confirmed = true;
            match time::timeout(CHECK_TIMEOUT, receiver.recv()).await {
                Ok(Some(StreamInput::Data {
                    acquisition: current,
                    message,
                })) if current == acquisition => {
                    match validate_observation(case, channel, config, message) {
                        Ok(_) => {
                            evidence.data = true;
                            Outcome::Pass(
                                "validated native data from prepared Source subscription".into(),
                            )
                        }
                        Err(error) => Outcome::Fail(error),
                    }
                }
                Ok(event) => {
                    Outcome::Fail(format!("unexpected native acquisition event: {event:?}"))
                }
                Err(_) => Outcome::Idle,
            }
        }
        Ok(Err(error)) => Outcome::Fail(error),
        Err(_) => Outcome::Fail("physical connection or internal ACK deadline expired".into()),
    };
    match time::timeout(SHUTDOWN_TIMEOUT, manager.shutdown()).await {
        Ok(Ok(())) => {
            if matches!(outcome, Outcome::Idle) {
                while let Ok(event) = receiver.try_recv() {
                    match event {
                        StreamInput::Data { message, .. } => {
                            match validate_observation(case, channel, config, message) {
                                Ok(_) => {
                                    evidence.data = true;
                                    outcome =
                                        Outcome::Pass("native data queued at shutdown".into());
                                }
                                Err(error) => {
                                    outcome = Outcome::Fail(error);
                                    break;
                                }
                            }
                        }
                        _ => {
                            outcome = Outcome::Fail(format!(
                                "unexpected pending acquisition event: {event:?}"
                            ));
                            break;
                        }
                    }
                }
            }
        }
        result => outcome = Outcome::Fail(format!("manager shutdown failed: {result:?}")),
    }
    Ok(Verification { evidence, outcome })
}

#[tokio::test]
#[ignore = "public exchange variant verification using current authoritative instrument metadata"]
async fn native_linear_variants_live() {
    let names = VARIANTS
        .iter()
        .map(|variant| variant.profile.to_owned())
        .collect();
    let configs = Arc::new(
        ExchangeConfigStore::load_dir(crate::support::data(".config/live/exchanges"))
            .unwrap()
            .selected(&names)
            .unwrap(),
    );
    let formats = Arc::new(
        FormatConfigStore::load_dir(crate::support::data(".config/live/formats")).unwrap(),
    );
    let builder = SourceRuntimeBuilder::new(
        configs.clone(),
        formats.clone(),
        SourceInitializationLimits::default(),
    )
    .unwrap();
    let limiters = compile_limiters(&configs, "primary", &BTreeMap::new()).unwrap();
    let mut totals = Totals {
        selected: 4,
        ..Default::default()
    };
    for variant in VARIANTS {
        let source = load_metadata(variant);
        for channel in [SourceChannel::Trades, SourceChannel::Depth] {
            let label = format!(
                "{} {} {}",
                variant.profile,
                variant.settlement,
                channel.as_str()
            );
            let result = match &source {
                Ok(source) => {
                    let mut source = source.clone();
                    source.channel = channel;
                    totals.executed += 1;
                    verify_variant(variant, source, &builder, &configs, &formats, &limiters)
                        .await
                        .unwrap_or_else(|error| Verification::failed(Evidence::default(), error))
                }
                Err(reason) => Verification {
                    evidence: Evidence::default(),
                    outcome: Outcome::Blocked(reason.clone()),
                },
            };
            totals.record(&label, result);
        }
    }
    totals.report();
    assert!(
        totals.accepted(std::env::var("MF_LIVE_REQUIRE_DATA").as_deref() == Ok("1")),
        "incomplete public native variant verification"
    );
}
