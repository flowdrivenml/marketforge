use marketforge_engine::live::{
    config::ExchangeConfigStore,
    infrastructure::{
        normalization::{NormalizationSource, NormalizedMessage},
        rest::{ExchangeRestResponse, RestRequest},
    },
    stream::request::SourceChannel,
};
use serde_json::Value;
use std::time::Duration;

use super::fixtures::{
    PROFILES, captured_symbol, data_root, factory, native_rest_body, native_ws_messages, request,
    rest_fixture, ws_fixture,
};

// ============================================================
// REST DEPTH
// ============================================================
pub fn captured_depth_size(body: &Value) -> Option<u64> {
    fn level_count(value: &Value) -> Option<u64> {
        let bids = value.get("bids").or_else(|| value.get("b"))?.as_array()?;

        let asks = value.get("asks").or_else(|| value.get("a"))?.as_array()?;

        u64::try_from(bids.len().max(asks.len())).ok()
    }

    if let Some(count) = level_count(body) {
        return Some(count);
    }

    for key in ["data", "result"] {
        if let Some(child) = body.get(key) {
            if let Some(count) = level_count(child) {
                return Some(count);
            }

            if let Some(array) = child.as_array() {
                for item in array {
                    if let Some(count) = level_count(item) {
                        return Some(count);
                    }
                }
            }
        }
    }

    None
}

#[test]
fn all_exchange_rest_depth_fixtures() {
    let factory = factory();

    let mut failures = Vec::new();

    for profile in PROFILES {
        let ws_capture = ws_fixture(SourceChannel::Depth, profile);

        let messages = native_ws_messages(&ws_capture);

        let Some(first_message) = messages.first() else {
            failures.push(format!("{profile}: empty WebSocket fixture"));
            continue;
        };

        let Some(symbol) = captured_symbol(first_message) else {
            failures.push(format!("{profile}: cannot resolve native symbol"));
            continue;
        };

        let stream_id = format!("{profile}-rest-depth");

        let request = request(profile, symbol, SourceChannel::Depth);

        let normalizer = match factory.create(&stream_id, &request) {
            Ok(normalizer) => normalizer,
            Err(error) => {
                failures.push(format!("{profile}: initialization: {error}"));
                continue;
            }
        };

        if !normalizer.supports_rest() {
            failures.push(format!("{profile}: REST depth unavailable"));
            continue;
        }

        let fixture = rest_fixture(profile, "depth_snapshot");

        let body = native_rest_body(&fixture);

        let bytes = serde_json::to_vec(body).expect("fixture JSON serialization");

        // Positive, trusted test timestamp.
        let receive_timestamp_ns = 1_720_000_000_000_000_000_i64;

        // This must ultimately be read from the captured
        // REST request's authorized depth/limit parameter.
        // Fixture-decoding test: use the captured level count.
        //
        // This verifies decoding compatibility, not whether
        // the original REST request authorized this depth.
        let Some(requested_depth) = captured_depth_size(body) else {
            failures.push(format!("{profile}: cannot determine captured REST depth"));
            continue;
        };

        if requested_depth == 0 {
            failures.push(format!("{profile}: captured REST depth is empty"));
            continue;
        }

        match normalizer.normalize_rest_depth(&bytes, receive_timestamp_ns, requested_depth) {
            Ok(NormalizedMessage::Depth(depth)) => {
                if depth.source() != NormalizationSource::Rest {
                    failures.push(format!("{profile}: incorrect source"));
                }

                if depth.stream_id() != stream_id {
                    failures.push(format!("{profile}: incorrect stream identity"));
                }

                if depth.level_count() == 0 {
                    failures.push(format!("{profile}: empty depth snapshot"));
                }
            }

            Ok(_) => {
                failures.push(format!("{profile}: returned trades instead of depth"));
            }

            Err(error) => {
                failures.push(format!("{profile}: REST normalization: {error}"));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "REST depth failures:\n{}",
        failures.join("\n")
    );
}

// ============================================================
// REST TRADES
// ============================================================

#[test]
fn all_exchange_rest_trade_fixtures() {
    let factory = factory();

    let exchanges = ExchangeConfigStore::load_dir(data_root().join(".config/live/exchanges"))
        .expect("exchange configs must load");

    let mut failures = Vec::new();

    for profile in PROFILES {
        let Some(exchange) = exchanges.get(profile) else {
            failures.push(format!("{profile}: exchange configuration missing"));
            continue;
        };
        let endpoint_name = if exchange
            .endpoint("recent_trades")
            .is_some_and(|endpoint| endpoint.trade_normalization.is_some())
        {
            "recent_trades"
        } else if exchange
            .endpoint("historical_trades")
            .is_some_and(|endpoint| endpoint.trade_normalization.is_some())
        {
            "historical_trades"
        } else {
            failures.push(format!(
                "{profile}: no REST trade endpoint supports normalization"
            ));
            continue;
        };

        let Some(endpoint) = exchange.endpoint(endpoint_name) else {
            unreachable!();
        };

        let Some(mapping) = endpoint.trade_normalization.as_ref() else {
            failures.push(format!("{profile}: endpoint has no trade normalization"));
            continue;
        };

        let ws_capture = ws_fixture(SourceChannel::Trades, profile);

        let messages = native_ws_messages(&ws_capture);

        let Some(first_message) = messages.first() else {
            failures.push(format!("{profile}: empty WebSocket fixture"));
            continue;
        };

        let Some(symbol) = captured_symbol(first_message) else {
            failures.push(format!("{profile}: cannot resolve native symbol"));
            continue;
        };

        let stream_id = format!("{profile}-rest-trades");

        let stream_request = request(profile, symbol.clone(), SourceChannel::Trades);

        let normalizer = match factory.create(&stream_id, &stream_request) {
            Ok(normalizer) => normalizer,
            Err(error) => {
                failures.push(format!("{profile}: initialization: {error}"));
                continue;
            }
        };

        if !normalizer.supports_rest() {
            failures.push(format!("{profile}: REST trade normalization unavailable"));
            continue;
        }

        let fixture = rest_fixture(profile, endpoint_name);

        let body = native_rest_body(&fixture).clone();

        let body_bytes = serde_json::to_vec(&body).expect("JSON serialization").len();

        let response = ExchangeRestResponse::new(
            profile,
            endpoint_name,
            200,
            body,
            Default::default(),
            Duration::from_millis(5),
            body_bytes,
        );

        let rest_request = RestRequest::new(profile, endpoint_name)
            .with_parameter(&mapping.request_symbol_parameter, symbol);

        match normalizer.normalize_rest_trades(&response, &rest_request) {
            Ok(NormalizedMessage::Trades(trades)) => {
                if trades.source() != NormalizationSource::Rest {
                    failures.push(format!("{profile}: incorrect source"));
                }

                if trades.stream_id() != stream_id {
                    failures.push(format!("{profile}: incorrect stream identity"));
                }

                if trades.record_count() == 0 {
                    failures.push(format!("{profile}: zero recovered trades"));
                }
            }

            Ok(_) => {
                failures.push(format!("{profile}: returned depth instead of trades"));
            }

            Err(error) => {
                failures.push(format!("{profile}: REST normalization: {error}"));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "REST trade failures:\n{}",
        failures.join("\n")
    );
}
