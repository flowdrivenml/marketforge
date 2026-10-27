use marketforge_engine::live::{
    infrastructure::normalization::{NormalizationSource, NormalizedMessage},
    stream::request::SourceChannel,
};

use super::fixtures::{
    PROFILES, captured_symbol, factory, native_ws_messages, request, ws_fixture,
};
// ============================================================
// WEBSOCKET TRADES
// ============================================================

#[test]
fn all_exchange_websocket_trade_fixtures() {
    let factory = factory();

    let mut failures = Vec::new();

    for profile in PROFILES {
        let fixture = ws_fixture(SourceChannel::Trades, profile);

        let messages = native_ws_messages(&fixture);

        assert!(
            !messages.is_empty(),
            "{profile}: WebSocket trade capture is empty"
        );

        let Some(symbol) = captured_symbol(&messages[0]) else {
            failures.push(format!("{profile}: cannot resolve native trade symbol"));
            continue;
        };

        let stream_id = format!("{profile}-trades");

        let request = request(profile, symbol, SourceChannel::Trades);

        let normalizer = match factory.create(&stream_id, &request) {
            Ok(normalizer) => normalizer,
            Err(error) => {
                failures.push(format!("{profile}: initialization: {error}"));
                continue;
            }
        };

        let mut total_trades = 0usize;

        for (index, message) in messages.iter().enumerate() {
            match normalizer.normalize_ws(message) {
                Ok(NormalizedMessage::Trades(trades)) => {
                    assert_eq!(
                        trades.source(),
                        NormalizationSource::WebSocket,
                        "{profile}: message {index}: wrong source"
                    );

                    assert_eq!(
                        trades.stream_id(),
                        stream_id,
                        "{profile}: message {index}: wrong stream"
                    );

                    total_trades += trades.record_count();
                }

                Ok(_) => {
                    failures.push(format!(
                        "{profile}: message {index}: returned depth instead of trades"
                    ));
                }

                Err(error) => {
                    failures.push(format!("{profile}: message {index}: {error}"));
                }
            }
        }

        if total_trades == 0 {
            failures.push(format!("{profile}: no trade records normalized"));
        }
    }

    assert!(
        failures.is_empty(),
        "WebSocket trade failures:\n{}",
        failures.join("\n")
    );
}

// ============================================================
// WEBSOCKET DEPTH
// ============================================================

#[test]
fn all_exchange_websocket_depth_fixtures() {
    let factory = factory();

    let mut failures = Vec::new();

    for profile in PROFILES {
        let fixture = ws_fixture(SourceChannel::Depth, profile);

        let messages = native_ws_messages(&fixture);

        assert!(
            !messages.is_empty(),
            "{profile}: WebSocket depth capture is empty"
        );

        let Some(symbol) = captured_symbol(&messages[0]) else {
            failures.push(format!("{profile}: cannot resolve native depth symbol"));
            continue;
        };

        let stream_id = format!("{profile}-depth");

        let request = request(profile, symbol, SourceChannel::Depth);

        let normalizer = match factory.create(&stream_id, &request) {
            Ok(normalizer) => normalizer,
            Err(error) => {
                failures.push(format!("{profile}: initialization: {error}"));
                continue;
            }
        };

        let mut total_levels = 0usize;

        for (index, message) in messages.iter().enumerate() {
            match normalizer.normalize_ws(message) {
                Ok(NormalizedMessage::Depth(depth)) => {
                    assert_eq!(
                        depth.source(),
                        NormalizationSource::WebSocket,
                        "{profile}: message {index}: wrong source"
                    );

                    assert_eq!(
                        depth.stream_id(),
                        stream_id,
                        "{profile}: message {index}: wrong stream"
                    );

                    total_levels += depth.level_count();
                }

                Ok(_) => {
                    failures.push(format!(
                        "{profile}: message {index}: returned trades instead of depth"
                    ));
                }

                Err(error) => {
                    failures.push(format!("{profile}: message {index}: {error}"));
                }
            }
        }

        if total_levels == 0 {
            failures.push(format!("{profile}: no depth levels normalized"));
        }
    }

    assert!(
        failures.is_empty(),
        "WebSocket depth failures:\n{}",
        failures.join("\n")
    );
}
