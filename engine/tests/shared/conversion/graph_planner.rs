use marketforge_engine::shared::{
    conversion::{
        ConversionDirection, ConversionGraph,
        planner::{ResolvedConversionInstrument, plan_conversion},
    },
    domain::instrument::{Currency, InstrumentIdentity, InstrumentKind},
};

fn currency(value: &str) -> Currency {
    Currency::new(value).unwrap()
}
fn instrument(rank: u32, stream: &str, base: &str, quote: &str) -> ResolvedConversionInstrument {
    ResolvedConversionInstrument {
        dataset_id: u64::from(rank),
        stream_rank: rank,
        stream_id: stream.into(),
        identity: InstrumentIdentity {
            exchange: "test".into(),
            symbol: stream.into(),
            instrument_kind: InstrumentKind::Spot,
            contract_kind: None,
        },
        base_currency: currency(base),
        quote_currency: currency(quote),
    }
}

#[test]
fn graph_preserves_direct_inverse_multihop_identity_and_limits() {
    let references = [
        instrument(1, "btc-usdt", "BTC", "USDT"),
        instrument(2, "btc-usdc", "BTC", "USDC"),
    ];
    let graph = ConversionGraph::new(&references).unwrap();
    let paths = graph
        .find_paths(&currency("USDC"), &currency("USDT"), 2)
        .unwrap();
    assert_eq!(paths.len(), 1);
    assert_eq!(
        paths[0].currencies,
        [currency("USDC"), currency("BTC"), currency("USDT")]
    );
    assert_eq!(paths[0].edges[0].direction, ConversionDirection::Inverse);
    assert_eq!(paths[0].edges[1].direction, ConversionDirection::Direct);
    assert_eq!(paths[0].edges[0].reference_dataset_id, 2);
    assert!(
        graph
            .find_paths(&currency("USDC"), &currency("USDT"), 1)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        graph
            .find_paths(&currency("USDT"), &currency("USDT"), 1)
            .unwrap()[0]
            .hops(),
        0
    );
    assert!(
        graph
            .find_paths(&currency("BTC"), &currency("USDT"), 0)
            .is_err()
    );
}

#[test]
fn graph_priority_is_stable_under_reference_permutation() {
    let references = [
        instrument(2, "z", "BTC", "USDT"),
        instrument(1, "a", "BTC", "USDT"),
        instrument(3, "b", "BTC", "USDC"),
    ];
    let mut reversed = references.clone();
    reversed.reverse();
    let first = ConversionGraph::new(&references)
        .unwrap()
        .find_paths(&currency("BTC"), &currency("USDT"), 3)
        .unwrap();
    let second = ConversionGraph::new(&reversed)
        .unwrap()
        .find_paths(&currency("BTC"), &currency("USDT"), 3)
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(
        first
            .iter()
            .map(|p| p.edges[0].reference_stream_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "z"]
    );
}

#[test]
fn graph_rejects_duplicate_streams_and_identical_currencies() {
    let reference = instrument(1, "btc", "BTC", "USDT");
    assert!(ConversionGraph::new(&[reference.clone(), reference]).is_err());
    assert!(ConversionGraph::new(&[instrument(1, "bad", "BTC", "BTC")]).is_err());
}

#[test]
fn planner_preserves_selection_order_accounting_and_duplicate_rank_rejection() {
    let sources = [
        instrument(0, "identity", "BTC", "USDT"),
        instrument(1, "direct", "ETH", "BTC"),
        instrument(2, "inverse", "BTC", "USDC"),
        instrument(3, "missing", "ETH", "EUR"),
        instrument(4, "skipped", "ETH", "USDT"),
    ];
    let references = [
        instrument(10, "btc-usdt", "BTC", "USDT"),
        instrument(11, "btc-usdc", "BTC", "USDC"),
    ];
    let plan = plan_conversion(
        currency("USDT"),
        &sources,
        &references,
        |s| s.stream_rank != 4,
        2,
    )
    .unwrap();
    assert_eq!(
        (
            plan.identity_streams,
            plan.convertible_streams,
            plan.unresolved_streams,
            plan.skipped_streams
        ),
        (1, 2, 1, 1)
    );
    assert_eq!(
        plan.streams
            .iter()
            .map(|s| s.source.stream_rank)
            .collect::<Vec<_>>(),
        [0, 1, 2, 3, 4]
    );
    assert!(plan.streams[0].identity_conversion);
    assert_eq!(plan.streams[2].paths[0].hops(), 2);
    assert!(plan.streams[4].paths.is_empty());
    assert!(
        plan_conversion(
            currency("USDT"),
            &[sources[0].clone(), sources[0].clone()],
            &references,
            |_| true,
            2
        )
        .is_err()
    );
}

#[test]
fn limited_search_preserves_existing_path_order_and_planner_results() {
    use marketforge_engine::shared::conversion::{
        graph::PathSearchLimits, planner::plan_conversion_limited,
    };
    let references = [
        instrument(1, "btc-usdt", "BTC", "USDT"),
        instrument(2, "btc-usdc", "BTC", "USDC"),
        instrument(3, "usdc-usdt", "USDC", "USDT"),
    ];
    let graph = ConversionGraph::new(&references).unwrap();
    let limits = PathSearchLimits {
        max_paths: 32,
        max_edges_examined: 1000,
    };
    assert_eq!(
        graph
            .find_paths(&currency("BTC"), &currency("USDT"), 3)
            .unwrap(),
        graph
            .find_paths_limited(&currency("BTC"), &currency("USDT"), 3, limits)
            .unwrap()
    );
    let sources = [instrument(0, "eth-btc", "ETH", "BTC")];
    let old = plan_conversion(currency("USDT"), &sources, &references, |_| true, 3).unwrap();
    let new = plan_conversion_limited(currency("USDT"), &sources, &references, |_| true, 3, limits)
        .unwrap();
    assert_eq!(old.streams[0].paths, new.streams[0].paths);
    assert_eq!(old.convertible_streams, new.convertible_streams);
}
#[test]
fn limited_search_rejects_path_explosion_and_dead_end_search_work() {
    use marketforge_engine::shared::conversion::graph::PathSearchLimits;
    let references = (1..=8)
        .map(|i| instrument(i, &format!("rate-{i}"), "BTC", "USDT"))
        .collect::<Vec<_>>();
    let graph = ConversionGraph::new(&references).unwrap();
    assert!(
        graph
            .find_paths_limited(
                &currency("BTC"),
                &currency("USDT"),
                3,
                PathSearchLimits {
                    max_paths: 2,
                    max_edges_examined: 1000
                }
            )
            .unwrap_err()
            .to_string()
            .contains("path limit")
    );
    assert!(
        graph
            .find_paths_limited(
                &currency("BTC"),
                &currency("EUR"),
                3,
                PathSearchLimits {
                    max_paths: 32,
                    max_edges_examined: 2
                }
            )
            .unwrap_err()
            .to_string()
            .contains("search budget")
    );
}
