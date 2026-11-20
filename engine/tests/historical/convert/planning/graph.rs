use {
    marketforge_engine::historical::convert::ConversionDirection,
    marketforge_engine::historical::convert::ConversionGraph,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
    marketforge_engine::historical::merge::UnifiedMergeMode,
    marketforge_engine::shared::domain::instrument::Currency,
};

fn graph() -> ConversionGraph {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = crate::conversion_fixture::canonical_job();
    job.input_merge = root.join("trades");
    let manifest = crate::market::create(&job.input_merge, UnifiedMergeMode::Trades, false);
    let catalog = crate::market::catalog(root);
    // The graph cases also probe ETH as a currency through an explicit extra reference.
    let mut ethbtc = job.conversion.references[0].clone();
    ethbtc.symbol = "ETHBTC".into();
    job.conversion.references.push(ethbtc);
    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, root).unwrap();
    ConversionGraph::new(&resolved.references).unwrap()
}

fn currency(value: &str) -> Currency {
    Currency::new(value).unwrap()
}

fn path_currencies(
    graph: &ConversionGraph,
    source: &str,
    target: &str,
    max_hops: usize,
) -> Vec<Vec<String>> {
    graph
        .find_paths(&currency(source), &currency(target), max_hops)
        .unwrap()
        .iter()
        .map(|path| {
            path.currencies
                .iter()
                .map(|currency| currency.as_str().to_owned())
                .collect()
        })
        .collect()
}

// -----------------------------------------------------------------------------
// Direct and inverse paths
// -----------------------------------------------------------------------------

#[test]
fn finds_direct_btc_to_usdt() {
    let graph = graph();

    let paths = graph
        .find_paths(&currency("BTC"), &currency("USDT"), 3)
        .unwrap();

    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].hops(), 1);
    assert_eq!(paths[0].edges[0].direction, ConversionDirection::Direct);
    assert_eq!(paths[0].edges[0].reference_dataset_id, 101);
}

#[test]
fn finds_inverse_usdt_to_btc() {
    let graph = graph();

    let paths = graph
        .find_paths(&currency("USDT"), &currency("BTC"), 3)
        .unwrap();

    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].hops(), 1);
    assert_eq!(paths[0].edges[0].direction, ConversionDirection::Inverse);
    assert_eq!(paths[0].edges[0].reference_dataset_id, 101);
}

// -----------------------------------------------------------------------------
// Multi-hop paths
// -----------------------------------------------------------------------------

#[test]
fn finds_eth_to_usdt_multihop() {
    let graph = graph();

    let paths = path_currencies(&graph, "ETH", "USDT", 3);

    assert_eq!(paths, vec![vec!["ETH", "BTC", "USDT"]]);
}

#[test]
fn finds_usdc_to_usdt_multihop() {
    let graph = graph();

    let paths = path_currencies(&graph, "USDC", "USDT", 3);

    assert_eq!(paths, vec![vec!["USDC", "BTC", "USDT"]]);
}

#[test]
fn finds_eth_to_usdc() {
    let graph = graph();

    let paths = path_currencies(&graph, "ETH", "USDC", 3);

    assert_eq!(paths, vec![vec!["ETH", "BTC", "USDC"]]);
}

// -----------------------------------------------------------------------------
// Hop limits
// -----------------------------------------------------------------------------

#[test]
fn respects_maximum_hops() {
    let graph = graph();

    assert!(path_currencies(&graph, "ETH", "USDT", 1).is_empty());

    assert_eq!(path_currencies(&graph, "ETH", "USDT", 2).len(), 1);
}

#[test]
fn rejects_zero_hop_limit() {
    let graph = graph();

    assert!(
        graph
            .find_paths(&currency("ETH"), &currency("USDT"), 0)
            .is_err()
    );
}

// -----------------------------------------------------------------------------
// Identity and missing paths
// -----------------------------------------------------------------------------

#[test]
fn identity_conversion_has_zero_edges() {
    let graph = graph();

    let paths = graph
        .find_paths(&currency("USDT"), &currency("USDT"), 3)
        .unwrap();

    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].hops(), 0);
}

#[test]
fn returns_no_path_for_unknown_currency() {
    let graph = graph();

    assert!(path_currencies(&graph, "SOL", "USDT", 3).is_empty());
}

// -----------------------------------------------------------------------------
// Cycle prevention
// -----------------------------------------------------------------------------

#[test]
fn paths_do_not_repeat_currencies() {
    let graph = graph();

    for source in ["ETH", "BTC", "USDC", "USDT"] {
        for target in ["ETH", "BTC", "USDC", "USDT"] {
            let paths = graph
                .find_paths(&currency(source), &currency(target), 3)
                .unwrap();

            for path in paths {
                let unique: std::collections::HashSet<_> = path.currencies.iter().collect();

                assert_eq!(unique.len(), path.currencies.len());
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Determinism
// -----------------------------------------------------------------------------

#[test]
fn path_enumeration_is_deterministic() {
    let graph = graph();

    let first = graph
        .find_paths(&currency("ETH"), &currency("USDT"), 3)
        .unwrap();

    let second = graph
        .find_paths(&currency("ETH"), &currency("USDT"), 3)
        .unwrap();

    assert_eq!(first, second);
}

#[test]
fn preserves_reference_stream_identity() {
    let graph = graph();

    let paths = graph
        .find_paths(&currency("ETH"), &currency("USDT"), 3)
        .unwrap();

    let edges = &paths[0].edges;

    assert_eq!(edges.len(), 2);

    assert_eq!(edges[0].reference_dataset_id, 101);
    assert_eq!(edges[1].reference_dataset_id, 101);

    assert_eq!(
        edges[0].reference_stream_id,
        crate::market::stream_id(3, false)
    );

    assert_eq!(
        edges[1].reference_stream_id,
        crate::market::stream_id(0, false)
    );
}
