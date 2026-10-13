use std::path::PathBuf;

use marketforge_engine::{
    instrument::{Currency, InstrumentCatalog},
    job::load_convert_job,
    process::{
        convert::{
            ConversionDirection, ConversionGraph, ResolvedConversionCatalog,
            resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn graph() -> ConversionGraph {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-multihop.json")).unwrap();

    let manifest: PhysicalMergeManifest = serde_json::from_slice(
        &std::fs::read(root.join("data/merged/conversion-multihop/manifest.json")).unwrap(),
    )
    .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved: ResolvedConversionCatalog =
        resolve_conversion_catalog(&job, &manifest, &catalog, &root).unwrap();

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
    assert_eq!(paths[0].edges[0].reference_dataset_id, 106);
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
    assert_eq!(paths[0].edges[0].reference_dataset_id, 106);
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

    assert_eq!(edges[0].reference_dataset_id, 129);
    assert_eq!(edges[1].reference_dataset_id, 106);

    assert_eq!(
        edges[0].reference_stream_id,
        "binance:spot:spot:ETHBTC:trade"
    );

    assert_eq!(
        edges[1].reference_stream_id,
        "binance:spot:spot:BTCUSDT:trade"
    );
}
