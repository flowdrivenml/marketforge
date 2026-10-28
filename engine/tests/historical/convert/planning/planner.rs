use std::path::PathBuf;

use {
    marketforge_engine::historical::convert::ConversionPlan,
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
    marketforge_engine::historical::job::ConvertJob,
    marketforge_engine::historical::job::load_convert_job,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
    marketforge_engine::shared::domain::instrument::InstrumentCatalog,
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn fixture() -> (ConvertJob, ConversionPlan) {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-multihop.json")).unwrap();

    let manifest: PhysicalMergeManifest = serde_json::from_slice(
        &std::fs::read(root.join("data/merged/conversion-multihop/manifest.json")).unwrap(),
    )
    .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    (job, plan)
}

fn stream<'a>(
    plan: &'a ConversionPlan,
    symbol: &str,
) -> &'a marketforge_engine::historical::convert::PlannedConversionStream {
    plan.streams
        .iter()
        .find(|stream| stream.source.identity.symbol == symbol)
        .unwrap()
}

fn currencies(path: &marketforge_engine::historical::convert::ConversionPath) -> Vec<&str> {
    path.currencies.iter().map(|c| c.as_str()).collect()
}

// -----------------------------------------------------------------------------
// Plan summary
// -----------------------------------------------------------------------------

#[test]
fn plans_all_multihop_streams() {
    let (_, plan) = fixture();

    assert_eq!(plan.source_streams, 3);
    assert_eq!(plan.reference_streams, 3);
    assert_eq!(plan.identity_streams, 1);
    assert_eq!(plan.convertible_streams, 2);
    assert_eq!(plan.unresolved_streams, 0);
}

#[test]
fn preserves_source_stream_order() {
    let (_, plan) = fixture();

    let ranks: Vec<_> = plan
        .streams
        .iter()
        .map(|stream| stream.source.stream_rank)
        .collect();

    assert_eq!(ranks, [0, 1, 2]);
}

// -----------------------------------------------------------------------------
// Identity conversion
// -----------------------------------------------------------------------------

#[test]
fn btcusdt_requires_no_reference_conversion() {
    let (_, plan) = fixture();

    let source = stream(&plan, "BTCUSDT");

    assert!(source.identity_conversion);
    assert_eq!(source.paths.len(), 1);
    assert_eq!(source.paths[0].hops(), 0);
}

// -----------------------------------------------------------------------------
// Direct conversion
// -----------------------------------------------------------------------------

#[test]
fn ethbtc_uses_btcusdt_reference() {
    let (_, plan) = fixture();

    let source = stream(&plan, "ETHBTC");

    assert!(!source.identity_conversion);
    assert_eq!(source.paths.len(), 1);

    let path = &source.paths[0];

    assert_eq!(currencies(path), ["BTC", "USDT"]);
    assert_eq!(path.hops(), 1);
    assert_eq!(path.edges[0].reference_dataset_id, 106);
}

// -----------------------------------------------------------------------------
// Inverse and multi-hop conversion
// -----------------------------------------------------------------------------

#[test]
fn btcusdc_uses_inverse_then_direct() {
    let (_, plan) = fixture();

    let source = stream(&plan, "BTCUSDC");

    assert_eq!(source.paths.len(), 1);

    let path = &source.paths[0];

    assert_eq!(currencies(path), ["USDC", "BTC", "USDT"]);
    assert_eq!(path.hops(), 2);

    assert_eq!(path.edges[0].reference_dataset_id, 130);
    assert_eq!(path.edges[1].reference_dataset_id, 106);
}

// -----------------------------------------------------------------------------
// Hop limits
// -----------------------------------------------------------------------------

#[test]
fn one_hop_limit_makes_btcusdc_unresolved() {
    let (mut job, _) = fixture();

    job.conversion.max_hops = 1;

    let root = root();

    let manifest: PhysicalMergeManifest = serde_json::from_slice(
        &std::fs::read(root.join("data/merged/conversion-multihop/manifest.json")).unwrap(),
    )
    .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    assert_eq!(plan.identity_streams, 1);
    assert_eq!(plan.convertible_streams, 1);
    assert_eq!(plan.unresolved_streams, 1);

    assert!(stream(&plan, "BTCUSDC").paths.is_empty());
}

// -----------------------------------------------------------------------------
// Determinism
// -----------------------------------------------------------------------------

#[test]
fn planning_is_deterministic() {
    let (_, first) = fixture();
    let (_, second) = fixture();

    assert_eq!(first.source_streams, second.source_streams);

    for (a, b) in first.streams.iter().zip(&second.streams) {
        assert_eq!(a.source.stream_id, b.source.stream_id);
        assert_eq!(a.paths, b.paths);
    }
}

// -----------------------------------------------------------------------------
// Invalid configuration
// -----------------------------------------------------------------------------

#[test]
fn rejects_invalid_target_currency() {
    let (mut job, _) = fixture();

    job.conversion.target_quote = "usdt".to_owned();

    assert!(job.validate().is_err());
}
