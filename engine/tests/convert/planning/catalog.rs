use std::path::PathBuf;

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::load_convert_job,
    process::{convert::resolve_conversion_catalog, merge::PhysicalMergeManifest},
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn fixture() -> (
    marketforge_engine::job::ConvertJob,
    PhysicalMergeManifest,
    InstrumentCatalog,
) {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-multihop.json")).unwrap();

    let manifest: PhysicalMergeManifest = serde_json::from_slice(
        &std::fs::read(root.join("data/merged/conversion-multihop/manifest.json")).unwrap(),
    )
    .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    (job, manifest, catalog)
}

#[test]
fn resolves_all_multihop_instruments() {
    let (job, manifest, catalog) = fixture();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root()).unwrap();

    assert_eq!(resolved.sources.len(), 3);
    assert_eq!(resolved.references.len(), 3);

    let symbols: Vec<_> = resolved
        .sources
        .iter()
        .map(|instrument| instrument.identity.symbol.as_str())
        .collect();

    assert_eq!(symbols, ["BTCUSDT", "ETHBTC", "BTCUSDC"]);
}

#[test]
fn resolves_correct_currencies() {
    let (job, manifest, catalog) = fixture();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root()).unwrap();

    let expected = [
        ("BTCUSDT", "BTC", "USDT"),
        ("ETHBTC", "ETH", "BTC"),
        ("BTCUSDC", "BTC", "USDC"),
    ];

    for (instrument, (symbol, base, quote)) in resolved.sources.iter().zip(expected) {
        assert_eq!(instrument.identity.symbol, symbol);
        assert_eq!(instrument.base_currency.as_str(), base);
        assert_eq!(instrument.quote_currency.as_str(), quote);
    }
}

#[test]
fn preserves_merge_stream_ranks() {
    let (job, manifest, catalog) = fixture();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root()).unwrap();

    let ranks: Vec<_> = resolved
        .sources
        .iter()
        .map(|instrument| instrument.stream_rank)
        .collect();

    assert_eq!(ranks, [0, 1, 2]);
}

#[test]
fn rejects_missing_reference_instrument() {
    let (mut job, manifest, catalog) = fixture();

    job.conversion.references[0].symbol = "UNKNOWN".to_owned();

    let result = resolve_conversion_catalog(&job, &manifest, &catalog, &root());

    assert!(result.is_err());
}

#[test]
fn rejects_missing_catalog_instrument() {
    let (job, mut manifest, catalog) = fixture();

    manifest.streams[0].symbol = "UNKNOWN".to_owned();

    let result = resolve_conversion_catalog(&job, &manifest, &catalog, &root());

    assert!(result.is_err());
}

#[test]
fn rejects_processing_identity_mismatch() {
    let (job, mut manifest, catalog) = fixture();

    manifest.streams[0].instrument_id = -1;

    let result = resolve_conversion_catalog(&job, &manifest, &catalog, &root());

    assert!(result.is_err());
}
