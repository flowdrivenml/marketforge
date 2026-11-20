use {
    marketforge_engine::historical::convert::resolve_conversion_catalog,
    marketforge_engine::historical::merge::{PhysicalMergeManifest, UnifiedMergeMode},
    marketforge_engine::shared::domain::instrument::InstrumentCatalog,
};

fn fixture() -> (
    tempfile::TempDir,
    marketforge_engine::historical::job::ConvertJob,
    PhysicalMergeManifest,
    InstrumentCatalog,
) {
    let directory = tempfile::tempdir().unwrap();
    let mut job = crate::conversion_fixture::canonical_job();
    job.input_merge = directory.path().join("trades");
    let manifest = crate::market::create(&job.input_merge, UnifiedMergeMode::Trades, false);
    let catalog = crate::market::catalog(directory.path());
    (directory, job, manifest, catalog)
}

#[test]
fn resolves_all_multihop_instruments() {
    let (directory, job, manifest, catalog) = fixture();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, directory.path()).unwrap();

    assert_eq!(resolved.sources.len(), 4);
    assert_eq!(resolved.references.len(), 2);

    let symbols: Vec<_> = resolved
        .sources
        .iter()
        .map(|instrument| instrument.identity.symbol.as_str())
        .collect();

    assert_eq!(symbols, ["BTCUSDT", "BTCUSDC", "ETHUSDT", "ETHBTC"]);
}

#[test]
fn resolves_correct_currencies() {
    let (directory, job, manifest, catalog) = fixture();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, directory.path()).unwrap();

    let expected = [
        ("BTCUSDT", "BTC", "USDT"),
        ("BTCUSDC", "BTC", "USDC"),
        ("ETHUSDT", "ETH", "USDT"),
        ("ETHBTC", "ETH", "BTC"),
    ];

    for (instrument, (symbol, base, quote)) in resolved.sources.iter().zip(expected) {
        assert_eq!(instrument.identity.symbol, symbol);
        assert_eq!(instrument.base_currency.as_str(), base);
        assert_eq!(instrument.quote_currency.as_str(), quote);
    }
}

#[test]
fn preserves_merge_stream_ranks() {
    let (directory, job, manifest, catalog) = fixture();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, directory.path()).unwrap();

    let ranks: Vec<_> = resolved
        .sources
        .iter()
        .map(|instrument| instrument.stream_rank)
        .collect();

    assert_eq!(ranks, [0, 1, 2, 3]);
}

#[test]
fn rejects_missing_reference_instrument() {
    let (directory, mut job, manifest, catalog) = fixture();

    job.conversion.references[0].symbol = "UNKNOWN".to_owned();

    let result = resolve_conversion_catalog(&job, &manifest, &catalog, directory.path());

    assert!(result.is_err());
}

#[test]
fn rejects_missing_catalog_instrument() {
    let (directory, job, manifest, _) = fixture();
    let path = crate::market::write_catalog(directory.path());
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    value["instruments"].as_array_mut().unwrap().remove(0);
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    let catalog = InstrumentCatalog::load(&path).unwrap();

    let result = resolve_conversion_catalog(&job, &manifest, &catalog, directory.path());

    assert!(result.is_err());
}

#[test]
fn rejects_processing_identity_mismatch() {
    let (directory, job, mut manifest, catalog) = fixture();

    manifest.streams[0].instrument_id = -1;

    let result = resolve_conversion_catalog(&job, &manifest, &catalog, directory.path());

    assert!(result.is_err());
}
