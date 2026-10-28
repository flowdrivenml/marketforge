use std::{collections::HashSet, fs, path::PathBuf};

use {
    marketforge_engine::historical::convert::execution::CombinedReferenceSelection,
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
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

fn fixture() -> (
    marketforge_engine::historical::convert::ConversionPlan,
    PhysicalMergeManifest,
    PhysicalMergeManifest,
) {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let reference_path = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_path.join("manifest.json")).unwrap()).unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    (plan, source, external)
}

#[test]
fn selects_real_internal_and_external_references() {
    let (plan, source, external) = fixture();

    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();

    let internal = &selection.internal_streams;
    let external = &selection.external_streams;

    assert!(
        internal.iter().any(|stream| stream.contains("ETHBTC")),
        "ETHBTC must be selected internally"
    );

    assert!(
        internal.iter().any(|stream| stream.contains("BTCUSDC")),
        "BTCUSDC must be selected internally"
    );

    assert!(
        external.iter().any(|stream| stream.contains("BTCUSDT")),
        "BTCUSDT must be selected externally"
    );

    assert!(
        internal.is_disjoint(external),
        "internal and external references must not overlap"
    );

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — COMBINED REFERENCE SELECTION");
    println!("{}", "=".repeat(90));

    println!("Internal references:");
    for stream in internal {
        println!("  {stream}");
    }

    println!("External references:");
    for stream in external {
        println!("  {stream}");
    }

    println!("STATUS: PASS");
}

#[test]
fn internal_references_take_precedence() {
    let (plan, source, external) = fixture();

    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();

    let source_ids = source
        .streams
        .iter()
        .map(|stream| stream.stream_id.as_str())
        .collect::<HashSet<_>>();

    for stream in &selection.external_streams {
        assert!(
            !source_ids.contains(stream.as_str()),
            "external selection duplicates an internal stream"
        );
    }
}

#[test]
fn rejects_missing_external_references() {
    let (plan, source, _) = fixture();

    let result = CombinedReferenceSelection::resolve(&plan, &source, None);

    assert!(
        result.is_err(),
        "missing required external reference must be rejected"
    );
}

#[test]
fn selected_reference_sets_are_disjoint() {
    let (plan, source, external) = fixture();

    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();

    assert!(
        selection
            .internal_streams
            .is_disjoint(&selection.external_streams)
    );
}
