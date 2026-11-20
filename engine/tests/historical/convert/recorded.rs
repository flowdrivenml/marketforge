//! Full recorded publication. This target requires historical-real-tests.
#[path = "../support/conversion_fixture.rs"]
mod conversion_fixture;

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use marketforge_engine::historical::{
    convert::{execution::validate_conversion_input, output::ConversionManifest},
    job::{ConvertJob, MergeTimelineOrdering},
    merge::{PhysicalMergeManifest, UnifiedMergeMode},
    normalize::manifest::file_sha256,
};

fn recorded_input() -> (ConvertJob, PhysicalMergeManifest, PathBuf) {
    let mut job = conversion_fixture::canonical_job();
    if let Some(path) = env::var_os("MARKETFORGE_RECORDED_CONVERT_INPUT") {
        job.input_merge = PathBuf::from(path);
    }
    assert!(
        job.input_merge.is_absolute(),
        "MARKETFORGE_RECORDED_CONVERT_INPUT must be absolute"
    );
    let catalog = PathBuf::from(env::var_os("MARKETFORGE_RECORDED_CONVERT_CATALOG").expect(
        "Set MARKETFORGE_RECORDED_CONVERT_CATALOG to the verified conversion catalog snapshot",
    ));
    assert!(
        catalog.is_absolute() && catalog.is_file(),
        "Supply an absolute catalog file via MARKETFORGE_RECORDED_CONVERT_CATALOG"
    );
    let manifest_path = job.input_merge.join("manifest.json");
    let source: PhysicalMergeManifest = serde_json::from_reader(fs::File::open(&manifest_path).unwrap_or_else(|error| {
        panic!("{}: {error}. Set MARKETFORGE_RECORDED_CONVERT_INPUT to engine publication 2079541492669036539 (registry publication 60, primary dataset 360)", manifest_path.display())
    })).unwrap();
    assert_eq!(source.job_id, 2079541492669036539);
    assert_eq!(source.mode, UnifiedMergeMode::Combined);
    assert_eq!(source.timeline_ordering, MergeTimelineOrdering::Causal);
    assert_eq!(source.physical_schema_version, 2);
    assert_eq!(
        (source.time_range.start_ns, source.time_range.end_ns),
        (1793059200000000000, 1793318400000000000)
    );
    assert_eq!(
        (
            source.events_written,
            source.trade_events,
            source.depth_events
        ),
        (12_477_919, 5_233_312, 7_244_607)
    );
    assert_eq!(source.streams.len(), 8);
    let checksums: serde_json::Value = serde_json::from_reader(
        fs::File::open(job.input_merge.join("publication-checksums.json"))
            .expect("Copy publication-checksums.json from the committed merge"),
    )
    .unwrap();
    assert_eq!(
        checksums["manifest.json"],
        file_sha256(&manifest_path).unwrap()
    );
    validate_conversion_input(&job.input_merge, &source).expect(
        "Copy the complete committed merge inventory to MARKETFORGE_RECORDED_CONVERT_INPUT",
    );

    (job, source, catalog)
}

#[test]
fn validates_canonical_recorded_prefix_with_causal_prehistory() {
    use marketforge_engine::{
        historical::convert::{
            execution::materialize_combined_conversion, plan_conversion,
            resolve_conversion_catalog_with_normalization_root,
        },
        shared::domain::instrument::InstrumentCatalog,
    };

    let (mut job, source, catalog) = recorded_input();
    let normalization_root =
        env::var_os("MARKETFORGE_RECORDED_CONVERT_NORMALIZED").map(PathBuf::from);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let catalog = InstrumentCatalog::load(&catalog).unwrap();
    let resolved = resolve_conversion_catalog_with_normalization_root(
        &job,
        &source,
        &catalog,
        root,
        normalization_root.as_deref(),
    )
    .expect("Supply committed normalized provenance or MARKETFORGE_RECORDED_CONVERT_NORMALIZED");
    let plan = plan_conversion(&job, &resolved).unwrap();
    let temp = tempfile::tempdir().unwrap();
    job.output.dataset_path = temp.path().join("converted");
    let staging = temp.path().join("prefix");
    let parquet = marketforge_engine::historical::job::MergeParquetConfig {
        row_group_target_mb: 4,
        file_target_mb: 16,
        compression_level: 1,
    };
    let metrics =
        materialize_combined_conversion(&job, &source, None, &plan, &staging, &parquet, Some(4096))
            .unwrap();
    assert_eq!(metrics.events_written, 4096);
    assert!(metrics.trade_events > 0 && metrics.depth_events > 0);
    assert!(metrics.internal_reference_updates > 0);
    assert!(metrics.identity_events > 0 && metrics.converted_events > 0);
    assert!(!job.output.dataset_path.exists());
}

#[test]
fn publishes_canonical_recorded_conversion() {
    let (mut job, source, catalog) = recorded_input();
    let temp = tempfile::tempdir().unwrap();
    job.output.dataset_path = temp.path().join("converted");
    let derived_path = temp.path().join("job.json");
    serde_json::to_writer(fs::File::create(&derived_path).unwrap(), &job).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_marketforge-convert"));
    command
        .args(["--project-root"])
        .arg(root)
        .arg("--job")
        .arg(derived_path)
        .arg("--catalog")
        .arg(catalog)
        .arg("--run");
    if let Some(path) = env::var_os("MARKETFORGE_RECORDED_CONVERT_CONFIG") {
        assert!(
            Path::new(&path).is_absolute(),
            "Recorded runtime config must be absolute"
        );
        command.arg("--config").arg(path);
    }
    if let Some(path) = env::var_os("MARKETFORGE_RECORDED_CONVERT_NORMALIZED") {
        assert!(
            Path::new(&path).is_absolute(),
            "Normalized provenance root must be absolute"
        );
        command.arg("--normalization-root").arg(path);
    }
    assert!(
        command.status().unwrap().success(),
        "Check the supplied catalog, committed normalization directories (or MARKETFORGE_RECORDED_CONVERT_NORMALIZED), and temporary disk space"
    );
    let output: ConversionManifest = serde_json::from_reader(
        fs::File::open(job.output.dataset_path.join("manifest.json")).unwrap(),
    )
    .unwrap();
    output.validate().unwrap();
    assert_eq!(output.statistics.events_written, source.events_written);
    assert_eq!(
        output.integrity.source_events_compared,
        source.events_written
    );
    assert!(output.statistics.identity > 0);
    assert!(output.statistics.converted > 0);
}
