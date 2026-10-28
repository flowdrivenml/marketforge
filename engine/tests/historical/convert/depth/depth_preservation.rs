use std::{fs, path::PathBuf, time::Instant};

use {
    marketforge_engine::historical::convert::execution::materialize_depth_conversion,
    marketforge_engine::historical::convert::output::validate_depth_source_preservation,
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::job::load_convert_job,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
    marketforge_engine::shared::domain::instrument::InstrumentCatalog,
};

const EXPECTED_EVENTS: u64 = 12_194;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

struct Fixture {
    _temporary: tempfile::TempDir,
    source_directory: PathBuf,
    source_manifest: PhysicalMergeManifest,
    converted_directory: PathBuf,
}

fn fixture() -> Fixture {
    let root = root();
    let temporary = tempfile::tempdir().unwrap();

    let converted_directory = temporary.path().join("depth.staging");

    let job = load_convert_job(root.join("data/.jobs/convert/convert-depth-test.json")).unwrap();

    let source_manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let reference_manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source_manifest, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let parquet = MergeParquetConfig {
        row_group_target_mb: 16,
        file_target_mb: 64,
        compression_level: 3,
    };

    materialize_depth_conversion(
        &job,
        &source_manifest,
        &reference_manifest,
        &plan,
        &converted_directory,
        &parquet,
        Some(EXPECTED_EVENTS),
    )
    .unwrap();

    Fixture {
        _temporary: temporary,
        source_directory: job.input_merge,
        source_manifest,
        converted_directory,
    }
}

// -----------------------------------------------------------------------------
// Bounded source preservation
// -----------------------------------------------------------------------------

#[test]
fn preserves_real_depth_source_prefix() {
    let fixture = fixture();

    let start = Instant::now();

    let result = validate_depth_source_preservation(
        &fixture.source_directory,
        &fixture.source_manifest,
        &fixture.converted_directory,
        Some(EXPECTED_EVENTS),
    )
    .unwrap();

    assert_eq!(result.events_compared, EXPECTED_EVENTS);
    assert_eq!(result.columns_compared, 17);
    assert!(result.source_batches > 0);
    assert!(result.converted_batches > 0);
    assert_eq!(result.converted_files, 1);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — DEPTH SOURCE PRESERVATION");
    println!("{}", "=".repeat(90));

    println!("Events compared   : {}", result.events_compared);
    println!("Columns compared  : {}", result.columns_compared);
    println!("Source batches    : {}", result.source_batches);
    println!("Converted batches : {}", result.converted_batches);
    println!("Converted files   : {}", result.converted_files);
    println!("Elapsed           : {:.3} s", start.elapsed().as_secs_f64());
    println!("STATUS            : PASS");
}

// -----------------------------------------------------------------------------
// Full preservation rejects bounded output
// -----------------------------------------------------------------------------

#[test]
fn rejects_incomplete_full_depth_conversion() {
    let fixture = fixture();

    let result = validate_depth_source_preservation(
        &fixture.source_directory,
        &fixture.source_manifest,
        &fixture.converted_directory,
        None,
    );

    assert!(result.is_err());
}

// -----------------------------------------------------------------------------
// Reject invalid prefix
// -----------------------------------------------------------------------------

#[test]
fn rejects_zero_event_prefix() {
    let fixture = fixture();

    assert!(
        validate_depth_source_preservation(
            &fixture.source_directory,
            &fixture.source_manifest,
            &fixture.converted_directory,
            Some(0),
        )
        .is_err()
    );
}

#[test]
fn rejects_prefix_exceeding_source_count() {
    let fixture = fixture();

    assert!(
        validate_depth_source_preservation(
            &fixture.source_directory,
            &fixture.source_manifest,
            &fixture.converted_directory,
            Some(fixture.source_manifest.events_written + 1),
        )
        .is_err()
    );
}

// -----------------------------------------------------------------------------
// Reject incorrect expected event count
// -----------------------------------------------------------------------------

#[test]
fn rejects_shorter_expected_prefix() {
    let fixture = fixture();

    assert!(
        validate_depth_source_preservation(
            &fixture.source_directory,
            &fixture.source_manifest,
            &fixture.converted_directory,
            Some(EXPECTED_EVENTS - 1),
        )
        .is_err()
    );
}
