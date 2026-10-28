use std::{fs, path::PathBuf};

use marketforge_engine::historical::convert::output::ConversionExecutionReceipt;
use {
    marketforge_engine::historical::convert::execution::PhysicalConversionMetrics,
    marketforge_engine::historical::convert::execution::materialize_trade_conversion,
    marketforge_engine::historical::convert::output::ConversionManifest,
    marketforge_engine::historical::convert::output::publish_conversion,
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::job::load_convert_job,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
    marketforge_engine::shared::domain::instrument::InstrumentCatalog,
};
#[path = "../support/multihop_fixture.rs"]
mod multihop_fixture;

use multihop_fixture::create_fixture;

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

struct PublicationFixture {
    _temporary: tempfile::TempDir,
    job: marketforge_engine::historical::job::ConvertJob,
    source: PhysicalMergeManifest,
    staging: PathBuf,
    metrics: PhysicalConversionMetrics,
}

fn fixture() -> PublicationFixture {
    let temporary = tempfile::tempdir().unwrap();

    let input = temporary.path().join("input");
    let staging = temporary.path().join("converted.staging");
    let final_path = temporary.path().join("converted");

    fs::create_dir_all(&input).unwrap();

    let source = create_fixture(&input);

    let mut job =
        load_convert_job(root().join("data/.jobs/convert/convert-multihop.json")).unwrap();

    job.input_merge = input.clone();
    job.output.dataset_path = final_path;

    let catalog = InstrumentCatalog::load(&root().join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root()).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let config = MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 4,
        compression_level: 3,
    };

    let metrics = materialize_trade_conversion(&job, &source, &plan, &staging, &config).unwrap();
    ConversionExecutionReceipt::new(job.job_id, &metrics)
        .unwrap()
        .write(&staging)
        .unwrap();
    PublicationFixture {
        _temporary: temporary,
        job,
        source,
        staging,
        metrics,
    }
}

// -----------------------------------------------------------------------------
// Successful publication
// -----------------------------------------------------------------------------

#[test]
fn publishes_validated_conversion() {
    let fixture = fixture();

    let result = publish_conversion(&fixture.job, &fixture.source, &fixture.staging).unwrap();

    assert!(!fixture.staging.exists());
    assert!(result.dataset_path.is_dir());
    assert!(result.manifest_path.is_file());

    assert_eq!(result.events_written, fixture.metrics.events_written);

    assert_eq!(result.files_written, fixture.metrics.files_written);

    let manifest: ConversionManifest =
        serde_json::from_slice(&fs::read(&result.manifest_path).unwrap()).unwrap();

    manifest.validate().unwrap();

    assert_eq!(manifest.job_id, fixture.job.job_id);

    assert_eq!(
        manifest.statistics.events_written,
        fixture.metrics.events_written
    );

    assert_eq!(
        manifest.statistics.reference_updates,
        fixture.metrics.reference_updates
    );

    assert_eq!(
        manifest.integrity.source_events_compared,
        fixture.metrics.events_written
    );

    assert!(manifest.integrity.conversion_validation);
    assert!(manifest.integrity.source_preservation);
}

// -----------------------------------------------------------------------------
// Existing destination
// -----------------------------------------------------------------------------

#[test]
fn rejects_existing_final_dataset() {
    let fixture = fixture();

    let final_path = &fixture.job.output.dataset_path;

    fs::create_dir_all(final_path).unwrap();
    fs::write(final_path.join("marker.txt"), "existing").unwrap();

    assert!(publish_conversion(&fixture.job, &fixture.source, &fixture.staging,).is_err());

    assert!(fixture.staging.is_dir());

    assert_eq!(
        fs::read_to_string(final_path.join("marker.txt")).unwrap(),
        "existing"
    );
}

// -----------------------------------------------------------------------------
// Missing staging
// -----------------------------------------------------------------------------

#[test]
fn rejects_missing_staging_directory() {
    let fixture = fixture();

    fs::remove_dir_all(&fixture.staging).unwrap();

    assert!(publish_conversion(&fixture.job, &fixture.source, &fixture.staging,).is_err());

    assert!(!fixture.job.output.dataset_path.exists());
}

// -----------------------------------------------------------------------------
// Corrupted physical output
// -----------------------------------------------------------------------------

#[test]
fn rejects_corrupted_parquet() {
    let fixture = fixture();

    let parquet = fixture.staging.join("part-000000.parquet");

    fs::write(&parquet, b"corrupted parquet").unwrap();

    assert!(publish_conversion(&fixture.job, &fixture.source, &fixture.staging,).is_err());

    assert!(fixture.staging.is_dir());
    assert!(!fixture.job.output.dataset_path.exists());
}

// -----------------------------------------------------------------------------
// Incomplete physical output
// -----------------------------------------------------------------------------

#[test]
fn rejects_missing_parquet_file() {
    let fixture = fixture();

    for entry in fs::read_dir(&fixture.staging).unwrap() {
        let path = entry.unwrap().path();

        if path.extension().is_some_and(|ext| ext == "parquet") {
            fs::remove_file(path).unwrap();
        }
    }

    assert!(publish_conversion(&fixture.job, &fixture.source, &fixture.staging,).is_err());

    assert!(fixture.staging.is_dir());
    assert!(!fixture.job.output.dataset_path.exists());
}

// -----------------------------------------------------------------------------
// Invalid source identity
// -----------------------------------------------------------------------------

#[test]
fn rejects_incorrect_source_event_count() {
    let fixture = fixture();

    let mut invalid_source = fixture.source.clone();

    invalid_source.events_written += 1;

    assert!(publish_conversion(&fixture.job, &invalid_source, &fixture.staging,).is_err());

    assert!(fixture.staging.is_dir());
    assert!(!fixture.job.output.dataset_path.exists());
}

// -----------------------------------------------------------------------------
// Existing staging manifest
// -----------------------------------------------------------------------------

#[test]
fn rejects_existing_staging_manifest() {
    let fixture = fixture();

    fs::write(fixture.staging.join("manifest.json"), "{}").unwrap();

    assert!(publish_conversion(&fixture.job, &fixture.source, &fixture.staging,).is_err());

    assert!(fixture.staging.is_dir());
    assert!(!fixture.job.output.dataset_path.exists());
}
