use std::{fs, path::PathBuf};

use {
    marketforge_engine::historical::convert::execution::build_conversion_checkpoints,
    marketforge_engine::historical::convert::execution::build_conversion_partition_plan,
    marketforge_engine::historical::convert::execution::execute_parallel_conversion,
    marketforge_engine::historical::convert::output::ConversionExecutionReceipt,
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

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn publishes_parallel_conversion() {
    let root = root();
    let temporary = tempfile::tempdir().unwrap();

    let input = temporary.path().join("input");
    let staging = temporary.path().join("parallel.staging");
    let final_path = temporary.path().join("parallel");

    fs::create_dir_all(&input).unwrap();

    // -------------------------------------------------------------
    // Create physical input fixture
    // -------------------------------------------------------------

    let source: PhysicalMergeManifest = create_fixture(&input);

    let mut job = load_convert_job(root.join("data/.jobs/convert/convert-multihop.json")).unwrap();

    job.input_merge = input.clone();
    job.output.dataset_path = final_path.clone();

    // -------------------------------------------------------------
    // Resolve conversion plan
    // -------------------------------------------------------------

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    // -------------------------------------------------------------
    // Build two partitions
    // -------------------------------------------------------------

    let partitions = build_conversion_partition_plan(&input, &source, 2).unwrap();

    assert_eq!(partitions.partitions.len(), 2);

    // -------------------------------------------------------------
    // Build reference checkpoints
    // -------------------------------------------------------------

    let prepass = build_conversion_checkpoints(&job, &input, &source, &plan, &partitions).unwrap();

    // -------------------------------------------------------------
    // Execute parallel conversion
    // -------------------------------------------------------------

    let parquet = MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 4,
        compression_level: 3,
    };

    let result = execute_parallel_conversion(
        &job,
        &source,
        &plan,
        &partitions,
        &prepass,
        &staging,
        2,
        &parquet,
    )
    .unwrap();

    assert_eq!(result.metrics.events_written, source.events_written,);

    // -------------------------------------------------------------
    // Persist execution receipt
    // -------------------------------------------------------------

    ConversionExecutionReceipt::new(job.job_id, &result.metrics)
        .unwrap()
        .write(&staging)
        .unwrap();

    // -------------------------------------------------------------
    // Publish through production workflow
    // -------------------------------------------------------------

    let publication = publish_conversion(&job, &source, &staging).unwrap();

    assert!(!staging.exists());
    assert!(final_path.is_dir());
    assert!(publication.manifest_path.is_file());

    // -------------------------------------------------------------
    // Validate published manifest
    // -------------------------------------------------------------

    let manifest: ConversionManifest =
        serde_json::from_slice(&fs::read(&publication.manifest_path).unwrap()).unwrap();

    manifest.validate().unwrap();

    assert_eq!(manifest.statistics.events_written, source.events_written,);

    assert_eq!(
        manifest.statistics.reference_updates,
        result.metrics.reference_updates,
    );

    assert_eq!(
        manifest.integrity.source_events_compared,
        source.events_written,
    );

    assert_eq!(manifest.output.files_written, result.metrics.files_written,);

    assert_eq!(
        manifest.output.row_groups,
        result.metrics.row_groups_written,
    );

    // -------------------------------------------------------------
    // Verify partitioned file inventory
    // -------------------------------------------------------------

    assert_eq!(result.partitions.len(), 2);

    for partition in &result.partitions {
        let prefix = format!("partition-{:06}/", partition.partition_id,);

        assert!(
            manifest
                .output
                .files
                .iter()
                .any(|file| { file.path.to_string_lossy().starts_with(&prefix) }),
            "missing partition {} in manifest",
            partition.partition_id,
        );
    }

    println!();
    println!("{}", "=".repeat(85));
    println!("MARKETFORGE — PARALLEL PUBLICATION");
    println!("{}", "=".repeat(85));

    println!("Workers          : {}", result.workers_used);
    println!("Partitions       : {}", result.partitions.len());
    println!("Events           : {}", manifest.statistics.events_written);
    println!("Files            : {}", manifest.output.files_written);
    println!("Row groups       : {}", manifest.output.row_groups);
    println!(
        "References       : {}",
        manifest.integrity.references_checked
    );
    println!(
        "Source preserved : {}",
        manifest.integrity.source_events_compared
    );

    println!("STATUS           : PASS");
}
