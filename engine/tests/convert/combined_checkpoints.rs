use std::{fs, path::PathBuf};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::load_convert_job,
    process::{
        convert::{
            execution::{
                CombinedReferenceSelection, build_combined_reference_checkpoints,
                build_conversion_partition_plan,
            },
            plan_conversion, resolve_conversion_catalog,
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

#[test]
fn builds_real_combined_reference_checkpoints() {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();

    // Use two partitions initially.
    let partitions = build_conversion_partition_plan(&job.input_merge, &source, 2).unwrap();

    let start = std::time::Instant::now();

    let prepass = build_combined_reference_checkpoints(
        &job.input_merge,
        &source,
        Some(reference_directory),
        Some(&external),
        &selection,
        &partitions,
    )
    .unwrap();

    let elapsed = start.elapsed().as_secs_f64();

    // ---------------------------------------------------------------------
    // Validate checkpoint accounting
    // ---------------------------------------------------------------------

    assert_eq!(prepass.events_scanned, source.events_written);

    assert_eq!(prepass.checkpoints.len(), partitions.partitions.len(),);

    for (checkpoint, partition) in prepass.checkpoints.iter().zip(&partitions.partitions) {
        assert_eq!(checkpoint.partition_id, partition.partition_id,);

        assert_eq!(checkpoint.start_row, partition.start_row,);

        assert!(checkpoint.external_position.is_some());

        for observation in checkpoint.state.observations() {
            assert!(
                observation.key.timestamp_ns <= checkpoint.start_timestamp_ns,
                "checkpoint contains future reference"
            );
        }
    }

    // ---------------------------------------------------------------------
    // First checkpoint must have no internal references
    // ---------------------------------------------------------------------

    let first = &prepass.checkpoints[0];

    assert_eq!(first.start_row, 0);

    assert!(
        first
            .state
            .observations()
            .all(|observation| { !selection.internal_streams.contains(&observation.stream_id) })
    );

    // ---------------------------------------------------------------------
    // Second checkpoint must contain internal references
    // ---------------------------------------------------------------------

    let second = &prepass.checkpoints[1];

    assert!(second.start_row > 0);

    assert!(
        second
            .state
            .observations()
            .any(|observation| { selection.internal_streams.contains(&observation.stream_id) }),
        "second checkpoint missing internal references"
    );

    // ---------------------------------------------------------------------
    // Verify external cursor progress
    // ---------------------------------------------------------------------

    let first_position = first.external_position.unwrap();
    let second_position = second.external_position.unwrap();

    assert!(second_position.observations_consumed >= first_position.observations_consumed);

    assert!(prepass.internal_updates > 0);
    assert!(prepass.external_updates > 0);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — COMBINED REFERENCE CHECKPOINTS");
    println!("{}", "=".repeat(90));

    println!("Events scanned     : {}", prepass.events_scanned);
    println!("Partitions         : {}", prepass.checkpoints.len());
    println!("Internal updates   : {}", prepass.internal_updates);
    println!("External updates   : {}", prepass.external_updates);
    println!("Observations read  : {}", prepass.observations_read);

    for checkpoint in &prepass.checkpoints {
        println!();
        println!("Partition          : {}", checkpoint.partition_id);
        println!("Start row          : {}", checkpoint.start_row);
        println!("Start timestamp    : {}", checkpoint.start_timestamp_ns);

        println!("Reference position : {:?}", checkpoint.external_position);

        println!("Reference streams  : {}", checkpoint.state.len());
    }

    println!();
    println!("Elapsed            : {:.3} s", elapsed);
    println!("STATUS             : PASS");
}

#[test]
fn validates_combined_worker_fixture() {
    let root = root();

    let path = root.join("data/fixtures/combined-worker");

    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(path.join("manifest.json")).unwrap()).unwrap();

    manifest.validate().unwrap();

    let partitions = build_conversion_partition_plan(&path, &manifest, 2).unwrap();

    assert_eq!(manifest.events_written, 20_000);
    assert_eq!(manifest.files_written, 1);
    assert_eq!(manifest.row_groups, 4);

    assert_eq!(partitions.total_rows, 20_000);
    assert_eq!(partitions.row_groups.len(), 4);
    assert_eq!(partitions.partitions.len(), 2);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — COMBINED WORKER FIXTURE VALIDATION");
    println!("{}", "=".repeat(90));

    println!("Events     : {}", partitions.total_rows);
    println!("Row groups : {}", partitions.row_groups.len());
    println!("Partitions : {}", partitions.partitions.len());
    println!("STATUS     : PASS");
}
