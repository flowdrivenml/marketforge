use arrow_array::{Array, Decimal256Array, StructArray};
use std::{fs, path::PathBuf};

use marketforge_engine::historical::convert::execution::{
    CONVERSION_READ_BATCH_SIZE, PhysicalConversionReader,
};
use {
    marketforge_engine::historical::convert::execution::CombinedReferenceSelection,
    marketforge_engine::historical::convert::execution::build_combined_reference_checkpoints,
    marketforge_engine::historical::convert::execution::build_combined_reference_checkpoints_full_reader,
    marketforge_engine::historical::convert::execution::build_conversion_partition_plan,
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

#[test]
fn projected_reference_reader_matches_full_reader() {
    let root = root();
    let path = root.join("data/fixtures/combined-worker");

    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(path.join("manifest.json")).unwrap()).unwrap();

    let mut full =
        PhysicalConversionReader::new(&path, &manifest, CONVERSION_READ_BATCH_SIZE).unwrap();

    let mut projected = PhysicalConversionReader::new_reference_prepass(
        &path,
        &manifest,
        CONVERSION_READ_BATCH_SIZE,
    )
    .unwrap();

    let mut total_rows = 0_u64;

    loop {
        let full_batch = full.next_batch().unwrap();
        let projected_batch = projected.next_batch().unwrap();

        match (full_batch, projected_batch) {
            (Some(full_batch), Some(projected_batch)) => {
                assert_eq!(full_batch.num_rows(), projected_batch.num_rows(),);

                // Verify all projected columns match the original.
                for name in [
                    "event_timestamp_ns",
                    "event_kind",
                    "stream_id",
                    "stream_rank",
                    "dataset_id",
                    "task_id",
                    "event_ordinal",
                ] {
                    let original = full_batch.column_by_name(name).unwrap();
                    let selected = projected_batch.column_by_name(name).unwrap();

                    assert_eq!(
                        original.to_data(),
                        selected.to_data(),
                        "projected column mismatch: {name}",
                    );
                }

                // Verify nested trade.price remains accessible.
                let full_trade = full_batch
                    .column_by_name("trade")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();

                let projected_trade = projected_batch
                    .column_by_name("trade")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();

                let original_price = full_trade
                    .column_by_name("price")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap();

                let selected_price = projected_trade
                    .column_by_name("price")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap();

                assert_eq!(
                    original_price.to_data(),
                    selected_price.to_data(),
                    "projected trade prices differ",
                );

                // Depth payload must not be decoded.
                assert!(
                    projected_batch.column_by_name("depth").is_none(),
                    "reference prepass unexpectedly decoded depth",
                );

                total_rows += projected_batch.num_rows() as u64;
            }

            (None, None) => break,

            _ => panic!("full and projected readers returned different batch counts"),
        }
    }

    assert_eq!(total_rows, 20_000);
    assert_eq!(projected.records_read(), manifest.events_written);

    println!(
        "Projected reference reader validated: {} events",
        total_rows,
    );
}

#[test]
fn projected_checkpoints_match_full_reader() {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let fixture_path = root.join("data/fixtures/combined-worker");

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(fixture_path.join("manifest.json")).unwrap()).unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    // Resolve the conversion plan using the production manifest.
    let production_source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let resolved = resolve_conversion_catalog(&job, &production_source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();

    let partitions = build_conversion_partition_plan(&fixture_path, &source, 4).unwrap();

    // Original implementation: full Parquet reader.
    let full = build_combined_reference_checkpoints_full_reader(
        &fixture_path,
        &source,
        Some(reference_directory),
        Some(&external),
        &selection,
        &partitions,
    )
    .unwrap();

    // Optimized implementation: projected Parquet reader.
    let projected = build_combined_reference_checkpoints(
        &fixture_path,
        &source,
        Some(reference_directory),
        Some(&external),
        &selection,
        &partitions,
    )
    .unwrap();

    assert_eq!(full.events_scanned, projected.events_scanned);
    assert_eq!(full.events_scanned, 20_000);

    assert_eq!(full.internal_updates, projected.internal_updates);
    assert_eq!(full.external_updates, projected.external_updates);
    assert_eq!(full.observations_read, projected.observations_read);

    assert_eq!(full.checkpoints.len(), projected.checkpoints.len());
    assert_eq!(full.checkpoints.len(), 4);

    for (original, optimized) in full.checkpoints.iter().zip(&projected.checkpoints) {
        assert_eq!(original.partition_id, optimized.partition_id);
        assert_eq!(original.start_row, optimized.start_row);
        assert_eq!(original.start_timestamp_ns, optimized.start_timestamp_ns);

        assert_eq!(original.external_position, optimized.external_position);

        let mut original_observations = original.state.observations().collect::<Vec<_>>();

        let mut optimized_observations = optimized.state.observations().collect::<Vec<_>>();

        original_observations.sort_by(|a, b| {
            a.stream_id
                .cmp(&b.stream_id)
                .then_with(|| a.dataset_id.cmp(&b.dataset_id))
                .then_with(|| a.key.cmp(&b.key))
        });

        optimized_observations.sort_by(|a, b| {
            a.stream_id
                .cmp(&b.stream_id)
                .then_with(|| a.dataset_id.cmp(&b.dataset_id))
                .then_with(|| a.key.cmp(&b.key))
        });

        assert_eq!(
            original_observations, optimized_observations,
            "projected reference state differs from full reader",
        );
    }

    println!("Events scanned : {}", projected.events_scanned);
    println!("Checkpoints    : {}", projected.checkpoints.len());
    println!("Internal refs  : {}", projected.internal_updates);
    println!("External refs  : {}", projected.external_updates);
    println!("STATUS         : PASS");
}
