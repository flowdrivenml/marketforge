use arrow_array::{Array, RecordBatch, UInt8Array};
use arrow_array::{Decimal256Array, ListArray, StructArray};
use arrow_schema::DataType;
use marketforge_engine::historical::convert::output::validate_depth_file;
use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{MergeParquetConfig, load_convert_job},
    process::{
        convert::{
            execution::materialize_depth_conversion, output::validate_converted_depth,
            plan_conversion, resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use std::sync::Arc;
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::OnceLock,
    time::Instant,
};

const EXPECTED_EVENTS: u64 = 12_194;
const EXPECTED_LEVELS: u64 = 154_826;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

// -----------------------------------------------------------------------------
// Shared physical fixture
// -----------------------------------------------------------------------------

fn fixture_directory() -> &'static Path {
    static FIXTURE: OnceLock<PathBuf> = OnceLock::new();

    FIXTURE.get_or_init(|| {
        let root = root();

        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path().join("depth.staging");

        let job =
            load_convert_job(root.join("data/.jobs/convert/convert-depth-test.json")).unwrap();

        let source: PhysicalMergeManifest =
            serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap())
                .unwrap();

        let reference_path = job.conversion.reference_merge.as_ref().unwrap();

        let reference_manifest: PhysicalMergeManifest =
            serde_json::from_slice(&fs::read(reference_path.join("manifest.json")).unwrap())
                .unwrap();

        let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

        let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

        let plan = plan_conversion(&job, &resolved).unwrap();

        let parquet = MergeParquetConfig {
            row_group_target_mb: 16,
            file_target_mb: 64,
            compression_level: 3,
        };

        materialize_depth_conversion(
            &job,
            &source,
            &reference_manifest,
            &plan,
            &directory,
            &parquet,
            Some(EXPECTED_EVENTS),
        )
        .unwrap();

        // Keep the fixture alive for the entire test process.
        let persistent = temporary.keep();

        persistent.join("depth.staging")
    })
}

// -----------------------------------------------------------------------------
// Copy fixture into isolated test directory
// -----------------------------------------------------------------------------

fn copy_fixture() -> tempfile::TempDir {
    let temporary = tempfile::tempdir().unwrap();

    let target = temporary.path().join("depth");

    fs::create_dir_all(&target).unwrap();

    for entry in fs::read_dir(fixture_directory()).unwrap() {
        let entry = entry.unwrap();

        fs::copy(entry.path(), target.join(entry.file_name())).unwrap();
    }

    temporary
}

// -----------------------------------------------------------------------------
// Rewrite one Parquet file
// -----------------------------------------------------------------------------

fn rewrite_parquet(path: &Path, transform: impl FnOnce(RecordBatch) -> RecordBatch) {
    let file = File::open(path).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    let schema = builder.schema().clone();

    let batches = builder
        .with_batch_size(65_536)
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let combined = arrow_select::concat::concat_batches(&schema, &batches).unwrap();

    let modified = transform(combined);

    let file = File::create(path).unwrap();

    let mut writer = ArrowWriter::try_new(file, modified.schema(), None).unwrap();

    writer.write(&modified).unwrap();
    writer.close().unwrap();
}
// -----------------------------------------------------------------------------
// Valid output
// -----------------------------------------------------------------------------

#[test]
fn validates_real_physical_depth_output() {
    let start = Instant::now();

    let result = validate_converted_depth(fixture_directory(), 5_000_000_000).unwrap();

    assert_eq!(result.events, EXPECTED_EVENTS);
    assert_eq!(result.levels, EXPECTED_LEVELS);

    assert_eq!(result.identity, 10_695);
    assert_eq!(result.converted, 779);
    assert_eq!(result.unavailable, 720);

    assert_eq!(result.levels_converted, 140_332);
    assert_eq!(result.levels_unavailable, 14_494);

    assert!(result.references_checked > 0);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — PHYSICAL DEPTH VALIDATION");
    println!("{}", "=".repeat(90));

    println!("Events             : {}", result.events);
    println!("Depth levels       : {}", result.levels);
    println!("Identity           : {}", result.identity);
    println!("Converted          : {}", result.converted);
    println!("Unavailable        : {}", result.unavailable);
    println!("References checked : {}", result.references_checked);
    println!("Files              : {}", result.files);
    println!("Row groups         : {}", result.row_groups);
    println!(
        "Elapsed            : {:.3} s",
        start.elapsed().as_secs_f64()
    );
    println!("STATUS             : PASS");
}

// -----------------------------------------------------------------------------
// Reject invalid status
// -----------------------------------------------------------------------------

#[test]
fn rejects_invalid_conversion_status() {
    let temporary = copy_fixture();

    let path = temporary.path().join("depth/part-000000.parquet");

    rewrite_parquet(&path, |batch| {
        let index = batch.schema().index_of("conversion_status").unwrap();

        let statuses = batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        let mut values = statuses.values().to_vec();
        values[0] = 255;

        let mut columns = batch.columns().to_vec();

        columns[index] = std::sync::Arc::new(UInt8Array::from(values));

        RecordBatch::try_new(batch.schema(), columns).unwrap()
    });

    assert!(validate_converted_depth(&temporary.path().join("depth"), 5_000_000_000,).is_err());
}

#[test]
fn rejects_missing_parquet_files() {
    let temporary = tempfile::tempdir().unwrap();

    assert!(validate_converted_depth(temporary.path(), 5_000_000_000,).is_err());
}

#[test]
fn rejects_null_converted_price() {
    let temporary = copy_fixture();
    let path = temporary.path().join("depth/part-000000.parquet");

    rewrite_parquet(&path, |batch| {
        let index = batch.schema().index_of("changes_target").unwrap();

        let list = batch
            .column(index)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        let levels = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let prices = levels
            .column_by_name("price_target")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();

        let mut values = (0..prices.len())
            .map(|i| (!prices.is_null(i)).then(|| prices.value(i)))
            .collect::<Vec<_>>();

        // Event 0 is an identity conversion.
        assert!(!prices.is_null(0));
        values[0] = None;

        let modified_prices = Decimal256Array::from(values)
            .with_precision_and_scale(76, 28)
            .unwrap();

        let mut level_columns = levels.columns().to_vec();

        let price_index = levels
            .column_names()
            .iter()
            .position(|name| *name == "price_target")
            .unwrap();

        level_columns[price_index] = Arc::new(modified_prices);

        let modified_levels = StructArray::try_new(
            levels.fields().clone(),
            level_columns,
            levels.nulls().cloned(),
        )
        .unwrap();

        let DataType::List(item_field) = batch.schema().field(index).data_type().clone() else {
            panic!("changes_target must be a List");
        };

        let modified_list = ListArray::try_new(
            item_field,
            list.offsets().clone(),
            Arc::new(modified_levels),
            list.nulls().cloned(),
        )
        .unwrap();

        let mut columns = batch.columns().to_vec();
        columns[index] = Arc::new(modified_list);

        RecordBatch::try_new(batch.schema(), columns).unwrap()
    });

    assert!(validate_converted_depth(&temporary.path().join("depth"), 5_000_000_000,).is_err());
}

#[test]
fn rejects_future_reference_timestamp() {
    let temporary = copy_fixture();
    let path = temporary.path().join("depth/part-000000.parquet");

    rewrite_parquet(&path, |batch| {
        use arrow_array::Int64Array;
        use arrow_schema::DataType;

        let index = batch.schema().index_of("conversion_references").unwrap();

        let list = batch
            .column(index)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        let references = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let timestamps = references
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let event_timestamps = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut values = timestamps.values().to_vec();

        // Find an event containing reference provenance.
        let row = (0..batch.num_rows())
            .find(|&row| list.value_length(row) > 0)
            .unwrap();

        let reference_index = list.value_offsets()[row] as usize;

        // Make the reference timestamp equal to its source event.
        // External references must be strictly earlier.
        values[reference_index] = event_timestamps.value(row);

        let mut fields = references.columns().to_vec();

        let timestamp_index = references
            .column_names()
            .iter()
            .position(|name| *name == "event_timestamp_ns")
            .unwrap();

        fields[timestamp_index] = Arc::new(Int64Array::from(values));

        let modified_references = StructArray::try_new(
            references.fields().clone(),
            fields,
            references.nulls().cloned(),
        )
        .unwrap();

        let DataType::List(item_field) = batch.schema().field(index).data_type().clone() else {
            panic!("conversion_references must be a List");
        };

        let modified_list = ListArray::try_new(
            item_field,
            list.offsets().clone(),
            Arc::new(modified_references),
            list.nulls().cloned(),
        )
        .unwrap();

        let mut columns = batch.columns().to_vec();
        columns[index] = Arc::new(modified_list);

        RecordBatch::try_new(batch.schema(), columns).unwrap()
    });

    assert!(validate_converted_depth(&temporary.path().join("depth"), 5_000_000_000,).is_err());
}

#[test]
fn rejects_mismatched_depth_offsets() {
    let temporary = copy_fixture();
    let path = temporary.path().join("depth/part-000000.parquet");

    rewrite_parquet(&path, |batch| {
        use arrow_buffer::OffsetBuffer;
        use arrow_schema::DataType;

        let index = batch.schema().index_of("changes_target").unwrap();

        let list = batch
            .column(index)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        let mut offsets = list.value_offsets().to_vec();

        // Find a nonempty event with at least two levels.
        let row = (0..batch.num_rows())
            .find(|&row| offsets[row + 1] - offsets[row] >= 2)
            .unwrap();

        // Shift one boundary without changing the total number
        // of nested values.
        offsets[row + 1] -= 1;

        let DataType::List(item_field) = batch.schema().field(index).data_type().clone() else {
            panic!("changes_target must be a List");
        };

        let modified_list = ListArray::try_new(
            item_field,
            OffsetBuffer::new(offsets.into()),
            list.values().clone(),
            list.nulls().cloned(),
        )
        .unwrap();

        let mut columns = batch.columns().to_vec();
        columns[index] = Arc::new(modified_list);

        RecordBatch::try_new(batch.schema(), columns).unwrap()
    });

    assert!(validate_converted_depth(&temporary.path().join("depth"), 5_000_000_000,).is_err());
}

fn corrupt_converted_price(batch: RecordBatch) -> RecordBatch {
    use arrow_buffer::i256;
    use arrow_schema::DataType;

    let index = batch.schema().index_of("changes_target").unwrap();

    let list = batch
        .column(index)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    let levels = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let prices = levels
        .column_by_name("price_target")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let mut values = (0..prices.len())
        .map(|i| (!prices.is_null(i)).then(|| prices.value(i)))
        .collect::<Vec<_>>();

    let target_index = values.iter().position(Option::is_some).unwrap();

    let original = values[target_index].unwrap();

    values[target_index] = Some(original + i256::from_i128(1));

    let modified_prices = Decimal256Array::from(values)
        .with_precision_and_scale(76, 28)
        .unwrap();

    let mut level_columns = levels.columns().to_vec();

    let price_index = levels
        .column_names()
        .iter()
        .position(|name| *name == "price_target")
        .unwrap();

    level_columns[price_index] = Arc::new(modified_prices);

    let modified_levels = StructArray::try_new(
        levels.fields().clone(),
        level_columns,
        levels.nulls().cloned(),
    )
    .unwrap();

    let DataType::List(item_field) = batch.schema().field(index).data_type().clone() else {
        panic!("changes_target must be a List");
    };

    let modified_list = ListArray::try_new(
        item_field,
        list.offsets().clone(),
        Arc::new(modified_levels),
        list.nulls().cloned(),
    )
    .unwrap();

    let mut columns = batch.columns().to_vec();
    columns[index] = Arc::new(modified_list);

    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

#[test]
fn rejects_incorrect_converted_price() {
    let temporary = copy_fixture();

    let path = temporary.path().join("depth/part-000000.parquet");

    // Corrupt one converted Decimal256 price.
    rewrite_parquet(&path, corrupt_converted_price);

    // Validation must reject the corrupted dataset.
    assert!(validate_converted_depth(&temporary.path().join("depth"), 5_000_000_000,).is_err());
}

#[test]
fn parallel_validation_matches_sequential() {
    use marketforge_engine::historical::convert::output::validate_converted_depth_parallel;

    let temporary = copy_fixture();
    let directory = temporary.path().join("depth");

    let sequential = validate_converted_depth(&directory, 5_000_000_000).unwrap();

    let parallel = validate_converted_depth_parallel(&directory, 5_000_000_000, 16).unwrap();

    assert_eq!(sequential.events, parallel.events);
    assert_eq!(sequential.identity, parallel.identity);
    assert_eq!(sequential.converted, parallel.converted);
    assert_eq!(sequential.unavailable, parallel.unavailable);

    assert_eq!(sequential.levels, parallel.levels);
    assert_eq!(sequential.levels_converted, parallel.levels_converted);
    assert_eq!(sequential.levels_unavailable, parallel.levels_unavailable);

    assert_eq!(sequential.references_checked, parallel.references_checked);

    assert_eq!(
        sequential.numerical_levels_checked,
        parallel.numerical_levels_checked
    );

    assert_eq!(
        sequential.numerical_references_checked,
        parallel.numerical_references_checked
    );

    assert_eq!(sequential.files, parallel.files);
    assert_eq!(sequential.row_groups, parallel.row_groups);
}

#[test]
fn parallel_validation_matches_sequential_across_files() {
    use marketforge_engine::historical::convert::output::validate_converted_depth_parallel;

    let temporary = copy_fixture();
    let directory = temporary.path().join("depth");

    let source = directory.join("part-000000.parquet");

    let file = std::fs::File::open(&source).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(4096)
        .build()
        .unwrap();

    let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();

    let schema = batches[0].schema();

    std::fs::remove_file(&source).unwrap();

    for (index, batch) in batches.iter().enumerate() {
        let path = directory.join(format!("part-{index:06}.parquet"));

        let file = std::fs::File::create(path).unwrap();

        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    let sequential = validate_converted_depth(&directory, 5_000_000_000).unwrap();

    let parallel = validate_converted_depth_parallel(&directory, 5_000_000_000, 16).unwrap();

    assert_eq!(sequential.events, parallel.events);
    assert_eq!(sequential.identity, parallel.identity);
    assert_eq!(sequential.converted, parallel.converted);
    assert_eq!(sequential.unavailable, parallel.unavailable);

    assert_eq!(sequential.levels, parallel.levels);
    assert_eq!(sequential.levels_converted, parallel.levels_converted,);
    assert_eq!(sequential.levels_unavailable, parallel.levels_unavailable,);

    assert_eq!(sequential.references_checked, parallel.references_checked,);

    assert_eq!(
        sequential.numerical_levels_checked,
        parallel.numerical_levels_checked,
    );

    assert_eq!(
        sequential.numerical_references_checked,
        parallel.numerical_references_checked,
    );

    assert_eq!(sequential.files, parallel.files);
    assert_eq!(sequential.row_groups, parallel.row_groups);

    assert!(parallel.files >= 3);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — MULTI-FILE PARALLEL DEPTH VALIDATION");
    println!("{}", "=".repeat(90));

    println!("Events          : {}", parallel.events);
    println!("Levels          : {}", parallel.levels);
    println!("Files           : {}", parallel.files);
    println!("Row groups      : {}", parallel.row_groups);
    println!("Workers         : 16");
    println!("Numerical levels: {}", parallel.numerical_levels_checked);
    println!("STATUS          : PASS");
}

#[test]
fn parallel_validation_rejects_cross_file_regression() {
    use marketforge_engine::historical::convert::output::validate_converted_depth_parallel;

    let temporary = copy_fixture();
    let directory = temporary.path().join("depth");
    let original = directory.join("part-000000.parquet");

    let file = std::fs::File::open(&original).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(4096)
        .build()
        .unwrap();

    let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();

    assert!(batches.len() >= 2);

    let schema = batches[0].schema();

    std::fs::remove_file(&original).unwrap();

    // Reverse chronological file order while preserving each
    // individual batch's internal ordering.
    for (index, batch) in batches.iter().rev().enumerate() {
        let path = directory.join(format!("part-{index:06}.parquet"));

        let file = std::fs::File::create(path).unwrap();

        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    assert!(validate_converted_depth_parallel(&directory, 5_000_000_000, 16,).is_err());
}

#[test]
fn parallel_validation_rejects_numerical_corruption_in_later_file() {
    use marketforge_engine::historical::convert::output::validate_converted_depth_parallel;

    let temporary = copy_fixture();
    let directory = temporary.path().join("depth");
    let original = directory.join("part-000000.parquet");

    let file = std::fs::File::open(&original).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(4096)
        .build()
        .unwrap();

    let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();

    assert!(batches.len() >= 3);

    let schema = batches[0].schema();

    std::fs::remove_file(&original).unwrap();

    for (index, batch) in batches.into_iter().enumerate() {
        let batch = if index == 1 {
            corrupt_converted_price(batch)
        } else {
            batch
        };

        let path = directory.join(format!("part-{index:06}.parquet"));

        let file = std::fs::File::create(path).unwrap();

        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    // The first file remains valid.
    let first = directory.join("part-000000.parquet");

    validate_depth_file(&first, 5_000_000_000).unwrap();

    // The corrupted second file must fail independently.
    let second = directory.join("part-000001.parquet");

    assert!(validate_depth_file(&second, 5_000_000_000,).is_err());

    // The complete parallel validation must also fail.
    assert!(validate_converted_depth_parallel(&directory, 5_000_000_000, 16,).is_err());
}

#[test]
fn parallel_validation_rejects_equal_timestamp_key_regression() {
    use marketforge_engine::historical::convert::output::validate_converted_depth_parallel;

    let temporary = copy_fixture();
    let directory = temporary.path().join("depth");
    let path = directory.join("part-000000.parquet");

    rewrite_parquet(&path, |batch| {
        use arrow_array::{Int64Array, UInt32Array};

        let timestamps = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let ranks = batch
            .column_by_name("stream_rank")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();

        let mut rows = (0..batch.num_rows()).collect::<Vec<_>>();

        let pair = rows.windows(2).position(|pair| {
            timestamps.value(pair[0]) == timestamps.value(pair[1])
                && ranks.value(pair[0]) < ranks.value(pair[1])
        });

        let index = pair.expect("fixture must contain equal-timestamp events with different ranks");

        rows.swap(index, index + 1);

        let indices = UInt32Array::from(rows.into_iter().map(|row| row as u32).collect::<Vec<_>>());

        let columns = batch
            .columns()
            .iter()
            .map(|column| arrow_select::take::take(column.as_ref(), &indices, None).unwrap())
            .collect::<Vec<_>>();

        RecordBatch::try_new(batch.schema(), columns).unwrap()
    });

    assert!(validate_converted_depth_parallel(&directory, 5_000_000_000, 16,).is_err());
}
