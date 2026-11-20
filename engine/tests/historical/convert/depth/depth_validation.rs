use arrow_array::{Array, Decimal256Array, ListArray, RecordBatch, StructArray, UInt8Array};
use arrow_schema::DataType;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use std::{fs::File, path::Path, sync::Arc};

use marketforge_engine::historical::{
    convert::{
        execution::materialize_depth_conversion, output::validate_converted_depth, plan_conversion,
        resolve_conversion_catalog,
    },
    merge::UnifiedMergeMode,
};

fn converted_fixture() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = crate::conversion_fixture::canonical_job();
    job.input_merge = root.join("source");
    job.output.dataset_path = root.join("output");
    let reference = root.join("references");
    job.conversion.reference_merge = Some(reference.clone());
    let source = crate::market::create(&job.input_merge, UnifiedMergeMode::Depth, false);
    let external = crate::market::create(&reference, UnifiedMergeMode::Trades, false);
    let catalog = crate::market::catalog(root);
    let resolved = resolve_conversion_catalog(&job, &source, &catalog, root).unwrap();
    let plan = plan_conversion(&job, &resolved).unwrap();
    materialize_depth_conversion(
        &job,
        &source,
        &external,
        &plan,
        &root.join("depth"),
        &crate::market::parquet(),
        None,
    )
    .unwrap();
    directory
}

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
fn validates_complete_physical_depth_output() {
    let directory = converted_fixture();
    let result = validate_converted_depth(&directory.path().join("depth"), 5_000_000_000).unwrap();
    assert_eq!(
        (
            result.events,
            result.identity,
            result.converted,
            result.unavailable
        ),
        (6, 3, 3, 0)
    );
    assert_eq!(
        (
            result.levels,
            result.levels_converted,
            result.levels_unavailable
        ),
        (5, 5, 0)
    );
    assert_eq!(result.references_checked, 4);
}

#[test]
fn rejects_invalid_conversion_status() {
    let temporary = converted_fixture();

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
    let temporary = converted_fixture();
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
    let temporary = converted_fixture();
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
    let temporary = converted_fixture();
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

        // Move the initial level into the following event without changing the total.
        assert_eq!(offsets[1], 1);
        offsets[1] = 0;

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
    let temporary = converted_fixture();

    let path = temporary.path().join("depth/part-000000.parquet");

    // Corrupt one converted Decimal256 price.
    rewrite_parquet(&path, corrupt_converted_price);

    // Validation must reject the corrupted dataset.
    assert!(validate_converted_depth(&temporary.path().join("depth"), 5_000_000_000,).is_err());
}

#[test]
fn parallel_validation_matches_sequential() {
    use marketforge_engine::historical::convert::output::validate_converted_depth_parallel;

    let temporary = converted_fixture();
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
