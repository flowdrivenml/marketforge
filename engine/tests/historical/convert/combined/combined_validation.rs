use arrow_array::{Array, Decimal256Array, ListArray, RecordBatch, RecordBatchReader, StructArray};
use arrow_buffer::i256;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use std::sync::Arc;
use std::{fs, path::PathBuf};
use {
    marketforge_engine::historical::convert::execution::materialize_combined_conversion,
    marketforge_engine::historical::convert::output::validate_converted_combined,
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
};

const STALENESS_NS: u64 = 5_000_000_000;

fn rewrite_parquet(path: &std::path::Path, mut transform: impl FnMut(RecordBatch) -> RecordBatch) {
    let file = fs::File::open(path).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(4096)
        .build()
        .unwrap();

    let schema = reader.schema().clone();

    let batches = reader
        .map(|batch| transform(batch.unwrap()))
        .collect::<Vec<_>>();

    let temporary = path.with_extension("rewritten");

    let mut writer = ArrowWriter::try_new(
        fs::File::create(&temporary).unwrap(),
        schema,
        Some(WriterProperties::builder().build()),
    )
    .unwrap();

    for batch in batches {
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();

    fs::rename(temporary, path).unwrap();
}

fn replace_column(batch: RecordBatch, name: &str, replacement: Arc<dyn Array>) -> RecordBatch {
    let index = batch.schema().index_of(name).unwrap();

    let mut columns = batch.columns().to_vec();

    columns[index] = replacement;

    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

fn corrupt_decimal(array: &Decimal256Array) -> Decimal256Array {
    let mut values = (0..array.len())
        .map(|index| (!array.is_null(index)).then(|| array.value(index)))
        .collect::<Vec<_>>();

    let index = values
        .iter()
        .position(Option::is_some)
        .expect("no non-null decimal available for corruption");

    values[index] = Some(values[index].unwrap() + i256::from_i128(1));

    Decimal256Array::from(values)
        .with_precision_and_scale(76, 28)
        .unwrap()
}
fn corrupt_trade_price(batch: RecordBatch) -> RecordBatch {
    let targets = batch
        .column_by_name("trade_target")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let prices = targets
        .column_by_name("price_target")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let modified = corrupt_decimal(prices);

    let mut columns = targets.columns().to_vec();

    let index = targets
        .column_names()
        .iter()
        .position(|name| *name == "price_target")
        .unwrap();

    columns[index] = Arc::new(modified);

    let replacement =
        StructArray::try_new(targets.fields().clone(), columns, targets.nulls().cloned()).unwrap();

    replace_column(batch, "trade_target", Arc::new(replacement))
}
fn corrupt_depth_price(batch: RecordBatch) -> RecordBatch {
    let targets = batch
        .column_by_name("depth_target")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    let levels = targets
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

    let modified = corrupt_decimal(prices);

    let mut level_columns = levels.columns().to_vec();

    let price_index = levels
        .column_names()
        .iter()
        .position(|name| *name == "price_target")
        .unwrap();

    level_columns[price_index] = Arc::new(modified);

    let modified_levels = StructArray::try_new(
        levels.fields().clone(),
        level_columns,
        levels.nulls().cloned(),
    )
    .unwrap();

    let arrow_schema::DataType::List(item_field) = targets.data_type().clone() else {
        panic!("depth_target must be a list");
    };

    let modified_list = ListArray::try_new(
        item_field,
        targets.offsets().clone(),
        Arc::new(modified_levels),
        targets.nulls().cloned(),
    )
    .unwrap();

    replace_column(batch, "depth_target", Arc::new(modified_list))
}
fn corrupt_conversion_rate(batch: RecordBatch) -> RecordBatch {
    let modified = {
        let rates = batch
            .column_by_name("conversion_rate")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();

        corrupt_decimal(rates)
    };

    replace_column(batch, "conversion_rate", Arc::new(modified))
}
fn converted_fixture() -> (tempfile::TempDir, PathBuf) {
    use marketforge_engine::historical::merge::UnifiedMergeMode;

    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let mut job = crate::conversion_fixture::canonical_job();
    job.input_merge = root.join("source");
    job.output.dataset_path = root.join("output");
    job.conversion.max_staleness_ns = STALENESS_NS;
    let manifest = crate::market::create(&job.input_merge, UnifiedMergeMode::Combined, true);
    let catalog = crate::market::catalog(root);
    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, root).unwrap();
    let plan = plan_conversion(&job, &resolved).unwrap();
    let output = root.join("staging");
    materialize_combined_conversion(
        &job,
        &manifest,
        None,
        &plan,
        &output,
        &crate::market::parquet(),
        None,
    )
    .unwrap();
    (temporary, output)
}

#[test]
fn rejects_incorrect_combined_trade_price() {
    let (_temporary, output) = converted_fixture();

    let files = marketforge_engine::historical::convert::output::discover_conversion_files(&output)
        .unwrap();

    rewrite_parquet(&files[0], corrupt_trade_price);

    assert!(validate_converted_combined(&output, STALENESS_NS).is_err());
}

#[test]
fn rejects_incorrect_combined_depth_price() {
    let (_temporary, output) = converted_fixture();

    let files = marketforge_engine::historical::convert::output::discover_conversion_files(&output)
        .unwrap();

    rewrite_parquet(&files[0], corrupt_depth_price);

    assert!(validate_converted_combined(&output, STALENESS_NS).is_err());
}

#[test]
fn rejects_incorrect_combined_conversion_rate() {
    let (_temporary, output) = converted_fixture();

    let files = marketforge_engine::historical::convert::output::discover_conversion_files(&output)
        .unwrap();

    rewrite_parquet(&files[0], corrupt_conversion_rate);

    assert!(validate_converted_combined(&output, STALENESS_NS).is_err());
}

#[test]
fn parallel_combined_validation_matches_sequential() {
    use marketforge_engine::historical::convert::output::validate_converted_combined_parallel;

    let (_temporary, output) = converted_fixture();

    let sequential = validate_converted_combined(&output, STALENESS_NS).unwrap();

    let parallel = validate_converted_combined_parallel(&output, STALENESS_NS, 4).unwrap();

    assert_eq!(parallel.events, sequential.events);
    assert_eq!(parallel.trade_events, sequential.trade_events);
    assert_eq!(parallel.depth_events, sequential.depth_events);

    assert_eq!(parallel.identity, sequential.identity);
    assert_eq!(parallel.converted, sequential.converted);
    assert_eq!(parallel.unavailable, sequential.unavailable);

    assert_eq!(parallel.depth_levels, sequential.depth_levels);

    assert_eq!(
        parallel.numerical_trade_values,
        sequential.numerical_trade_values,
    );

    assert_eq!(
        parallel.numerical_depth_levels,
        sequential.numerical_depth_levels,
    );

    assert_eq!(parallel.references_checked, sequential.references_checked,);

    assert_eq!(parallel.files, sequential.files);
    assert_eq!(parallel.row_groups, sequential.row_groups);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — PARALLEL COMBINED VALIDATION");
    println!("{}", "=".repeat(90));

    println!("Events          : {}", parallel.events);
    println!("Trades          : {}", parallel.trade_events);
    println!("Depth           : {}", parallel.depth_events);
    println!("Depth levels    : {}", parallel.depth_levels);
    println!("Numerical trades: {}", parallel.numerical_trade_values);
    println!("Numerical levels: {}", parallel.numerical_depth_levels);
    println!("References      : {}", parallel.references_checked);
    println!("Files           : {}", parallel.files);
    println!("STATUS          : PASS");
}
