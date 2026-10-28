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
    marketforge_engine::historical::convert::output::SourcePreservationMode,
    marketforge_engine::historical::convert::output::validate_converted_combined,
    marketforge_engine::historical::convert::output::validate_source_preservation_with_mode,
    marketforge_engine::historical::convert::plan_conversion,
    marketforge_engine::historical::convert::resolve_conversion_catalog,
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::job::load_convert_job,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
    marketforge_engine::shared::domain::instrument::InstrumentCatalog,
};

const EVENTS: u64 = 20_000;
const STALENESS_NS: u64 = 5_000_000_000;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}
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
    let root = root();

    let mut job =
        load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let fixture_path = root.join("data/fixtures/combined-worker");

    job.input_merge = fixture_path.clone();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(fixture_path.join("manifest.json")).unwrap()).unwrap();

    source.validate().unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let temporary = tempfile::tempdir().unwrap();

    let output = temporary.path().join("combined");

    let parquet_config = MergeParquetConfig {
        row_group_target_mb: 32,
        file_target_mb: 128,
        compression_level: 3,
    };

    let metrics = materialize_combined_conversion(
        &job,
        &source,
        Some(&external),
        &plan,
        &output,
        &parquet_config,
        None,
    )
    .unwrap();

    assert_eq!(metrics.events_read, EVENTS);
    assert_eq!(metrics.events_written, EVENTS);

    (temporary, output)
}

#[test]
fn validates_real_combined_physical_output() {
    let root = root();

    let mut job =
        load_convert_job(root.join("data/.jobs/convert/convert-combined-test.json")).unwrap();

    let fixture_path = root.join("data/fixtures/combined-worker");
    job.input_merge = fixture_path.clone();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(fixture_path.join("manifest.json")).unwrap()).unwrap();

    source.validate().unwrap();

    let reference_directory = job.conversion.reference_merge.as_ref().unwrap();

    let external: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_directory.join("manifest.json")).unwrap())
            .unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let temporary = tempfile::tempdir().unwrap();
    let output = temporary.path().join("combined");

    let parquet_config = MergeParquetConfig {
        row_group_target_mb: 32,
        file_target_mb: 128,
        compression_level: 3,
    };

    let materialized = materialize_combined_conversion(
        &job,
        &source,
        Some(&external),
        &plan,
        &output,
        &parquet_config,
        None,
    )
    .unwrap();

    assert_eq!(materialized.events_read, EVENTS);
    assert_eq!(materialized.events_written, EVENTS);

    assert_eq!(
        materialized.trade_events,
        materialized.events_read - materialized.depth_events,
    );

    // ---------------------------------------------------------------------
    // Independent physical validation
    // ---------------------------------------------------------------------

    let start = std::time::Instant::now();

    let validated = validate_converted_combined(&output, STALENESS_NS).unwrap();

    let validation_elapsed = start.elapsed().as_secs_f64();

    assert_eq!(validated.events, EVENTS);
    assert_eq!(validated.trade_events, 3_954);
    assert_eq!(validated.depth_events, 16_046);

    assert_eq!(validated.identity, 8_069);
    assert_eq!(validated.converted, 3_926);
    assert_eq!(validated.unavailable, 8_005);

    assert_eq!(validated.depth_levels, 741_562);

    assert!(
        validated.numerical_trade_values <= validated.trade_events,
        "numerical trade validation exceeds trade count"
    );

    assert!(
        validated.numerical_depth_levels > 0,
        "no depth levels underwent numerical validation"
    );

    assert!(
        validated.numerical_depth_levels <= validated.depth_levels,
        "numerical depth validation exceeds physical depth levels"
    );

    // ---------------------------------------------------------------------
    // Exact source preservation
    // ---------------------------------------------------------------------

    let preservation = validate_source_preservation_with_mode(
        &fixture_path,
        &source,
        &output,
        SourcePreservationMode::Full,
        8,
    )
    .unwrap();

    assert_eq!(preservation.events_compared, EVENTS);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — PHYSICAL COMBINED VALIDATION");
    println!("{}", "=".repeat(90));

    println!("Events              : {}", validated.events);
    println!("Trades              : {}", validated.trade_events);
    println!("Depth               : {}", validated.depth_events);
    println!("Depth levels        : {}", validated.depth_levels);

    println!("Identity            : {}", validated.identity);
    println!("Converted           : {}", validated.converted);
    println!("Unavailable         : {}", validated.unavailable);

    println!("Numerical trades    : {}", validated.numerical_trade_values);

    println!("Numerical levels    : {}", validated.numerical_depth_levels);

    println!("References checked  : {}", validated.references_checked);

    println!("Source columns      : {}", preservation.columns_compared);

    println!("Validation elapsed  : {:.3} s", validation_elapsed);

    println!("STATUS              : PASS");
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
