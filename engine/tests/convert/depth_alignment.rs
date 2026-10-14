use arrow_array::{Array, Decimal256Array, ListArray, RecordBatch, StructArray, UInt8Array};
use marketforge_engine::process::convert::ConversionDecimal;
use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::load_convert_job,
    process::{
        convert::{
            execution::{DepthConversionAlignment, DepthConversionKernel},
            output::{
                CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY,
                CONVERSION_STATUS_UNAVAILABLE,
            },
            plan_conversion,
            reference::ExternalReferenceCursor,
            resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};
use num_bigint::BigInt;
use std::{fs, path::PathBuf, time::Instant};

const WINDOW_NS: i64 = 10_000_000_000;
const EXPECTED_EVENTS: u64 = 12_194;
const BATCH_SIZE: usize = 4_096;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn validate_exact_conversion(source: &RecordBatch, converted: &RecordBatch) -> u64 {
    let timestamps = source
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();

    let original = source
        .column_by_name("changes")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    let target = converted
        .column_by_name("changes_target")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    let statuses = converted
        .column_by_name("conversion_status")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();

    let rates = converted
        .column_by_name("conversion_rate")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let original_levels = original
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let target_levels = target
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let original_prices = original_levels
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let original_quantities = original_levels
        .column_by_name("quantity_quote")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let target_prices = target_levels
        .column_by_name("price_target")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let target_quantities = target_levels
        .column_by_name("quantity_quote_target")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let references = converted
        .column_by_name("conversion_references")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    let reference_values = references
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let reference_timestamps = reference_values
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();

    let reference_prices = reference_values
        .column_by_name("reference_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let directions = reference_values
        .column_by_name("direction")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();

    let scale = BigInt::from(10_u8).pow(28);

    let mut checked = 0_u64;

    for row in 0..source.num_rows() {
        let status = statuses.value(row);

        let start = original.value_offsets()[row] as usize;
        let end = original.value_offsets()[row + 1] as usize;

        if status == CONVERSION_STATUS_UNAVAILABLE {
            assert!(rates.is_null(row));

            for index in start..end {
                assert!(target_prices.is_null(index));
                assert!(target_quantities.is_null(index));
            }

            continue;
        }

        assert!(!rates.is_null(row));

        let mut numerator = BigInt::from(1);
        let mut denominator = BigInt::from(1);

        let reference_start = references.value_offsets()[row] as usize;

        let reference_end = references.value_offsets()[row + 1] as usize;

        if status == CONVERSION_STATUS_IDENTITY {
            assert_eq!(reference_start, reference_end);
        } else {
            assert_eq!(status, CONVERSION_STATUS_CONVERTED);
            assert!(reference_end > reference_start);
        }

        for index in reference_start..reference_end {
            let timestamp = reference_timestamps.value(index);

            // External references must strictly precede the depth event.
            assert!(
                timestamp < timestamps.value(row),
                "future reference at depth event {row}",
            );

            let price = ConversionDecimal::new(reference_prices.value(index)).unwrap();

            let mantissa = BigInt::from_signed_bytes_be(&price.mantissa().to_be_bytes());

            match directions.value(index) {
                0 => {
                    numerator *= mantissa;
                    denominator *= &scale;
                }

                1 => {
                    numerator *= &scale;
                    denominator *= mantissa;
                }

                direction => panic!("invalid conversion direction: {direction}"),
            }
        }

        let expected_rate =
            ConversionDecimal::from_ratio(numerator.clone(), denominator.clone()).unwrap();

        assert_eq!(
            rates.value(row),
            expected_rate.mantissa(),
            "conversion rate mismatch at event {row}",
        );

        for index in start..end {
            let price = ConversionDecimal::new(original_prices.value(index)).unwrap();

            let expected_price = price.checked_mul_ratio(&numerator, &denominator).unwrap();

            assert_eq!(
                target_prices.value(index),
                expected_price.mantissa(),
                "converted depth price mismatch at event {row}",
            );

            if original_quantities.is_null(index) {
                assert!(target_quantities.is_null(index));
            } else {
                let quantity = ConversionDecimal::new(original_quantities.value(index)).unwrap();

                let expected_quantity = quantity
                    .checked_mul_ratio(&numerator, &denominator)
                    .unwrap();

                assert_eq!(
                    target_quantities.value(index),
                    expected_quantity.mantissa(),
                    "converted depth quantity mismatch at event {row}",
                );
            }

            checked += 1;
        }
    }

    checked
}

#[test]
fn converts_real_depth_with_external_references() {
    let mut numerical_levels_checked = 0_u64;

    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-depth-test.json")).unwrap();

    let source: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let reference_path = job.conversion.reference_merge.as_ref().unwrap();

    let reference_manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(reference_path.join("manifest.json")).unwrap()).unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &source, &catalog, &root).unwrap();

    let reference_ids = resolved
        .references
        .iter()
        .map(|reference| reference.stream_id.clone())
        .collect::<Vec<_>>();

    let plan = plan_conversion(&job, &resolved).unwrap();

    assert_eq!(plan.source_streams, 15);
    assert_eq!(plan.identity_streams, 8);
    assert_eq!(plan.convertible_streams, 5);
    assert_eq!(plan.unresolved_streams, 2);

    // -------------------------------------------------------------------------
    // Construct external reference alignment
    // -------------------------------------------------------------------------

    let cursor =
        ExternalReferenceCursor::new(reference_path, &reference_manifest, reference_ids).unwrap();

    let kernel = DepthConversionKernel::new(&job, &plan).unwrap();

    let mut alignment = DepthConversionAlignment::new(kernel, cursor);

    // -------------------------------------------------------------------------
    // Read real physical depth
    // -------------------------------------------------------------------------

    let mut reader =
        marketforge_engine::process::convert::execution::PhysicalConversionReader::new(
            &job.input_merge,
            &source,
            BATCH_SIZE,
        )
        .unwrap();

    let mut start_timestamp = None;
    let mut processed = 0_u64;
    let mut original_columns_checked = 0_u64;
    let mut converted_levels_checked = 0_u64;

    let start = Instant::now();

    while let Some(batch) = reader.next_batch().unwrap() {
        let timestamps = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();

        let first = *start_timestamp.get_or_insert(timestamps.value(0));

        let end = first + WINDOW_NS;

        let count = timestamps
            .values()
            .iter()
            .take_while(|timestamp| **timestamp < end)
            .count();

        if count == 0 {
            break;
        }

        let bounded = batch.slice(0, count);

        let converted = alignment.convert_batch(&bounded).unwrap();

        numerical_levels_checked += validate_exact_conversion(&bounded, &converted);

        // ---------------------------------------------------------------------
        // Preserve original physical depth columns
        // ---------------------------------------------------------------------

        assert_eq!(bounded.num_columns(), 17);
        assert_eq!(converted.num_columns(), 24);

        for column in 0..17 {
            assert_eq!(
                bounded.column(column).to_data(),
                converted.column(column).to_data(),
                "source column changed: {}",
                bounded.schema().field(column).name(),
            );

            original_columns_checked += 1;
        }

        // ---------------------------------------------------------------------
        // Preserve nested list offsets
        // ---------------------------------------------------------------------

        let original = bounded
            .column_by_name("changes")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        let target = converted
            .column_by_name("changes_target")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert_eq!(original.value_offsets(), target.value_offsets(),);

        // ---------------------------------------------------------------------
        // Validate converted depth levels
        // ---------------------------------------------------------------------

        let statuses = converted
            .column_by_name("conversion_status")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        let levels = target
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

        for row in 0..converted.num_rows() {
            let offset = target.value_offsets()[row] as usize;
            let length = target.value_length(row) as usize;

            match statuses.value(row) {
                CONVERSION_STATUS_IDENTITY | CONVERSION_STATUS_CONVERTED => {
                    for index in offset..offset + length {
                        assert!(!prices.is_null(index));
                        converted_levels_checked += 1;
                    }
                }

                CONVERSION_STATUS_UNAVAILABLE => {
                    for index in offset..offset + length {
                        assert!(prices.is_null(index));
                    }
                }

                status => panic!("invalid conversion status: {status}"),
            }
        }

        processed += count as u64;

        if count < batch.num_rows() {
            break;
        }
    }

    // -------------------------------------------------------------------------
    // Verify complete window
    // -------------------------------------------------------------------------

    assert_eq!(processed, EXPECTED_EVENTS);

    let metrics = alignment.metrics();

    assert_eq!(metrics.events_read, EXPECTED_EVENTS);

    assert_eq!(
        metrics.identity_events + metrics.converted_events + metrics.unavailable_events,
        EXPECTED_EVENTS,
    );

    assert_eq!(
        metrics.levels_converted + metrics.levels_unavailable,
        metrics.levels_read,
    );

    // -------------------------------------------------------------------------
    // Diagnostics
    // -------------------------------------------------------------------------

    let elapsed = start.elapsed().as_secs_f64();

    assert_eq!(numerical_levels_checked, converted_levels_checked,);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — REAL DEPTH CONVERSION ALIGNMENT");
    println!("{}", "=".repeat(90));

    println!("Depth events       : {}", metrics.events_read);
    println!("Identity events    : {}", metrics.identity_events);
    println!("Converted events   : {}", metrics.converted_events);
    println!("Unavailable events : {}", metrics.unavailable_events);

    println!();
    println!("Depth levels       : {}", metrics.levels_read);
    println!("Converted levels   : {}", metrics.levels_converted);
    println!("Unavailable levels : {}", metrics.levels_unavailable);

    println!();
    println!("Reference updates  : {}", alignment.reference_updates());
    println!("Observations read  : {}", alignment.observations_read());

    println!();
    println!("Columns checked    : {}", original_columns_checked);
    println!("Levels checked     : {}", converted_levels_checked);

    println!("Elapsed            : {:.3} s", elapsed);

    if elapsed > 0.0 {
        println!(
            "Throughput         : {:.0} events/s",
            processed as f64 / elapsed,
        );

        println!(
            "Level throughput   : {:.0} levels/s",
            metrics.levels_read as f64 / elapsed,
        );
    }
    println!("Numerical levels checked: {}", numerical_levels_checked,);
    println!("STATUS             : PASS");
}
