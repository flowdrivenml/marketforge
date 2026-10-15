#[path = "../support/depth_fixture.rs"]
mod depth_fixture;

use depth_fixture::{decimal, source_batch};

use arrow_array::{Array, Decimal256Array, ListArray, RecordBatch, StructArray};

use marketforge_engine::historical::convert::{
    ConvertedDepthLevel,
    output::{
        CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY, CONVERSION_STATUS_UNAVAILABLE,
        ConvertedDepthBatchBuilder, DepthConversionRow, TradeConversionRow,
    },
};

// -----------------------------------------------------------------------------
// Conversion results
// -----------------------------------------------------------------------------

fn metadata(status: u8, failure: Option<u8>) -> TradeConversionRow {
    let success = status != CONVERSION_STATUS_UNAVAILABLE;

    TradeConversionRow {
        target_quote: "USDT".to_owned(),
        status,
        failure,
        rate: success.then(|| decimal(2)),
        price_target: None,
        quantity_quote_target: None,
        path: success.then(|| vec!["BTC".to_owned(), "USDT".to_owned()]),
        references: Vec::new(),
    }
}

fn conversion_rows() -> Vec<DepthConversionRow> {
    vec![
        DepthConversionRow {
            metadata: metadata(CONVERSION_STATUS_CONVERTED, None),
            levels: Some(vec![
                ConvertedDepthLevel {
                    price_target: decimal(200),
                    quantity_quote_target: Some(decimal(2000)),
                },
                ConvertedDepthLevel {
                    price_target: decimal(202),
                    quantity_quote_target: None,
                },
            ]),
        },
        DepthConversionRow {
            metadata: metadata(CONVERSION_STATUS_IDENTITY, None),
            levels: Some(Vec::new()),
        },
        DepthConversionRow {
            metadata: metadata(CONVERSION_STATUS_UNAVAILABLE, Some(2)),
            levels: None,
        },
    ]
}

// -----------------------------------------------------------------------------
// Build converted batch
// -----------------------------------------------------------------------------

fn converted_batch() -> RecordBatch {
    let source = source_batch();

    ConvertedDepthBatchBuilder::new(source.schema())
        .unwrap()
        .build(&source, &conversion_rows())
        .unwrap()
}

// -----------------------------------------------------------------------------
// Preserve source data
// -----------------------------------------------------------------------------

#[test]
fn preserves_all_original_columns() {
    let source = source_batch();
    let converted = converted_batch();

    assert_eq!(source.num_columns(), 17);
    assert_eq!(converted.num_columns(), 24);

    for index in 0..source.num_columns() {
        assert_eq!(
            source.column(index).to_data(),
            converted.column(index).to_data(),
            "original column {} changed",
            source.schema().field(index).name(),
        );
    }
}

// -----------------------------------------------------------------------------
// Nested offsets
// -----------------------------------------------------------------------------

#[test]
fn preserves_depth_list_offsets() {
    let converted = converted_batch();

    let original = converted
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

    assert_eq!(target.value_length(0), 2);
    assert_eq!(target.value_length(1), 0);
    assert_eq!(target.value_length(2), 1);
}

// -----------------------------------------------------------------------------
// Converted level values
// -----------------------------------------------------------------------------

#[test]
fn converts_nested_level_values() {
    let converted = converted_batch();

    let target = converted
        .column_by_name("changes_target")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
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

    let quantities = levels
        .column_by_name("quantity_quote_target")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(prices.value(0), decimal(200).mantissa());
    assert_eq!(prices.value(1), decimal(202).mantissa());

    assert_eq!(quantities.value(0), decimal(2000).mantissa());
    assert!(quantities.is_null(1));
}

// -----------------------------------------------------------------------------
// Unavailable conversion
// -----------------------------------------------------------------------------

#[test]
fn unavailable_event_has_null_converted_levels() {
    let converted = converted_batch();

    let target = converted
        .column_by_name("changes_target")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
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

    let quantities = levels
        .column_by_name("quantity_quote_target")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert!(prices.is_null(2));
    assert!(quantities.is_null(2));
}

// -----------------------------------------------------------------------------
// Zero-level events
// -----------------------------------------------------------------------------

#[test]
fn preserves_zero_level_events() {
    let converted = converted_batch();

    assert_eq!(converted.num_rows(), 3);

    let target = converted
        .column_by_name("changes_target")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert_eq!(target.value_length(1), 0);
    assert!(!target.is_null(1));
}

// -----------------------------------------------------------------------------
// Reject mismatched level counts
// -----------------------------------------------------------------------------

#[test]
fn rejects_incorrect_level_count() {
    let source = source_batch();

    let mut rows = conversion_rows();

    rows[0].levels = Some(vec![ConvertedDepthLevel {
        price_target: decimal(200),
        quantity_quote_target: None,
    }]);

    let result = ConvertedDepthBatchBuilder::new(source.schema())
        .unwrap()
        .build(&source, &rows);

    assert!(result.is_err());
}

// -----------------------------------------------------------------------------
// Reject unavailable levels
// -----------------------------------------------------------------------------

#[test]
fn rejects_unavailable_event_with_converted_levels() {
    let source = source_batch();

    let mut rows = conversion_rows();

    rows[2].levels = Some(vec![ConvertedDepthLevel {
        price_target: decimal(400),
        quantity_quote_target: None,
    }]);

    let result = ConvertedDepthBatchBuilder::new(source.schema())
        .unwrap()
        .build(&source, &rows);

    assert!(result.is_err());
}

// -----------------------------------------------------------------------------
// Schema
// -----------------------------------------------------------------------------

#[test]
fn preserves_output_schema() {
    let converted = converted_batch();

    assert_eq!(converted.num_columns(), 24);

    assert_eq!(
        converted
            .schema()
            .field_with_name("changes_target")
            .unwrap()
            .is_nullable(),
        false,
    );
}
