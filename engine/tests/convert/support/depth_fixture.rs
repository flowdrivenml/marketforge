use std::sync::Arc;

use arrow_array::{
    ArrayRef, Decimal256Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    UInt8Array, UInt32Array, UInt64Array,
};

use arrow_buffer::{OffsetBuffer, i256};
use arrow_schema::DataType;

use marketforge_engine::historical::normalize::{
    convert::ConversionDecimal, merge::physical_depth_schema,
};

pub fn decimal(value: i128) -> ConversionDecimal {
    ConversionDecimal::new(i256::from_i128(value * 10_i128.pow(28))).unwrap()
}

pub fn decimal_array(values: &[Option<i128>]) -> ArrayRef {
    let values = values
        .iter()
        .map(|value| value.map(|value| decimal(value).mantissa()))
        .collect::<Vec<_>>();

    Arc::new(
        Decimal256Array::from(values)
            .with_precision_and_scale(76, 28)
            .unwrap(),
    )
}

// -----------------------------------------------------------------------------
// Physical depth fixture
// -----------------------------------------------------------------------------

pub fn source_batch() -> RecordBatch {
    let schema = physical_depth_schema();

    // Three events:
    //
    // Event 0: two levels
    // Event 1: zero levels
    // Event 2: one level

    let offsets = OffsetBuffer::new(vec![0_i32, 2, 2, 3].into());

    let changes_field = schema.field_with_name("changes").unwrap();

    let DataType::List(item_field) = changes_field.data_type() else {
        panic!("changes must be a list");
    };

    let DataType::Struct(level_fields) = item_field.data_type() else {
        panic!("changes must contain structs");
    };

    let levels = StructArray::try_new(
        level_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["bid", "ask", "bid"])),
            decimal_array(&[Some(100), Some(101), Some(200)]),
            decimal_array(&[Some(10), Some(20), Some(30)]),
            decimal_array(&[Some(1000), None, Some(6000)]),
            decimal_array(&[None, None, None]),
            Arc::new(UInt64Array::from(vec![Some(1), Some(2), None])),
        ],
        None,
    )
    .unwrap();

    let changes = ListArray::try_new(item_field.clone(), offsets, Arc::new(levels), None).unwrap();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![100, 101, 102])),
        Arc::new(Int64Array::from(vec![Some(100), None, Some(102)])),
        Arc::new(StringArray::from(vec!["binance", "binance", "binance"])),
        Arc::new(Int64Array::from(vec![1, 1, 1])),
        Arc::new(StringArray::from(vec!["ETHBTC", "ETHBTC", "ETHBTC"])),
        Arc::new(StringArray::from(vec![
            "binance:spot:spot:ETHBTC:depth",
            "binance:spot:spot:ETHBTC:depth",
            "binance:spot:spot:ETHBTC:depth",
        ])),
        Arc::new(UInt32Array::from(vec![0, 0, 0])),
        Arc::new(UInt64Array::from(vec![129, 129, 129])),
        Arc::new(UInt64Array::from(vec![1, 1, 1])),
        Arc::new(UInt64Array::from(vec![0, 1, 2])),
        Arc::new(UInt64Array::from(vec![0, 2, 2])),
        Arc::new(UInt64Array::from(vec![2, 0, 1])),
        Arc::new(UInt8Array::from(vec![0, 1, 0])),
        Arc::new(UInt8Array::from(vec![0, 1, 0])),
        Arc::new(UInt64Array::from(vec![Some(1), Some(2), Some(3)])),
        Arc::new(UInt64Array::from(vec![Some(1), Some(2), Some(3)])),
        Arc::new(changes),
    ];

    RecordBatch::try_new(schema, columns).unwrap()
}
