#![cfg(feature = "process")]

use arrow_array::{Array, Decimal256Array, Int64Array, StringArray, UInt64Array};

use marketforge_engine::{
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate},
    process::parquet::{DepthBatchBuilder, depth_schema, i256_to_decimal},
};

use rust_decimal::Decimal;

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

fn level(timestamp: i64, side: BookSide, price: i64, quantity: i64) -> L2LevelUpdate {
    let price = Decimal::new(price, 0);
    let quantity = Decimal::new(quantity, 0);

    L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: timestamp,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
            instrument_id: 1,
            symbol: "BTCUSDT".to_owned(),
            stream_id: "bybit:BTCUSDT:depth".to_owned(),
        },

        side,
        price,

        quantity_base: Some(quantity),
        quantity_quote: Some(price * quantity),
        quantity_contracts: None,

        order_count: None,
    }
}

// -----------------------------------------------------------------------------
// Schema
// -----------------------------------------------------------------------------

#[test]
fn depth_schema_has_twelve_columns() {
    let schema = depth_schema();

    assert_eq!(schema.fields().len(), 12);

    let expected = [
        "event_timestamp_ns",
        "system_timestamp_ns",
        "exchange",
        "instrument_id",
        "symbol",
        "stream_id",
        "side",
        "price",
        "quantity_base",
        "quantity_quote",
        "quantity_contracts",
        "order_count",
    ];

    for (field, name) in schema.fields().iter().zip(expected) {
        assert_eq!(field.name(), name);
    }
}

// -----------------------------------------------------------------------------
// Decimal round trip
// -----------------------------------------------------------------------------

#[test]
fn preserves_exact_decimal_values() {
    let mut builder = DepthBatchBuilder::new();

    let mut record = level(1000, BookSide::Bid, 85000, 2);

    record.price = Decimal::new(8500012345678, 8);
    record.quantity_base = Some(Decimal::new(12345678, 8));
    record.quantity_quote = Some(Decimal::new(10493842, 6));

    builder.push(record);

    let batch = builder.finish().unwrap().unwrap();

    let price = batch
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let quantity = batch
        .column_by_name("quantity_base")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(
        i256_to_decimal(price.value(0)).unwrap(),
        Decimal::new(8500012345678, 8)
    );

    assert_eq!(
        i256_to_decimal(quantity.value(0)).unwrap(),
        Decimal::new(12345678, 8)
    );
}

// -----------------------------------------------------------------------------
// Nullable quantities
// -----------------------------------------------------------------------------

#[test]
fn preserves_nullable_quantities() {
    let mut builder = DepthBatchBuilder::new();

    let mut record = level(1000, BookSide::Bid, 85000, 2);

    record.quantity_contracts = None;
    record.order_count = None;

    builder.push(record);

    let batch = builder.finish().unwrap().unwrap();

    let contracts = batch
        .column_by_name("quantity_contracts")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert!(contracts.is_null(0));

    let order_count = batch
        .column_by_name("order_count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    assert!(order_count.is_null(0));
}

// -----------------------------------------------------------------------------
// Event ordering
// -----------------------------------------------------------------------------

#[test]
fn preserves_source_event_order() {
    let mut builder = DepthBatchBuilder::new();

    builder.push(level(1000, BookSide::Bid, 100, 10));
    builder.push(level(1000, BookSide::Ask, 101, 20));
    builder.push(level(1001, BookSide::Bid, 99, 5));

    let batch = builder.finish().unwrap().unwrap();

    let timestamps = batch
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(timestamps.values(), &[1000, 1000, 1001]);

    let sides = batch
        .column_by_name("side")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(sides.value(0), "bid");
    assert_eq!(sides.value(1), "ask");
    assert_eq!(sides.value(2), "bid");
}

// -----------------------------------------------------------------------------
// Empty batches
// -----------------------------------------------------------------------------

#[test]
fn empty_builder_returns_none() {
    let mut builder = DepthBatchBuilder::new();

    assert!(builder.finish().unwrap().is_none());
    assert!(builder.is_empty());
}

// -----------------------------------------------------------------------------
// Buffer clearing
// -----------------------------------------------------------------------------

#[test]
fn finish_clears_buffer() {
    let mut builder = DepthBatchBuilder::new();

    builder.push(level(1000, BookSide::Bid, 100, 10));

    assert_eq!(builder.len(), 1);

    let batch = builder.finish().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert!(builder.is_empty());
}
