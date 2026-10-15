use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::DataType;

use marketforge_engine::historical::normalize::{
    merge::{
        PhysicalTradeBatchBuilder, ResolvedMarketEvent, UnifiedTimelineRecord,
        physical_trade_schema,
    },
    parquet::trade_schema,
};

fn reference(ordinal: u64, timestamp: i64) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        event_timestamp_ns: timestamp,
        stream_rank: 0,
        event_kind: 0,
        dataset_id: 88,
        task_id: 1,
        event_ordinal: ordinal,
        canonical_row_offset: ordinal,
        canonical_row_count: 1,
        event_boundary: None,
        source_operation: None,
    }
}

fn canonical_batch(timestamp: i64) -> RecordBatch {
    let schema = trade_schema();

    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "event_timestamp_ns" => Arc::new(Int64Array::from(vec![timestamp])) as ArrayRef,

            "instrument_id" => Arc::new(Int64Array::from(vec![688])) as ArrayRef,

            "exchange" => Arc::new(arrow_array::StringArray::from(vec!["bybit"])) as ArrayRef,

            "symbol" => Arc::new(arrow_array::StringArray::from(vec!["BTCUSDT"])) as ArrayRef,

            "stream_id" => {
                Arc::new(arrow_array::StringArray::from(vec!["bybit:btc:trade"])) as ArrayRef
            }

            "side" => Arc::new(arrow_array::StringArray::from(vec!["buy"])) as ArrayRef,

            "price" => {
                use arrow_array::Decimal256Array;
                use arrow_buffer::i256;

                Arc::new(
                    Decimal256Array::from(vec![i256::from_i128(78500_i128 * 10_i128.pow(28))])
                        .with_precision_and_scale(76, 28)
                        .unwrap(),
                ) as ArrayRef
            }

            _ => arrow_array::new_null_array(field.data_type(), 1),
        })
        .collect();

    RecordBatch::try_new(schema, columns).unwrap()
}

fn event(ordinal: u64, timestamp: i64) -> ResolvedMarketEvent {
    ResolvedMarketEvent {
        reference: reference(ordinal, timestamp),
        canonical_batches: vec![canonical_batch(timestamp)],
    }
}

#[test]
fn builds_physical_trade_batch() {
    let events = vec![event(0, 100), event(1, 200), event(2, 300)];

    let batch = PhysicalTradeBatchBuilder::new()
        .build(&events)
        .unwrap()
        .unwrap();

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 24);
    assert_eq!(batch.schema(), physical_trade_schema());
}

#[test]
fn preserves_chronological_order() {
    let events = vec![event(0, 100), event(1, 200), event(2, 300)];

    let batch = PhysicalTradeBatchBuilder::new()
        .build(&events)
        .unwrap()
        .unwrap();

    let timestamps = batch
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(timestamps.values(), &[100, 200, 300]);
}

#[test]
fn preserves_provenance() {
    let events = vec![event(42, 100)];

    let batch = PhysicalTradeBatchBuilder::new()
        .build(&events)
        .unwrap()
        .unwrap();

    let ordinals = batch
        .column_by_name("event_ordinal")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    assert_eq!(ordinals.value(0), 42);
}

#[test]
fn preserves_decimal256() {
    let events = vec![event(0, 100)];

    let batch = PhysicalTradeBatchBuilder::new()
        .build(&events)
        .unwrap()
        .unwrap();

    assert_eq!(
        batch.schema().field_with_name("price").unwrap().data_type(),
        &DataType::Decimal256(76, 28),
    );

    let price = batch
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Decimal256Array>()
        .unwrap();

    assert_eq!(
        price.value(0),
        arrow_buffer::i256::from_i128(78500_i128 * 10_i128.pow(28)),
    );
}

#[test]
fn rejects_depth_events() {
    let mut invalid = event(0, 100);
    invalid.reference.event_kind = 1;

    assert!(PhysicalTradeBatchBuilder::new().build(&[invalid]).is_err());
}

#[test]
fn empty_input_produces_no_batch() {
    assert!(
        PhysicalTradeBatchBuilder::new()
            .build(&[])
            .unwrap()
            .is_none()
    );
}
