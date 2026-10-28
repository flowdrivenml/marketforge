use std::{fs::File, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Decimal256Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    UInt8Array, UInt64Array,
};
use arrow_buffer::i256;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

use {
    marketforge_engine::historical::merge::PhysicalDepthBatchBuilder,
    marketforge_engine::historical::merge::PhysicalDepthEvent,
    marketforge_engine::historical::merge::ResolvedDepthMetadata,
    marketforge_engine::historical::merge::ResolvedMarketEvent,
    marketforge_engine::historical::merge::UnifiedMergeStream,
    marketforge_engine::historical::merge::UnifiedMergeStreamKind,
    marketforge_engine::historical::merge::UnifiedTimelineRecord,
    marketforge_engine::historical::merge::physical_depth_schema,
    marketforge_engine::shared::persistence::parquet::depth_schema,
};

fn stream() -> UnifiedMergeStream {
    UnifiedMergeStream {
        stream_rank: 0,
        dataset_id: 89,
        kind: UnifiedMergeStreamKind::Depth,
        exchange: "bybit".into(),
        instrument_id: 688,
        symbol: "BTCUSDT".into(),
        stream_id: "bybit:perpetual:linear:BTCUSDT:l2".into(),
        base_currency: None,
        quote_currency: None,
    }
}

fn decimal(values: &[i64]) -> ArrayRef {
    let scale = 10_i128.pow(28);

    Arc::new(
        Decimal256Array::from(
            values
                .iter()
                .map(|value| i256::from_i128(*value as i128 * scale))
                .collect::<Vec<_>>(),
        )
        .with_precision_and_scale(76, 28)
        .unwrap(),
    )
}

fn canonical_depth(timestamp: i64, prices: &[i64]) -> RecordBatch {
    let n = prices.len();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![timestamp; n])),
        Arc::new(Int64Array::from(vec![Some(timestamp + 5); n])),
        Arc::new(StringArray::from(vec!["bybit"; n])),
        Arc::new(Int64Array::from(vec![688; n])),
        Arc::new(StringArray::from(vec!["BTCUSDT"; n])),
        Arc::new(StringArray::from(vec![
            "bybit:perpetual:linear:BTCUSDT:l2";
            n
        ])),
        Arc::new(StringArray::from(vec!["bid"; n])),
        decimal(prices),
        decimal(&vec![1; n]),
        decimal(prices),
        Arc::new(
            Decimal256Array::from(vec![None; n])
                .with_precision_and_scale(76, 28)
                .unwrap(),
        ),
        Arc::new(UInt64Array::from(vec![None; n])),
    ];

    RecordBatch::try_new(depth_schema(), columns).unwrap()
}

fn resolved(
    ordinal: u64,
    timestamp: i64,
    batches: Vec<RecordBatch>,
    count: u64,
) -> ResolvedMarketEvent {
    ResolvedMarketEvent {
        reference: UnifiedTimelineRecord {
            event_timestamp_ns: timestamp,
            stream_rank: 0,
            event_kind: 1,
            dataset_id: 89,
            task_id: 0,
            event_ordinal: ordinal,
            canonical_row_offset: 0,
            canonical_row_count: count,
            event_boundary: Some(0),
            source_operation: Some(1),
        },
        canonical_batches: batches,
    }
}

fn metadata() -> ResolvedDepthMetadata {
    ResolvedDepthMetadata {
        system_timestamp_ns: Some(105),
        sequence_start: Some(10),
        sequence_end: Some(12),
    }
}

fn build<'a>(events: &'a [ResolvedMarketEvent], stream: &'a UnifiedMergeStream) -> RecordBatch {
    let inputs = events
        .iter()
        .map(|event| PhysicalDepthEvent {
            event,
            metadata: metadata(),
            stream,
        })
        .collect::<Vec<_>>();

    PhysicalDepthBatchBuilder::new()
        .build(&inputs)
        .unwrap()
        .unwrap()
}

fn changes(batch: &RecordBatch) -> &ListArray {
    batch
        .column_by_name("changes")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
}

#[test]
fn builds_one_row_per_depth_event() {
    let stream = stream();

    let events = vec![
        resolved(0, 100, vec![canonical_depth(100, &[100, 101])], 2),
        resolved(1, 200, vec![canonical_depth(200, &[102])], 1),
    ];

    let batch = build(&events, &stream);

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema(), physical_depth_schema());
    assert_eq!(changes(&batch).value_offsets(), &[0, 2, 3]);
}

#[test]
fn preserves_zero_level_events() {
    let stream = stream();

    let events = vec![
        resolved(0, 100, vec![canonical_depth(100, &[100])], 1),
        resolved(1, 200, vec![], 0),
        resolved(2, 300, vec![canonical_depth(300, &[101])], 1),
    ];

    let batch = build(&events, &stream);
    let list = changes(&batch);

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(list.value_offsets(), &[0, 1, 1, 2]);
    assert_eq!(list.value_length(1), 0);
    assert!(!list.is_null(1));
}

#[test]
fn preserves_levels_across_multiple_batches() {
    let stream = stream();

    let events = vec![resolved(
        0,
        100,
        vec![
            canonical_depth(100, &[100, 101]),
            canonical_depth(100, &[102, 103]),
        ],
        4,
    )];

    let batch = build(&events, &stream);
    let list = changes(&batch);

    assert_eq!(list.value_offsets(), &[0, 4]);

    let levels = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let prices = levels
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let expected = (100..104)
        .map(|price| i256::from_i128(price * 10_i128.pow(28)))
        .collect::<Vec<_>>();

    assert_eq!(prices.values(), expected.as_slice());
}

#[test]
fn preserves_depth_metadata() {
    let stream = stream();

    let events = vec![resolved(0, 100, vec![canonical_depth(100, &[100])], 1)];

    let batch = build(&events, &stream);

    let sequences = batch
        .column_by_name("sequence_start")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    assert_eq!(sequences.value(0), 10);

    let operations = batch
        .column_by_name("source_operation")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();

    assert_eq!(operations.value(0), 1);
}

#[test]
fn rejects_canonical_identity_mismatch() {
    let stream = stream();

    let events = vec![resolved(0, 100, vec![canonical_depth(999, &[100])], 1)];

    let inputs = vec![PhysicalDepthEvent {
        event: &events[0],
        metadata: metadata(),
        stream: &stream,
    }];

    assert!(PhysicalDepthBatchBuilder::new().build(&inputs).is_err());
}

#[test]
fn rejects_incorrect_level_count() {
    let stream = stream();

    let events = vec![resolved(0, 100, vec![canonical_depth(100, &[100, 101])], 3)];

    let inputs = vec![PhysicalDepthEvent {
        event: &events[0],
        metadata: metadata(),
        stream: &stream,
    }];

    assert!(PhysicalDepthBatchBuilder::new().build(&inputs).is_err());
}

#[test]
fn parquet_round_trip_preserves_nested_levels() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("depth.parquet");

    let stream = stream();

    let events = vec![
        resolved(0, 100, vec![canonical_depth(100, &[100, 101])], 2),
        resolved(1, 200, vec![], 0),
    ];

    let batch = build(&events, &stream);

    let file = File::create(&path).unwrap();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let file = File::open(&path).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let restored = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();

    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0], batch);
}
