use std::{fs::File, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Decimal256Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    new_null_array,
};
use arrow_buffer::i256;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

use marketforge_engine::process::{
    merge::{
        PhysicalCombinedBatchBuilder, PhysicalCombinedEvent, ResolvedDepthMetadata,
        ResolvedMarketEvent, UnifiedMergeStream, UnifiedMergeStreamKind, UnifiedTimelineRecord,
        physical_combined_schema,
    },
    parquet::{depth_schema, trade_schema},
};

fn stream(kind: UnifiedMergeStreamKind, dataset_id: u64, rank: u32) -> UnifiedMergeStream {
    UnifiedMergeStream {
        stream_rank: rank,
        dataset_id,
        kind,
        exchange: "bybit".into(),
        instrument_id: 688,
        symbol: "BTCUSDT".into(),
        stream_id: match kind {
            UnifiedMergeStreamKind::Trade => "bybit:perpetual:linear:BTCUSDT:trade".into(),
            UnifiedMergeStreamKind::Depth => "bybit:perpetual:linear:BTCUSDT:l2".into(),
        },
        base_currency: None,
        quote_currency: None,
    }
}

fn decimal(value: i64) -> ArrayRef {
    Arc::new(
        Decimal256Array::from(vec![i256::from_i128(value as i128 * 10_i128.pow(28))])
            .with_precision_and_scale(76, 28)
            .unwrap(),
    )
}

fn canonical_batch(kind: u8, timestamp: i64) -> RecordBatch {
    let schema = if kind == 0 {
        trade_schema()
    } else {
        depth_schema()
    };

    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "event_timestamp_ns" => Arc::new(Int64Array::from(vec![timestamp])) as ArrayRef,
            "instrument_id" => Arc::new(Int64Array::from(vec![688])) as ArrayRef,
            "exchange" => Arc::new(StringArray::from(vec!["bybit"])) as ArrayRef,
            "symbol" => Arc::new(StringArray::from(vec!["BTCUSDT"])) as ArrayRef,
            "stream_id" => {
                let id = if kind == 0 {
                    "bybit:perpetual:linear:BTCUSDT:trade"
                } else {
                    "bybit:perpetual:linear:BTCUSDT:l2"
                };

                Arc::new(StringArray::from(vec![id])) as ArrayRef
            }
            "side" => Arc::new(StringArray::from(vec![if kind == 0 {
                "buy"
            } else {
                "bid"
            }])) as ArrayRef,
            "price" => decimal(78500),
            _ => new_null_array(field.data_type(), 1),
        })
        .collect();

    RecordBatch::try_new(schema, columns).unwrap()
}

fn event(kind: u8, dataset_id: u64, rank: u32, timestamp: i64) -> ResolvedMarketEvent {
    ResolvedMarketEvent {
        reference: UnifiedTimelineRecord {
            event_timestamp_ns: timestamp,
            stream_rank: rank,
            event_kind: kind,
            dataset_id,
            task_id: 0,
            event_ordinal: timestamp as u64,
            canonical_row_offset: 0,
            canonical_row_count: 1,
            event_boundary: (kind == 1).then_some(0),
            source_operation: (kind == 1).then_some(1),
        },
        canonical_batches: vec![canonical_batch(kind, timestamp)],
    }
}

fn metadata() -> ResolvedDepthMetadata {
    ResolvedDepthMetadata {
        system_timestamp_ns: Some(123),
        sequence_start: Some(10),
        sequence_end: Some(11),
    }
}

fn build(
    events: &[ResolvedMarketEvent],
    trade_stream: &UnifiedMergeStream,
    depth_stream: &UnifiedMergeStream,
) -> RecordBatch {
    let inputs = events
        .iter()
        .map(|event| {
            let depth = event.reference.event_kind == 1;

            PhysicalCombinedEvent {
                event,
                stream: if depth { depth_stream } else { trade_stream },
                depth_metadata: depth.then_some(metadata()),
            }
        })
        .collect::<Vec<_>>();

    PhysicalCombinedBatchBuilder::new()
        .build(&inputs)
        .unwrap()
        .unwrap()
}

fn payload<'a>(batch: &'a RecordBatch, name: &str) -> &'a StructArray {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
}

#[test]
fn preserves_interleaved_chronological_order() {
    let trades = stream(UnifiedMergeStreamKind::Trade, 88, 0);
    let depth = stream(UnifiedMergeStreamKind::Depth, 89, 1);

    let events = vec![
        event(0, 88, 0, 100),
        event(1, 89, 1, 200),
        event(0, 88, 0, 300),
        event(1, 89, 1, 400),
    ];

    let batch = build(&events, &trades, &depth);

    assert_eq!(batch.num_rows(), 4);
    assert_eq!(batch.schema(), physical_combined_schema());

    let timestamps = batch
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(timestamps.values(), &[100, 200, 300, 400]);
}

#[test]
fn nullable_payloads_are_mutually_exclusive() {
    let trades = stream(UnifiedMergeStreamKind::Trade, 88, 0);
    let depth = stream(UnifiedMergeStreamKind::Depth, 89, 1);

    let events = vec![
        event(0, 88, 0, 100),
        event(1, 89, 1, 200),
        event(0, 88, 0, 300),
    ];

    let batch = build(&events, &trades, &depth);

    let trade = payload(&batch, "trade");
    let depth = payload(&batch, "depth");

    assert!(!trade.is_null(0));
    assert!(trade.is_null(1));
    assert!(!trade.is_null(2));

    assert!(depth.is_null(0));
    assert!(!depth.is_null(1));
    assert!(depth.is_null(2));
}

#[test]
fn preserves_nested_depth_changes() {
    let trades = stream(UnifiedMergeStreamKind::Trade, 88, 0);
    let depth = stream(UnifiedMergeStreamKind::Depth, 89, 1);

    let events = vec![event(0, 88, 0, 100), event(1, 89, 1, 200)];

    let batch = build(&events, &trades, &depth);

    let depth = payload(&batch, "depth");

    let changes = depth
        .column_by_name("changes")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert_eq!(changes.value_length(1), 1);
}

#[test]
fn preserves_decimal256_trade_values() {
    let trades = stream(UnifiedMergeStreamKind::Trade, 88, 0);
    let depth = stream(UnifiedMergeStreamKind::Depth, 89, 1);

    let events = vec![event(0, 88, 0, 100)];

    let batch = build(&events, &trades, &depth);

    let trade = payload(&batch, "trade");

    let prices = trade
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(
        prices.value(0),
        i256::from_i128(78500_i128 * 10_i128.pow(28))
    );
}

#[test]
fn supports_trade_only_combined_batch() {
    let trades = stream(UnifiedMergeStreamKind::Trade, 88, 0);
    let depth = stream(UnifiedMergeStreamKind::Depth, 89, 1);

    let events = vec![event(0, 88, 0, 100), event(0, 88, 0, 200)];

    let batch = build(&events, &trades, &depth);

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(payload(&batch, "depth").null_count(), 2);
}

#[test]
fn supports_depth_only_combined_batch() {
    let trades = stream(UnifiedMergeStreamKind::Trade, 88, 0);
    let depth = stream(UnifiedMergeStreamKind::Depth, 89, 1);

    let events = vec![event(1, 89, 1, 100), event(1, 89, 1, 200)];

    let batch = build(&events, &trades, &depth);

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(payload(&batch, "trade").null_count(), 2);
}

#[test]
fn parquet_round_trip_preserves_combined_events() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("combined.parquet");

    let trades = stream(UnifiedMergeStreamKind::Trade, 88, 0);
    let depth = stream(UnifiedMergeStreamKind::Depth, 89, 1);

    let events = vec![
        event(0, 88, 0, 100),
        event(1, 89, 1, 200),
        event(0, 88, 0, 300),
    ];

    let batch = build(&events, &trades, &depth);

    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None).unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
        .unwrap()
        .build()
        .unwrap();

    let restored = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();

    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0], batch);
}

#[test]
fn combined_preserves_zero_level_depth_event() {
    let trades = stream(UnifiedMergeStreamKind::Trade, 88, 0);
    let depth = stream(UnifiedMergeStreamKind::Depth, 89, 1);

    let mut empty = event(1, 89, 1, 200);
    empty.reference.canonical_row_count = 0;
    empty.canonical_batches.clear();

    let events = vec![event(0, 88, 0, 100), empty, event(0, 88, 0, 300)];

    let batch = build(&events, &trades, &depth);

    let depth_payload = payload(&batch, "depth");

    assert!(depth_payload.is_null(0));
    assert!(!depth_payload.is_null(1));
    assert!(depth_payload.is_null(2));

    let changes = depth_payload
        .column_by_name("changes")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert_eq!(changes.value_length(1), 0);
    assert!(!changes.is_null(1));
}
