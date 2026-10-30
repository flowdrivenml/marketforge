//! Self-contained baseline witness: exact values, status codes and provenance.
use arrow_array::{
    Array, ArrayRef, Decimal256Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    UInt8Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer, i256};
use arrow_schema::{Field, Schema};
use marketforge_engine::{
    historical::{
        convert::{execution::*, reference::*, *},
        job::ConvertJob,
    },
    shared::domain::instrument::{Currency, InstrumentIdentity, InstrumentKind},
};
use std::{collections::HashSet, sync::Arc};

fn instrument(rank: u32, base: &str, quote: &str) -> ResolvedConversionInstrument {
    ResolvedConversionInstrument {
        dataset_id: 100 + u64::from(rank),
        stream_rank: rank,
        stream_id: format!("stream-{rank}"),
        identity: InstrumentIdentity {
            exchange: "test".into(),
            symbol: format!("{base}{quote}"),
            instrument_kind: InstrumentKind::Spot,
            contract_kind: None,
        },
        base_currency: Currency::new(base).unwrap(),
        quote_currency: Currency::new(quote).unwrap(),
    }
}
fn decimal(value: i128) -> i256 {
    i256::from_i128(value * 10_i128.pow(28))
}
fn decimal_array(values: Vec<i256>) -> ArrayRef {
    Arc::new(
        Decimal256Array::from(values)
            .with_precision_and_scale(76, 28)
            .unwrap(),
    )
}
fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    let fields = columns
        .iter()
        .map(|(name, a)| Field::new(*name, a.data_type().clone(), true))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns.into_iter().map(|(_, a)| a).collect(),
    )
    .unwrap()
}
fn source_batch(rank: u32, timestamp: i64, prices: Vec<i256>) -> RecordBatch {
    let levels = StructArray::from(vec![
        (
            Arc::new(Field::new(
                "price",
                arrow_schema::DataType::Decimal256(76, 28),
                true,
            )),
            decimal_array(prices.clone()),
        ),
        (
            Arc::new(Field::new(
                "quantity_quote",
                arrow_schema::DataType::Decimal256(76, 28),
                true,
            )),
            decimal_array(vec![decimal(9); prices.len()]),
        ),
    ]);
    let changes: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", levels.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, prices.len() as i32])),
        Arc::new(levels),
        None,
    ));
    let trade = StructArray::from(vec![
        (
            Arc::new(Field::new(
                "price",
                arrow_schema::DataType::Decimal256(76, 28),
                true,
            )),
            decimal_array(vec![prices[0]]),
        ),
        (
            Arc::new(Field::new(
                "quantity_quote",
                arrow_schema::DataType::Decimal256(76, 28),
                true,
            )),
            decimal_array(vec![decimal(9)]),
        ),
    ]);
    let depth = StructArray::from(vec![(
        Arc::new(Field::new("changes", changes.data_type().clone(), true)),
        changes.clone(),
    )]);
    batch(vec![
        (
            "event_timestamp_ns",
            Arc::new(Int64Array::from(vec![timestamp])),
        ),
        ("stream_rank", Arc::new(UInt32Array::from(vec![rank]))),
        (
            "dataset_id",
            Arc::new(UInt64Array::from(vec![100 + u64::from(rank)])),
        ),
        ("task_id", Arc::new(UInt64Array::from(vec![7]))),
        ("event_ordinal", Arc::new(UInt64Array::from(vec![3]))),
        (
            "stream_id",
            Arc::new(StringArray::from(vec![format!("stream-{rank}")])),
        ),
        (
            "event_kind",
            Arc::new(UInt8Array::from(vec![if rank == 0 { 0 } else { 1 }])),
        ),
        ("price", decimal_array(vec![prices[0]])),
        ("quantity_quote", decimal_array(vec![decimal(9)])),
        ("changes", changes),
        ("trade", Arc::new(trade)),
        ("depth", Arc::new(depth)),
    ])
}
fn state(rank: u32, timestamp: i64, dataset: u64) -> ReferenceState {
    let mut state = ReferenceState::new();
    state
        .update(ReferenceObservation {
            dataset_id: dataset,
            stream_id: format!("stream-{rank}"),
            key: ReferenceKey::new(timestamp, rank, 7, 0),
            price: ConversionDecimal::new(decimal(if rank == 0 { 3 } else { 2 })).unwrap(),
        })
        .unwrap();
    state
}
#[test]
fn exact_kernel_baseline_transcript() {
    let mut job: ConvertJob =
        serde_json::from_str(include_str!("../fixtures/jobs/convert-combined.json")).unwrap();
    job.conversion.max_staleness_ns = 10;
    let sources = vec![
        instrument(0, "BTC", "USDT"),
        instrument(1, "ETH", "BTC"),
        instrument(2, "BTC", "USDC"),
        instrument(3, "ETH", "EUR"),
    ];
    job.conversion.references = vec![sources[0].identity.clone(), sources[2].identity.clone()];
    let plan = plan_conversion(
        &job,
        &ResolvedConversionCatalog {
            sources: sources.clone(),
            references: vec![sources[0].clone(), sources[2].clone()],
        },
    )
    .unwrap();
    let trade = TradeConversionKernel::new(&job, &plan).unwrap();
    let depth = DepthConversionKernel::new(&job, &plan).unwrap();
    let mut transcript = String::new();
    for (label, rank, mut state) in [
        ("identity", 0, ReferenceState::new()),
        ("direct", 1, state(0, 99, 100)),
        ("missing", 1, ReferenceState::new()),
        ("stale", 1, state(0, 89, 100)),
        ("future", 1, state(0, 101, 100)),
        ("same_timestamp", 1, state(0, 100, 100)),
        ("mismatch", 1, state(0, 99, 999)),
        ("no_path", 3, ReferenceState::new()),
    ] {
        let batch = source_batch(rank, 100, vec![decimal(2), decimal(4)]);
        let t = trade
            .evaluate_adapted_event(
                &batch,
                0,
                &mut state,
                &mut PhysicalConversionMetrics::default(),
            )
            .unwrap();
        let d = depth
            .convert_event(&batch, 0, &state, &mut DepthConversionMetrics::default())
            .unwrap();
        transcript.push_str(&format!("{label}: trade={t:?}; depth={d:?}\n"));
    }
    let mut multihop = state(0, 99, 100);
    multihop
        .update(state(2, 99, 102).observations().next().unwrap().clone())
        .unwrap();
    let t = trade
        .evaluate_adapted_event(
            &source_batch(2, 100, vec![decimal(2)]),
            0,
            &mut multihop,
            &mut PhysicalConversionMetrics::default(),
        )
        .unwrap();
    transcript.push_str(&format!("inverse_multihop: {t:?}\n"));
    let huge = i256::from_string(&"9".repeat(76)).unwrap();
    let overflow = source_batch(1, 100, vec![decimal(2), huge]);
    let d = depth
        .convert_event(
            &overflow,
            0,
            &state(0, 99, 100),
            &mut DepthConversionMetrics::default(),
        )
        .unwrap();
    assert!(d.levels.is_none());
    transcript.push_str(&format!("atomic_overflow: {d:?}\n"));
    let kernel = CombinedConversionKernel::new(&job, &plan).unwrap();
    let mut alignment = CombinedReferenceAlignment::new(
        CombinedReferenceSelection {
            internal_streams: HashSet::from(["stream-0".into()]),
            external_streams: HashSet::new(),
        },
        None,
    )
    .unwrap();
    let mut metrics = CombinedConversionMetrics::default();
    for rank in [0, 1] {
        let batch = source_batch(rank, 100, vec![decimal(2), decimal(4)]);
        let original = batch.clone();
        let result = kernel
            .convert_event(&batch, 0, &mut alignment, &mut metrics)
            .unwrap();
        assert_eq!(batch, original);
        transcript.push_str(&format!("combined_{rank}: {result:?}\n"));
    }
    transcript.push_str(&format!(
        "combined_counts: {} {} {}\n",
        metrics.events_read, metrics.trade_events, metrics.depth_events
    ));
    println!("{transcript}");
    let expected = include_str!("../fixtures/core-regression.txt");
    assert_eq!(transcript, expected);
}
