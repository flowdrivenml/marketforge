// #![cfg(feature = "process")]

use arrow_array::{Array, BooleanArray, Decimal256Array, Int64Array, StringArray, UInt64Array};

use marketforge_engine::{
    canonical::{EventEnvelope, Exchange, Trade, TradeSide},
    process::parquet::{TradeBatchBuilder, decimal_to_i256, trade_schema},
};

fn sample_trade() -> Trade {
    Trade {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_788_220_800_123_456_789,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
            instrument_id: 42,
            symbol: "BTCUSDT".to_owned(),
            stream_id: "bybit:BTCUSDT:trades".to_owned(),
        },

        trade_id: Some("trade-123".to_owned()),
        sequence: Some(456),

        side: TradeSide::Buy,

        price: "84500.12345678".parse().unwrap(),

        quantity_base: Some("0.0000000000000000000000000001".parse().unwrap()),

        quantity_quote: None,
        quantity_contracts: None,

        is_rpi: Some(true),

        trade_iv: Some("0.5123456789".parse().unwrap()),
        mark_iv: None,

        index_price: Some("84499.99".parse().unwrap()),
        mark_price: None,
    }
}

#[test]
fn builds_complete_canonical_trade_batch() {
    let trade = sample_trade();

    let mut builder = TradeBatchBuilder::new();

    builder.push(trade.clone());

    let batch = builder
        .finish()
        .expect("build batch")
        .expect("nonempty batch");

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 18);

    assert_eq!(batch.schema(), trade_schema());

    let timestamp = batch
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(timestamp.value(0), trade.envelope.event_timestamp_ns);

    let exchange = batch
        .column_by_name("exchange")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(exchange.value(0), "bybit");

    let side = batch
        .column_by_name("side")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(side.value(0), "buy");

    let trade_id = batch
        .column_by_name("trade_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(trade_id.value(0), "trade-123");

    let sequence = batch
        .column_by_name("sequence")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    assert_eq!(sequence.value(0), 456);

    let price = batch
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(price.value(0), decimal_to_i256(trade.price).unwrap());

    let quantity_base = batch
        .column_by_name("quantity_base")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(
        quantity_base.value(0),
        decimal_to_i256(trade.quantity_base.unwrap()).unwrap()
    );

    let quantity_quote = batch
        .column_by_name("quantity_quote")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert!(quantity_quote.is_null(0));

    let is_rpi = batch
        .column_by_name("is_rpi")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();

    assert!(is_rpi.value(0));

    let trade_iv = batch
        .column_by_name("trade_iv")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(
        trade_iv.value(0),
        decimal_to_i256(trade.trade_iv.unwrap()).unwrap()
    );

    let mark_iv = batch
        .column_by_name("mark_iv")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert!(mark_iv.is_null(0));

    let index_price = batch
        .column_by_name("index_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(
        index_price.value(0),
        decimal_to_i256(trade.index_price.unwrap()).unwrap()
    );

    let mark_price = batch
        .column_by_name("mark_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert!(mark_price.is_null(0));
}

#[test]
fn empty_builder_returns_none() {
    let mut builder = TradeBatchBuilder::new();

    assert!(builder.finish().unwrap().is_none());
}

#[test]
fn builder_can_be_reused() {
    let mut builder = TradeBatchBuilder::new();

    builder.push(sample_trade());

    let first = builder.finish().unwrap().unwrap();

    assert_eq!(first.num_rows(), 1);
    assert!(builder.is_empty());

    builder.push(sample_trade());
    builder.push(sample_trade());

    let second = builder.finish().unwrap().unwrap();

    assert_eq!(second.num_rows(), 2);
    assert!(builder.is_empty());
}

#[test]
fn preserves_null_values() {
    let mut trade = sample_trade();

    trade.trade_id = None;
    trade.sequence = None;
    trade.is_rpi = None;
    trade.trade_iv = None;
    trade.index_price = None;

    let mut builder = TradeBatchBuilder::new();

    builder.push(trade);

    let batch = builder.finish().unwrap().unwrap();

    for name in [
        "system_timestamp_ns",
        "trade_id",
        "sequence",
        "quantity_quote",
        "quantity_contracts",
        "is_rpi",
        "trade_iv",
        "mark_iv",
        "index_price",
        "mark_price",
    ] {
        let column = batch.column_by_name(name).unwrap();

        assert!(column.is_null(0), "expected null in column {name}");
    }
}

#[test]
fn preserves_multiple_records() {
    let mut builder = TradeBatchBuilder::with_capacity(3);

    for index in 0..3 {
        let mut trade = sample_trade();

        trade.envelope.event_timestamp_ns += index;
        trade.trade_id = Some(format!("trade-{index}"));

        builder.push(trade);
    }

    let batch = builder.finish().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 3);

    let ids = batch
        .column_by_name("trade_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(ids.value(0), "trade-0");
    assert_eq!(ids.value(1), "trade-1");
    assert_eq!(ids.value(2), "trade-2");
}
