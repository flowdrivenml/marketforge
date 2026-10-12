// #![cfg(feature = "process")]

use std::{
    fs::File,
    sync::atomic::{AtomicU64, Ordering},
};

use arrow_array::{Decimal256Array, Int64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;

use marketforge_engine::{
    canonical::{EventEnvelope, Exchange, Trade, TradeSide},
    job::ParquetResourceConfig,
    process::{
        parquet::{ParquetTradeWriter, decimal_to_i256},
        worker::TradeSink,
    },
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

fn sample_trade(index: i64) -> Trade {
    Trade {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_788_220_800_000_000_000 + index,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
            instrument_id: 1,
            symbol: "BTCUSDT".to_owned(),
            stream_id: "bybit:BTCUSDT:trades".to_owned(),
        },
        trade_id: Some(format!("trade-{index}")),
        sequence: Some(index as u64),
        side: TradeSide::Buy,
        price: "84500.12345678".parse().unwrap(),
        quantity_base: Some("0.00000001".parse().unwrap()),
        quantity_quote: None,
        quantity_contracts: None,
        is_rpi: Some(false),
        trade_iv: None,
        mark_iv: None,
        index_price: None,
        mark_price: None,
    }
}

fn test_dir() -> std::path::PathBuf {
    let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

    let path = std::env::temp_dir().join(format!(
        "marketforge-parquet-test-{}-{id}",
        std::process::id()
    ));

    std::fs::create_dir_all(&path).unwrap();

    path
}

#[test]
fn writes_and_reads_parquet_trades() {
    let directory = test_dir();

    let resources = ParquetResourceConfig {
        row_group_target_bytes: 1024,
        file_target_bytes: 4096,
    };

    let mut writer = ParquetTradeWriter::new(&directory, resources).unwrap();

    for index in 0..100 {
        writer.write_trade(sample_trade(index)).unwrap();
    }

    writer.finish().unwrap();

    let metrics = writer.metrics();

    assert_eq!(metrics.trades_written, 100);
    assert!(metrics.files_written >= 1);

    let mut total_rows = 0;

    for entry in std::fs::read_dir(&directory).unwrap() {
        let path = entry.unwrap().path();

        let file = File::open(path).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            total_rows += batch.num_rows();

            let price = batch
                .column_by_name("price")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            assert_eq!(
                price.value(0),
                decimal_to_i256("84500.12345678".parse::<Decimal>().unwrap()).unwrap()
            );

            let timestamp = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            assert!(timestamp.value(0) > 0);
        }
    }

    assert_eq!(total_rows, 100);

    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn rejects_writes_after_finish() {
    let directory = test_dir();

    let resources = ParquetResourceConfig {
        row_group_target_bytes: 1024,
        file_target_bytes: 4096,
    };

    let mut writer = ParquetTradeWriter::new(&directory, resources).unwrap();

    writer.write_trade(sample_trade(0)).unwrap();
    writer.finish().unwrap();

    assert!(writer.write_trade(sample_trade(1)).is_err());

    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn finish_is_idempotent() {
    let directory = test_dir();

    let resources = ParquetResourceConfig {
        row_group_target_bytes: 1024,
        file_target_bytes: 4096,
    };

    let mut writer = ParquetTradeWriter::new(&directory, resources).unwrap();

    writer.write_trade(sample_trade(0)).unwrap();

    writer.finish().unwrap();
    writer.finish().unwrap();

    assert_eq!(writer.metrics().trades_written, 1);
    assert_eq!(writer.metrics().files_written, 1);

    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn rejects_writes_after_failed_serialization() {
    let directory = test_dir();

    let resources = ParquetResourceConfig {
        row_group_target_bytes: 1,
        file_target_bytes: 4096,
    };

    let mut writer = ParquetTradeWriter::new(&directory, resources).unwrap();

    let trade = sample_trade(0);

    // The canonical decimal conversion must reject values
    // that cannot be represented by the configured Arrow type.
    //
    // Instead of manufacturing an impossible rust_decimal value,
    // force an output-file creation failure.

    let conflicting_path = directory.join("part-000000.parquet");

    std::fs::create_dir(&conflicting_path).unwrap();

    let result = writer.write_trade(trade.clone());

    assert!(result.is_err());
    assert!(writer.is_failed());

    assert!(writer.write_trade(trade).is_err());
    assert!(writer.finish().is_err());

    std::fs::remove_dir_all(directory).unwrap();
}
