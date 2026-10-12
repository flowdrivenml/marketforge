// #![cfg(feature = "process")]

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use csv::ByteRecord;

use marketforge_engine::{
    formats::trades::{TradeContext, TradeProcessor, TradeSpec, resolve_trade_headers},
    job::{TargetSchema, load_processing_job, validate_processing_job},
};

fn process_jobs_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.jobs/process")
}

#[test]
fn all_trade_job_configurations_are_supported() {
    let directory = process_jobs_dir();

    assert!(
        directory.is_dir(),
        "process job directory does not exist: {}",
        directory.display()
    );

    let mut seen = HashSet::new();
    let mut checked = 0;

    for entry in fs::read_dir(&directory).expect("read process jobs") {
        let path = entry.expect("read directory entry").path();

        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let job = load_processing_job(&path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));

        validate_processing_job(&job).unwrap_or_else(|error| panic!("{}: {error}", path.display()));

        for task in &job.tasks {
            for normalization in &task.normalizations {
                if normalization.target_schema != TargetSchema::Trade {
                    continue;
                }

                let key = format!(
                    "{}:{}:{:?}:{:?}:{:?}",
                    task.exchange as u8,
                    task.format_code.0,
                    task.instrument.instrument_kind,
                    task.instrument.contract_kind,
                    normalization.quantity_encoding,
                );

                if !seen.insert(key) {
                    continue;
                }

                let spec = TradeSpec::from_normalization(normalization)
                    .expect("compile trade specification");

                let has_headers = task
                    .raw_schema
                    .get("header")
                    .and_then(|value| value.as_bool())
                    .expect("raw schema header flag");

                let headers = if has_headers {
                    let fields = task
                        .raw_schema
                        .get("fields")
                        .and_then(|value| value.as_array())
                        .expect("raw schema fields");

                    let mut record = ByteRecord::new();

                    for field in fields {
                        let name = field["name"].as_str().expect("raw schema field name");

                        record.push_field(name.as_bytes());
                    }

                    resolve_trade_headers(&task.raw_schema, Some(&record))
                        .expect("resolve headered schema")
                } else {
                    resolve_trade_headers(&task.raw_schema, None)
                        .expect("resolve headerless schema")
                };

                let context = TradeContext {
                    exchange: task.exchange,
                    instrument_id: task.instrument_id,
                    symbol: task.symbol.clone(),
                    stream_id: task.stream_id.0.clone(),
                };

                TradeProcessor::new(
                    &headers,
                    normalization.clone(),
                    task.instrument.clone(),
                    context,
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "failed to construct processor for {}: {error}",
                        task.format_code.0
                    )
                });

                println!(
                    "\nExchange       : {:?}\n\
                    Format         : {}\n\
                    Instrument     : {}\n\
                    Instrument kind: {:?}\n\
                    Contract kind  : {:?}\n\
                    Quantity       : {:?}\n\
                    Timestamp      : {:?}\n\
                    CSV headers    : {}\n\
                    Field mappings : {:#?}\n",
                    task.exchange,
                    task.format_code.0,
                    task.symbol,
                    task.instrument.instrument_kind,
                    task.instrument.contract_kind,
                    normalization.quantity_encoding,
                    normalization.timestamp_encoding,
                    headers.len(),
                    spec,
                );

                assert!(!spec.event_timestamp.source.is_empty());

                checked += 1;
            }
        }
    }

    assert_eq!(
        checked, 18,
        "expected 18 distinct trade configurations, found {checked}"
    );
}
