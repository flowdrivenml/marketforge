// #![cfg(feature = "process")]

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use csv::ByteRecord;

use marketforge_engine::{
    canonical::Trade,
    decode::CsvDecoder,
    error::MarketForgeError,
    formats::trades::{
        IdentityDecision, InstrumentMatcher, TradeContext, TradeProcessor, resolve_trade_headers,
    },
    job::{TargetSchema, WorkTask, load_processing_job, validate_processing_job},
    source::with_source_reader,
};

const SAMPLE_SIZE: usize = 5;
const EXPECTED_CONFIGURATIONS: usize = 18;

// -----------------------------------------------------------------------------
// Job discovery
// -----------------------------------------------------------------------------

fn process_jobs_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.jobs/process")
}

fn configuration_key(task: &WorkTask) -> String {
    let normalization = task
        .normalizations
        .iter()
        .find(|n| n.target_schema == TargetSchema::Trade)
        .expect("trade task must contain trade normalization");

    format!(
        "{:?}:{:?}:{:?}:{}:{:?}:{:?}",
        task.exchange,
        task.instrument.instrument_kind,
        task.instrument.contract_kind,
        task.format_code.0,
        normalization.quantity_encoding,
        normalization.timestamp_encoding,
    )
}

fn discover_trade_tasks() -> BTreeMap<String, WorkTask> {
    let directory = process_jobs_dir();

    assert!(
        directory.is_dir(),
        "process job directory does not exist: {}",
        directory.display()
    );

    let mut paths: Vec<PathBuf> = fs::read_dir(&directory)
        .expect("read process jobs")
        .map(|entry| entry.expect("read directory entry").path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect();

    paths.sort();

    let mut tasks = BTreeMap::new();

    for path in paths {
        let job = load_processing_job(&path)
            .unwrap_or_else(|error| panic!("failed to load {}: {error}", path.display()));

        validate_processing_job(&job)
            .unwrap_or_else(|error| panic!("invalid job {}: {error}", path.display()));

        for task in job.tasks {
            if !task
                .normalizations
                .iter()
                .any(|n| n.target_schema == TargetSchema::Trade)
            {
                continue;
            }

            let key = configuration_key(&task);

            // Prefer an existing archive when duplicate configurations
            // appear in multiple generated processing jobs.
            match tasks.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(task);
                }

                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if !entry.get().input_path.is_file() && task.input_path.is_file() {
                        entry.insert(task);
                    }
                }
            }
        }
    }

    tasks
}

// -----------------------------------------------------------------------------
// Known source mismatch
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Integration test
// -----------------------------------------------------------------------------

#[test]
fn normalize_real_trade_archives() {
    let tasks = discover_trade_tasks();

    println!("\n{}", "=".repeat(100));
    println!("MARKETFORGE — REAL TRADE ARCHIVE NORMALIZATION");
    println!("{}", "=".repeat(100));

    println!("Configurations discovered : {}", tasks.len());
    println!("Sample size               : {SAMPLE_SIZE} records");
    println!("Expected configurations   : {EXPECTED_CONFIGURATIONS}");

    assert_eq!(
        tasks.len(),
        EXPECTED_CONFIGURATIONS,
        "unexpected number of trade configurations"
    );

    let mut successful = 0usize;
    let mut failed = 0usize;
    let mut total_records = 0usize;

    let mut failures = Vec::new();

    for (key, task) in tasks {
        println!("\n{}", "-".repeat(100));
        println!("CONFIGURATION: {key}");
        println!("{}", "-".repeat(100));

        print_task_metadata(&task);

        match process_sample(&task) {
            Ok(count) if count == SAMPLE_SIZE => {
                successful += 1;
                total_records += count;

                println!("\nRESULT: PASS — {count} canonical trades");
            }

            Ok(count) => {
                failed += 1;

                let message = format!("expected {SAMPLE_SIZE} records, processed {count}");

                println!("\nRESULT: FAIL — {message}");

                failures.push((key, message));
            }

            Err(error) => {
                failed += 1;

                println!("\nRESULT: FAIL — {error}");

                failures.push((key, error));
            }
        }
    }

    println!("\n{}", "=".repeat(100));
    println!("SUMMARY");
    println!("{}", "=".repeat(100));

    println!("Successful configurations : {successful}");
    println!("Unexpected failures       : {failed}");
    println!("Total configurations      : {}", successful + failed);
    println!("Canonical trades          : {total_records}");

    if !failures.is_empty() {
        println!("\nFAILURES");
        println!("{}", "-".repeat(100));

        for (key, error) in &failures {
            println!("{key}");
            println!("  {error}");
        }
    }

    println!("{}", "=".repeat(100));

    assert_eq!(failed, 0, "{failed} trade configurations failed");

    assert_eq!(
        successful, EXPECTED_CONFIGURATIONS,
        "not all trade configurations passed"
    );

    assert_eq!(
        total_records,
        EXPECTED_CONFIGURATIONS * SAMPLE_SIZE,
        "unexpected number of canonical trades"
    );
}

// -----------------------------------------------------------------------------
// Task metadata
// -----------------------------------------------------------------------------

fn print_task_metadata(task: &WorkTask) {
    let normalization = task
        .normalizations
        .iter()
        .find(|n| n.target_schema == TargetSchema::Trade)
        .expect("trade normalization");

    println!("Exchange       : {:?}", task.exchange);
    println!("Format         : {}", task.format_code.0);
    println!("Symbol         : {}", task.symbol);

    println!("Instrument     : {:?}", task.instrument.instrument_kind);

    println!("Contract       : {:?}", task.instrument.contract_kind);

    println!("Contract value : {:?}", task.instrument.contract_value);

    println!(
        "Contract asset : {:?}",
        task.instrument.contract_value_asset
    );

    println!("Quantity       : {:?}", normalization.quantity_encoding);

    println!("Timestamp      : {:?}", normalization.timestamp_encoding);

    println!("Source ordering: {:?}", task.source_ordering);

    println!("Compression    : {:?}", task.source_compression);

    println!("Archive member : {:?}", task.archive_member);

    println!("Archive        : {}", task.input_path.display());
}

// -----------------------------------------------------------------------------
// Real archive processing
// -----------------------------------------------------------------------------

fn process_sample(task: &WorkTask) -> Result<usize, String> {
    let normalization = task
        .normalizations
        .iter()
        .find(|n| n.target_schema == TargetSchema::Trade)
        .ok_or("missing trade normalization")?
        .clone();

    let has_headers = task
        .raw_schema
        .get("header")
        .and_then(|value| value.as_bool())
        .ok_or("missing raw_schema.header")?;

    let context = TradeContext {
        exchange: task.exchange,
        instrument_id: task.instrument_id,
        symbol: task.symbol.clone(),
        stream_id: task.stream_id.0.clone(),
    };

    if !task.input_path.is_file() {
        return Err(format!(
            "archive does not exist: {}",
            task.input_path.display()
        ));
    }

    with_source_reader(
        &task.input_path,
        task.source_compression,
        task.archive_member.as_deref(),
        |reader| {
            // Initialize the CSV decoder.
            let mut decoder = CsvDecoder::with_headers(reader, has_headers);

            // Read actual CSV headers when present.
            let actual_headers = if has_headers {
                Some(decoder.headers().map_err(|error| {
                    MarketForgeError::InvalidConfiguration(format!("CSV header error: {error}"))
                })?)
            } else {
                None
            };

            // Resolve actual or synthetic headers.
            let headers = resolve_trade_headers(&task.raw_schema, actual_headers.as_ref())?;

            // Construct the canonical trade processor.
            let processor =
                TradeProcessor::new(&headers, normalization, task.instrument.clone(), context)?;

            // Construct the instrument identity matcher.
            let mut matcher = InstrumentMatcher::from_task(&headers, task)?;

            println!("\nCSV header present : {has_headers}");
            println!("Resolved columns   : {:?}", headers);
            println!("Identity policy    : {:?}", matcher.policy());

            let mut count = 0usize;

            for record in decoder.records() {
                let record = record.map_err(|error| {
                    MarketForgeError::InvalidCanonical(format!("CSV record error: {error}"))
                })?;

                // Check instrument identity before normalization.
                match matcher.check(&record)? {
                    IdentityDecision::Skip => continue,
                    IdentityDecision::Match => {}
                }

                // Normalize only matching records.
                let trade = processor.process_record(&record)?;

                print_trade(count + 1, &record, &trade);

                count += 1;

                if count >= SAMPLE_SIZE {
                    break;
                }
            }

            // Detect archives containing no matching instruments.
            matcher.finish()?;

            println!("\nIdentity metrics:");
            println!("  Records read    : {}", matcher.metrics().records_read);
            println!("  Records matched : {}", matcher.metrics().records_matched);
            println!(
                "  Records skipped : {}",
                matcher.metrics().records_skipped_instrument
            );

            Ok(count)
        },
    )
    .map_err(|error| error.to_string())
}

// -----------------------------------------------------------------------------
// Canonical trade diagnostics
// -----------------------------------------------------------------------------

fn print_trade(index: usize, raw: &ByteRecord, trade: &Trade) {
    println!("\n  TRADE {index}");
    println!("  {}", "-".repeat(70));

    println!("  Raw record        : {:?}", raw);

    println!(
        "  Timestamp (ns)    : {}",
        trade.envelope.event_timestamp_ns
    );

    println!("  Exchange          : {:?}", trade.envelope.exchange);

    println!("  Instrument ID     : {}", trade.envelope.instrument_id);

    println!("  Symbol            : {}", trade.envelope.symbol);

    println!("  Stream ID         : {}", trade.envelope.stream_id);

    println!("  Trade ID          : {:?}", trade.trade_id);
    println!("  Side              : {:?}", trade.side);
    println!("  Price             : {}", trade.price);

    println!("  Quantity base     : {:?}", trade.quantity_base);

    println!("  Quantity quote    : {:?}", trade.quantity_quote);

    println!("  Quantity contracts: {:?}", trade.quantity_contracts);

    println!("  RPI               : {:?}", trade.is_rpi);
    println!("  Sequence          : {:?}", trade.sequence);
}
