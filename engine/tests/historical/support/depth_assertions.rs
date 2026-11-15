use std::{
    fs::{self, File},
    path::PathBuf,
};
use std::{path::Path, time::Instant};
use {
    marketforge_engine::historical::normalize::boundary::DepthBoundaryManifest,
    marketforge_engine::historical::normalize::boundary::fingerprint_book,
    marketforge_engine::historical::normalize::metrics::ScopedIntegrityMetrics,
    marketforge_engine::historical::normalize::metrics::TaskMetrics,
    marketforge_engine::historical::normalize::worker::process_depth_task_with_metrics,
    marketforge_engine::shared::persistence::parquet::ParquetDepthWriter,
};

use arrow_array::{Array, Decimal256Array, Int64Array, StringArray, UInt64Array};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use {
    marketforge_engine::historical::normalize::boundary::BoundaryBookState,
    marketforge_engine::shared::domain::book::BookLevel,
    marketforge_engine::shared::domain::book::BookStore,
    marketforge_engine::shared::domain::canonical::BookSide,
    marketforge_engine::shared::persistence::parquet::i256_to_decimal,
    marketforge_engine::shared::persistence::parquet::read_depth_segments,
};

use rust_decimal::Decimal;

pub struct DepthFormatResult {
    pub records_read: u64,
    pub records_processed: u64,
    pub records_rejected: u64,
    pub levels_written: u64,
    pub boundary: DepthBoundaryManifest,
    pub segment_count: usize,
}

fn parquet_files(directory: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("part-") && name.ends_with(".parquet"))
        })
        .collect::<Vec<_>>();

    files.sort();
    files
}

fn decimal_at(array: &Decimal256Array, index: usize) -> Option<Decimal> {
    if array.is_null(index) {
        None
    } else {
        Some(i256_to_decimal(array.value(index)).unwrap())
    }
}

pub fn replay_parquet(directory: &Path) -> (BookStore, u64) {
    let manifest = read_depth_segments(directory).unwrap();

    let files = parquet_files(directory);

    assert!(!files.is_empty());
    assert!(!manifest.segments.is_empty());

    let mut book = BookStore::new();

    let mut row_offset = 0u64;
    let mut segment_index = 0usize;

    let mut initialization_bids = Vec::new();
    let mut initialization_asks = Vec::new();

    for path in files {
        let file = fs::File::open(&path).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(8192)
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            let sides = batch
                .column_by_name("side")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let prices = batch
                .column_by_name("price")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            let quantities_base = batch
                .column_by_name("quantity_base")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            let quantities_quote = batch
                .column_by_name("quantity_quote")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            let quantities_contracts = batch
                .column_by_name("quantity_contracts")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            let order_counts = batch
                .column_by_name("order_count")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            for index in 0..batch.num_rows() {
                let side = match sides.value(index) {
                    "bid" => BookSide::Bid,
                    "ask" => BookSide::Ask,
                    other => panic!("invalid depth side: {other}"),
                };

                let price = decimal_at(prices, index).unwrap();

                let level = BookLevel {
                    quantity_base: decimal_at(quantities_base, index),
                    quantity_quote: decimal_at(quantities_quote, index),
                    quantity_contracts: decimal_at(quantities_contracts, index),

                    order_count: if order_counts.is_null(index) {
                        None
                    } else {
                        Some(order_counts.value(index))
                    },
                };

                // ---------------------------------------------------------
                // Segment initialization
                // ---------------------------------------------------------

                if segment_index < manifest.segments.len() {
                    let segment = &manifest.segments[segment_index];

                    let start = segment.initial_event_offset;

                    let end = start.checked_add(segment.initial_level_count).unwrap();

                    if row_offset == start {
                        book.clear();

                        initialization_bids.clear();
                        initialization_asks.clear();
                    }

                    if row_offset >= start && row_offset < end {
                        assert_eq!(timestamps.value(index), segment.start_timestamp_ns);

                        match side {
                            BookSide::Bid => {
                                initialization_bids.push((price, level));
                            }

                            BookSide::Ask => {
                                initialization_asks.push((price, level));
                            }
                        }

                        row_offset += 1;

                        if row_offset == end {
                            book.apply_snapshot(
                                std::mem::take(&mut initialization_bids),
                                std::mem::take(&mut initialization_asks),
                            )
                            .unwrap();

                            segment_index += 1;
                        }

                        continue;
                    }
                }

                // ---------------------------------------------------------
                // Absolute level update
                // ---------------------------------------------------------

                assert!(
                    book.is_initialized(),
                    "replay update before initialization at row {row_offset}"
                );

                book.set_level(side, price, level).unwrap();

                row_offset += 1;
            }
        }
    }

    assert_eq!(row_offset, manifest.total_levels);

    assert_eq!(
        segment_index,
        manifest.segments.len(),
        "not all reconstruction segments were initialized"
    );

    assert!(book.is_initialized());

    (book, row_offset)
}

fn verify_parquet_replay(directory: &Path, boundary: &DepthBoundaryManifest, expected_rows: u64) {
    let start = Instant::now();

    let (replayed_book, replayed_rows) = replay_parquet(directory);

    let elapsed = start.elapsed().as_secs_f64();

    assert_eq!(
        replayed_rows, expected_rows,
        "Parquet replay row count mismatch"
    );

    let replayed_state =
        BoundaryBookState::from_book(&replayed_book).expect("replayed book is not initialized");

    let final_boundary = boundary
        .final_state
        .as_ref()
        .expect("missing final boundary state");

    assert_eq!(
        replayed_state, final_boundary.state,
        "Parquet replay differs from boundary.json"
    );

    let replayed_hash = fingerprint_book(&replayed_state);

    assert_eq!(
        replayed_hash, final_boundary.fingerprint,
        "Parquet replay fingerprint mismatch"
    );

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — PARQUET REPLAY EQUIVALENCE");
    println!("{}", "=".repeat(90));

    println!("Rows replayed     : {replayed_rows}");
    println!("Replay time       : {elapsed:.3}s");

    println!(
        "Replay throughput : {:.0} levels/sec",
        replayed_rows as f64 / elapsed.max(f64::EPSILON)
    );

    println!("Final bids        : {}", replayed_state.bids.len());
    println!("Final asks        : {}", replayed_state.asks.len());

    println!("\nEQUIVALENCE");
    println!("Parquet ↔ Boundary: MATCH");
    println!("SHA-256           : MATCH");

    println!("\nREPLAY RESULT: PASS");
}

// Real-depth tests select only current prepared sources.
// Keep the existing independent Parquet replay and boundary assertions.
pub fn run_prepared_real_depth_test(
    exchange: marketforge_engine::shared::domain::canonical::Exchange,
    instrument_type: &str,
    category: &str,
    source_index: usize,
) -> DepthFormatResult {
    run_prepared_depth_impl(exchange, instrument_type, category, source_index, None)
}

pub fn run_prepared_real_depth_family_test(
    exchange: marketforge_engine::shared::domain::canonical::Exchange,
    instrument_type: &str,
    category: &str,
    source_index: usize,
    member_index: usize,
) -> DepthFormatResult {
    run_prepared_depth_impl(
        exchange,
        instrument_type,
        category,
        source_index,
        Some(member_index),
    )
}

fn run_prepared_depth_impl(
    exchange: marketforge_engine::shared::domain::canonical::Exchange,
    instrument_type: &str,
    category: &str,
    source_index: usize,
    family_member_index: Option<usize>,
) -> DepthFormatResult {
    use crate::support::{fixtures::workload, runtime::TestRun};
    use marketforge_engine::historical::job::ContentType;
    use marketforge_engine::shared::domain::book::SequencePolicy;
    use marketforge_engine::shared::historical::limits::LimitScope;

    let mut batch = workload().expect("load authoritative normalization batch");
    batch.jobs.retain_mut(|job| {
        if job.output.content_type != ContentType::Depth {
            return false;
        }
        job.tasks.retain(|task| {
            task.exchange == exchange
                && serde_json::to_value(task.instrument_type).unwrap().as_str()
                    == Some(instrument_type)
                && serde_json::to_value(task.market_category).unwrap().as_str() == Some(category)
                && if family_member_index.is_some() {
                    task.scope.is_some()
                } else {
                    task.scope.is_none() && task.symbol.is_some()
                }
        });
        !job.tasks.is_empty()
    });
    assert_eq!(
        batch.jobs.len(),
        1,
        "expected one matching concrete depth job"
    );
    let tasks = &mut batch.jobs[0].tasks;

    if let Some(member_index) = family_member_index {
        // Choose an archive from the prepared batch.
        tasks.sort_by(|a, b| a.input_path.cmp(&b.input_path));

        assert!(
            tasks.len() > source_index,
            "missing family source index {source_index}"
        );

        let selected_id = tasks[source_index].task_id.0;
        tasks.retain(|task| task.task_id.0 == selected_id);

        assert_eq!(tasks.len(), 1);

        let task = &mut tasks[0];
        let family = task
            .scope
            .as_ref()
            .expect("expected family scope")
            .identifier
            .clone();

        // Select authoritative concrete metadata belonging to this family.
        let mut members = batch
            .instruments
            .iter()
            .filter(|instrument| {
                if instrument.exchange != exchange {
                    return false;
                }

                if serde_json::to_value(instrument.instrument_type)
                    .unwrap()
                    .as_str()
                    != Some(instrument_type)
                {
                    return false;
                }

                if serde_json::to_value(instrument.market_category)
                    .unwrap()
                    .as_str()
                    != Some(category)
                {
                    return false;
                }

                match instrument_type {
                    "perpetual" => instrument.symbol.strip_suffix("-SWAP") == Some(family.as_str()),

                    "future" => instrument
                        .symbol
                        .strip_prefix(&format!("{family}-"))
                        .is_some(),

                    _ => false,
                }
            })
            .collect::<Vec<_>>();

        members.sort_by(|a, b| a.symbol.cmp(&b.symbol));

        let member = members.get(member_index).unwrap_or_else(|| {
            panic!(
                "missing authoritative family member index {} for {}",
                member_index, family
            )
        });

        println!(
            "Selected OKX family={} member={} instrument_id={}",
            family, member.symbol, member.instrument_id
        );

        // Explicitly test this concrete member. Other symbols in a
        // mixed archive are outside this particular test's selection.
        task.instrument_selection = Some(
            serde_json::from_value(serde_json::json!({
                "mode": "include",
                "symbols": [member.symbol.clone()]
            }))
            .expect("valid concrete family-member selection"),
        );

        // OKX futures-chain archives contain separate TAR members
        // for individual contracts. Route the selected instrument
        // to its exact source member.
        if instrument_type == "future" {
            let archive_name = task
                .input_path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("valid futures archive filename");

            let prefix = format!("{family}-futureschain-");

            let suffix = archive_name
                .strip_prefix(&prefix)
                .and_then(|name| name.strip_suffix(".tar.gz"))
                .expect("expected OKX futures-chain archive");

            let archive_member = format!("{}-{}.data", member.symbol, suffix);

            task.scope
                .as_mut()
                .expect("missing family scope")
                .members
                .push(
                    serde_json::from_value(serde_json::json!({
                        "instrument_id": member.instrument_id,
                        "archive_member": archive_member
                    }))
                    .expect("valid family member mapping"),
                );

            println!("Selected futures archive member: {}", archive_member);
        }
    } else {
        // Preserve the existing concrete-source selection behavior.
        tasks.sort_by(|a, b| {
            a.symbol
                .cmp(&b.symbol)
                .then(a.input_path.cmp(&b.input_path))
        });

        let selected_symbol = tasks[0].symbol.clone();

        tasks.retain(|task| task.symbol == selected_symbol);
        tasks.sort_by(|a, b| a.input_path.cmp(&b.input_path));

        assert!(
            tasks.len() > source_index,
            "missing source index {source_index} for {exchange:?}/{instrument_type}/{category}"
        );

        let selected_id = tasks[source_index].task_id.0;

        tasks.retain(|task| task.task_id.0 == selected_id);
        assert_eq!(tasks.len(), 1);
    }

    let mut run =
        TestRun::new("exchanges/prepared_real_depth").expect("create isolated depth test run");
    let config = run
        .runtime_from_general()
        .expect("load production general.toml");
    let resolved = run
        .prepare(&batch, &config)
        .expect("resolve prepared depth source");
    assert_eq!(resolved.jobs.len(), 1);
    assert_eq!(resolved.jobs[0].tasks.len(), 1);
    // Family-selected inputs must be decoded and routed by the
    // production stream executor. The isolated worker intentionally
    // rejects them because it cannot validate the selected population.
    if family_member_index.is_some() {
        return run_resolved_family_depth(&run, &resolved.jobs, &config);
    }

    let task = &resolved.jobs[0].tasks[0];
    assert!(
        task.input_path.is_file(),
        "missing archive: {}",
        task.input_path.display()
    );
    let policy = marketforge_engine::historical::normalize::resolve_sequence_policy(task)
        .expect("resolve source sequence policy");

    let _limits = LimitScope::enter(
        config.resources.normalization_limits,
        format!("prepared depth test: {}", task.input_path.display()),
    );
    let output = run.path.join("depth-parquet");
    run.owned(&output).expect("output must belong to test run");
    let mut writer = ParquetDepthWriter::new(&output, config.resources.parquet.clone())
        .expect("create depth writer");
    let mut metrics = TaskMetrics::default();
    process_depth_task_with_metrics(
        task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        policy,
    )
    .expect("process prepared depth archive");
    writer.finish().expect("finish depth writer");

    let segments = read_depth_segments(&output).expect("read reconstruction segments");
    segments
        .validate()
        .expect("validate reconstruction segments");
    let written = writer.metrics().levels_written;
    assert_eq!(
        segments.total_levels, written,
        "segment/Parquet row mismatch"
    );
    assert!(written > 0, "no canonical depth levels");
    assert!(!segments.segments.is_empty(), "no reconstruction segment");

    let boundary: DepthBoundaryManifest = serde_json::from_reader(
        File::open(output.join("boundary.json")).expect("open boundary metadata"),
    )
    .expect("deserialize boundary metadata");
    assert_eq!(boundary.version, 1);
    let initial = boundary
        .initial
        .as_ref()
        .expect("missing initial depth snapshot");
    let final_state = boundary
        .final_state
        .as_ref()
        .expect("missing final depth state");
    assert!(!initial.state.bids.is_empty() && !initial.state.asks.is_empty());
    assert!(!final_state.state.bids.is_empty() && !final_state.state.asks.is_empty());
    assert!(initial.timestamp_ns <= final_state.timestamp_ns);
    assert_eq!(initial.fingerprint, fingerprint_book(&initial.state));
    assert_eq!(
        final_state.fingerprint,
        fingerprint_book(&final_state.state)
    );
    match policy {
        SequencePolicy::Unsequenced => assert_eq!(
            boundary.continuity,
            marketforge_engine::historical::normalize::boundary::BoundaryContinuity::Unverifiable
        ),
        SequencePolicy::Consecutive | SequencePolicy::Ranged => assert_eq!(
            boundary.continuity,
            marketforge_engine::historical::normalize::boundary::BoundaryContinuity::Verified
        ),
    }
    assert!(metrics.counters.records_read > 0);
    if family_member_index.is_some() {
        assert!(
            metrics.counters.records_matched > 0,
            "selected family member produced no matched records"
        );

        assert!(metrics.counters.records_processed <= metrics.counters.records_read);
    } else {
        assert_eq!(
            metrics.counters.records_read,
            metrics.counters.records_processed
        );
    }
    assert_eq!(metrics.counters.records_rejected, 0);
    if family_member_index.is_some() {
        let contract_value = task
            .instrument
            .contract_value
            .expect("derivative contract value required");

        let contract_asset = task
            .instrument
            .contract_value_asset
            .as_deref()
            .expect("derivative contract denomination required");

        let instrument = &task.raw_schema["marketforge_source"]["instrument"];
        let base_asset = instrument["base_asset"]
            .as_str()
            .expect("authoritative base asset");
        let quote_asset = instrument["quote_asset"]
            .as_str()
            .expect("authoritative quote asset");
        assert!(contract_value > rust_decimal::Decimal::ZERO);

        let levels = initial.state.bids.iter().chain(&initial.state.asks);

        let mut checked = 0usize;

        for level in levels {
            let contracts = level
                .quantity_contracts
                .expect("canonical contract quantity missing");

            let base = level
                .quantity_base
                .expect("canonical base quantity missing");

            let quote = level
                .quantity_quote
                .expect("canonical quote quantity missing");

            match contract_asset {
                asset if asset == base_asset => {
                    assert_eq!(
                        base,
                        contracts * contract_value,
                        "linear contract-to-base conversion"
                    );

                    assert_eq!(quote, base * level.price, "linear base-to-quote conversion");
                }

                asset if asset == quote_asset => {
                    assert_eq!(
                        quote,
                        contracts * contract_value,
                        "inverse contract-to-quote conversion"
                    );

                    assert_eq!(
                        base,
                        quote / level.price,
                        "inverse quote-to-base conversion"
                    );
                }

                other => panic!("unsupported test contract denomination: {other}"),
            }

            checked += 1;
        }

        assert!(checked > 0, "no derivative levels checked");

        println!(
            "Contract conversion verified: {} levels, {} {} per contract",
            checked, contract_value, contract_asset
        );
    }

    verify_parquet_replay(&output, &boundary, written);
    println!(
        "PREPARED DEPTH PASS: task={} stream={} source={} records={} levels={} segments={}",
        task.task_id.0,
        task.stream_id.0,
        task.input_path.display(),
        metrics.counters.records_read,
        written,
        segments.segments.len()
    );
    DepthFormatResult {
        records_read: metrics.counters.records_read,
        records_processed: metrics.counters.records_processed,
        records_rejected: metrics.counters.records_rejected,
        levels_written: written,
        segment_count: segments.segments.len(),
        boundary,
    }
}

/// Execute one explicitly selected concrete member of a prepared family
/// through the production stream-routing pipeline.
fn run_resolved_family_depth(
    run: &crate::support::runtime::TestRun,
    jobs: &[marketforge_engine::historical::job::ProcessingJob],
    config: &marketforge_engine::historical::normalize::config::ProcessingConfig,
) -> DepthFormatResult {
    use marketforge_engine::historical::normalize::{
        boundary::BoundaryContinuity, session::execute_session_tasks,
    };
    use marketforge_engine::shared::domain::book::SequencePolicy;
    use marketforge_engine::shared::persistence::parquet::read_depth_segments;
    use rust_decimal::Decimal;

    let job = &jobs[0];
    assert_eq!(job.tasks.len(), 1);

    let task = &job.tasks[0];

    assert!(task.input_path.is_file());

    let staging = &job.output.staging_path;

    run.owned(staging)
        .expect("family output must belong to isolated run");

    println!(
        "FAMILY SESSION: task={} instrument={} source={}",
        task.task_id.0,
        task.symbol,
        task.input_path.display(),
    );

    let session = execute_session_tasks(jobs, config)
        .expect("execute family through production stream routing");

    assert_eq!(session.jobs.len(), 1);
    assert_eq!(session.total_tasks, 1);

    let executions = &session.jobs[0].executions;
    assert_eq!(executions.len(), 1);

    let execution = &executions[0];

    assert_eq!(execution.task_id, task.task_id.0);

    assert!(
        execution.is_success(),
        "family task failed: {:?}",
        execution.error
    );

    let metrics = &execution.metrics;
    let counters = &metrics.counters;

    assert!(counters.records_read > 0);
    assert!(
        counters.records_matched > 0,
        "selected concrete instrument produced no matched records"
    );

    assert_eq!(counters.records_rejected, 0);
    assert_eq!(counters.tasks_completed, 1);
    assert_eq!(counters.tasks_failed, 0);

    assert!(!execution.output_paths.is_empty());

    // Production writes stream-oriented artifacts. Find the stream's
    // persisted boundary instead of assuming the legacy tasks/ layout.
    let mut boundaries = Vec::new();
    let mut pending = vec![staging.to_path_buf()];

    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).expect("inspect staging directory") {
            let entry = entry.expect("read staging entry");
            let path = entry.path();

            if path.is_dir() {
                pending.push(path);
            } else if path
                .file_name()
                .is_some_and(|name| name == std::ffi::OsStr::new("boundary.json"))
            {
                boundaries.push(path);
            }
        }
    }

    assert_eq!(
        boundaries.len(),
        1,
        "expected exactly one selected concrete depth stream; found {:?}",
        boundaries
    );

    let boundary_path = &boundaries[0];
    let output = boundary_path
        .parent()
        .expect("boundary must have a stream directory");

    // Production session output uses a V2 stream boundary
    // containing independently validated V1 source boundaries.
    let stream_boundaries =
        marketforge_engine::shared::persistence::parquet::read_depth_stream_boundaries(output)
            .expect("read production V2 stream boundaries");

    assert_eq!(stream_boundaries.version, 2);
    assert_eq!(stream_boundaries.continuity, "independent_sources");

    assert_eq!(
        stream_boundaries.sources.len(),
        1,
        "expected one selected source in this family stream"
    );

    let source = &stream_boundaries.sources[0];

    assert_eq!(source.task_id, task.task_id.0);
    assert_eq!(source.start_row, 0);
    assert_eq!(source.start_event, 0);

    let boundary = source.boundary.clone();

    assert_eq!(boundary.version, 1);

    let segments = read_depth_segments(output).expect("read stream reconstruction segments");

    segments
        .validate()
        .expect("validate reconstruction segment manifest");

    let written = segments.total_levels;

    assert_eq!(
        source.end_row, written,
        "V2 source row range differs from reconstruction inventory"
    );

    assert!(
        source.end_event > source.start_event,
        "selected source produced no persisted depth events"
    );

    assert!(written > 0);
    assert!(!segments.segments.is_empty());

    let initial = boundary.initial.as_ref().expect("missing initial snapshot");

    let final_state = boundary.final_state.as_ref().expect("missing final book");

    assert!(!initial.state.bids.is_empty());
    assert!(!initial.state.asks.is_empty());

    assert!(!final_state.state.bids.is_empty());
    assert!(!final_state.state.asks.is_empty());

    assert!(initial.timestamp_ns <= final_state.timestamp_ns);

    assert_eq!(initial.fingerprint, fingerprint_book(&initial.state));

    assert_eq!(
        final_state.fingerprint,
        fingerprint_book(&final_state.state)
    );

    let policy = marketforge_engine::historical::normalize::resolve_sequence_policy(task)
        .expect("resolve family sequence policy");

    match policy {
        SequencePolicy::Unsequenced => {
            assert_eq!(boundary.continuity, BoundaryContinuity::Unverifiable);
        }
        SequencePolicy::Consecutive | SequencePolicy::Ranged => {
            assert_eq!(boundary.continuity, BoundaryContinuity::Verified);
        }
    }

    // Independently validate the conversion arithmetic using
    // authoritative contract specifications from the prepared batch.
    let value = task
        .instrument
        .contract_value
        .expect("missing authoritative contract value");

    let denomination = task
        .instrument
        .contract_value_asset
        .as_deref()
        .expect("missing authoritative contract denomination");

    let instrument = &task.raw_schema["marketforge_source"]["instrument"];
    let base_asset = instrument["base_asset"]
        .as_str()
        .expect("authoritative base asset");
    let quote_asset = instrument["quote_asset"]
        .as_str()
        .expect("authoritative quote asset");
    assert!(value > Decimal::ZERO);

    let mut verified = 0usize;

    for level in initial.state.bids.iter().chain(&initial.state.asks) {
        let contracts = level
            .quantity_contracts
            .expect("missing canonical contracts");

        let base = level
            .quantity_base
            .expect("missing canonical base quantity");

        let quote = level
            .quantity_quote
            .expect("missing canonical quote quantity");

        match denomination {
            asset if asset == base_asset => {
                assert_eq!(
                    base,
                    contracts * value,
                    "incorrect contract-to-base conversion"
                );

                assert_eq!(
                    quote,
                    base * level.price,
                    "incorrect base-to-quote conversion"
                );
            }

            asset if asset == quote_asset => {
                assert_eq!(
                    quote,
                    contracts * value,
                    "incorrect inverse contract-to-quote conversion"
                );

                assert_eq!(
                    base,
                    quote / level.price,
                    "incorrect inverse quote-to-base conversion"
                );
            }

            other => panic!("unexpected contract denomination: {other}"),
        }

        verified += 1;
    }

    assert!(verified > 0);

    // This reuses the independent Parquet replay implementation already
    // exercised by the other real-depth integration tests.
    verify_parquet_replay(output, &boundary, written);

    println!(
        "OKX FAMILY PASS: symbol={} records={} matched={} levels={} \
         segments={} contract={} {}",
        task.symbol,
        counters.records_read,
        counters.records_matched,
        written,
        segments.segments.len(),
        value,
        denomination
    );

    DepthFormatResult {
        records_read: counters.records_read,
        records_processed: counters.records_processed,
        records_rejected: counters.records_rejected,
        levels_written: written,
        segment_count: segments.segments.len(),
        boundary,
    }
}
