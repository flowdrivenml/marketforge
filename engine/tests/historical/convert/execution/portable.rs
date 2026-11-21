use std::{fs, path::Path, sync::mpsc};

use arrow_array::{Array, Decimal256Array, ListArray, StructArray, UInt8Array};
use arrow_buffer::i256;
use marketforge_engine::historical::{
    convert::{execution::*, output::*, progress::*, reference::PhysicalReferenceReader, *},
    job::{ConversionPathPolicy, ConvertJob, InstrumentKind, MergeTimelineOrdering},
    merge::{PhysicalMergeManifest, UnifiedMergeMode},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{arrow_comparison::compare_datasets, conversion_fixture::canonical_job, market};

fn plan(job: &mut ConvertJob, source: &PhysicalMergeManifest) -> ConversionPlan {
    job.source = market::prepared_binding(&job.input_merge, source);
    job.external_reference = job.conversion.reference_merge.as_ref().map(|path| {
        let manifest =
            serde_json::from_slice(&fs::read(path.join("manifest.json")).unwrap()).unwrap();
        market::prepared_binding(path, &manifest)
    });
    prepare_conversion_execution(job).unwrap().plan
}

fn combined_manifest(directory: &Path) -> ConversionManifest {
    serde_json::from_reader(fs::File::open(directory.join("manifest.json")).unwrap()).unwrap()
}

fn amount(value: i128) -> i256 {
    i256::from_i128(value * 10_i128.pow(28))
}

#[test]
fn canonical_market_plan_resolves_every_stream_and_only_two_trade_references() {
    let directory = tempfile::tempdir().unwrap();
    let mut job = canonical_job();
    job.input_merge = directory.path().join("source");
    let source = market::create(&job.input_merge, UnifiedMergeMode::Combined, true);
    let plan = plan(&mut job, &source);
    assert_eq!(
        (
            plan.source_streams,
            plan.identity_streams,
            plan.convertible_streams,
            plan.unresolved_streams
        ),
        (8, 4, 4, 0)
    );
    let selection = CombinedReferenceSelection::resolve(&plan, &source, None).unwrap();
    assert_eq!(
        selection.internal_streams,
        [market::stream_id(0, false), market::stream_id(1, false)].into()
    );
    assert!(selection.external_streams.is_empty());
    for stream in plan.streams {
        let routes = match stream.source.identity.symbol.as_str() {
            "BTCUSDT" | "ETHUSDT" => vec!["USDT"],
            "ETHBTC" => vec!["BTC", "USDT"],
            "BTCUSDC" => vec!["USDC", "BTC", "USDT"],
            _ => unreachable!(),
        };
        assert_eq!(stream.paths.len(), 1);
        assert_eq!(
            stream.paths[0]
                .currencies
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>(),
            routes
        );
    }
}

#[test]
fn rejects_missing_reference_and_conflicting_committed_stream_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let mut job = canonical_job();
    job.input_merge = directory.path().join("source");
    let source = market::create(&job.input_merge, UnifiedMergeMode::Combined, true);
    let catalog = market::catalog(directory.path());
    let mut missing = job.clone();
    missing.conversion.references[0].symbol = "UNKNOWN".into();
    let error =
        resolve_conversion_catalog(&missing, &source, &catalog, directory.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("reference instrument not present")
    );
    let data = Path::new(&source.input_datasets[0].dataset_path);
    let path = data.join("processing-job.json");
    let mut committed: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    committed["tasks"][1]["instrument"]["tick_size"] = "0.2".into();
    fs::write(&path, serde_json::to_vec(&committed).unwrap()).unwrap();
    fs::write(
        data.join("processing-job.sha256"),
        format!(
            "{}  processing-job.json\n",
            marketforge_engine::historical::normalize::manifest::file_sha256(&path).unwrap()
        ),
    )
    .unwrap();
    let error = resolve_conversion_catalog(&job, &source, &catalog, directory.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("conflicting instrument metadata"),
        "{error}"
    );
}

#[test]
fn causal_combined_publications_agree_across_checkpoint_and_file_boundaries() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = canonical_job();
    job.input_merge = root.join("source");
    job.output.dataset_path = root.join("sequential");
    // The reference at 120 is future for the subsequent causal row at 110.
    job.conversion.max_staleness_ns = 30;
    let source = market::create(&job.input_merge, UnifiedMergeMode::Combined, true);
    validate_conversion_input(&job.input_merge, &source).unwrap();
    let plan = plan(&mut job, &source);
    let sequential_staging = root.join("sequential-staging");
    let metrics = materialize_combined_conversion(
        &job,
        &source,
        None,
        &plan,
        &sequential_staging,
        &market::parquet(),
        None,
    )
    .unwrap();
    assert_eq!(metrics.events_written, 12);
    assert_eq!(
        (
            metrics.trade_events,
            metrics.depth_events,
            metrics.internal_reference_updates
        ),
        (6, 6, 4)
    );
    ConversionExecutionReceipt::new_combined(job.job_id, &metrics)
        .unwrap()
        .write(&sequential_staging)
        .unwrap();
    let published = publish_combined_conversion(&job, &source, &sequential_staging, 1).unwrap();
    assert!(publish_combined_conversion(&job, &source, &sequential_staging, 1).is_err());
    let sequential = combined_manifest(&published.dataset_path);
    assert_eq!(
        sequential.output.timeline_ordering,
        Some(MergeTimelineOrdering::Causal)
    );
    assert_eq!(sequential.output.reconstruction_events, 1);
    assert_eq!(sequential.integrity.source_events_compared, 12);
    let inverted = published
        .dataset_path
        .join(&sequential.output.files[0].path);
    let batches = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(inverted).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    let statuses = batch
        .column_by_name("conversion_status")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    assert_eq!(
        statuses.values().as_ref(),
        &[0, 0, 2, 0, 1, 0, 1, 1, 1, 0, 1, 0]
    );
    let trade = batch
        .column_by_name("trade_target")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let prices = trade
        .column_by_name("price_target")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();
    assert_eq!(prices.value(4), amount(12));
    assert_eq!(prices.value(7), amount(20)); // 4 USDC / prior 2 USDC per BTC × 10 USDT per BTC.
    let depth = batch
        .column_by_name("depth_target")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let levels = depth.value(6);
    let levels = levels.as_any().downcast_ref::<StructArray>().unwrap();
    let price = levels
        .column_by_name("price_target")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();
    assert_eq!(price.value(0), amount(12)); // 2 BTC × earlier 6 USDT per BTC.
    assert_eq!(depth.value_length(10), 0);
    let references = batch
        .column_by_name("conversion_references")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(references.value_length(7), 2);

    let mut parallel_job = job.clone();
    parallel_job.resources.workers = 2;
    parallel_job.output.dataset_path = root.join("parallel");
    let parallel_staging = root.join("parallel-staging");
    let partitions =
        build_conversion_partition_plan_with_count(&job.input_merge, &source, 6).unwrap();
    let selection = CombinedReferenceSelection::resolve(&plan, &source, None).unwrap();
    let (sender, _receiver) = mpsc::channel();
    let reporter = ConversionProgressReporter::new(sender);
    let checkpoints = build_combined_reference_checkpoints_with_progress(
        &job.input_merge,
        &source,
        None,
        None,
        &selection,
        &partitions,
        &reporter,
    )
    .unwrap();
    assert_eq!(reporter.state().total().events_read, 0);
    assert_eq!(checkpoints.checkpoints.len(), 6);
    let result = execute_parallel_combined_conversion_with_progress(
        &parallel_job,
        &source,
        None,
        &plan,
        &partitions,
        &checkpoints,
        &selection,
        &parallel_staging,
        2,
        &market::parquet(),
        &reporter,
    )
    .unwrap();
    assert_eq!(result.workers_used, 2);
    let receipt = ConversionExecutionReceipt::new_combined(job.job_id, &result.metrics).unwrap();
    assert_eq!(
        reporter.state().total().events_written,
        receipt.events_written
    );
    assert_eq!(reporter.state().total().trade_events, 6);
    assert_eq!(
        reporter.state().total().failures.iter().sum::<u64>(),
        receipt.unavailable_events
    );
    assert_eq!(reporter.state().total().failures[0], 1);
    assert_eq!(reporter.state().total().depth_events, 6);
    receipt.write(&parallel_staging).unwrap();
    fs::create_dir(&parallel_job.output.dataset_path).unwrap();
    assert!(
        publish_combined_conversion(&parallel_job, &source, &parallel_staging, 2)
            .unwrap_err()
            .to_string()
            .contains("already exists")
    );
    fs::remove_dir(&parallel_job.output.dataset_path).unwrap();
    let parallel =
        publish_combined_conversion(&parallel_job, &source, &parallel_staging, 2).unwrap();
    assert_eq!(
        compare_datasets(&published.dataset_path, &parallel.dataset_path),
        12
    );
}

#[test]
fn trade_only_reader_progress_and_publication() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = canonical_job();
    job.input_merge = root.join("trades");
    job.output.dataset_path = root.join("output");
    job.resources.workers = 2;
    let source = market::create(&job.input_merge, UnifiedMergeMode::Trades, false);
    let plan = plan(&mut job, &source);
    let mut reader = PhysicalReferenceReader::new(&job.input_merge, &source, 2).unwrap();
    let first = reader.next_observation().unwrap().unwrap();
    assert_eq!(first.stream_id, market::stream_id(0, false));
    assert_eq!(first.price.mantissa(), amount(6));
    let partitions = build_conversion_partition_plan(&job.input_merge, &source, 2).unwrap();
    let checkpoints =
        build_conversion_checkpoints(&job, &job.input_merge, &source, &plan, &partitions).unwrap();
    let (sender, receiver) = mpsc::channel();
    let reporter = ConversionProgressReporter::new(sender);
    let staging = root.join("stage");
    let result = execute_parallel_conversion_with_progress(
        &job,
        &source,
        &plan,
        &partitions,
        &checkpoints,
        &staging,
        2,
        &market::parquet(),
        &reporter,
    )
    .unwrap();
    let mut completed = 0;
    for event in receiver.try_iter() {
        if let ConversionProgressEvent::PartitionCompleted { key, metrics } = event {
            assert_eq!(metrics.trade_events, metrics.events_written);
            assert_eq!(
                reporter.state().snapshots()[&key].trade_events,
                metrics.trade_events
            );
            completed += metrics.trade_events;
        }
    }
    assert_eq!(completed, 6);
    let receipt = ConversionExecutionReceipt::new(job.job_id, &result.metrics).unwrap();
    assert_eq!(
        reporter.state().total().trade_events,
        receipt.events_written
    );
    receipt.write(&staging).unwrap();
    publish_conversion(&job, &source, &staging).unwrap();
}

#[test]
fn depth_only_with_external_trade_references() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = canonical_job();
    job.input_merge = root.join("depth");
    job.output.dataset_path = root.join("output");
    let external_path = root.join("references");
    job.conversion.reference_merge = Some(external_path.clone());
    let source = market::create(&job.input_merge, UnifiedMergeMode::Depth, false);
    let external = market::create(&external_path, UnifiedMergeMode::Trades, false);
    let plan = plan(&mut job, &source);
    let (sender, _receiver) = mpsc::channel();
    let reporter = ConversionProgressReporter::new(sender);
    let staging = root.join("stage");
    let metrics = materialize_depth_conversion_with_progress(
        &job,
        &source,
        &external,
        &plan,
        &staging,
        &market::parquet(),
        None,
        &reporter,
    )
    .unwrap();
    assert_eq!(
        (
            metrics.identity_events,
            metrics.converted_events,
            metrics.unavailable_events
        ),
        (3, 3, 0)
    );
    assert_eq!(metrics.reference_updates, 4);
    assert_eq!(reporter.state().total().external_reference_updates, 4);
    let receipt = ConversionExecutionReceipt::new_depth(job.job_id, &metrics).unwrap();
    assert_eq!(
        reporter.state().total().depth_events,
        receipt.events_written
    );
    receipt.write(&staging).unwrap();
    publish_depth_conversion(&job, &source, &staging, 1).unwrap();
}

#[test]
fn instrument_selection_keeps_original_rows_and_marks_skipped() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = canonical_job();
    job.input_merge = root.join("source");
    job.output.dataset_path = root.join("output");
    job.conversion.instrument_kinds = Some(vec![InstrumentKind::Future]);
    let source = market::create(&job.input_merge, UnifiedMergeMode::Combined, true);
    let plan = plan(&mut job, &source);
    let stage = root.join("stage");
    let metrics = materialize_combined_conversion(
        &job,
        &source,
        None,
        &plan,
        &stage,
        &market::parquet(),
        None,
    )
    .unwrap();
    assert_eq!(metrics.skipped_events, 12);
    assert_eq!(metrics.internal_reference_updates, 4);
    ConversionExecutionReceipt::new_combined(job.job_id, &metrics)
        .unwrap()
        .write(&stage)
        .unwrap();
    let output = publish_combined_conversion(&job, &source, &stage, 1).unwrap();
    let manifest = combined_manifest(&output.dataset_path);
    assert_eq!(manifest.statistics.skipped, 12);
    assert_eq!(manifest.integrity.source_events_compared, 12);
}

#[test]
fn path_policy_all_tries_alternatives_and_emits_one_value() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = canonical_job();
    job.input_merge = root.join("source");
    let source = market::create(&job.input_merge, UnifiedMergeMode::Combined, true);
    let catalog = market::catalog(root);
    let resolved = resolve_conversion_catalog(&job, &source, &catalog, root).unwrap();
    for reference in resolved.sources.iter().filter(|stream| {
        stream.stream_id.ends_with("trades") && stream.identity.symbol.starts_with("ETH")
    }) {
        job.conversion.references.push(reference.identity.clone());
    }
    job.conversion.max_staleness_ns = 1;
    for (policy, expected_status) in [
        (ConversionPathPolicy::All, CONVERSION_STATUS_CONVERTED),
        (ConversionPathPolicy::Primary, CONVERSION_STATUS_UNAVAILABLE),
    ] {
        job.conversion.path_policy = policy;
        let plan = self::plan(&mut job, &source);
        let kernel = CombinedConversionKernel::new(&job, &plan).unwrap();
        let selection = CombinedReferenceSelection::resolve(&plan, &source, None).unwrap();
        let mut alignment = CombinedReferenceAlignment::new(selection, None).unwrap();
        // The graph visits ETH before USDT. The ETHBTC observation at 103 makes
        // that first path stale, so only All can fall back to the direct rate of 6.
        for (symbol, rank, ts, price) in [(0, 0, 104, 6), (2, 4, 104, 7), (3, 6, 103, 2)] {
            alignment
                .observe_internal(reference::ReferenceObservation {
                    dataset_id: 101,
                    stream_id: market::stream_id(symbol, false),
                    key: reference::ReferenceKey::new(ts, rank, 1, 0),
                    price: ConversionDecimal::new(amount(price)).unwrap(),
                })
                .unwrap();
        }
        let mut reader = PhysicalConversionReader::new(&job.input_merge, &source, 20).unwrap();
        let batch = reader.next_batch().unwrap().unwrap();
        let mut metrics = CombinedConversionMetrics::default();
        let CombinedConversionRow::Trade(result) = kernel
            .convert_event(&batch, 4, &mut alignment, &mut metrics)
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(result.status, expected_status);
        assert_eq!(metrics.events_read, 1);
        if policy == ConversionPathPolicy::All {
            assert_eq!(result.price_target.unwrap().mantissa(), amount(12));
        }
    }
}

#[cfg(feature = "monitor-tui")]
#[test]
fn dashboard_pty_restores_terminal_and_matches_plain_cli() {
    use std::process::Command;

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let cwd = tempfile::tempdir().unwrap();
    let mut job = canonical_job();
    job.input_merge = root.join("source");
    job.output.dataset_path = root.join("dashboard");
    let dashboard = job.output.dataset_path.clone();
    let source = market::create(&job.input_merge, UnifiedMergeMode::Combined, true);
    job.source = market::prepared_binding(&job.input_merge, &source);
    job.parquet = market::parquet();
    job.resources.workers = 2;
    market::remove_preparation_inputs(&source);
    let job_file = root.join("job.json");
    fs::write(&job_file, serde_json::to_vec(&job).unwrap()).unwrap();
    let binary = env!("CARGO_BIN_EXE_marketforge-convert");
    let pty = Command::new("python3")
        .current_dir(cwd.path())
        .arg(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/historical/convert/support/pty_conversion.py"),
        )
        .arg(binary)
        .arg(&job_file)
        .output()
        .unwrap();
    assert!(
        pty.status.success(),
        "{}",
        String::from_utf8_lossy(&pty.stderr)
    );

    job.output.dataset_path = root.join("plain");
    job.resources.workers = 2;
    fs::write(&job_file, serde_json::to_vec(&job).unwrap()).unwrap();
    let plain = Command::new(binary)
        .current_dir(cwd.path())
        .arg("--job")
        .arg(job_file)
        .arg("--workers")
        .arg("2")
        .arg("--run")
        .output()
        .unwrap();
    assert!(
        plain.status.success(),
        "{}",
        String::from_utf8_lossy(&plain.stderr)
    );
    assert_eq!(compare_datasets(&dashboard, &job.output.dataset_path), 12);
}

#[test]
fn causal_depth_checkpoints_preserve_external_updates_and_publication() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = canonical_job();
    job.input_merge = root.join("depth");
    job.output.dataset_path = root.join("output");
    let references = root.join("references");
    job.conversion.reference_merge = Some(references.clone());
    let source = market::create(&job.input_merge, UnifiedMergeMode::Depth, true);
    let external = market::create(&references, UnifiedMergeMode::Trades, false);
    let plan = plan(&mut job, &source);
    let sequential_staging = root.join("sequential-stage");
    let sequential = materialize_depth_conversion(
        &job,
        &source,
        &external,
        &plan,
        &sequential_staging,
        &market::parquet(),
        None,
    )
    .unwrap();
    ConversionExecutionReceipt::new_depth(job.job_id, &sequential)
        .unwrap()
        .write(&sequential_staging)
        .unwrap();
    let output = publish_depth_conversion(&job, &source, &sequential_staging, 1).unwrap();
    assert_eq!(
        combined_manifest(&output.dataset_path)
            .output
            .reconstruction_events,
        1
    );

    let mut parallel_job = job.clone();
    parallel_job.resources.workers = 2;
    parallel_job.output.dataset_path = root.join("parallel");
    let plan_rows =
        build_conversion_partition_plan_with_count(&job.input_merge, &source, 3).unwrap();
    let prepass = build_depth_reference_checkpoints(
        &job.input_merge,
        &source,
        &references,
        &external,
        &plan,
        &plan_rows,
    )
    .unwrap();
    let (sender, _receiver) = mpsc::channel();
    let reporter = ConversionProgressReporter::new(sender);
    let staging = root.join("parallel-stage");
    let parallel = execute_parallel_depth_conversion_with_progress(
        &parallel_job,
        &source,
        &external,
        &plan,
        &plan_rows,
        &prepass,
        &staging,
        2,
        &market::parquet(),
        &reporter,
    )
    .unwrap();
    assert_eq!(parallel.workers_used, 2);
    assert_eq!(prepass.reference_updates, 4);
    assert_eq!(
        parallel.metrics.reference_updates,
        sequential.reference_updates
    );
    assert_eq!(
        reporter.state().total().external_reference_updates,
        sequential.reference_updates
    );
    ConversionExecutionReceipt::new_depth(job.job_id, &parallel.metrics)
        .unwrap()
        .write(&staging)
        .unwrap();
    let published = publish_depth_conversion(&parallel_job, &source, &staging, 2).unwrap();
    assert_eq!(
        compare_datasets(&output.dataset_path, &published.dataset_path),
        6
    );
}

#[test]
fn cancellation_prevents_publication_of_staged_output() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = canonical_job();
    job.input_merge = root.join("source");
    job.output.dataset_path = root.join("output");
    let source = market::create(&job.input_merge, UnifiedMergeMode::Combined, true);
    let plan = plan(&mut job, &source);
    let stage = root.join("stage");
    let metrics = materialize_combined_conversion(
        &job,
        &source,
        None,
        &plan,
        &stage,
        &market::parquet(),
        None,
    )
    .unwrap();
    ConversionExecutionReceipt::new_combined(job.job_id, &metrics)
        .unwrap()
        .write(&stage)
        .unwrap();
    let (sender, _receiver) = mpsc::channel();
    let reporter = ConversionProgressReporter::new(sender);
    reporter.state().interrupt();
    let result = publish_combined_conversion_with_progress(&job, &source, &stage, 1, &reporter);
    assert!(matches!(
        result,
        Err(marketforge_engine::shared::error::MarketForgeError::ConversionInterrupted)
    ));
    assert!(stage.join("execution.json").is_file());
    assert!(!stage.join("manifest.json").exists());
    assert!(!job.output.dataset_path.exists());
}

#[test]
fn combined_uses_external_trades_only_for_missing_internal_references() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = canonical_job();
    job.input_merge = root.join("source");
    job.output.dataset_path = root.join("output");
    job.resources.workers = 2;
    let references = root.join("references");
    job.conversion.reference_merge = Some(references.clone());
    let source = market::create_with_omitted_trades(
        &job.input_merge,
        UnifiedMergeMode::Combined,
        true,
        &[1],
    );
    let external = market::create(&references, UnifiedMergeMode::Trades, false);
    let plan = plan(&mut job, &source);
    let selection = CombinedReferenceSelection::resolve(&plan, &source, Some(&external)).unwrap();
    assert_eq!(
        selection.internal_streams,
        [market::stream_id(0, false)].into()
    );
    assert_eq!(
        selection.external_streams,
        [market::stream_id(1, false)].into()
    );
    let partitions =
        build_conversion_partition_plan_with_count(&job.input_merge, &source, 5).unwrap();
    let prepass = build_combined_reference_checkpoints(
        &job.input_merge,
        &source,
        Some(&references),
        Some(&external),
        &selection,
        &partitions,
    )
    .unwrap();
    assert_eq!((prepass.internal_updates, prepass.external_updates), (2, 2));
    let stage = root.join("stage");
    let combined = execute_parallel_combined_conversion(
        &job,
        &source,
        Some(&external),
        &plan,
        &partitions,
        &prepass,
        &selection,
        &stage,
        2,
        &market::parquet(),
    )
    .unwrap();
    assert_eq!(
        (
            combined.metrics.internal_reference_updates,
            combined.metrics.external_reference_updates
        ),
        (2, 2)
    );
    assert_eq!(combined.metrics.events_written, 10);
    let receipt = ConversionExecutionReceipt::new_combined(job.job_id, &combined.metrics).unwrap();
    receipt.write(&stage).unwrap();
    publish_combined_conversion(&job, &source, &stage, 2).unwrap();
}

#[test]
fn validation_cli_checks_inventory_and_planning_without_writing() {
    use std::process::Command;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = canonical_job();
    job.input_merge = root.join("source");
    job.output.dataset_path = root.join("output");
    let manifest = market::create(&job.input_merge, UnifiedMergeMode::Combined, true);
    job.source = market::prepared_binding(&job.input_merge, &manifest);
    job.parquet = market::parquet();
    market::remove_preparation_inputs(&manifest);
    let unrelated = tempfile::tempdir().unwrap();
    let job_path = root.join("job.json");
    let save = |job: &ConvertJob| {
        fs::write(&job_path, serde_json::to_vec(job).unwrap()).unwrap();
    };
    save(&job);
    let run = |mode| {
        Command::new(env!("CARGO_BIN_EXE_marketforge-convert"))
            .current_dir(unrelated.path())
            .arg("--job")
            .arg(&job_path)
            .arg(mode)
            .output()
            .unwrap()
    };
    let success = |mode| {
        let output = run(mode);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    let valid = success("--validate");
    assert!(String::from_utf8_lossy(&valid.stdout).contains("8 streams · 2 references"));
    let staging = job.output.dataset_path.with_extension("staging");
    assert!(!job.output.dataset_path.exists());
    assert!(!staging.exists());
    success("--materialize");
    assert!(staging.is_dir());
    let mut changed = job.clone();
    changed.catalog_sha256 = "a".repeat(64);
    save(&changed);
    let incompatible = run("--publish");
    assert!(!incompatible.status.success());
    assert!(String::from_utf8_lossy(&incompatible.stderr).contains("differs from materialization"));
    assert!(!job.output.dataset_path.exists());
    save(&job);
    success("--publish");
    assert!(
        job.output
            .dataset_path
            .join("conversion-runtime.json")
            .is_file()
    );
    changed.output.dataset_path = root.join("standalone-run");
    changed.catalog_sha256 = job.catalog_sha256.clone();
    save(&changed);
    success("--run");
    compare_datasets(&job.output.dataset_path, &changed.output.dataset_path);

    let primary = job.input_merge.join(&manifest.files[0].path);
    let mut bytes = fs::read(&primary).unwrap();
    bytes[4] ^= 1;
    fs::write(&primary, bytes).unwrap();
    let changed_input = run("--validate");
    assert!(!changed_input.status.success());
    assert!(String::from_utf8_lossy(&changed_input.stderr).contains("content checksum mismatch"));
    fs::remove_file(primary).unwrap();
    let missing = run("--validate");
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("cannot open"));
}

#[test]
fn prepared_job_rejects_binding_errors_before_writing() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = canonical_job();
    job.input_merge = root.join("source");
    job.output.dataset_path = root.join("output");
    let source = market::create(&job.input_merge, UnifiedMergeMode::Combined, true);
    job.source = market::prepared_binding(&job.input_merge, &source);
    let assert_invalid = |invalid: &ConvertJob, reason: &str| {
        let error = prepare_conversion_execution(invalid)
            .err()
            .expect("expected invalid job");
        assert!(error.to_string().contains(reason), "{error}");
        assert!(!invalid.output.dataset_path.exists());
        assert!(
            !invalid
                .output
                .dataset_path
                .with_extension("staging")
                .exists()
        );
    };
    let mut invalid = job.clone();
    invalid.source.streams.pop();
    assert_invalid(&invalid, "do not cover");
    invalid = job.clone();
    invalid
        .source
        .streams
        .push(invalid.source.streams[0].clone());
    assert_invalid(&invalid, "duplicate prepared");
    invalid = job.clone();
    invalid.source.streams[0].instrument_id += 100;
    assert_invalid(&invalid, "metadata mismatch");
    invalid = job.clone();
    invalid.source.streams[0].dataset_id = invalid.source.normalized_inputs[1].dataset_id;
    assert_invalid(&invalid, "metadata mismatch");
    invalid = job.clone();
    invalid.source.streams[0].quote_currency.0 = "DAI".into();
    assert_invalid(&invalid, "conflicting prepared metadata");
    invalid = job.clone();
    for binding in invalid
        .source
        .streams
        .iter_mut()
        .filter(|item| item.instrument_id == job.source.streams[0].instrument_id)
    {
        binding.quote_currency.0 = "DAI".into();
    }
    assert_invalid(&invalid, "metadata mismatch");
    invalid = job.clone();
    invalid.source.merge_job_id += 1;
    assert_invalid(&invalid, "identity");
    invalid = job.clone();
    invalid.source.manifest_sha256 = "0".repeat(64);
    assert_invalid(&invalid, "pin mismatch");
    invalid = job.clone();
    invalid.conversion.reference_merge = Some(root.join("external"));
    assert_invalid(&invalid, "must both be present");
    invalid.external_reference = Some(job.source.clone());
    assert_invalid(&invalid, "cannot read");
    invalid = job.clone();
    invalid.parquet.file_target_mb = 0;
    assert_invalid(&invalid, "Parquet settings");
}

#[test]
fn standalone_cli_uses_only_the_declared_external_trade_publication() {
    use std::process::Command;

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let mut job = canonical_job();
    job.input_merge = root.join("depth");
    job.output.dataset_path = root.join("output");
    job.parquet = market::parquet();
    let reference = root.join("reference");
    let depth = market::create(&job.input_merge, UnifiedMergeMode::Depth, true);
    let trades = market::create(&reference, UnifiedMergeMode::Trades, true);
    job.source = market::prepared_binding(&job.input_merge, &depth);
    let mut depth_as_reference = job.clone();
    depth_as_reference.conversion.reference_merge = Some(job.input_merge.clone());
    depth_as_reference.external_reference = Some(job.source.clone());
    let error = prepare_conversion_execution(&depth_as_reference)
        .err()
        .unwrap();
    assert!(error.to_string().contains("trade-only"));
    job.external_reference = Some(market::prepared_binding(&reference, &trades));
    job.conversion.reference_merge = Some(reference);
    market::remove_preparation_inputs(&depth);
    market::remove_preparation_inputs(&trades);
    let path = root.join("job.json");
    fs::write(&path, serde_json::to_vec(&job).unwrap()).unwrap();
    for flag in ["--validate", "--run"] {
        let result = Command::new(env!("CARGO_BIN_EXE_marketforge-convert"))
            .current_dir("/")
            .arg("--job")
            .arg(&path)
            .arg(flag)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    assert_eq!(
        combined_manifest(&job.output.dataset_path)
            .statistics
            .events_written,
        depth.events_written
    );
}
