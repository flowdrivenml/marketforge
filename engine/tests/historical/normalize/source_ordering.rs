use super::{
    integrity_accounting::{allow, input, subset},
    support::runtime::*,
};
use marketforge_engine::{
    historical::{
        config::*,
        job::*,
        normalize::{self, boundary::DepthBoundaryManifest, metrics::*, worker::*},
    },
    shared::{
        domain::canonical::{L2LevelUpdate, Trade},
        error::{MarketForgeError, Result},
        formats::depth::DepthProcessingOutcome,
        io::source::with_validated_source_reader,
    },
};
use std::{fs, io::Write};
#[derive(Default)]
struct Trades(Vec<Trade>);
impl TradeSink for Trades {
    fn write_trade(&mut self, t: Trade) -> Result<()> {
        self.0.push(t);
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
#[derive(Default)]
struct Depth(Vec<L2LevelUpdate>);
impl DepthSink for Depth {
    fn write_outcome(&mut self, o: DepthProcessingOutcome) -> Result<()> {
        self.0.extend(o.events);
        Ok(())
    }
    fn write_boundary(&mut self, _: DepthBoundaryManifest) -> Result<()> {
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
fn trade_bytes(times: &[i64]) -> String {
    let mut text = "id,timestamp,price,volume,side,rpi\n".to_string();
    for (id, t) in times.iter().enumerate() {
        text += &format!("{id},{t},100,1,buy,0\n");
    }
    text
}
#[test]
fn trade_equal_and_increasing_timestamps_pass() {
    for times in [vec![100, 101, 102], vec![100, 100, 101]] {
        let mut b = subset(false, false);
        let mut run = TestRun::new("ordering_trade_valid").unwrap();
        input(&run, &mut b, trade_bytes(&times).as_bytes(), false);
        let c = run.runtime(Default::default()).unwrap();
        let resolved = run.prepare(&b, &c).unwrap();
        let mut m = TaskMetrics::default();
        let mut sink = Trades::default();
        process_trade_task_with_metrics(
            &resolved.jobs[0].tasks[0],
            &mut sink,
            &mut m,
            &c.integrity_policy,
            &Default::default(),
        )
        .unwrap();
        assert_eq!(m.scoped_integrity.global.timestamp_regression.eligible, 2);
        assert_eq!(m.scoped_integrity.global.timestamp_regression.violations, 0);
        assert_eq!(sink.0.len(), 3);
        run.finish(&Ok(())).unwrap();
    }
}
#[test]
fn trade_regression_fails_or_degrades_without_silent_drop_or_sort() {
    for degrade in [false, true] {
        let mut b = subset(false, false);
        let mut run = TestRun::new("ordering_trade_regression").unwrap();
        input(&run, &mut b, trade_bytes(&[100, 99, 101]).as_bytes(), false);
        let mut c = run.runtime(Default::default()).unwrap();
        if degrade {
            allow(&mut c.integrity_policy.timestamp_regression);
        }
        let resolved = run.prepare(&b, &c).unwrap();
        let mut m = TaskMetrics::default();
        let mut sink = Trades::default();
        let result = process_trade_task_with_metrics(
            &resolved.jobs[0].tasks[0],
            &mut sink,
            &mut m,
            &c.integrity_policy,
            &Default::default(),
        );
        assert_eq!(result.is_ok(), degrade);
        assert_eq!(m.scoped_integrity.global.timestamp_regression.violations, 1);
        assert_eq!(m.integrity.diagnostics[0].source_record, Some(2));
        assert!(m.integrity.diagnostics[0].message.contains("100000000"));
        if degrade {
            assert_eq!(
                sink.0
                    .iter()
                    .map(|t| t.envelope.event_timestamp_ns)
                    .collect::<Vec<_>>(),
                vec![100_000_000, 99_000_000, 101_000_000]
            );
        } else {
            assert_eq!(sink.0.len(), 1);
            assert_eq!(m.counters.records_rejected, 1);
        }
        run.finish(&Ok(())).unwrap();
    }
}
fn depth_event(kind: &str, u: u64, ts: u64, q: &str) -> String {
    let asks = if kind == "snapshot" {
        serde_json::json!([["101", "1"]])
    } else {
        serde_json::json!([])
    };
    format!(
        "{}\n",
        serde_json::json!({"type":kind,"ts":ts,"cts":ts,"data":{"s":"BTCUSDT","u":u,"b":[["100",q]],"a":asks}})
    )
}

#[test]
fn depth_regression_obeys_policy_and_preserves_sequence_order() {
    for degrade in [false, true] {
        let mut b = subset(true, false);
        let mut run = TestRun::new("ordering_depth_regression").unwrap();
        let text = depth_event("snapshot", 1, 100, "1")
            + &depth_event("delta", 2, 99, "2")
            + &depth_event("delta", 3, 101, "3");
        input(&run, &mut b, text.as_bytes(), true);
        let mut c = run.runtime(Default::default()).unwrap();
        if degrade {
            allow(&mut c.integrity_policy.timestamp_regression);
        }
        let r = run.prepare(&b, &c).unwrap();
        let t = &r.jobs[0].tasks[0];
        let mut m = TaskMetrics::default();
        let mut sink = Depth::default();
        let result = process_depth_task_with_metrics(
            t,
            &mut sink,
            &mut m,
            &c.integrity_policy,
            &Default::default(),
            normalize::resolve_sequence_policy(t).unwrap(),
        );
        assert_eq!(result.is_ok(), degrade);
        assert_eq!(m.scoped_integrity.global.timestamp_regression.violations, 1);
        if degrade {
            let bids: Vec<_> = sink
                .0
                .iter()
                .filter(|e| e.price == rust_decimal::Decimal::from(100))
                .map(|e| e.quantity_base.unwrap())
                .collect();
            assert_eq!(bids, vec![1.into(), 2.into(), 3.into()]);
            assert_eq!(m.scoped_integrity.global.sequence_gap.violations, 0);
        } else {
            assert_eq!(sink.0.len(), 2);
            assert_eq!(m.counters.records_processed, 1);
        }
        run.finish(&Ok(())).unwrap();
    }
}
#[test]
fn malformed_depth_invalidates_state_until_authoritative_snapshot() {
    let mut b = subset(true, false);
    let mut run = TestRun::new("source_depth_recovery").unwrap();
    let text = depth_event("snapshot", 1, 100, "1")
        + "{malformed}\n"
        + &depth_event("delta", 2, 102, "99")
        + &depth_event("snapshot", 10, 103, "2");
    input(&run, &mut b, text.as_bytes(), true);
    let mut c = run.runtime(Default::default()).unwrap();
    allow(&mut c.integrity_policy.parse_failure);
    allow(&mut c.integrity_policy.missing_snapshot);
    let r = run.prepare(&b, &c).unwrap();
    let t = &r.jobs[0].tasks[0];
    let mut m = TaskMetrics::default();
    let mut sink = Depth::default();
    process_depth_task_with_metrics(
        t,
        &mut sink,
        &mut m,
        &c.integrity_policy,
        &Default::default(),
        normalize::resolve_sequence_policy(t).unwrap(),
    )
    .unwrap();
    assert_eq!(m.counters.records_rejected, 2);
    assert_eq!(m.scoped_integrity.global.parse_failure.violations, 1);
    assert_eq!(m.scoped_integrity.global.missing_snapshot.violations, 1);
    assert!(!sink.0.iter().any(|e| e.quantity_base == Some(99.into())));
    assert_eq!(sink.0.len(), 4);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn truncated_gzip_is_fatal_even_with_disabled_policy_and_no_dataset_commit() {
    let mut b = subset(false, false);
    let mut run = TestRun::new("source_truncated_gzip").unwrap();
    input(&run, &mut b, trade_bytes(&[100, 101]).as_bytes(), false);
    let path = &b.jobs[0].tasks[0].input_path;
    let mut bytes = fs::read(path).unwrap();
    bytes.truncate(bytes.len() - 5);
    fs::write(path, bytes).unwrap();
    let mut c = run.runtime(Default::default()).unwrap();
    c.integrity_policy.enabled = false;
    let r = run.prepare(&b, &c).unwrap();
    let j = &r.jobs[0];
    let result = normalize::execute_processing_job(j, &c).unwrap();
    assert_eq!(result.status, normalize::ProcessingStatus::Failed);
    assert_eq!(
        result.failure.unwrap().code,
        normalize::ProcessingErrorCode::CorruptSource
    );
    assert!(!j.output.dataset_path.exists());
    let failure: serde_json::Value =
        serde_json::from_slice(&fs::read(j.output.staging_path.join("failure.json")).unwrap())
            .unwrap();
    assert_eq!(
        failure["failed_task_id"],
        serde_json::json!(j.tasks[0].task_id)
    );
    assert!(
        failure["error_message"]
            .as_str()
            .unwrap()
            .contains("failed to read source")
    );
    run.finish(&Ok(())).unwrap();
}
#[test]
fn corrupt_jsonl_gzip_terminates_instead_of_becoming_repeated_parse_errors() {
    let source = super::support::fixtures::workload().unwrap();
    let mut b = super::support::fixtures::profile_source(
        &source,
        marketforge_engine::shared::domain::canonical::Exchange::GateIo,
        InstrumentKind::Perpetual,
        "linear",
        ContentType::Depth,
        Some(Feed::Snapshot),
        false,
    )
    .unwrap();
    let mut run = TestRun::new("source_corrupt_jsonl_gzip").unwrap();
    input(
        &run,
        &mut b,
        b"{\"update\":1,\"bids\":[{\"p\":\"100\",\"s\":2}],\"asks\":[{\"p\":\"101\",\"s\":2}]}\n",
        false,
    );
    let path = &b.jobs[0].tasks[0].input_path;
    let mut bytes = fs::read(path).unwrap();
    bytes.truncate(bytes.len() - 5);
    fs::write(path, bytes).unwrap();
    let mut c = run.runtime(Default::default()).unwrap();
    c.integrity_policy.enabled = false;
    let resolved = run.prepare(&b, &c).unwrap();
    let t = &resolved.jobs[0].tasks[0];
    let mut m = TaskMetrics::default();
    let result = process_depth_task_with_metrics(
        t,
        &mut Depth::default(),
        &mut m,
        &c.integrity_policy,
        &Default::default(),
        normalize::resolve_sequence_policy(t).unwrap(),
    );
    assert!(matches!(result, Err(MarketForgeError::SourceRead { .. })));
    assert_eq!(m.scoped_integrity.global.parse_failure.violations, 0);
    assert!(m.counters.records_read <= 1);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn tar_gzip_footer_and_missing_members_are_fatal() {
    let mut run = TestRun::new("source_tar_validation").unwrap();
    let path = run.path.join("source.tar.gz");
    let e = flate2::write::GzEncoder::new(
        fs::File::create(&path).unwrap(),
        flate2::Compression::default(),
    );
    let mut tar = tar::Builder::new(e);
    let mut h = tar::Header::new_gnu();
    h.set_size(3);
    h.set_mode(0o644);
    h.set_cksum();
    tar.append_data(&mut h, "required.data", &b"{}\n"[..])
        .unwrap();
    tar.into_inner().unwrap().finish().unwrap();
    assert!(matches!(
        with_validated_source_reader(&path, SourceContainer::TarGzip, Some("absent"), |_| Ok(())),
        Err(MarketForgeError::ArchiveMemberNotFound { .. })
    ));
    let mut bytes = fs::read(&path).unwrap();
    bytes.truncate(bytes.len() - 5);
    fs::write(&path, bytes).unwrap();
    let result = with_validated_source_reader(
        &path,
        SourceContainer::TarGzip,
        Some("required.data"),
        |reader| {
            let mut text = String::new();
            reader
                .read_to_string(&mut text)
                .map_err(|source| MarketForgeError::SourceRead {
                    path: path.clone(),
                    source,
                })?;
            Ok(text)
        },
    );
    assert!(matches!(result, Err(MarketForgeError::SourceRead { .. })));
    run.finish(&Ok(())).unwrap();
}
#[test]
fn unrelated_jobs_keep_independent_failure_boundaries() {
    let mut b = subset(false, false);
    let mut run = TestRun::new("source_independent_session").unwrap();
    input(&run, &mut b, trade_bytes(&[50, 51]).as_bytes(), false);
    let mut bad = b.jobs[0].clone();
    bad.job_id.0 += 1;
    bad.dataset_id.0 += 1;
    bad.tasks[0].input_path = run.path.join("absent");
    b.jobs.push(bad);
    let c = run
        .runtime(Overrides {
            workers: Some(2),
            ..Default::default()
        })
        .unwrap();
    let r = run.prepare(&b, &c).unwrap();
    let result = normalize::session::execute_processing_session(&r.jobs, &c).unwrap();
    assert_eq!(result.jobs[0].status, normalize::ProcessingStatus::Complete);
    assert_eq!(result.jobs[1].status, normalize::ProcessingStatus::Failed);
    assert_eq!(
        result.jobs[1].failure.as_ref().unwrap().code,
        normalize::ProcessingErrorCode::MissingSource
    );
    assert!(r.jobs[0].output.dataset_path.join("manifest.json").exists());
    assert!(!r.jobs[1].output.dataset_path.exists());
    run.finish(&Ok(())).unwrap();
}
#[test]
fn independent_family_streams_do_not_share_timestamp_watermarks() {
    use super::support::fixtures::*;
    use marketforge_engine::shared::domain::canonical::Exchange;
    let original = workload().unwrap();
    let mut b = super::support::fixtures::profile_source(
        &original,
        Exchange::Okx,
        InstrumentKind::Future,
        "linear",
        ContentType::Trades,
        None,
        true,
    )
    .unwrap();
    let mut run = TestRun::new("ordering_family_streams").unwrap();
    b.instruments.retain(|instrument| {
        !["BTC-USD_UM-261030", "BTC-USD_UM-261225"].contains(&instrument.symbol.as_str())
    });
    // The following records and instruments define an independent synthetic
    // family; only the production profile is borrowed from the prepared batch.
    b.jobs[0].tasks[0].scope.as_mut().unwrap().identifier = "BTC-USD_UM".into();
    b.jobs[0].tasks[0].scope.as_mut().unwrap().members.clear();
    b.jobs[0].tasks[0].instrument_selection = Some(InstrumentSelection::Include {
        symbols: vec!["BTC-USD_UM-261030".into(), "BTC-USD_UM-261225".into()],
    });
    let next_id = b
        .instruments
        .iter()
        .map(|i| i.instrument_id)
        .max()
        .unwrap_or(0)
        + 1;
    for (id, symbol, value) in [
        (next_id, "BTC-USD_UM-261030", "0.01"),
        (next_id + 1, "BTC-USD_UM-261225", "0.02"),
    ] {
        b.instruments.push(OfflineInstrument {
            instrument_id: id,
            exchange: Exchange::Okx,
            instrument_type: InstrumentKind::Future,
            market_category: MarketCategory::Linear,
            symbol: symbol.into(),
            base_asset: "BTC".into(),
            quote_asset: "USD".into(),
            instrument: InstrumentMetadata {
                tick_size: "0.1".parse().unwrap(),
                contract_value: Some(value.parse().unwrap()),
                contract_value_asset: Some("BTC".into()),
            },
        });
        b.jobs[0].tasks[0]
            .scope
            .as_mut()
            .unwrap()
            .members
            .push(FamilyMember {
                instrument_id: id,
                archive_member: None,
            });
    }
    let text=b"instrument_name,trade_id,side,price,size,created_time,source\nBTC-USD_UM-261030,1,buy,100,1,100,0\nBTC-USD_UM-261225,2,buy,100,1,50,0\nBTC-USD_UM-261030,3,buy,100,1,101,0\nBTC-USD_UM-261225,4,buy,100,1,51,0\n";
    input(&run, &mut b, text, true);
    let c = run.runtime(Default::default()).unwrap();
    let resolved = run.prepare(&b, &c).unwrap();
    assert_eq!(resolved.jobs[0].tasks.len(), 2);
    let result = normalize::execute_processing_job(&resolved.jobs[0], &c).unwrap();
    assert_eq!(
        result.status,
        normalize::ProcessingStatus::Complete,
        "{:?}",
        result.failure
    );
    let manifest: normalize::manifest::DatasetManifest = serde_json::from_slice(
        &fs::read(resolved.jobs[0].output.dataset_path.join("manifest.json")).unwrap(),
    )
    .unwrap();
    for task in &resolved.jobs[0].tasks {
        let metrics = manifest
            .processing_metrics
            .tasks
            .iter()
            .find(|m| m.stream_id == task.stream_id.0)
            .unwrap();
        assert_eq!(
            metrics
                .scoped_integrity
                .global
                .timestamp_regression
                .eligible,
            1
        );
        assert_eq!(
            metrics
                .scoped_integrity
                .global
                .timestamp_regression
                .violations,
            0
        );
        assert_eq!(metrics.counters.events_written, 2);
    }
    // Independently verify concrete ownership after the actual family router.
    let mut rows = Vec::new();
    for file in &manifest.files {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            fs::File::open(resolved.jobs[0].output.dataset_path.join(&file.path)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let ids = batch
                .column_by_name("instrument_id")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            let trade_ids = batch
                .column_by_name("trade_id")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((ids.value(i), trade_ids.value(i).to_owned()));
            }
        }
    }
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (next_id, "1".into()),
            (next_id, "3".into()),
            (next_id + 1, "2".into()),
            (next_id + 1, "4".into())
        ]
    );
    assert_eq!(manifest.streams.len(), 2);
    assert_eq!(manifest.events_written, 4);
    assert!(
        manifest
            .streams
            .iter()
            .all(|stream| stream.sources.len() == 1)
    );
    run.finish(&Ok(())).unwrap();
}
#[test]
fn unsupported_stateful_sorting_is_rejected_explicitly() {
    use super::support::fixtures::*;
    use marketforge_engine::shared::domain::canonical::Exchange;
    let original = workload().unwrap();
    let mut b = super::support::fixtures::profile_source(
        &original,
        Exchange::GateIo,
        InstrumentKind::Perpetual,
        "linear",
        ContentType::Depth,
        Some(Feed::Snapshot),
        false,
    )
    .unwrap();
    b.jobs[0].tasks[0].source_options = Some(SourceOptions {
        feed: Feed::Incremental,
    });
    let mut run = TestRun::new("ordering_unsupported_stateful_sort").unwrap();
    input(&run, &mut b, b"1,set,100,1,1,0\n", false);
    let c = run.runtime(Default::default()).unwrap();
    let resolved = run.prepare(&b, &c).unwrap();
    let t = &resolved.jobs[0].tasks[0];
    let error = process_depth_task_with_metrics(
        t,
        &mut Depth::default(),
        &mut TaskMetrics::default(),
        &c.integrity_policy,
        &Default::default(),
        normalize::resolve_sequence_policy(t).unwrap(),
    )
    .unwrap_err();
    assert!(matches!(error, MarketForgeError::InvalidConfiguration(_)));
    assert!(error.to_string().contains("no supported sorting path"));
    run.finish(&Ok(())).unwrap();
}
#[test]
fn zip_member_crc_failure_and_damaged_workbook_are_terminal() {
    let mut run = TestRun::new("source_zip_crc").unwrap();
    let path = run.path.join("crc.zip");
    let mut writer = zip::ZipWriter::new(fs::File::create(&path).unwrap());
    writer
        .start_file(
            "data",
            zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored),
        )
        .unwrap();
    writer.write_all(b"CRC_VALIDATION_PAYLOAD").unwrap();
    writer.finish().unwrap();
    let mut bytes = fs::read(&path).unwrap();
    let offset = bytes
        .windows(22)
        .position(|b| b == b"CRC_VALIDATION_PAYLOAD")
        .unwrap();
    bytes[offset] ^= 1;
    fs::write(&path, bytes).unwrap();
    let result = with_validated_source_reader(&path, SourceContainer::Zip, None, |reader| {
        let mut bytes = vec![];
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| MarketForgeError::SourceRead {
                path: path.clone(),
                source,
            })?;
        Ok(())
    });
    assert!(matches!(result, Err(MarketForgeError::SourceRead { .. })));
    run.finish(&Ok(())).unwrap();
    use super::support::fixtures::*;
    use marketforge_engine::shared::domain::canonical::Exchange;
    let original = workload().unwrap();
    let mut b = super::support::fixtures::profile_source(
        &original,
        Exchange::Bitget,
        InstrumentKind::Spot,
        "spot",
        ContentType::Depth,
        None,
        false,
    )
    .unwrap();
    let mut run = TestRun::new("source_bad_workbook").unwrap();
    input(&run, &mut b, b"broken workbook", true);
    let mut c = run.runtime(Default::default()).unwrap();
    c.integrity_policy.enabled = false;
    let r = run.prepare(&b, &c).unwrap();
    let result = normalize::execute_processing_job(&r.jobs[0], &c).unwrap();
    assert_eq!(result.status, normalize::ProcessingStatus::Failed);
    assert_eq!(
        result.failure.unwrap().code,
        normalize::ProcessingErrorCode::CorruptSource
    );
    assert!(!r.jobs[0].output.dataset_path.exists());
    run.finish(&Ok(())).unwrap();
}
#[test]
fn task_local_ordering_does_not_claim_cross_archive_chronology() {
    let mut b = subset(false, false);
    let mut run = TestRun::new("ordering_task_boundaries").unwrap();
    input(&run, &mut b, trade_bytes(&[100]).as_bytes(), false);
    let mut next = b.jobs[0].tasks[0].clone();
    next.task_id.0 += 1;
    let next_path = run.path.join("next.archive");
    let mut e = flate2::write::GzEncoder::new(
        fs::File::create(&next_path).unwrap(),
        flate2::Compression::default(),
    );
    e.write_all(trade_bytes(&[50]).as_bytes()).unwrap();
    e.finish().unwrap();
    next.input_path = next_path;
    b.jobs[0].tasks.push(next);
    let c = run.runtime(Default::default()).unwrap();
    let resolved = run.prepare(&b, &c).unwrap();
    for t in &resolved.jobs[0].tasks {
        let mut m = TaskMetrics::default();
        process_trade_task_with_metrics(
            t,
            &mut Trades::default(),
            &mut m,
            &c.integrity_policy,
            &Default::default(),
        )
        .unwrap();
        assert_eq!(m.scoped_integrity.global.timestamp_regression.eligible, 0);
    }
    run.finish(&Ok(())).unwrap();
}
