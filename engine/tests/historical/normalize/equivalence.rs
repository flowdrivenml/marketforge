use super::configuration::{Temp, formats, lightweight, root};
use marketforge_engine::{
    historical::{
        config::load_document_with_catalog,
        job::*,
        normalize::{
            boundary::DepthBoundaryManifest,
            metrics::{ScopedIntegrityMetrics, TaskMetrics},
            sequence_policy::resolve_sequence_policy,
            worker::{
                DepthSink, TradeSink, process_depth_task_with_metrics,
                process_trade_task_with_metrics,
            },
        },
    },
    shared::{
        domain::canonical::{
            DepthEventBoundary, DepthSourceEventMetadata, DepthSourceOperation, L2LevelUpdate,
            Trade,
        },
        error::Result,
        formats::depth::DepthProcessingOutcome,
        io::{decode::CsvDecoder, source::with_source_reader},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Write},
};

fn representatives() -> BTreeMap<String, serde_json::Value> {
    let catalog = formats();
    let mut jobs = BTreeMap::new();
    let mut paths = fs::read_dir(root().join("data/.jobs/process"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        let mut raw: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        raw["tasks"].as_array_mut().unwrap().truncate(1);
        let task: WorkTask = serde_json::from_value(raw["tasks"][0].clone()).unwrap();
        let p = catalog.for_task(&task).unwrap().unwrap();
        jobs.entry(p.definition.id.clone()).or_insert(raw);
    }
    assert_eq!(jobs.len(), 31);
    jobs
}
fn archive(task: &mut WorkTask, directory: &Temp, bytes: &[u8]) {
    let member = task
        .archive_member
        .clone()
        .unwrap_or_else(|| "sample.data".into());
    let path = directory.0.join("small-input.archive");
    match task.source_compression {
        SourceContainer::Plain => fs::write(&path, bytes).unwrap(),
        SourceContainer::Gzip => {
            let mut writer = flate2::write::GzEncoder::new(
                File::create(&path).unwrap(),
                flate2::Compression::default(),
            );
            writer.write_all(bytes).unwrap();
            writer.finish().unwrap();
        }
        SourceContainer::Zip => {
            let mut writer = zip::ZipWriter::new(File::create(&path).unwrap());
            writer
                .start_file(&member, zip::write::SimpleFileOptions::default())
                .unwrap();
            writer.write_all(bytes).unwrap();
            writer.finish().unwrap();
        }
        SourceContainer::TarGzip => {
            let encoder = flate2::write::GzEncoder::new(
                File::create(&path).unwrap(),
                flate2::Compression::default(),
            );
            let mut tar = tar::Builder::new(encoder);
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o600);
            header.set_cksum();
            tar.append_data(&mut header, &member, bytes).unwrap();
            tar.into_inner().unwrap().finish().unwrap();
            task.archive_member = Some(member);
        }
    }
    task.input_path = path;
}
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
struct Depth {
    events: Vec<(
        DepthEventBoundary,
        DepthSourceOperation,
        Vec<L2LevelUpdate>,
        DepthSourceEventMetadata,
    )>,
    boundary: Option<serde_json::Value>,
}
impl DepthSink for Depth {
    fn write_outcome(&mut self, e: DepthProcessingOutcome) -> Result<()> {
        self.events
            .push((e.boundary, e.operation, e.events, e.source));
        Ok(())
    }
    fn write_boundary(&mut self, b: DepthBoundaryManifest) -> Result<()> {
        self.boundary = Some(serde_json::to_value(b).unwrap());
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
#[test]
fn real_trade_prefix_archives_legacy_and_toml_have_exact_events_and_counters() {
    let catalog = formats();
    let mut covered = BTreeSet::new();
    for (id, raw) in representatives() {
        let legacy: ProcessingJob = serde_json::from_value(raw.clone()).unwrap();
        if legacy.tasks[0].normalizations[0].target_schema != TargetSchema::Trade {
            continue;
        }
        let temp = Temp::new();
        let resolved =
            load_document_with_catalog(temp.job(&lightweight(raw, &catalog)), &catalog).unwrap();
        let mut old = legacy.tasks[0].clone();
        let mut new = resolved.tasks[0].clone();
        // Bound both bytes and records read from the existing archive. Trades use
        // gzip/ZIP; no whole-file TAR member inventory or large dataset processing.
        let bytes = with_source_reader(
            &old.input_path,
            old.source_compression,
            old.archive_member.as_deref(),
            |reader| {
                let mut limited = reader.take(4 * 1024 * 1024);
                let mut csv = CsvDecoder::with_headers(
                    &mut limited,
                    old.raw_schema["header"].as_bool().unwrap(),
                );
                let mut output = csv::WriterBuilder::new()
                    .has_headers(false)
                    .from_writer(vec![]);
                use marketforge_engine::shared::formats::trades::{
                    IdentityDecision, InstrumentMatcher, resolve_trade_headers,
                };
                let actual = if old.raw_schema["header"] == true {
                    Some(csv.headers().unwrap())
                } else {
                    None
                };
                if let Some(headers) = &actual {
                    output.write_byte_record(headers).unwrap();
                }
                let headers = resolve_trade_headers(&old.raw_schema, actual.as_ref()).unwrap();
                let mut matcher = InstrumentMatcher::from_task(&headers, &old).unwrap();
                let mut matched = 0;
                let mut skipped = 0;
                for row in csv.records().take(65_536) {
                    let row = row.unwrap();
                    match matcher.check(&row).unwrap() {
                        IdentityDecision::Match => {
                            output.write_byte_record(&row).unwrap();
                            matched += 1;
                        }
                        IdentityDecision::Skip if skipped < 8 => {
                            output.write_byte_record(&row).unwrap();
                            skipped += 1;
                        }
                        _ => {}
                    }
                    if matched == 8 {
                        break;
                    }
                }
                assert!(
                    matched > 0,
                    "{id}: source byte/record bound exhausted before matching instrument"
                );
                Ok(output.into_inner().unwrap())
            },
        )
        .unwrap();
        archive(&mut old, &temp, &bytes);
        new.input_path = old.input_path.clone();
        new.archive_member = old.archive_member.clone();
        let mut a = Trades::default();
        let mut b = Trades::default();
        let mut ma = TaskMetrics::default();
        let mut mb = TaskMetrics::default();
        let previous = ScopedIntegrityMetrics::default();
        let ra = process_trade_task_with_metrics(
            &old,
            &mut a,
            &mut ma,
            &legacy.integrity_policy,
            &previous,
        )
        .map_err(|e| e.to_string());
        let rb = process_trade_task_with_metrics(
            &new,
            &mut b,
            &mut mb,
            &legacy.integrity_policy,
            &previous,
        )
        .map_err(|e| e.to_string());
        assert_eq!(ra, rb, "{id}");
        assert_eq!(a.0, b.0, "{id}: canonical values");
        assert_eq!(ma, mb, "{id}: integrity and ordering");
        assert!(
            !a.0.is_empty(),
            "{id}: no matching trades in bounded prefix"
        );
        assert!(ra.is_ok(), "{id}: {ra:?}");
        covered.insert(id);
    }
    assert_eq!(covered.len(), 18);
}
fn depth_bytes(task: &WorkTask) -> Vec<u8> {
    match task.exchange {
  marketforge_engine::canonical::Exchange::Bybit=>format!("{}\n{}\n",serde_json::json!({"topic":"orderbook.200","type":"snapshot","ts":1001,"cts":1000,"data":{"s":task.symbol,"u":100,"seq":200,"b":[["99","10"]],"a":[["101","5"]]}}),serde_json::json!({"topic":"orderbook.200","type":"delta","ts":1002,"cts":1001,"data":{"s":task.symbol,"u":101,"seq":201,"b":[["99","13"]],"a":[["101","0"]]}})).into_bytes(),
  marketforge_engine::canonical::Exchange::Okx=>format!("{}\n{}\n",serde_json::json!({"instId":task.symbol,"action":"snapshot","ts":"1000","bids":[["99","10","2"]],"asks":[["101","5","1"]]}),serde_json::json!({"instId":task.symbol,"action":"update","ts":"1001","bids":[["99","13","3"]],"asks":[["101","0","0"]]})).into_bytes(),
  marketforge_engine::canonical::Exchange::GateIo=>if task.format_code.0=="GATE-B1"{b"1000,2,set,99,10,100,0\n1000,1,set,101,5,100,0\n1000.1,2,make,99,3,101,1\n1000.2,1,take,101,2,102,1\n".to_vec()}else{b"1000,set,99,10,100,0\n1000,set,101,-5,100,0\n1000.1,make,99,3,101,1\n1000.2,take,101,-2,102,1\n".to_vec()},
  marketforge_engine::canonical::Exchange::Bitget=>super::depth_xlsx_pipeline::create_xlsx(),
  other=>panic!("unsupported depth fixture {other:?}"),
 }
}
#[test]
fn small_depth_archives_legacy_and_toml_preserve_snapshots_deltas_sequences_and_errors() {
    let catalog = formats();
    let mut count = 0;
    for (id, raw) in representatives() {
        let legacy: ProcessingJob = serde_json::from_value(raw.clone()).unwrap();
        if legacy.tasks[0].normalizations[0].target_schema != TargetSchema::Depth {
            continue;
        }
        let temp = Temp::new();
        let resolved =
            load_document_with_catalog(temp.job(&lightweight(raw, &catalog)), &catalog).unwrap();
        let mut old = legacy.tasks[0].clone();
        let mut new = resolved.tasks[0].clone();
        let bytes = depth_bytes(&old);
        for malformed in [false, true] {
            let bytes = if malformed {
                b"invalid archive payload\n".as_slice()
            } else {
                bytes.as_slice()
            };
            archive(&mut old, &temp, bytes);
            new.input_path = old.input_path.clone();
            new.archive_member = old.archive_member.clone();
            let mut a = Depth::default();
            let mut b = Depth::default();
            let mut ma = TaskMetrics::default();
            let mut mb = TaskMetrics::default();
            let previous = ScopedIntegrityMetrics::default();
            let sequence = resolve_sequence_policy(&old).unwrap();
            let ra = process_depth_task_with_metrics(
                &old,
                &mut a,
                &mut ma,
                &legacy.integrity_policy,
                &previous,
                sequence,
            )
            .map_err(|e| e.to_string());
            let rb = process_depth_task_with_metrics(
                &new,
                &mut b,
                &mut mb,
                &legacy.integrity_policy,
                &previous,
                sequence,
            )
            .map_err(|e| e.to_string());
            assert_eq!(ra, rb, "{id}: errors");
            assert_eq!(a.events, b.events, "{id}: events");
            assert_eq!(a.boundary, b.boundary, "{id}: book boundaries");
            assert_eq!(ma, mb, "{id}: integrity counters");
            if !malformed {
                assert!(ra.is_ok(), "{id}: {ra:?}");
                assert!(!a.events.is_empty(), "{id}: no events");
                assert!(a.boundary.is_some());
            } else {
                assert!(
                    ra.is_err() || ma.counters.records_rejected > 0,
                    "{id}: malformed input not rejected"
                );
            }
        }
        count += 1;
    }
    assert_eq!(count, 13);
}

#[test]
fn observed_gateio_spot_snapshot_keeps_bid_and_ask_orientation() {
    use marketforge_engine::shared::domain::canonical::BookSide;
    use rust_decimal::Decimal;
    let catalog = formats();
    let raw = representatives()
        .remove("gateio.spot.spot.gate-b1.v1")
        .unwrap();
    let temp = Temp::new();
    let mut job =
        load_document_with_catalog(temp.job(&lightweight(raw, &catalog)), &catalog).unwrap();
    let task = &mut job.tasks[0];
    archive(
        task,
        &temp,
        include_bytes!("resources/gateio-spot-observed-snapshot.csv"),
    );
    let mut sink = Depth::default();
    let mut metrics = TaskMetrics::default();
    process_depth_task_with_metrics(
        task,
        &mut sink,
        &mut metrics,
        &job.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        resolve_sequence_policy(task).unwrap(),
    )
    .unwrap();
    assert_eq!(sink.events.len(), 1);
    let levels = &sink.events[0].2;
    assert_eq!(levels.len(), 2);
    let bid = levels.iter().find(|l| l.side == BookSide::Bid).unwrap();
    let ask = levels.iter().find(|l| l.side == BookSide::Ask).unwrap();
    assert_eq!(bid.price, Decimal::new(785856, 1));
    assert_eq!(ask.price, Decimal::new(785857, 1));
    assert!(bid.price < ask.price);
    assert!(bid.quantity_base.unwrap() > Decimal::ZERO);
    assert_eq!(
        bid.quantity_quote,
        Some(bid.price * bid.quantity_base.unwrap())
    );
    assert_eq!(bid.quantity_contracts, None);
    assert_eq!(metrics.counters.records_rejected, 0);
}

#[test]
fn committed_configuration_preserves_effective_runtime_settings_and_checksum() {
    use marketforge_engine::historical::normalize::{
        ProcessingConfig, ProcessingStatus, execute_processing_job,
        session::execute_processing_session,
    };
    use sha2::{Digest, Sha256};
    let catalog = formats();
    for session in [false, true] {
        let temp = Temp::new();
        let raw = representatives()
            .remove("bybit.spot.spot.bybit-t1.v1")
            .unwrap();
        let mut job =
            load_document_with_catalog(temp.job(&lightweight(raw, &catalog)), &catalog).unwrap();
        archive(
            &mut job.tasks[0],
            &temp,
            b"id,timestamp,price,volume,side,rpi\n1,1788220800000,100,2,Buy,0\n",
        );
        job.output.staging_path = temp.0.join("staging");
        job.output.dataset_path = temp.0.join("dataset");
        let mut config = ProcessingConfig {
            resources: job.resources.clone(),
            integrity_policy: job.integrity_policy.clone(),
        };
        config.resources.workers = 1;
        config.resources.parquet.row_group_target_bytes = 4096;
        config.resources.parquet.file_target_bytes = 16384;
        config.resources.scratch_path = temp.0.join("scratch");
        config.integrity_policy.enabled = false;
        let result = if session {
            execute_processing_session(std::slice::from_ref(&job), &config)
                .unwrap()
                .jobs
                .remove(0)
        } else {
            execute_processing_job(&job, &config).unwrap()
        };
        assert_eq!(result.status, ProcessingStatus::Complete);
        assert_eq!(result.events_written, 1);
        assert!(!job.output.staging_path.exists());
        let path = job.output.dataset_path.join("processing-job.json");
        let bytes = fs::read(&path).unwrap();
        let snapshot: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            snapshot["resources"],
            serde_json::to_value(&config.resources).unwrap()
        );
        assert_eq!(
            snapshot["integrity_policy"],
            serde_json::to_value(&config.integrity_policy).unwrap()
        );
        assert_eq!(snapshot["tasks"], serde_json::to_value(&job.tasks).unwrap());
        let hash: String = Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(
            fs::read_to_string(job.output.dataset_path.join("processing-job.sha256")).unwrap(),
            format!("{hash}  processing-job.json\n")
        );
        let reloaded = load_document_with_catalog(&path, &catalog).unwrap();
        assert_eq!(reloaded.tasks, job.tasks);
        assert!(job.output.dataset_path.join("manifest.json").is_file());
    }
}
