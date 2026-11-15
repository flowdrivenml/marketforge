use super::configuration::{Temp, formats, lightweight, root};
use marketforge_engine::{
    historical::{
        config::load_document_with_catalog,
        job::*,
        normalize::{
            boundary::DepthBoundaryManifest,
            metrics::{ScopedIntegrityMetrics, TaskMetrics},
            sequence_policy::resolve_sequence_policy,
            worker::{DepthSink, process_depth_task_with_metrics},
        },
    },
    shared::{
        domain::canonical::{
            DepthEventBoundary, DepthSourceEventMetadata, DepthSourceOperation, L2LevelUpdate,
        },
        error::Result,
        formats::depth::DepthProcessingOutcome,
    },
};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Write,
};

fn representatives() -> BTreeMap<String, serde_json::Value> {
    [
        (
            "gateio.spot.spot.gate-b1.v1",
            "gateio.spot.spot.depth:BTC_USDT",
        ),
        (
            "bybit.spot.spot.bybit-t1.v1",
            "bybit.spot.spot.trades:BTCUSDT",
        ),
    ]
    .into_iter()
    .map(|(key, label)| {
        (
            key.into(),
            serde_json::to_value(crate::support::migration::template(label).unwrap()).unwrap(),
        )
    })
    .collect()
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
        ProcessingStatus, execute_processing_job, session::execute_processing_session,
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
        job.tasks.truncate(1);
        job.output.staging_path = temp.0.join("staging");
        job.output.dataset_path = temp.0.join("dataset");
        let mut config = marketforge_engine::shared::historical::load_processing_config(
            "data/.config/historical/general.toml",
            root(),
        )
        .unwrap();
        config.merge_fan_in = 8;
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
