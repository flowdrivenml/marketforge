use super::*;
use crate::historical::normalize::{
    metrics::{ScopedIntegrityMetrics, TaskMetrics},
    resolve_sequence_policy,
    worker::*,
};
use crate::shared::domain::canonical::{L2LevelUpdate, Trade};
use std::io::Write;
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .into()
}
fn fixture() -> PreparedBatch {
    read_prepared_batch("engine/tests/historical/fixtures/normalize.json", root()).unwrap()
}
fn catalog() -> FormatCatalog {
    FormatCatalog::load(root().join("data/.config/historical")).unwrap()
}
fn runtime() -> ProcessingConfig {
    crate::shared::historical::load_processing_config(
        "data/.config/historical/general.toml",
        root(),
    )
    .unwrap()
}
fn selected(kind: &str, exchange: Exchange) -> (PreparedBatch, PreparedJob) {
    let b = fixture();
    let mut j = b
        .jobs
        .iter()
        .find(|j| token(&j.output.content_type).unwrap() == kind)
        .unwrap()
        .clone();
    j.tasks
        .retain(|t| t.exchange == exchange && t.instrument_type == InstrumentKind::Spot);
    j.tasks.truncate(1);
    (b, j)
}
#[test]
fn fixture_preserves_large_ids_shared_ranks_and_authority() {
    let b = fixture();
    assert!(b.jobs[0].job_id.0 > 9_007_199_254_740_992);
    for j in &b.jobs {
        let mut selected = j.clone();
        selected.tasks.retain(|t| t.scope.is_none());
        let r = b
            .resolve_job(&selected, &catalog(), &runtime(), &root())
            .unwrap();
        assert_eq!(r.tasks.len(), selected.tasks.len());
        assert_eq!(r.job_id, j.job_id);
        for t in &r.tasks {
            assert!(t.instrument_id > 0);
            assert_eq!(
                t.raw_schema["marketforge_source"]["instrument_namespace"],
                b.instrument_namespace
            );
            assert!(
                t.raw_schema["marketforge_profile"]["sha256"]
                    .as_str()
                    .unwrap()
                    .len()
                    == 64
            );
        }
    }
}
#[test]
fn invalid_batches_fail_structural_validation() {
    let original = fixture();
    for mutate in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10] {
        let mut b = original.clone();
        match mutate {
            0 => b.schema_version = 2,
            1 => b.kind = "job".into(),
            2 => b.status = "partial".into(),
            3 => b.jobs.clear(),
            4 => b.jobs.push(b.jobs[0].clone()),
            5 => b.jobs[1].dataset_id = b.jobs[0].dataset_id,
            6 => {
                let task = b.jobs[0].tasks[0].clone();
                b.jobs[0].tasks.push(task);
            }
            7 => b.jobs[0].output.dataset_path = b.jobs[0].output.staging_path.clone(),
            8 => b.instruments[0].instrument_id = 0,
            9 => b.instruments.push(b.instruments[0].clone()),
            _ => b.jobs[0].tasks[1].stream_rank = b.jobs[0].tasks[0].stream_rank + 1,
        };
        assert!(b.validate(&root()).is_err(), "case {mutate}");
    }
}
#[test]
fn unknown_nested_fields_and_invalid_wire_values_fail() {
    let b = fixture();
    let original = serde_json::to_value(&b).unwrap();
    for path in [
        "/unknown",
        "/jobs/0/unknown",
        "/jobs/0/output/unknown",
        "/jobs/0/ordering/unknown",
        "/jobs/0/tasks/0/unknown",
        "/instruments/0/instrument/unknown",
    ] {
        let mut v = original.clone();
        let (parent, key) = path.rsplit_once('/').unwrap();
        v.pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(key.into(), json!(1));
        assert!(
            serde_json::from_value::<PreparedBatch>(v).is_err(),
            "{path}"
        );
    }
    for (path, value) in [
        ("/jobs/0/operation", json!("merge")),
        ("/jobs/0/output/content_type", json!("guess")),
        ("/jobs/0/ordering/primary", json!("task_id")),
        ("/jobs/0/job_id", json!(-1)),
    ] {
        let mut v = original.clone();
        *v.pointer_mut(path).unwrap() = value;
        assert!(
            serde_json::from_value::<PreparedBatch>(v).is_err(),
            "{path}"
        );
    }
}
#[test]
fn resolution_rejects_conflicts_missing_profiles_and_unmapped_family() {
    let (b, mut j) = selected("trades", Exchange::Bybit);
    j.tasks[0].instrument_id = Some(999);
    assert!(
        b.resolve_job(&j, &catalog(), &runtime(), &root())
            .unwrap_err()
            .to_string()
            .contains("conflicts")
    );
    j.tasks[0].instrument_id = None;
    j.tasks[0].market_category = MarketCategory::Inverse;
    assert!(
        b.resolve_job(&j, &catalog(), &runtime(), &root())
            .unwrap_err()
            .to_string()
            .contains("expected profile")
    );
    let mut j = b.jobs[0].clone();
    j.tasks.retain(|t| t.scope.is_some());
    assert!(
        b.resolve_job(&j, &catalog(), &runtime(), &root())
            .unwrap_err()
            .to_string()
            .contains("authoritative concrete")
    );
}
#[test]
fn family_expansion_uses_each_concrete_metadata_and_preserves_source_identity() {
    let mut b = fixture();
    let mut j = b.jobs[0].clone();
    j.tasks.retain(|t| {
        t.scope
            .as_ref()
            .is_some_and(|s| s.identifier == "BTC-USD_UM")
    });
    j.tasks.truncate(1);
    for (id, symbol, tick, value) in [
        (1001, "BTC-USD_UM-261030", "0.1", "0.01"),
        (1002, "BTC-USD_UM-261225", "0.5", "0.02"),
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
                tick_size: tick.parse().unwrap(),
                contract_value: Some(value.parse().unwrap()),
                contract_value_asset: Some("BTC".into()),
            },
        });
        j.tasks[0]
            .scope
            .as_mut()
            .unwrap()
            .members
            .push(FamilyMember {
                instrument_id: id,
                archive_member: None,
            });
    }
    let r = b.resolve_job(&j, &catalog(), &runtime(), &root()).unwrap();
    assert_eq!(r.tasks.len(), 2);
    assert_ne!(r.tasks[0].task_id, r.tasks[1].task_id);
    assert_eq!(r.tasks[0].instrument_id, 1001);
    assert_eq!(r.tasks[1].instrument_id, 1002);
    assert_ne!(
        r.tasks[0].instrument.contract_value,
        r.tasks[1].instrument.contract_value
    );
    for t in &r.tasks {
        assert_eq!(
            t.raw_schema["marketforge_source"]["task_id"],
            json!(j.tasks[0].task_id)
        );
    }
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
struct Depth(Vec<L2LevelUpdate>);
impl DepthSink for Depth {
    fn write_outcome(
        &mut self,
        o: crate::shared::formats::depth::DepthProcessingOutcome,
    ) -> Result<()> {
        self.0.extend(o.events);
        Ok(())
    }
    fn write_boundary(
        &mut self,
        _: crate::historical::normalize::boundary::DepthBoundaryManifest,
    ) -> Result<()> {
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
fn gzip(path: &Path, bytes: &[u8]) {
    let mut e = flate2::write::GzEncoder::new(
        std::fs::File::create(path).unwrap(),
        flate2::Compression::default(),
    );
    e.write_all(bytes).unwrap();
    e.finish().unwrap();
}
#[test]
fn resolved_trade_worker_has_independent_canonical_expectations() {
    let (b, mut j) = selected("trades", Exchange::Bybit);
    let temp = tempfile::tempdir().unwrap();
    j.tasks[0].input_path = temp.path().join("input.csv.gz");
    gzip(
        &j.tasks[0].input_path,
        b"id,timestamp,price,volume,side,rpi\n123,1000,100,0.25,sell,1\n",
    );
    let r = b.resolve_job(&j, &catalog(), &runtime(), &root()).unwrap();
    let mut sink = Trades::default();
    process_trade_task(&r.tasks[0], &mut sink).unwrap();
    assert_eq!(sink.0.len(), 1);
    let t = &sink.0[0];
    assert_eq!(t.envelope.event_timestamp_ns, 1_000_000_000);
    assert_eq!(t.price, Decimal::from(100));
    assert_eq!(t.quantity_base, Some("0.25".parse().unwrap()));
    assert_eq!(t.quantity_quote, Some(Decimal::from(25)));
    assert_eq!(t.is_rpi, Some(true));
    assert_eq!(t.side, crate::shared::domain::canonical::TradeSide::Sell);
}
#[test]
fn gate_snapshot_feed_uses_observed_json_object_levels() {
    let b = fixture();
    let mut j = b.jobs[1].clone();
    j.tasks
        .retain(|t| t.exchange == Exchange::GateIo && t.market_category == MarketCategory::Linear);
    j.tasks.truncate(1);
    let temp = tempfile::tempdir().unwrap();
    j.tasks[0].input_path = temp.path().join("input.jsonl.gz");
    gzip(&j.tasks[0].input_path,b"{\"update\":1.25,\"asks\":[{\"p\":\"101\",\"s\":2}],\"bids\":[{\"p\":\"100\",\"s\":3}]}\n");
    let runtime = runtime();
    let r = b.resolve_job(&j, &catalog(), &runtime, &root()).unwrap();
    let mut sink = Depth::default();
    let mut metrics = TaskMetrics::default();
    process_depth_task_with_metrics(
        &r.tasks[0],
        &mut sink,
        &mut metrics,
        &runtime.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        resolve_sequence_policy(&r.tasks[0]).unwrap(),
    )
    .unwrap();
    assert_eq!(sink.0.len(), 2);
    for e in &sink.0 {
        assert_eq!(e.envelope.event_timestamp_ns, 1_250_000_000);
        assert_eq!(e.envelope.instrument_id, r.tasks[0].instrument_id);
    }
    assert!(
        sink.0
            .iter()
            .any(|e| e.quantity_contracts == Some(Decimal::from(2))
                && e.quantity_base == Some("0.0002".parse().unwrap()))
    );
}
