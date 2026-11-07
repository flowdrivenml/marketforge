//! Small synthetic records, production resolution, no real archive requirements.
use super::support::{fixtures::*, runtime::*};
use marketforge_engine::{
    historical::{
        config::{Feed, PreparedBatch},
        job::*,
        normalize::{
            boundary::DepthBoundaryManifest, metrics::*, resolve_sequence_policy, worker::*,
        },
    },
    shared::{
        domain::canonical::{Exchange, Trade},
        error::Result,
        formats::depth::DepthProcessingOutcome,
    },
};
use std::{fs, io::Write};
#[derive(Default)]
struct Trades(usize);
impl TradeSink for Trades {
    fn write_trade(&mut self, _: Trade) -> Result<()> {
        self.0 += 1;
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
#[derive(Default)]
struct Depth(usize);
impl DepthSink for Depth {
    fn write_outcome(&mut self, o: DepthProcessingOutcome) -> Result<()> {
        self.0 += o.events.len();
        Ok(())
    }
    fn write_boundary(&mut self, _: DepthBoundaryManifest) -> Result<()> {
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
fn subset(depth: bool, gate: bool) -> PreparedBatch {
    let b = workload().unwrap();
    let s = Selection {
        exchange: if gate {
            Exchange::GateIo
        } else {
            Exchange::Bybit
        },
        instrument_type: InstrumentKind::Spot,
        market_category: "spot",
        content_type: if depth {
            ContentType::Depth
        } else {
            ContentType::Trades
        },
        symbol: Some(if gate { "BTC_USDT" } else { "BTCUSDT" }),
        family: None,
        feed: if gate { Some(Feed::Incremental) } else { None },
        filename: Some(if gate {
            "BTC_USDT-2026102700.csv.gz"
        } else if depth {
            "2026-10-27_BTCUSDT_ob200.data.zip"
        } else {
            "BTCUSDT_2026-10-27.csv.gz"
        }),
    };
    select(&b, "synthetic integrity", &s, 1).unwrap()
}
fn input(run: &TestRun, b: &mut PreparedBatch, bytes: &[u8], zip: bool) {
    let path = run.path.join("synthetic.archive");
    let f = fs::File::create(&path).unwrap();
    if zip {
        let mut z = zip::ZipWriter::new(f);
        z.start_file("sample.data", zip::write::SimpleFileOptions::default())
            .unwrap();
        z.write_all(bytes).unwrap();
        z.finish().unwrap();
    } else {
        let mut z = flate2::write::GzEncoder::new(f, flate2::Compression::default());
        z.write_all(bytes).unwrap();
        z.finish().unwrap();
    }
    b.jobs[0].tasks[0].input_path = path;
}
fn allow(rule: &mut IntegrityRule) {
    rule.action = IntegrityAction::Degrade;
    rule.max_count = Some(1000);
}
#[test]
fn parsing_denominator_includes_999_successes_and_one_failure() {
    let mut b = subset(false, false);
    let mut run = TestRun::new("integrity_parse_population").unwrap();
    let mut bytes = "id,timestamp,price,volume,side,rpi\n".to_owned();
    for id in 0..999 {
        bytes += &format!("{id},1000,100,1,buy,0\n");
    }
    bytes += "broken\n";
    input(&run, &mut b, bytes.as_bytes(), false);
    let mut c = run.runtime(Default::default()).unwrap();
    allow(&mut c.integrity_policy.parse_failure);
    c.integrity_policy.parse_failure.max_rate = Some(0.001);
    c.integrity_policy.parse_failure.minimum_samples = 1000;
    let jobs = run.prepare(&b, &c).unwrap();
    let mut metrics = TaskMetrics::default();
    let mut sink = Trades::default();
    process_trade_task_with_metrics(
        &jobs.jobs[0].tasks[0],
        &mut sink,
        &mut metrics,
        &c.integrity_policy,
        &Default::default(),
    )
    .unwrap();
    let p = &metrics.scoped_integrity.global.parse_failure;
    assert_eq!((p.eligible, p.violations, p.rate()), (1000, 1, Some(0.001)));
    assert_eq!(
        metrics
            .scoped_integrity
            .unattributed
            .parse_failure
            .violations,
        1
    );
    assert_eq!(
        metrics
            .scoped_integrity
            .global
            .transformation_failure
            .eligible,
        999
    );
    assert_eq!(sink.0, 999);
    assert_eq!(
        evaluate_integrity_policy(&c.integrity_policy, &metrics.scoped_integrity)
            .unwrap()
            .status,
        IntegrityStatus::Degraded
    );
    c.integrity_policy.parse_failure.windows.hourly = Some(IntegrityWindowRule {
        max_rate: 0.01,
        minimum_samples: 1000,
    });
    let e = evaluate_integrity_policy(&c.integrity_policy, &metrics.scoped_integrity).unwrap();
    assert!(e.is_failed());
    assert!(
        e.violations
            .iter()
            .any(|v| v.threshold == IntegrityThreshold::UnattributedWindow)
    );
    run.finish(&Ok(())).unwrap();
}
#[test]
fn partial_trade_success_and_known_timestamp_survive_transformation_failure() {
    let mut b = subset(false, false);
    let mut run = TestRun::new("integrity_partial_trade").unwrap();
    input(
        &run,
        &mut b,
        b"id,timestamp,price,volume,side,rpi\n1,86400000,79228162514264337593543950335,2,buy,0\n",
        false,
    );
    let mut c = run.runtime(Default::default()).unwrap();
    allow(&mut c.integrity_policy.transformation_failure);
    let resolved = run.prepare(&b, &c).unwrap();
    let mut metrics = TaskMetrics::default();
    process_trade_task_with_metrics(
        &resolved.jobs[0].tasks[0],
        &mut Trades::default(),
        &mut metrics,
        &c.integrity_policy,
        &Default::default(),
    )
    .unwrap();
    let s = &metrics.scoped_integrity;
    assert_eq!(
        (
            s.global.parse_failure.eligible,
            s.global.parse_failure.violations
        ),
        (1, 0)
    );
    assert_eq!(
        (
            s.global.invalid_record.eligible,
            s.global.invalid_record.violations
        ),
        (1, 0)
    );
    assert_eq!(
        (
            s.global.transformation_failure.eligible,
            s.global.transformation_failure.violations
        ),
        (1, 1)
    );
    assert_eq!(
        s.hourly
            .values()
            .next()
            .unwrap()
            .transformation_failure
            .violations,
        1
    );
    assert_eq!(
        metrics.integrity.diagnostics[0].event_timestamp_ns,
        Some(DAY_NS)
    );
    assert_eq!(s.unattributed.transformation_failure.eligible, 0);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn clean_population_and_minimum_sample_boundary() {
    let mut p = ScopedIntegrityMetrics::default();
    let scope = IntegrityScope {
        task_id: 1,
        stream_id: "s".into(),
        source_file: "synthetic".into(),
    };
    for _ in 0..999 {
        p.record(&scope, IntegrityCategory::ParseFailure, false, Some(0))
            .unwrap();
    }
    assert_eq!(p.global.parse_failure.rate(), Some(0.0));
    let mut run = TestRun::new("integrity_minimum").unwrap();
    let mut c = run.runtime(Default::default()).unwrap();
    allow(&mut c.integrity_policy.parse_failure);
    c.integrity_policy.parse_failure.max_rate = Some(0.0);
    c.integrity_policy.parse_failure.minimum_samples = 1000;
    assert_eq!(
        evaluate_integrity_policy(&c.integrity_policy, &p)
            .unwrap()
            .status,
        IntegrityStatus::Clean
    );
    p.record(&scope, IntegrityCategory::ParseFailure, true, Some(0))
        .unwrap();
    assert!(
        evaluate_integrity_policy(&c.integrity_policy, &p)
            .unwrap()
            .is_failed()
    );
    let mut clean = p.clone();
    clean.global.parse_failure.violations = 0;
    assert_eq!(clean.global.parse_failure.eligible, 1000);
    assert_eq!(clean.global.parse_failure.rate(), Some(0.0));
    run.finish(&Ok(())).unwrap();
}
#[test]
fn depth_counts_source_messages_not_levels_and_keeps_no_change_events() {
    let mut b = subset(true, false);
    let mut run = TestRun::new("integrity_depth_units").unwrap();
    let bytes=b"{\"ts\":0,\"cts\":0,\"type\":\"snapshot\",\"data\":{\"s\":\"BTCUSDT\",\"u\":1,\"b\":[[\"100\",\"1\"],[\"99\",\"2\"]],\"a\":[[\"101\",\"1\"]]}}\n{\"ts\":86400000,\"cts\":86400000,\"type\":\"delta\",\"data\":{\"s\":\"BTCUSDT\",\"u\":2,\"b\":[[\"100\",\"1\"]],\"a\":[]}}\n{\"ts\":86400001,\"cts\":86400001,\"type\":\"delta\",\"data\":{\"s\":\"BTCUSDT\",\"u\":3,\"b\":[[\"100\",\"bad\"]],\"a\":[]}}\n";
    input(&run, &mut b, bytes, true);
    let mut c = run.runtime(Default::default()).unwrap();
    allow(&mut c.integrity_policy.invalid_record);
    let resolved = run.prepare(&b, &c).unwrap();
    let t = &resolved.jobs[0].tasks[0];
    let mut m = TaskMetrics::default();
    let mut sink = Depth::default();
    process_depth_task_with_metrics(
        t,
        &mut sink,
        &mut m,
        &c.integrity_policy,
        &Default::default(),
        resolve_sequence_policy(t).unwrap(),
    )
    .unwrap();
    let s = &m.scoped_integrity;
    assert_eq!(
        (
            s.global.parse_failure.eligible,
            s.global.parse_failure.violations
        ),
        (3, 0)
    );
    assert_eq!(
        (
            s.global.invalid_record.eligible,
            s.global.invalid_record.violations
        ),
        (3, 1)
    );
    assert_eq!(s.global.sequence_gap.eligible, 1);
    assert_eq!(s.global.missing_snapshot.eligible, 1);
    assert_eq!(s.global.invalid_book.eligible, 0);
    assert_eq!(s.global.timestamp_regression.eligible, 0);
    assert_eq!(sink.0, 3);
    assert_eq!(m.counters.records_processed, 2);
    assert_eq!(s.hourly.len(), 2);
    assert_eq!(s.daily.len(), 2);
    assert_eq!(s.unattributed.invalid_record.violations, 0);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn csv_rows_and_assembled_snapshot_have_distinct_units() {
    let mut b = subset(true, true);
    let mut run = TestRun::new("integrity_csv_units").unwrap();
    input(
        &run,
        &mut b,
        b"1,2,set,100,1,10,0\n1,1,set,101,1,10,0\n2,2,make,100,0.1,11,1\n",
        false,
    );
    let c = run.runtime(Default::default()).unwrap();
    let resolved = run.prepare(&b, &c).unwrap();
    let t = &resolved.jobs[0].tasks[0];
    let mut m = TaskMetrics::default();
    let mut sink = Depth::default();
    process_depth_task_with_metrics(
        t,
        &mut sink,
        &mut m,
        &c.integrity_policy,
        &Default::default(),
        resolve_sequence_policy(t).unwrap(),
    )
    .unwrap();
    let s = &m.scoped_integrity.global;
    assert_eq!(s.parse_failure.eligible, 3);
    assert_eq!(s.invalid_record.eligible, 4);
    assert_eq!(s.transformation_failure.eligible, 2);
    assert_eq!(s.sequence_gap.eligible, 1);
    assert_eq!(m.counters.records_processed, 3);
    assert_eq!(sink.0, 3);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn aggregation_is_commutative_preserves_windows_and_unattributed() {
    let scope = IntegrityScope {
        task_id: 1,
        stream_id: "s".into(),
        source_file: "synthetic".into(),
    };
    let mut a = ScopedIntegrityMetrics::default();
    let mut b = ScopedIntegrityMetrics::default();
    a.record(&scope, IntegrityCategory::InvalidRecord, false, Some(-1))
        .unwrap();
    b.record(&scope, IntegrityCategory::InvalidRecord, true, None)
        .unwrap();
    let mut ab = a.clone();
    ab.merge(&b).unwrap();
    let mut ba = b;
    ba.merge(&a).unwrap();
    assert_eq!(ab, ba);
    assert_eq!(ab.unattributed.invalid_record.violations, 1);
    assert_eq!(ab.hourly.keys().next().unwrap().1, -HOUR_NS);
    assert_eq!(ab.daily.keys().next().unwrap().1, -DAY_NS);
    assert_eq!(ab.global.invalid_record.eligible, 2);
}
#[test]
fn clean_thousand_records_are_all_eligible() {
    let mut b = subset(false, false);
    let mut run = TestRun::new("integrity_clean_thousand").unwrap();
    let mut text = "id,timestamp,price,volume,side,rpi\n".to_string();
    for id in 0..1000 {
        text += &format!("{id},1000,100,1,buy,0\n");
    }
    input(&run, &mut b, text.as_bytes(), false);
    let c = run.runtime(Default::default()).unwrap();
    let r = run.prepare(&b, &c).unwrap();
    let mut m = TaskMetrics::default();
    process_trade_task_with_metrics(
        &r.jobs[0].tasks[0],
        &mut Trades::default(),
        &mut m,
        &c.integrity_policy,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(m.scoped_integrity.global.parse_failure.eligible, 1000);
    assert_eq!(m.scoped_integrity.global.parse_failure.rate(), Some(0.0));
    assert_eq!(m.scoped_integrity.global.invalid_record.eligible, 1000);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn missing_initialization_counts_only_checks_reached() {
    let mut b = subset(true, false);
    let mut run = TestRun::new("integrity_missing_snapshot").unwrap();
    input(&run,&mut b,b"{\"ts\":1000,\"cts\":1000,\"type\":\"delta\",\"data\":{\"s\":\"BTCUSDT\",\"u\":2,\"b\":[[\"100\",\"1\"]],\"a\":[]}}\n",true);
    let c = run.runtime(Default::default()).unwrap();
    let r = run.prepare(&b, &c).unwrap();
    let t = &r.jobs[0].tasks[0];
    let mut m = TaskMetrics::default();
    assert!(
        process_depth_task_with_metrics(
            t,
            &mut Depth::default(),
            &mut m,
            &c.integrity_policy,
            &Default::default(),
            resolve_sequence_policy(t).unwrap()
        )
        .is_err()
    );
    let s = &m.scoped_integrity.global;
    assert_eq!(s.parse_failure.eligible, 1);
    assert_eq!(s.invalid_record.eligible, 1);
    assert_eq!(
        (s.missing_snapshot.eligible, s.missing_snapshot.violations),
        (1, 1)
    );
    assert_eq!(s.sequence_gap.eligible, 0);
    assert_eq!(s.transformation_failure.eligible, 0);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn unsequenced_feed_does_not_invent_sequence_observations() {
    let source = workload().unwrap();
    let s = Selection {
        exchange: Exchange::GateIo,
        instrument_type: InstrumentKind::Perpetual,
        market_category: "linear",
        content_type: ContentType::Depth,
        symbol: Some("BTC_USDT"),
        family: None,
        feed: Some(Feed::Snapshot),
        filename: Some("BTC_USDT-2026102700.gz"),
    };
    let selected = select(&source, "unsequenced snapshot", &s, 1).unwrap();
    let mut b = selected;
    let mut run = TestRun::new("integrity_unsequenced").unwrap();
    input(
        &run,
        &mut b,
        b"{\"update\":1,\"bids\":[{\"p\":\"100\",\"s\":2}],\"asks\":[{\"p\":\"101\",\"s\":3}]}\n",
        false,
    );
    let c = run.runtime(Default::default()).unwrap();
    let r = run.prepare(&b, &c).unwrap();
    let t = &r.jobs[0].tasks[0];
    let mut m = TaskMetrics::default();
    process_depth_task_with_metrics(
        t,
        &mut Depth::default(),
        &mut m,
        &c.integrity_policy,
        &Default::default(),
        resolve_sequence_policy(t).unwrap(),
    )
    .unwrap();
    assert_eq!(m.scoped_integrity.global.sequence_gap.eligible, 0);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn policy_actions_and_window_boundaries_remain_explicit() {
    let mut run = TestRun::new("integrity_policy_actions").unwrap();
    let mut c = run.runtime(Default::default()).unwrap();
    let scope = IntegrityScope {
        task_id: 1,
        stream_id: "s".into(),
        source_file: "f".into(),
    };
    let mut m = ScopedIntegrityMetrics::default();
    m.record(&scope, IntegrityCategory::SequenceGap, true, Some(0))
        .unwrap();
    assert!(
        enforce_immediate_integrity(&c.integrity_policy, &m, IntegrityCategory::SequenceGap)
            .unwrap()
            .is_failed()
    );
    allow(&mut c.integrity_policy.sequence_gap);
    assert_eq!(
        evaluate_integrity_policy(&c.integrity_policy, &m)
            .unwrap()
            .status,
        IntegrityStatus::Degraded
    );
    c.integrity_policy.enabled = false;
    assert_eq!(
        evaluate_integrity_policy(&c.integrity_policy, &m)
            .unwrap()
            .status,
        IntegrityStatus::Degraded
    );
    let mut windows = ScopedIntegrityMetrics::default();
    for t in [-1, 0, HOUR_NS - 1, HOUR_NS, DAY_NS - 1, DAY_NS] {
        windows
            .record(&scope, IntegrityCategory::ParseFailure, false, Some(t))
            .unwrap();
    }
    assert_eq!(windows.daily.len(), 3);
    assert_eq!(
        windows.hourly[&(scope.clone(), 0)].parse_failure.eligible,
        2
    );
    assert_eq!(windows.daily[&(scope.clone(), 0)].parse_failure.eligible, 4);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn xlsx_observation_callback_preserves_parse_units_and_valid_failure_timestamp() {
    use marketforge_engine::shared::io::decode::XlsxDepthDecoder;
    let xml = r#"<worksheet><sheetData><row r="1"><c r="A1"><v>1</v></c><c r="B1" t="inlineStr"><is><t>[[101,1]]</t></is></c><c r="C1" t="inlineStr"><is><t>[[100,1]]</t></is></c></row><row r="2"><c r="A2"><v>2</v></c><c r="B2" t="inlineStr"><is><t>broken</t></is></c><c r="C2" t="inlineStr"><is><t>[[100,1]]</t></is></c></row></sheetData></worksheet>"#;
    let mut z = zip::ZipWriter::new(std::io::Cursor::new(vec![]));
    z.start_file(
        "xl/worksheets/sheet1.xml",
        zip::write::SimpleFileOptions::default(),
    )
    .unwrap();
    z.write_all(xml.as_bytes()).unwrap();
    let mut seen = vec![];
    let result =
        XlsxDepthDecoder::decode_observed(z.finish().unwrap(), |ordinal, timestamp, error| {
            seen.push((ordinal, timestamp, error.is_some()));
            Ok(())
        });
    assert!(result.is_err());
    assert_eq!(
        seen,
        vec![
            (1, Some(1_000_000_000), false),
            (2, Some(2_000_000_000), true)
        ]
    );
}
#[test]
fn invalid_event_time_is_unattributed_not_assigned_to_neighbors() {
    let mut b = subset(false, false);
    let mut run = TestRun::new("integrity_unknown_timestamp").unwrap();
    input(
        &run,
        &mut b,
        b"id,timestamp,price,volume,side,rpi\n1,not-time,100,1,buy,0\n",
        false,
    );
    let mut c = run.runtime(Default::default()).unwrap();
    allow(&mut c.integrity_policy.invalid_record);
    c.integrity_policy.invalid_record.windows.daily = Some(IntegrityWindowRule {
        max_rate: 0.5,
        minimum_samples: 1,
    });
    let r = run.prepare(&b, &c).unwrap();
    let mut m = TaskMetrics::default();
    process_trade_task_with_metrics(
        &r.jobs[0].tasks[0],
        &mut Trades::default(),
        &mut m,
        &c.integrity_policy,
        &Default::default(),
    )
    .unwrap();
    assert_eq!(m.scoped_integrity.global.parse_failure.eligible, 1);
    assert_eq!(m.scoped_integrity.unattributed.invalid_record.violations, 1);
    assert!(m.scoped_integrity.hourly.is_empty());
    assert!(m.scoped_integrity.daily.is_empty());
    assert_eq!(m.integrity.diagnostics[0].event_timestamp_ns, None);
    assert!(
        evaluate_integrity_policy(&c.integrity_policy, &m.scoped_integrity)
            .unwrap()
            .is_failed()
    );
    run.finish(&Ok(())).unwrap();
}
#[test]
fn xlsx_rows_are_observed_before_sorting_and_not_counted_per_level() {
    let source = workload().unwrap();
    let selection = Selection {
        exchange: Exchange::Bitget,
        instrument_type: InstrumentKind::Spot,
        market_category: "spot",
        content_type: ContentType::Depth,
        symbol: Some("BTCUSDT"),
        family: None,
        feed: None,
        filename: Some("BTC_USDT-BTCUSDT_1_20261027.zip"),
    };
    let mut b = select(&source, "xlsx accounting", &selection, 1).unwrap();
    let mut run = TestRun::new("integrity_xlsx_units").unwrap();
    let xml = r#"<worksheet><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>timestamp</t></is></c><c r="B1" t="inlineStr"><is><t>asks</t></is></c><c r="C1" t="inlineStr"><is><t>bids</t></is></c></row><row r="2"><c r="A2"><v>2</v></c><c r="B2" t="inlineStr"><is><t>[[101,1]]</t></is></c><c r="C2" t="inlineStr"><is><t>[[100,1]]</t></is></c></row><row r="3"><c r="A3"><v>1</v></c><c r="B3" t="inlineStr"><is><t>[[101,1]]</t></is></c><c r="C3" t="inlineStr"><is><t>[[100,1]]</t></is></c></row></sheetData></worksheet>"#;
    let mut z = zip::ZipWriter::new(std::io::Cursor::new(vec![]));
    z.start_file(
        "xl/worksheets/sheet1.xml",
        zip::write::SimpleFileOptions::default(),
    )
    .unwrap();
    z.write_all(xml.as_bytes()).unwrap();
    let bytes = z.finish().unwrap().into_inner();
    input(&run, &mut b, &bytes, true);
    let c = run.runtime(Default::default()).unwrap();
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
        resolve_sequence_policy(t).unwrap(),
    )
    .unwrap();
    assert_eq!(m.scoped_integrity.global.parse_failure.eligible, 2);
    assert_eq!(m.scoped_integrity.global.invalid_record.eligible, 2);
    assert_eq!(m.counters.records_processed, 2);
    assert_eq!(sink.0, 2);
    assert_eq!(m.scoped_integrity.global.sequence_gap.eligible, 0);
    run.finish(&Ok(())).unwrap();
}
#[test]
fn many_snapshot_levels_have_one_source_parse_observation() {
    let mut b = subset(true, false);
    let mut run = TestRun::new("integrity_large_snapshot_units").unwrap();
    let bids: Vec<_> = (0..250)
        .map(|n| serde_json::json!([(10000 - n).to_string(), "1"]))
        .collect();
    let asks: Vec<_> = (0..250)
        .map(|n| serde_json::json!([(11000 + n).to_string(), "1"]))
        .collect();
    let text = format!(
        "{}\n",
        serde_json::json!({"ts":1000,"cts":1000,"type":"snapshot","data":{"s":"BTCUSDT","u":1,"b":bids,"a":asks}})
    );
    input(&run, &mut b, text.as_bytes(), true);
    let c = run.runtime(Default::default()).unwrap();
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
        resolve_sequence_policy(t).unwrap(),
    )
    .unwrap();
    assert_eq!(sink.0, 500);
    assert_eq!(m.scoped_integrity.global.parse_failure.eligible, 1);
    assert_eq!(m.scoped_integrity.global.transformation_failure.eligible, 1);
    run.finish(&Ok(())).unwrap();
}
