//! Isolated forward reader/routing tests. Never invoke a worker or writer.
use super::*;
use crate::historical::config::{
    InstrumentSelection, PreparedBatch, selection_test_support as support,
};
use crate::historical::normalize::{
    metrics::ProcessingMetricsReport, reconcile::SourceObservation,
};
fn route_sources(
    mut batch: PreparedBatch,
    rows: &[&str],
) -> (
    crate::historical::job::ProcessingJob,
    Routed,
    tempfile::TempDir,
) {
    batch.jobs[0].tasks.truncate(rows.len());
    let mut job = support::resolved(&batch);
    let temp = tempfile::tempdir().unwrap();
    for (priority, rows) in rows.iter().enumerate() {
        std::fs::write(
            temp.path().join(format!("{priority}.csv")),
            format!("instrument_name\n{rows}"),
        )
        .unwrap();
    }
    for task in &mut job.tasks {
        let priority = task.raw_schema["marketforge_source"]["source_priority"]
            .as_u64()
            .unwrap();
        task.input_path = temp.path().join(format!("{priority}.csv"));
        task.source_compression = crate::shared::io::container::SourceContainer::Plain;
    }
    let groups = super::super::streams::plan(&job).unwrap();
    assert_eq!(groups.len(), 1);
    let config = support::runtime_in(temp.path());
    let quota = Quota::new(config.resources.normalization_limits.reconciliation_bytes);
    let routed = route(&groups[0], &quota, &config);
    drop(groups);
    (job, routed, temp)
}
fn report(job: &crate::historical::job::ProcessingJob, routed: &Routed) -> ProcessingMetricsReport {
    let mut report = ProcessingMetricsReport::default();
    for (index, task) in job.tasks.iter().enumerate() {
        let mut metrics = super::super::metrics::TaskMetrics::default();
        metrics.source = Some(SourceObservation {
            observed_selected_records: routed.stats.get(&index).map_or(0, |s| s.selected),
            ..Default::default()
        });
        report.tasks.push(metrics.into_report(task));
    }
    report
}
#[test]
fn include_filters_unknown_unselected_symbols_before_contract_lookup() {
    let mut b = support::batch();
    b.instruments.pop();
    for t in &mut b.jobs[0].tasks {
        t.instrument_selection = Some(InstrumentSelection::Include {
            symbols: vec!["BTC-USD_UM-261030".into()],
        });
    }
    let (job, routed, _temp) = route_sources(b, &["BTC-USD_UM-261225\nBTC-USD_UM-261030\n"]);
    assert!(routed.failure.is_none());
    assert_eq!(routed.stats.values().map(|s| s.physical).sum::<u64>(), 2);
    assert_eq!(routed.stats.values().map(|s| s.filtered).sum::<u64>(), 1);
    assert_eq!(routed.inputs.len(), 1);
    let DecodedSource::DiskCsv { spool, .. } = routed.inputs.values().next().unwrap() else {
        panic!()
    };
    let mut reader = spool.reader::<(u64, Vec<Vec<u8>>)>().unwrap();
    assert_eq!(reader.next().unwrap().unwrap().value.0, 2);
    assert!(reader.next().unwrap().is_none());
    validate_selection(&job, &report(&job, &routed)).unwrap();
}
#[test]
fn all_never_hides_encountered_missing_or_unsupported_metadata() {
    let mut b = support::batch();
    b.instruments.pop();
    let (_, routed, _temp) = route_sources(b, &["BTC-USD_UM-261225\n"]);
    assert!(
        routed
            .failure
            .as_ref()
            .unwrap()
            .1
            .to_string()
            .contains("metadata is missing")
    );
    assert_eq!(
        routed
            .stats
            .values()
            .map(|s| s.missing_metadata)
            .sum::<u64>(),
        1
    );
    let mut b = support::batch();
    b.instruments[1].instrument.contract_value = None;
    b.instruments[1].instrument.contract_value_asset = None;
    let (_, routed, _temp) = route_sources(b.clone(), &["BTC-USD_UM-261030\n"]);
    assert!(
        routed.failure.is_none(),
        "unencountered candidate must not require full contract metadata"
    );
    let (_, routed, _temp) = route_sources(b, &["BTC-USD_UM-261225\n"]);
    assert!(
        routed
            .failure
            .as_ref()
            .unwrap()
            .1
            .to_string()
            .contains("unsupported normalization")
    );
    assert_eq!(routed.stats.values().map(|s| s.unsupported).sum::<u64>(), 1);
}
#[test]
fn changing_monthly_population_is_routed_once_with_per_source_absence() {
    let (job, routed, temp) = route_sources(
        support::batch(),
        &["BTC-USD_UM-261030\n", "BTC-USD_UM-261225\n"],
    );
    assert!(routed.failure.is_none());
    assert_eq!(routed.stats.values().map(|s| s.physical).sum::<u64>(), 2);
    assert_eq!(routed.stats.values().map(|s| s.selected).sum::<u64>(), 2);
    assert_eq!(routed.stats.values().filter(|s| s.selected == 0).count(), 2);
    assert_eq!(
        job.tasks
            .iter()
            .map(|t| t.instrument_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2
    );
}
#[test]
fn never_observed_include_is_unsatisfied_but_absence_in_one_source_is_allowed() {
    let mut b = support::batch();
    for t in &mut b.jobs[0].tasks {
        t.instrument_selection = Some(InstrumentSelection::Include {
            symbols: vec!["BTC-USD_UM-261030".into(), "BTC-USD_UM-261225".into()],
        });
    }
    let (job, routed, _temp) =
        route_sources(b.clone(), &["BTC-USD_UM-261030\n", "BTC-USD_UM-261030\n"]);
    assert!(
        validate_selection(&job, &report(&job, &routed))
            .unwrap_err()
            .to_string()
            .contains("never observed")
    );
    let (job, routed, _temp) = route_sources(b, &["BTC-USD_UM-261030\n", "BTC-USD_UM-261225\n"]);
    validate_selection(&job, &report(&job, &routed)).unwrap();
}
#[test]
fn one_mixed_decode_and_bounded_queues_replace_per_symbol_archive_reads() {
    let (job, routed, temp) = route_sources(
        support::batch(),
        &["BTC-USD_UM-261030\nBTC-USD_UM-261225\n"],
    );
    assert!(routed.failure.is_none());
    assert_eq!(routed.stats.values().map(|s| s.physical).sum::<u64>(), 2);
    assert_eq!(
        routed
            .inputs
            .values()
            .map(|input| match input {
                DecodedSource::DiskCsv { spool, .. } => spool.count() as usize,
                _ => 0,
            })
            .sum::<usize>(),
        2
    );
    // Inject descriptor-only quota exhaustion: no oversized allocation or writer.
    let group = super::super::streams::StreamUnit {
        job: &job,
        tasks: job.tasks.iter().enumerate().collect(),
        key: "synthetic".into(),
    };
    let limited = route(&group, &Quota::new(1), &support::runtime_in(temp.path()));
    assert!(matches!(
        limited.failure,
        Some((_, MarketForgeError::ResourceLimit(_)))
    ));
    assert!(limited.inputs.is_empty());
}
#[test]
fn redundant_equivalent_family_selection_reads_first_only() {
    let mut b = support::batch();
    b.jobs[0].tasks[1].input_path = b.jobs[0].tasks[0].input_path.clone();
    let mut job = support::resolved(&b);
    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), "instrument_name\nBTC-USD_UM-261030\n").unwrap();
    for task in &mut job.tasks {
        task.input_path = temp.path().into();
        task.source_compression = crate::shared::io::container::SourceContainer::Plain;
    }
    let groups = super::super::streams::plan(&job).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let routed = route(
        &groups[0],
        &Quota::new(1024 * 1024),
        &support::runtime_in(scratch.path()),
    );
    assert!(routed.failure.is_none());
    assert_eq!(routed.stats.values().map(|s| s.physical).sum::<u64>(), 1);
    assert_eq!(
        routed
            .stats
            .values()
            .filter(|s| s.redundant_of == Some(900))
            .count(),
        2
    );
}

#[test]
fn jsonl_depth_routes_interleaved_instruments_without_a_shared_timestamp_domain() {
    let mut b = support::batch();
    b.jobs[0].tasks.truncate(1);
    b.jobs[0].output.content_type = crate::historical::job::ContentType::Depth;
    let mut job = support::resolved(&b);
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(),concat!(
        "{\"instId\":\"BTC-USD_UM-261030\",\"action\":\"snapshot\",\"ts\":\"10\",\"asks\":[],\"bids\":[]}\n",
        "{\"instId\":\"BTC-USD_UM-261225\",\"action\":\"snapshot\",\"ts\":\"5\",\"asks\":[],\"bids\":[]}\n",
        "{\"instId\":\"BTC-USD_UM-261030\",\"action\":\"update\",\"ts\":\"11\",\"asks\":[],\"bids\":[]}\n"
    )).unwrap();
    for task in &mut job.tasks {
        task.input_path = file.path().into();
        task.source_compression = crate::shared::io::container::SourceContainer::Plain;
    }
    let groups = super::super::streams::plan(&job).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let routed = route(
        &groups[0],
        &Quota::new(1024 * 1024),
        &support::runtime_in(scratch.path()),
    );
    assert!(routed.failure.is_none());
    assert_eq!(routed.stats.values().map(|s| s.physical).sum::<u64>(), 3);
    let DecodedSource::DiskJson(a) = &routed.inputs[&0] else {
        panic!()
    };
    let DecodedSource::DiskJson(b) = &routed.inputs[&1] else {
        panic!()
    };
    let read = |spool: &super::super::reconcile::spool::Spool| {
        let mut reader = spool.reader::<(u64, serde_json::Value)>().unwrap();
        let mut rows = Vec::new();
        while let Some(row) = reader.next().unwrap() {
            rows.push(row.value);
        }
        rows
    };
    let a = read(a);
    let b = read(b);
    assert_eq!(a.iter().map(|r| r.0).collect::<Vec<_>>(), [1, 3]);
    assert_eq!(b.iter().map(|r| r.0).collect::<Vec<_>>(), [2]);
    assert_eq!(a[0].1["ts"], "10");
    assert_eq!(b[0].1["ts"], "5");
}

#[test]
fn degradable_trade_csv_parse_errors_remain_bounded_file_diagnostics() {
    let mut b = support::batch();
    b.jobs[0].tasks.truncate(1);
    let mut job = support::resolved(&b);
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        file.path(),
        "instrument_name\nbad,extra-column\nBTC-USD_UM-261030\n",
    )
    .unwrap();
    for task in &mut job.tasks {
        task.input_path = file.path().into();
        task.source_compression = crate::shared::io::container::SourceContainer::Plain;
    }
    let groups = super::super::streams::plan(&job).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut config = support::runtime_in(scratch.path());
    config.integrity_policy.parse_failure.action = crate::historical::job::IntegrityAction::Degrade;
    config.integrity_policy.parse_failure.max_count = Some(10);
    let routed = route(&groups[0], &Quota::new(1024 * 1024), &config);
    assert!(routed.failure.is_none());
    assert_eq!(routed.stats.values().map(|s| s.physical).sum::<u64>(), 2);
    assert_eq!(
        routed.stats.values().map(|s| s.parse_rejected).sum::<u64>(),
        1
    );
    assert_eq!(
        routed
            .stats
            .values()
            .map(|s| s.diagnostics.diagnostics.len())
            .sum::<usize>(),
        1
    );
}

#[test]
fn csv_routing_uses_the_existing_authoritative_identity_column_precedence() {
    let mut b = support::batch();
    b.jobs[0].tasks.truncate(1);
    let mut job = support::resolved(&b);
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        file.path(),
        "symbol,instrument_name\nNOT-A-NATIVE-SYMBOL,BTC-USD_UM-261030\n",
    )
    .unwrap();
    for task in &mut job.tasks {
        task.input_path = file.path().into();
        task.source_compression = crate::shared::io::container::SourceContainer::Plain;
    }
    let groups = super::super::streams::plan(&job).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let routed = route(
        &groups[0],
        &Quota::new(1024 * 1024),
        &support::runtime_in(scratch.path()),
    );
    assert!(routed.failure.is_none());
    assert_eq!(routed.stats.values().map(|s| s.selected).sum::<u64>(), 1);
}

#[test]
fn routed_population_larger_than_old_queue_budget_stays_disk_backed() {
    let mut b = support::batch();
    b.jobs[0].tasks.truncate(1);
    let mut job = support::resolved(&b);
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        file.path(),
        format!("instrument_name\n{}", "BTC-USD_UM-261030\n".repeat(5000)),
    )
    .unwrap();
    for t in &mut job.tasks {
        t.input_path = file.path().into();
        t.source_compression = crate::shared::io::container::SourceContainer::Plain;
    }
    let quota = Quota::new(1 << 20);
    let units = super::super::streams::plan(&job).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let routed = route(&units[0], &quota, &support::runtime_in(scratch.path()));
    assert!(
        routed.failure.is_none(),
        "{:?}",
        routed.failure.map(|f| f.1)
    );
    assert_eq!(routed.stats.values().map(|s| s.physical).sum::<u64>(), 5000);
    assert_eq!(routed.stats.values().map(|s| s.selected).sum::<u64>(), 5000);
    assert!(quota.peak() < 1 << 20);
    assert!(quota.used() < 10000);
    let mut rows = 0;
    for input in routed.inputs.into_values() {
        super::super::worker::decoded::with_decoded(input, || {
            let (_, reader) = super::super::worker::decoded::take_csv().unwrap();
            for row in reader {
                row.unwrap();
                rows += 1;
            }
        });
    }
    assert_eq!(rows, 5000);
}
