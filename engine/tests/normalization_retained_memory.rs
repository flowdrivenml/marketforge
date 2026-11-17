//! Synthetic lifetime/overflow regressions. No real archive is opened by these tests.
use marketforge_engine::{
    historical::normalize::{manifest::*, metrics::*},
    shared::{
        domain::{book::boundary::*, canonical::Exchange},
        error::MarketForgeError,
        historical::limits::NormalizationLimits,
        persistence::parquet::*,
    },
};
use rust_decimal::Decimal;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

thread_local! {
    static TRACK:Cell<bool>=const{Cell::new(false)};
    static ALLOCS:Cell<usize>=const{Cell::new(0)};
    static LIVE:Cell<isize>=const{Cell::new(0)};
    static PEAK:Cell<isize>=const{Cell::new(0)};
}
struct Allocator;
fn account(bytes: isize, allocation: bool) {
    let _ = TRACK.try_with(|track| {
        if track.get() {
            LIVE.with(|l| {
                l.set(l.get() + bytes);
                PEAK.with(|p| p.set(p.get().max(l.get())));
            });
            if allocation {
                ALLOCS.with(|a| a.set(a.get() + 1));
            }
        }
    });
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            account(l.size() as isize, true);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        account(-(l.size() as isize), false);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, n) };
        if !q.is_null() {
            account(n as isize - l.size() as isize, true);
        }
        q
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
fn measured<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
    ALLOCS.with(|a| a.set(0));
    LIVE.with(|a| a.set(0));
    PEAK.with(|a| a.set(0));
    TRACK.with(|a| a.set(true));
    let result = f();
    TRACK.with(|a| a.set(false));
    (
        result,
        ALLOCS.with(Cell::get),
        PEAK.with(|a| a.get().max(0) as usize),
    )
}
fn source(id: u64, levels: usize) -> DepthSourceBoundary {
    let side = || {
        (0..levels)
            .map(|n| BoundaryBookLevel {
                price: Decimal::from(n as u64 + 1),
                quantity_base: Some(Decimal::ONE),
                quantity_quote: None,
                quantity_contracts: None,
                order_count: None,
            })
            .collect()
    };
    let mut boundary =
        DepthBoundaryManifest::new(Exchange::Bybit, 7, "SYNTH".into(), "synthetic.depth".into());
    boundary.initial = Some(BoundarySnapshot::new(
        None,
        1,
        BoundaryBookState {
            bids: side(),
            asks: vec![],
        },
        None,
    ));
    boundary.final_state = boundary.initial.clone();
    DepthSourceBoundary {
        task_id: id,
        start_row: id * levels as u64,
        end_row: (id + 1) * levels as u64,
        start_event: id,
        end_event: id + 1,
        boundary,
    }
}
fn write_sources(path: &Path, count: usize, levels: usize) {
    let mut writer = BufWriter::new(File::create(path.join("boundary.json")).unwrap());
    writer
        .write_all(br#"{"version":2,"continuity":"independent_sources","sources":["#)
        .unwrap();
    for n in 0..count {
        if n > 0 {
            writer.write_all(b",").unwrap();
        }
        serde_json::to_writer(&mut writer, &source(n as u64, levels)).unwrap();
    }
    writer.write_all(b"]}").unwrap();
    writer.flush().unwrap();
}
fn bounds(count: usize) -> BoundaryReadLimits {
    BoundaryReadLimits::for_sources(
        count,
        NormalizationLimits {
            depth_levels: 32,
            record_bytes: 1024,
            ..Default::default()
        },
    )
    .unwrap()
}
fn visit(
    path: &Path,
    b: BoundaryReadLimits,
) -> marketforge_engine::shared::error::Result<BoundaryReadSummary> {
    visit_depth_stream_boundaries(path, b, |_| Ok(()))
}
#[test]
fn streams_many_books_with_one_source_live_and_a_compact_id_index() {
    let mut peaks = Vec::new();
    for n in [20, 2000] {
        let dir = tempfile::tempdir().unwrap();
        write_sources(dir.path(), n, 8);
        let ((summary, callbacks), _, peak) = measured(|| {
            let mut callbacks = 0;
            let summary = visit_depth_stream_boundaries(dir.path(), bounds(n), |s| {
                assert_eq!(s.task_id, callbacks);
                assert_eq!(s.boundary.continuity, BoundaryContinuity::Unverifiable);
                callbacks += 1;
                Ok(())
            })
            .unwrap();
            (summary, callbacks)
        });
        assert_eq!(callbacks, n as u64);
        assert_eq!(summary.sources, n);
        assert_eq!(summary.total_rows, n as u64 * 8);
        assert_eq!(summary.total_events, n as u64);
        assert_eq!(summary.peak_source_levels, 16);
        assert!(summary.peak_source_bytes < 10000);
        peaks.push(peak);
    }
    // Counting allocator measures this reader's transient heap allocations, NOT
    // process RSS. Allow separately for HashSet capacity growth and fixed buffers.
    assert!(peaks[1] < peaks[0] + 2000 * 96 + 64 * 1024, "{peaks:?}");
}
#[test]
fn rejects_bad_envelopes_suffixes_ranges_identities_and_versions() {
    let dir = tempfile::tempdir().unwrap();
    write_sources(dir.path(), 2, 1);
    let text = std::fs::read_to_string(dir.path().join("boundary.json")).unwrap();
    let mut cases = vec![
        text[..text.len() - 1].to_string(),
        format!("{text} false"),
        text.replacen("\"version\":2", "\"version\":3", 1),
        text.replacen("\"version\":2", "\"version\":2,\"version\":2", 1),
        text.replacen("\"sources\":", "\"unknown\":0,\"sources\":", 1),
        text.replacen("\"task_id\":1", "\"task_id\":0", 1),
        text.replacen("\"start_row\":1", "\"start_row\":2", 1),
        text.replacen("\"end_event\":2", "\"end_event\":0", 1),
        text.replacen("\"version\":1", "\"version\":9", 1),
        text.replacen("\"task_id\":0", "\"task_id\":0,\"task_id\":0", 1),
        text.replacen("\"task_id\":0", "\"unexpected\":[],\"task_id\":0", 1),
    ];
    cases.push(text.replacen("\"independent_sources\"", "\"verified\"", 1));
    for (n, text) in cases.into_iter().enumerate() {
        std::fs::write(dir.path().join("boundary.json"), text).unwrap();
        assert!(visit(dir.path(), bounds(2)).is_err(), "case {n}");
    }
}
#[test]
fn a_valid_prefix_and_successful_callbacks_do_not_validate_an_invalid_suffix() {
    let dir = tempfile::tempdir().unwrap();
    write_sources(dir.path(), 3, 1);
    let path = dir.path().join("boundary.json");
    let text = std::fs::read_to_string(&path).unwrap();
    std::fs::write(
        &path,
        text.replacen("\"version\":2,", "", 1)
            .trim_end_matches('}')
            .to_string()
            + ",\"version\":7}",
    )
    .unwrap();
    let mut callbacks = 0;
    let result = visit_depth_stream_boundaries(dir.path(), bounds(3), |_| {
        callbacks += 1;
        Ok(())
    });
    assert_eq!(callbacks, 3);
    assert!(result.is_err());
}
#[test]
fn decoding_limits_apply_to_source_bytes_strings_arrays_and_inventory() {
    let dir = tempfile::tempdir().unwrap();
    write_sources(dir.path(), 2, 8);
    assert!(visit(dir.path(), bounds(1)).is_err());
    let mut b = bounds(2);
    b.max_book_levels = 4;
    assert!(visit(dir.path(), b).is_err());
    b = bounds(2);
    b.max_source_bytes = 128;
    assert!(matches!(
        visit(dir.path(), b),
        Err(MarketForgeError::ResourceLimit(_))
    ));
    b = bounds(2);
    b.max_string_bytes = 4;
    assert!(matches!(
        visit(dir.path(), b),
        Err(MarketForgeError::ResourceLimit(_))
    ));
    let b = bounds(0);
    write_sources(dir.path(), 0, 0);
    assert_eq!(visit(dir.path(), b).unwrap().sources, 0);
}
#[test]
fn reader_preserves_callback_errors_and_field_order() {
    let dir = tempfile::tempdir().unwrap();
    write_sources(dir.path(), 1, 1);
    let path = dir.path().join("boundary.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let sources = value["sources"].take();
    let text =
        format!("{{\"sources\":{sources},\"continuity\":\"independent_sources\",\"version\":2}}");
    std::fs::write(path, text).unwrap();
    assert_eq!(visit(dir.path(), bounds(1)).unwrap().sources, 1);
    let result = visit_depth_stream_boundaries(dir.path(), bounds(1), |_| {
        Err(MarketForgeError::NotAttempted {
            reason: "injected visitor failure".into(),
        })
    });
    assert!(matches!(result, Err(MarketForgeError::NotAttempted { .. })));
}
fn scope(id: u64) -> IntegrityScope {
    IntegrityScope {
        task_id: id,
        stream_id: "synthetic".into(),
        source_file: format!("source-{id}"),
    }
}
fn large_metrics() -> ScopedIntegrityMetrics {
    let mut m = ScopedIntegrityMetrics::default();
    for n in 0..5000 {
        m.files.insert(scope(n), CategoryStatistics::default());
    }
    m
}
#[test]
fn scoped_merge_does_not_clone_the_existing_map_and_is_atomic_on_late_overflow() {
    let mut left = large_metrics();
    let mut right = ScopedIntegrityMetrics::default();
    right
        .files
        .insert(scope(5001), CategoryStatistics::default());
    let (r, allocations, peak) = measured(|| left.merge(&right));
    r.unwrap();
    assert!(allocations < 32, "{allocations}");
    assert!(peak < 16384, "{peak}");
    let key = (scope(1), 0);
    left.daily
        .entry(key.clone())
        .or_default()
        .invalid_record
        .eligible = u64::MAX;
    right.daily.entry(key).or_default().invalid_record.eligible = 1;
    right
        .files
        .entry(scope(0))
        .or_default()
        .invalid_record
        .eligible = 5;
    let before = left.clone();
    assert!(left.merge(&right).is_err());
    assert_eq!(left, before);
}
#[test]
fn report_add_task_prevalidates_every_component_without_cloning_retained_scopes() {
    let mut report = ProcessingMetricsReport {
        scoped_integrity: large_metrics(),
        ..Default::default()
    };
    let task = |id| TaskMetricsReport {
        failure: None,
        task_id: id,
        stream_id: "synthetic".into(),
        source: None,
        accepted_depth_events: None,
        counters: Default::default(),
        integrity: Default::default(),
        scoped_integrity: Default::default(),
        start_timestamp_ns: None,
        end_timestamp_ns: None,
    };
    let (r, allocations, _) = measured(|| report.add_task(task(6)));
    r.unwrap();
    assert!(allocations < 32, "{allocations}");
    report.scoped_integrity.unattributed.invalid_record.eligible = u64::MAX;
    let mut bad = task(7);
    bad.scoped_integrity.unattributed.invalid_record.eligible = 1;
    let before = report.clone();
    assert!(report.add_task(bad).is_err());
    assert_eq!(report, before);
    assert!(report.add_task(task(6)).is_err());
    assert_eq!(report, before);
}
#[test]
fn borrowed_manifest_json_matches_the_owned_persisted_contract() {
    let report = ProcessingMetricsReport::default();
    let manifest = DatasetManifest {
        protocol_version: 3,
        job_id: 1,
        dataset_id: 2,
        content_type: marketforge_engine::historical::job::ContentType::Trades,
        processing_quality: None,
        events_written: 0,
        files_written: 0,
        start_timestamp_ns: None,
        end_timestamp_ns: None,
        source_tasks: vec![],
        instrument_namespace: None,
        resolved_instruments: vec![],
        streams: vec![],
        files: vec![],
        auxiliary_files: vec![],
        integrity_status: IntegrityStatus::Clean,
        integrity_evaluation: IntegrityEvaluation::default(),
        processing_metrics: &report,
    };
    let json = serde_json::to_vec(&manifest).unwrap();
    let owned: DatasetManifest = serde_json::from_slice(&json).unwrap();
    assert_eq!(owned.processing_metrics, report);
    assert_eq!(serde_json::to_vec(&owned).unwrap(), json);
    let dir = tempfile::tempdir().unwrap();
    manifest.write_to(dir.path()).unwrap();
    assert!(manifest.write_to(dir.path()).is_err());
}

#[test]
fn scope_map_deserialization_does_not_build_a_second_entry_inventory() {
    let original = large_metrics();
    let json = serde_json::to_vec(&original).unwrap();
    let ((decoded, retained), _, peak) = measured(|| {
        let decoded: ScopedIntegrityMetrics = serde_json::from_slice(&json).unwrap();
        let retained = LIVE.with(|n| n.get().max(0) as usize);
        (decoded, retained)
    });
    assert_eq!(decoded, original);
    assert!(
        peak <= retained + 64 * 1024,
        "peak={peak} retained={retained}"
    );
    let mut small = ScopedIntegrityMetrics::default();
    small
        .record(&scope(1), IntegrityCategory::InvalidRecord, false, Some(0))
        .unwrap();
    for field in ["files", "hourly", "daily"] {
        let mut value = serde_json::to_value(&small).unwrap();
        let duplicate = value[field][0].clone();
        value[field].as_array_mut().unwrap().push(duplicate);
        assert!(serde_json::from_value::<ScopedIntegrityMetrics>(value).is_err());
    }
}
#[test]
fn merged_integrity_preserves_denominators_windows_unattributed_and_order() {
    use marketforge_engine::historical::job::{
        IntegrityAction, IntegrityPolicy, IntegrityRule, IntegrityWindowRule, IntegrityWindows,
    };
    let rule = IntegrityRule {
        action: IntegrityAction::Degrade,
        max_count: None,
        max_rate: None,
        minimum_samples: 0,
        windows: Default::default(),
    };
    let mut policy = IntegrityPolicy {
        enabled: true,
        parse_failure: rule.clone(),
        invalid_record: rule.clone(),
        sequence_gap: rule.clone(),
        timestamp_regression: rule.clone(),
        missing_snapshot: rule.clone(),
        invalid_book: rule.clone(),
        transformation_failure: rule,
    };
    policy.invalid_record.max_rate = Some(0.15);
    policy.invalid_record.minimum_samples = 2;
    policy.invalid_record.windows = IntegrityWindows {
        daily: Some(IntegrityWindowRule {
            max_rate: 0.15,
            minimum_samples: 1,
        }),
        hourly: Some(IntegrityWindowRule {
            max_rate: 0.15,
            minimum_samples: 1,
        }),
    };
    let mut bad = ScopedIntegrityMetrics::default();
    bad.record(&scope(1), IntegrityCategory::InvalidRecord, true, Some(0))
        .unwrap();
    let mut good = ScopedIntegrityMetrics::default();
    for _ in 0..9 {
        good.record(&scope(2), IntegrityCategory::InvalidRecord, false, Some(0))
            .unwrap();
    }
    let mut first = bad.clone();
    first.merge(&good).unwrap();
    let mut reverse = good.clone();
    reverse.merge(&bad).unwrap();
    assert_eq!(first, reverse);
    assert_eq!(first.global.invalid_record.eligible, 10);
    assert_eq!(first.global.invalid_record.violations, 1);
    let evaluation = evaluate_integrity_policy(&policy, &first).unwrap();
    assert_eq!(evaluation.total_policy_violations, 2);
    assert_eq!(evaluation.status, IntegrityStatus::Failed);
    assert_eq!(
        evaluation
            .violations
            .iter()
            .map(|v| v.threshold)
            .collect::<Vec<_>>(),
        vec![
            IntegrityThreshold::DailyRate,
            IntegrityThreshold::HourlyRate
        ]
    );
    assert_eq!(
        evaluation,
        evaluate_integrity_policy(&policy, &reverse).unwrap()
    );
    first
        .record(&scope(3), IntegrityCategory::InvalidRecord, false, None)
        .unwrap();
    let result = evaluate_integrity_policy(&policy, &first).unwrap();
    assert_eq!(result.total_policy_violations, 3);
    assert_eq!(
        result.violations[0].threshold,
        IntegrityThreshold::UnattributedWindow
    );
    policy.enabled = false;
    let result = evaluate_integrity_policy(&policy, &first).unwrap();
    assert_eq!(result.status, IntegrityStatus::Degraded);
    assert_eq!(result.total_policy_violations, 0);
}
#[test]
fn oversized_nested_values_are_rejected_before_unbounded_decode_allocation() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = source(0, 1);
    s.boundary.symbol = "x".repeat(128 * 1024);
    let path = dir.path().join("boundary.json");
    let write = |s: &DepthSourceBoundary| {
        let mut f = File::create(&path).unwrap();
        f.write_all(br#"{"version":2,"continuity":"independent_sources","sources":["#)
            .unwrap();
        serde_json::to_writer(&mut f, s).unwrap();
        f.write_all(b"]}").unwrap();
    };
    write(&s);
    let mut b = bounds(1);
    b.max_source_bytes = 1024 * 1024;
    b.max_string_bytes = 64;
    let (result, _, peak) = measured(|| visit(dir.path(), b));
    assert!(matches!(result, Err(MarketForgeError::ResourceLimit(_))));
    assert!(peak < 64 * 1024, "{peak}");
    let s = source(0, 10000);
    write(&s);
    b.max_source_bytes = 4 * 1024 * 1024;
    b.max_string_bytes = 1024;
    let (result, _, peak) = measured(|| visit(dir.path(), b));
    assert!(result.is_err());
    assert!(peak < 64 * 1024, "{peak}");
}
#[test]
fn finalization_reserves_one_boundary_workspace_and_keeps_report_admission_honest() {
    use marketforge_engine::historical::{
        config::{FormatCatalog, read_prepared_batch},
        normalize::{load_processing_config, resources::inspect_admission},
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut config =
        load_processing_config(root.join("data/.config/historical/general.toml"), root).unwrap();
    let batch = read_prepared_batch(
        root.join("engine/tests/historical/fixtures/normalize.json"),
        root,
    )
    .unwrap();
    let catalog = FormatCatalog::load(root.join("data/.config/historical")).unwrap();
    let mut job = batch
        .jobs
        .iter()
        .map(|j| batch.resolve_job(j, &catalog, &config, root).unwrap())
        .find(|j| j.output.content_type == marketforge_engine::historical::job::ContentType::Depth)
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("not-an-archive");
    std::fs::write(
        &input,
        b"invalid archive payload; diagnostic must not decode this",
    )
    .unwrap();
    let mut template = job.tasks[0].clone();
    template.input_path = input;
    config.resources.normalization_limits = NormalizationLimits {
        metric_entries: 16,
        depth_levels: 32,
        record_bytes: 1024,
        ..Default::default()
    };
    config.resources.memory_budget_bytes = 1024; // diagnostic reports failure without acquiring or blocking
    let mut finalization = Vec::new();
    for n in [1, 200] {
        job.tasks = (0..n)
            .map(|i| {
                let mut t = template.clone();
                t.task_id.0 = i as u64;
                t
            })
            .collect();
        let d = inspect_admission(std::slice::from_ref(&job), &config).unwrap();
        assert_eq!(d.retained_boundary_allowance_bytes, 0);
        assert!(!d.memory_admission_fits);
        assert_eq!(d.retained_detailed_reports_bytes, 0);
        assert!(d.retained_compact_reports_bytes > 0);
        assert!(d.current_reserved_bytes.is_none());
        assert!(d.measured_process_rss_bytes.is_none());
        let sum = d.retained_metadata_bytes
            + d.failed_spill_retention_allowance_bytes
            + d.retained_detailed_reports_bytes
            + d.retained_compact_reports_bytes
            + d.largest_active_unit_bytes
                .max(d.finalization_allowance_bytes);
        assert_eq!(d.required_memory_bytes, sum);
        finalization.push(d.finalization_allowance_bytes);
    }
    // Only the compact duplicate-index allowance grows with source inventory.
    assert_eq!(finalization[1] - finalization[0], 199 * 128);
}
#[test]
fn prepared_identity_lengths_have_a_matching_decode_and_workspace_allowance() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = source(0, 1);
    s.boundary.symbol = "long-prepared-symbol".repeat(1024);
    let file = File::create(dir.path().join("boundary.json")).unwrap();
    serde_json::to_writer(
        file,
        &DepthStreamBoundaries {
            version: 2,
            continuity: "independent_sources".into(),
            sources: vec![s.clone()],
        },
    )
    .unwrap();
    let b = bounds(1);
    assert!(visit(dir.path(), b).is_err());
    let extended = b
        .with_identity_bytes((s.boundary.symbol.len() + s.boundary.stream_id.len()) as u64)
        .unwrap();
    assert!(extended.workspace_bytes().unwrap() > b.workspace_bytes().unwrap());
    assert_eq!(visit(dir.path(), extended).unwrap().sources, 1);
}
#[test]
fn book_vector_growth_stays_inside_the_admitted_capacity() {
    let dir = tempfile::tempdir().unwrap();
    write_sources(dir.path(), 2, 257);
    let b = BoundaryReadLimits::for_sources(
        2,
        NormalizationLimits {
            depth_levels: 1024,
            record_bytes: 1024,
            ..Default::default()
        },
    )
    .unwrap();
    let ((summary, count), _, peak) = measured(|| {
        let mut count = 0;
        let summary = visit_depth_stream_boundaries(dir.path(), b, |s| {
            let initial = s.boundary.initial.as_ref().unwrap();
            assert_eq!(initial.state.bids.len(), 257);
            assert!(initial.state.bids.capacity() <= 1024);
            count += 1;
            Ok(())
        })
        .unwrap();
        (summary, count)
    });
    assert_eq!(count, 2);
    assert_eq!(summary.peak_source_levels, 514);
    assert_eq!(summary.total_rows, 514);
    assert!(peak as u64 <= b.workspace_bytes().unwrap());
}

#[test]
fn completed_sources_keep_only_summaries_and_stream_exact_details() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut config = marketforge_engine::historical::normalize::load_processing_config(
        "data/.config/historical/general.toml",
        root,
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    config.resources.scratch_path = temp.path().into();
    let mut aggregate = ProcessingMetricsReport::default();
    let mut expected = ScopedIntegrityMetrics::default();
    for task_id in (1..=48).rev() {
        let mut metrics = TaskMetrics::default();
        let scope = IntegrityScope {
            task_id,
            stream_id: "long.synthetic.stream".into(),
            source_file: format!("source-{task_id}"),
        };
        for hour in 0..256 {
            metrics
                .scoped_integrity
                .record(
                    &scope,
                    IntegrityCategory::InvalidRecord,
                    hour == 5,
                    Some(hour * HOUR_NS),
                )
                .unwrap();
        }
        metrics
            .scoped_integrity
            .record(&scope, IntegrityCategory::ParseFailure, false, None)
            .unwrap();
        expected.merge(&metrics.scoped_integrity).unwrap();
        metrics.scoped_integrity.spill(7, task_id, &config).unwrap();
        let sealed = metrics.scoped_integrity.clone();
        assert!(
            metrics
                .scoped_integrity
                .record(&scope, IntegrityCategory::InvalidRecord, false, Some(0))
                .is_err()
        );
        assert_eq!(metrics.scoped_integrity, sealed);
        assert!(metrics.scoped_integrity.files.is_empty());
        assert!(metrics.scoped_integrity.hourly.is_empty());
        assert!(metrics.scoped_integrity.daily.is_empty());
        // Reconciliation amends the compact result after observation persistence.
        metrics.counters.events_normalized = 10;
        metrics.counters.events_written = 6;
        metrics.counters.rows_suppressed = 3;
        metrics.counters.rows_unpublished = 1;
        aggregate
            .add_task(TaskMetricsReport {
                failure: None,
                task_id,
                stream_id: scope.stream_id,
                counters: metrics.counters,
                integrity: metrics.integrity,
                scoped_integrity: metrics.scoped_integrity,
                source: None,
                accepted_depth_events: None,
                start_timestamp_ns: Some(3),
                end_timestamp_ns: Some(7),
            })
            .unwrap();
    }
    assert!(aggregate.scoped_integrity.hourly.is_empty());
    assert_eq!(aggregate.counters.events_written, 48 * 6);
    assert_eq!(aggregate.counters.rows_suppressed, 48 * 3);
    assert_eq!(aggregate.counters.rows_unpublished, 48);
    // Consumers decode one framed observation at a time, not one complete source.
    let (result, _, peak) = measured(|| serde_json::to_writer(std::io::sink(), &aggregate));
    result.unwrap();
    assert!(
        peak < 4 * 1024 * 1024,
        "streaming serialization peak allocated bytes: {peak}"
    );
    assert_eq!(aggregate.scoped_integrity, expected);
    let policy = &config.integrity_policy;
    assert_eq!(
        evaluate_integrity_policy(policy, &aggregate.scoped_integrity).unwrap(),
        evaluate_integrity_policy(policy, &expected).unwrap()
    );
    assert_eq!(
        aggregate.scoped_integrity.global.invalid_record.eligible,
        48 * 256
    );
    drop(aggregate);
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[test]
fn failed_report_spill_preserves_observations_and_shared_quota() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut config = marketforge_engine::historical::normalize::load_processing_config(
        "data/.config/historical/general.toml",
        root,
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    config.resources.scratch_path = temp.path().into();
    config.resources.scratch_budget_bytes = 1;
    let mut metrics = ScopedIntegrityMetrics::default();
    metrics
        .record(
            &IntegrityScope {
                task_id: 3,
                stream_id: "s".into(),
                source_file: "f".into(),
            },
            IntegrityCategory::InvalidRecord,
            true,
            Some(0),
        )
        .unwrap();
    let before = metrics.clone();
    let error = metrics.spill(1, 3, &config).unwrap_err();
    assert!(error.to_string().contains("scratch bytes"));
    assert_eq!(metrics, before);
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
    config.resources.scratch_budget_bytes = 1024 * 1024;
    let mut a = metrics.clone();
    let mut b = metrics.clone();
    a.spill(1, 3, &config).unwrap();
    b.spill(2, 3, &config).unwrap();
    drop(a);
    assert_eq!(b, before); // Identical task IDs in separate jobs have independent ownership.
}

#[test]
fn truncated_retained_observations_reject_serialization_instead_of_claiming_a_complete_report() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut config = marketforge_engine::historical::normalize::load_processing_config(
        "data/.config/historical/general.toml",
        root,
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    config.resources.scratch_path = temp.path().into();
    let mut metrics = ScopedIntegrityMetrics::default();
    metrics
        .record(
            &IntegrityScope {
                task_id: 1,
                stream_id: "s".into(),
                source_file: "f".into(),
            },
            IntegrityCategory::InvalidRecord,
            false,
            Some(0),
        )
        .unwrap();
    metrics.spill(1, 1, &config).unwrap();
    let path = std::fs::read_dir(temp.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.set_len(file.metadata().unwrap().len() - 1).unwrap();
    drop(file);
    let error = serde_json::to_writer(std::io::sink(), &metrics).unwrap_err();
    assert!(error.to_string().contains("scratch"), "{error}");
}

#[test]
fn report_frame_capacity_follows_scope_identity_without_lowering_metric_limits() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut config = marketforge_engine::historical::normalize::load_processing_config(
        "data/.config/historical/general.toml",
        root,
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    config.resources.scratch_path = temp.path().into();
    let scope = IntegrityScope {
        task_id: 7,
        stream_id: "s".repeat(70_000),
        source_file: "source".into(),
    };
    let mut metrics = ScopedIntegrityMetrics::default();
    metrics
        .record(&scope, IntegrityCategory::InvalidRecord, false, Some(0))
        .unwrap();
    let expected = metrics.clone();
    metrics.spill(8, 7, &config).unwrap();
    serde_json::to_writer(std::io::sink(), &metrics).unwrap();
    assert_eq!(metrics, expected);
}
