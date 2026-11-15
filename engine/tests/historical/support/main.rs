#[path = "mod.rs"]
mod support;
use marketforge_engine::{
    historical::job::{ContentType, InstrumentKind},
    shared::domain::canonical::Exchange,
};
use support::{TestResult, fixtures::*, runtime::*};
fn selection(kind: ContentType) -> Selection<'static> {
    Selection {
        exchange: Exchange::Bybit,
        instrument_type: InstrumentKind::Spot,
        market_category: "spot",
        content_type: kind,
        symbol: None,
        family: None,
        feed: None,
        filename: None,
    }
}
#[test]
fn selection_requires_exact_nonempty_matches() {
    let b = workload().unwrap();
    let mut s = selection(ContentType::Trades);
    let stream = concrete_stream(
        &b,
        Exchange::Bybit,
        InstrumentKind::Spot,
        ContentType::Trades,
        1,
    )
    .unwrap();
    s.symbol = stream.jobs[0].tasks[0].symbol.as_deref();
    s.filename = None;
    assert!(
        select(&b, "ambiguous", &s, stream.jobs[0].tasks.len() + 1)
            .unwrap_err()
            .contains(&format!("found {}", stream.jobs[0].tasks.len()))
    );
    s.symbol = Some("ABSENT");
    assert!(
        select(&b, "missing", &s, 1)
            .unwrap_err()
            .contains("found 0")
    );
    assert!(select(&b, "empty", &s, 0).is_err());
}
#[test]
fn persistent_runs_are_unique_safe_and_record_failures() {
    let mut a = TestRun::new("support_isolation").unwrap();
    let b = TestRun::new("support_isolation").unwrap();
    assert_ne!(a.path, b.path);
    assert!(a.path.exists() && b.path.exists());
    assert!(a.owned(&a.path.join("../escape")).is_err());
    assert!(a.owned(&b.path).is_err());
    assert!(TestRun::new("../bad").is_err());
    let failure: TestResult<()> = Err("synthetic diagnostic".into());
    a.finish(&failure).unwrap();
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(a.path.join("test.json")).unwrap()).unwrap();
    assert_eq!(metadata["status"], "failed");
    assert_eq!(metadata["failure"], "synthetic diagnostic");
}
#[test]
fn subsets_use_production_resolution_and_isolate_every_job() {
    let b = workload().unwrap();
    let mut s = selection(ContentType::Trades);
    let stream = concrete_stream(
        &b,
        Exchange::Bybit,
        InstrumentKind::Spot,
        ContentType::Trades,
        1,
    )
    .unwrap();
    s.symbol = stream.jobs[0].tasks[0].symbol.as_deref();
    s.filename = None;
    let count = stream.jobs[0].tasks.len();
    let subset = select(&b, "subset", &s, count).unwrap();
    let mut run = TestRun::new("support_subset").unwrap();
    let c = run
        .runtime(Overrides {
            workers: Some(2),
            ..Default::default()
        })
        .unwrap();
    let resolved = run.prepare(&subset, &c).unwrap();
    assert_eq!(resolved.jobs.len(), 1);
    assert_eq!(resolved.jobs[0].tasks.len(), count);
    assert_eq!(resolved.jobs[0].resources.workers, 2);
    assert_eq!(
        resolved.jobs[0].tasks[0].raw_schema["marketforge_profile"]["id"],
        "bybit.spot.spot.trades"
    );
    assert_eq!(resolved.jobs[0].integrity_policy, c.integrity_policy);
    assert!(
        resolved
            .jobs
            .iter()
            .all(|j| j.output.dataset_path.starts_with(&run.path)
                && j.output.staging_path.starts_with(&run.path))
    );
    assert!(c.resources.scratch_path.starts_with(&run.path));
    assert!(
        run.runtime(Overrides {
            workers: Some(0),
            ..Default::default()
        })
        .is_err()
    );
    let result = Ok(());
    run.finish(&result).unwrap();
}
#[test]
fn multi_job_redirection_retains_distinct_identities() {
    let source = workload().unwrap();
    let mut b = concrete_stream(
        &source,
        Exchange::Bybit,
        InstrumentKind::Spot,
        ContentType::Trades,
        1,
    )
    .unwrap();
    b.jobs[0].tasks.truncate(1);
    let mut second = b.jobs[0].clone();
    second.job_id.0 += 1;
    second.dataset_id.0 += 1;
    second.tasks[0].task_id.0 += 1;
    b.jobs.push(second);
    let mut r = TestRun::new("support_multi").unwrap();
    let c = r.runtime(Default::default()).unwrap();
    let jobs = r.prepare(&b, &c).unwrap().jobs;
    assert_ne!(jobs[0].output.dataset_path, jobs[1].output.dataset_path);
    assert_eq!(jobs[0].dataset_id, b.jobs[0].dataset_id);
    r.finish(&Ok(())).unwrap();
}
#[cfg(feature = "historical-real-tests")]
fn representative(kind: ContentType) -> TestResult<()> {
    let b = workload()?;
    let mut b = concrete_stream(&b, Exchange::Bybit, InstrumentKind::Spot, kind, 1)?;
    b.jobs[0].tasks.truncate(1);
    let mut run = TestRun::new(if kind == ContentType::Trades {
        "bybit_spot_trade_prefix"
    } else {
        "bybit_spot_depth_prefix"
    })?;
    let result = (|| {
        sample_prefix(
            &mut run,
            &mut b,
            if kind == ContentType::Trades { 3 } else { 2 },
        )?;
        let config = run.runtime_from_general()?;
        let resolved = run.prepare(&b, &config)?;
        let task = &resolved.jobs[0].tasks[0];
        let raw = support::runtime::sample_bytes(task)?;
        let (expected_price, expected_quantity): (rust_decimal::Decimal, rust_decimal::Decimal) =
            if kind == ContentType::Trades {
                let mut csv = csv::Reader::from_reader(raw.as_slice());
                let headers = csv.headers().map_err(|e| e.to_string())?.clone();
                let row = csv
                    .records()
                    .next()
                    .ok_or("missing sampled trade")?
                    .map_err(|e| e.to_string())?;
                let field = |name| {
                    row.get(headers.iter().position(|h| h == name).unwrap())
                        .unwrap()
                };
                (
                    field("price").parse().map_err(|e| format!("{e}"))?,
                    field("volume").parse().map_err(|e| format!("{e}"))?,
                )
            } else {
                let first = raw
                    .split(|b| *b == b'\n')
                    .next()
                    .ok_or("missing snapshot")?;
                let snapshot: serde_json::Value =
                    serde_json::from_slice(first).map_err(|e| e.to_string())?;
                let bid = &snapshot["data"]["b"][0];
                (
                    bid[0]
                        .as_str()
                        .ok_or("missing bid price")?
                        .parse()
                        .map_err(|e| format!("{e}"))?,
                    bid[1]
                        .as_str()
                        .ok_or("missing bid size")?
                        .parse()
                        .map_err(|e| format!("{e}"))?,
                )
            };
        let results = resolved
            .jobs
            .iter()
            .map(|j| {
                marketforge_engine::historical::normalize::execute_processing_job(j, &config)
                    .map_err(|e| e.to_string())
            })
            .collect::<TestResult<Vec<_>>>()?;
        for (j, r) in b.jobs.iter().zip(&results) {
            let manifest = support::assertions::committed(&run, j, r)?;
            if manifest.events_written == 0 {
                return Err("empty representative dataset".into());
            }
            let dataset = std::path::Path::new(r.manifest_path.as_ref().unwrap())
                .parent()
                .unwrap();
            if kind == ContentType::Trades && manifest.events_written != 2 {
                return Err("expected two sampled trades".into());
            }
            let mut found = false;
            for file in &manifest.files {
                let reader =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                        std::fs::File::open(dataset.join(&file.path)).map_err(|e| e.to_string())?,
                    )
                    .map_err(|e| e.to_string())?
                    .build()
                    .map_err(|e| e.to_string())?;
                for batch in reader {
                    let batch = batch.map_err(|e| e.to_string())?;
                    let prices = batch
                        .column_by_name("price")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<arrow_array::Decimal256Array>()
                        .unwrap();
                    let quantities = batch
                        .column_by_name("quantity_base")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<arrow_array::Decimal256Array>()
                        .unwrap();
                    for row in 0..batch.num_rows() {
                        let price =
                            marketforge_engine::shared::persistence::parquet::i256_to_decimal(
                                prices.value(row),
                            )
                            .map_err(|e| e.to_string())?;
                        let quantity =
                            marketforge_engine::shared::persistence::parquet::i256_to_decimal(
                                quantities.value(row),
                            )
                            .map_err(|e| e.to_string())?;
                        if price == expected_price && quantity == expected_quantity {
                            found = true;
                        }
                    }
                }
            }
            if !found {
                return Err(format!(
                    "missing independently inspected price/quantity {expected_price}/{expected_quantity}"
                ));
            }
        }
        Ok(())
    })();
    run.finish(&result)?;
    result
}
#[cfg(feature = "historical-real-tests")]
#[test]
fn representative_trade() {
    representative(ContentType::Trades).unwrap();
}
#[cfg(feature = "historical-real-tests")]
#[test]
fn representative_depth() {
    representative(ContentType::Depth).unwrap();
}

#[test]
fn concurrent_run_creation_is_exclusive() {
    let handles: Vec<_> = (0..8)
        .map(|_| {
            std::thread::spawn(|| {
                let mut run = TestRun::new("support_concurrent").unwrap();
                run.finish(&Ok(())).unwrap();
                run.path
            })
        })
        .collect();
    let paths: std::collections::BTreeSet<_> =
        handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(paths.len(), 8);
    assert!(paths.iter().all(|p| p.join("test.json").exists()));
}
#[test]
fn missing_source_is_an_actionable_failure_with_retained_metadata() {
    let b = workload().unwrap();
    let mut s = selection(ContentType::Trades);
    let stream = concrete_stream(
        &b,
        Exchange::Bybit,
        InstrumentKind::Spot,
        ContentType::Trades,
        1,
    )
    .unwrap();
    s.symbol = stream.jobs[0].tasks[0].symbol.as_deref();
    s.filename = None;
    let mut b = select(&b, "missing_source", &s, stream.jobs[0].tasks.len()).unwrap();
    b.jobs[0].tasks.truncate(1);
    let mut run = TestRun::new("support_missing_source").unwrap();
    b.jobs[0].tasks[0].input_path = run.path.join("absent.archive");
    let result = sample_prefix(&mut run, &mut b, 3);
    assert!(result.as_ref().unwrap_err().contains("required source"));
    run.finish(&result).unwrap();
    assert!(run.path.join("selected-batch.json").exists());
}

#[path = "../normalize/integrity_accounting.rs"]
mod integrity_accounting;

#[path = "../normalize/source_ordering.rs"]
mod source_ordering;

mod stream_depth;
mod streams;

mod case_run;
