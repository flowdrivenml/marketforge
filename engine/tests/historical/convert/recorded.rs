//! Bounded recorded materialization and opt-in full publication.
#[path = "../support/conversion_fixture.rs"]
mod conversion_fixture;

use std::{env, fs, path::PathBuf, process::Command};

use marketforge_engine::historical::{
    convert::output::ConversionManifest,
    job::{ConvertJob, MergeTimelineOrdering},
    merge::UnifiedMergeMode,
};

fn recorded_input() -> (
    ConvertJob,
    marketforge_engine::historical::convert::PreparedConversion,
) {
    let mut job = conversion_fixture::canonical_job();
    if let Some(path) = env::var_os("MARKETFORGE_RECORDED_CONVERT_INPUT") {
        job.input_merge = PathBuf::from(path);
    }
    let prepared = marketforge_engine::historical::convert::prepare_conversion_execution(&job)
        .expect("Supply the complete committed merge 2079541492669036539 or set MARKETFORGE_RECORDED_CONVERT_INPUT to its absolute location");
    let source = &prepared.source;
    assert_eq!(source.job_id, 2079541492669036539);
    assert_eq!(source.mode, UnifiedMergeMode::Combined);
    assert_eq!(source.timeline_ordering, MergeTimelineOrdering::Causal);
    assert_eq!(source.physical_schema_version, 2);
    assert_eq!(
        (source.time_range.start_ns, source.time_range.end_ns),
        (1793059200000000000, 1793318400000000000)
    );
    assert_eq!(
        (
            source.events_written,
            source.trade_events,
            source.depth_events
        ),
        (12_477_919, 5_233_312, 7_244_607)
    );
    assert_eq!(source.streams.len(), 8);
    (job, prepared)
}

#[test]
fn validates_canonical_recorded_prefix_with_causal_prehistory() {
    use marketforge_engine::historical::convert::execution::materialize_combined_conversion;

    let started = std::time::Instant::now();
    let (mut job, prepared) = recorded_input();
    let source = prepared.source;
    let plan = prepared.plan;
    eprintln!(
        "Recorded input inventory and provenance checked in {:?}",
        started.elapsed()
    );
    let started = std::time::Instant::now();
    let temp = tempfile::tempdir().unwrap();
    job.output.dataset_path = temp.path().join("converted");
    let staging = temp.path().join("prefix");
    let parquet = marketforge_engine::historical::job::MergeParquetConfig {
        row_group_target_mb: 4,
        file_target_mb: 16,
        compression_level: 1,
    };
    let metrics =
        materialize_combined_conversion(&job, &source, None, &plan, &staging, &parquet, Some(4096))
            .unwrap();
    assert_eq!(metrics.events_written, 4096);
    assert!(metrics.trade_events > 0 && metrics.depth_events > 0);
    assert!(metrics.internal_reference_updates > 0);
    assert!(metrics.identity_events > 0 && metrics.converted_events > 0);
    eprintln!(
        "Recorded prefix materialized in {:?}: {metrics:?}",
        started.elapsed()
    );
    use arrow_array::{Array, BooleanArray, ListArray, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::{BTreeMap, BTreeSet};
    let mut streams = BTreeMap::new();
    let mut routes = BTreeSet::new();
    let mut reconstruction = 0;
    for path in marketforge_engine::historical::convert::output::discover_conversion_files(&staging)
        .unwrap()
    {
        let batches = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        for batch in batches {
            let batch = batch.unwrap();
            let ids = batch
                .column_by_name("stream_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let paths = batch
                .column_by_name("conversion_path")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let context = batch
                .column_by_name("reconstruction_only")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            reconstruction += context.values().count_set_bits();
            for row in 0..batch.num_rows() {
                *streams.entry(ids.value(row).to_owned()).or_insert(0) += 1;
                if !paths.is_null(row) {
                    let values = paths.value(row);
                    let values = values.as_any().downcast_ref::<StringArray>().unwrap();
                    routes.insert(
                        values
                            .iter()
                            .map(Option::unwrap)
                            .collect::<Vec<_>>()
                            .join(" -> "),
                    );
                }
            }
        }
    }
    assert_eq!(streams.values().sum::<u64>(), 4096);
    assert_eq!(
        streams.keys().map(String::as_str).collect::<BTreeSet<_>>(),
        source
            .streams
            .iter()
            .map(|stream| stream.stream_id.as_str())
            .collect(),
    );
    assert_eq!(
        routes,
        BTreeSet::from([
            "USDT".to_owned(),
            "BTC -> USDT".to_owned(),
            "USDC -> BTC -> USDT".to_owned(),
        ])
    );
    println!(
        "Recorded prefix streams {streams:?}; valued routes {routes:?}; reconstruction events {reconstruction}"
    );
    assert!(!job.output.dataset_path.exists());
}

#[test]
#[ignore = "opt-in full recorded conversion publication"]
fn publishes_canonical_recorded_conversion() {
    let (mut job, prepared) = recorded_input();
    let source = prepared.source;
    let temp = tempfile::tempdir().unwrap();
    job.output.dataset_path = temp.path().join("converted");
    let derived_path = temp.path().join("job.json");
    serde_json::to_writer(fs::File::create(&derived_path).unwrap(), &job).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_marketforge-convert"));
    command.arg("--job").arg(derived_path).arg("--run");
    assert!(
        command.status().unwrap().success(),
        "Check the declared committed merge publication and temporary disk space"
    );
    let output: ConversionManifest = serde_json::from_reader(
        fs::File::open(job.output.dataset_path.join("manifest.json")).unwrap(),
    )
    .unwrap();
    output.validate().unwrap();
    assert_eq!(output.statistics.events_written, source.events_written);
    assert_eq!(
        output.integrity.source_events_compared,
        source.events_written
    );
    assert!(output.statistics.identity > 0);
    assert!(output.statistics.converted > 0);
}
