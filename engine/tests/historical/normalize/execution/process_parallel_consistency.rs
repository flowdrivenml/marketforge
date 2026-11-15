#![cfg(feature = "historical")]

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use {
    marketforge_engine::historical::normalize::ProcessingStatus,
    marketforge_engine::historical::normalize::execute_processing_job,
    marketforge_engine::historical::normalize::manifest::DatasetManifest,
    marketforge_engine::historical::normalize::manifest::inspect_parquet_files,
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

// -----------------------------------------------------------------------------
// Read canonical Parquet records
// -----------------------------------------------------------------------------

use sha2::{Digest, Sha256};

fn canonical_fingerprint(dataset: &Path) -> (String, u64) {
    let mut files = inspect_parquet_files(dataset).expect("discover Parquet files");

    // Deterministic ordering by task output path.
    files.sort_by(|a, b| a.path.cmp(&b.path));

    let mut hasher = Sha256::new();
    let mut total_rows = 0u64;

    for file in files {
        let path = dataset.join(&file.path);

        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(path).expect("open Parquet file"))
                .expect("read Parquet metadata")
                .build()
                .expect("build Parquet reader");

        for batch in reader {
            let batch = batch.expect("read RecordBatch");

            // Hash the canonical Arrow data in deterministic order.
            //
            // This initial implementation hashes complete batches.
            // It is suitable only when batch boundaries remain identical.
            // Hash individual logical rows so reader batch boundaries
            // cannot affect the comparison.
            for row in 0..batch.num_rows() {
                let record = batch.slice(row, 1);
                let representation = format!("{record:?}");
                hasher.update((representation.len() as u64).to_le_bytes());
                hasher.update(representation.as_bytes());
                total_rows += 1;
            }
        }
    }

    let digest = hasher.finalize();

    (
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        total_rows,
    )
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

fn parallel_fixture() -> marketforge_engine::historical::config::PreparedBatch {
    use crate::support::fixtures::workload;
    use marketforge_engine::historical::job::ContentType;
    use std::collections::BTreeSet;

    let mut batch = workload().expect("authoritative normalization fixture");

    batch
        .jobs
        .retain(|job| job.output.content_type == ContentType::Trades);
    assert_eq!(batch.jobs.len(), 1, "expected one prepared trade job");

    let job = &mut batch.jobs[0];

    // Choose the smallest available real archive from each concrete
    // stream. This keeps the equivalence test reasonably bounded.
    let mut candidates = job
        .tasks
        .iter()
        .filter(|task| task.scope.is_none() && task.symbol.is_some())
        .map(|task| {
            let size = fs::metadata(&task.input_path)
                .expect("required real archive")
                .len();

            (size, task.stream_id.0.clone(), task.task_id.0)
        })
        .collect::<Vec<_>>();

    candidates.sort();

    let mut streams = BTreeSet::new();
    let mut selected = BTreeSet::new();

    for (_, stream, task_id) in candidates {
        if streams.insert(stream) {
            selected.insert(task_id);
        }

        if selected.len() == 2 {
            break;
        }
    }

    assert_eq!(
        selected.len(),
        2,
        "authoritative fixture must provide two concrete trade streams"
    );

    job.tasks.retain(|task| selected.contains(&task.task_id.0));

    for task in &job.tasks {
        println!(
            "Selected stream={} task={} archive={}",
            task.stream_id.0,
            task.task_id.0,
            task.input_path.display()
        );
    }

    batch
}

fn execute_selected(
    batch: &marketforge_engine::historical::config::PreparedBatch,
    workers: usize,
    run: &mut crate::support::runtime::TestRun,
) -> (DatasetManifest, PathBuf) {
    let mut config = run
        .runtime_from_general()
        .expect("load production normalization settings");

    config.resources.workers = workers;

    let resolved = run
        .prepare(batch, &config)
        .expect("resolve selected real sources");

    assert_eq!(resolved.jobs.len(), 1);

    let job = &resolved.jobs[0];

    let result = execute_processing_job(job, &config).expect("execute selected trade job");

    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "processing failed: {:?}",
        result.failure
    );

    let dataset = job.output.dataset_path.clone();

    let manifest: DatasetManifest = serde_json::from_slice(
        &fs::read(dataset.join("manifest.json")).expect("read committed manifest"),
    )
    .expect("deserialize committed manifest");

    assert!(manifest.events_written > 0);
    assert_eq!(manifest.events_written, result.events_written);

    (manifest, dataset)
}

fn verify_parallel_consistency(
    batch: marketforge_engine::historical::config::PreparedBatch,
    name: &str,
    workers: usize,
) {
    use crate::support::runtime::TestRun;

    let mut sequential_run =
        TestRun::new(&format!("execution/{name}_sequential")).expect("create sequential test run");

    let mut parallel_run =
        TestRun::new(&format!("execution/{name}_parallel")).expect("create parallel test run");

    let (sequential, sequential_dataset) = execute_selected(&batch, 1, &mut sequential_run);

    let (parallel, parallel_dataset) = execute_selected(&batch, workers, &mut parallel_run);

    assert_eq!(
        sequential.processing_metrics.counters, parallel.processing_metrics.counters,
        "processing counters differ",
    );

    assert_eq!(
        sequential.processing_metrics.integrity, parallel.processing_metrics.integrity,
        "integrity metrics differ",
    );

    assert_eq!(
        sequential.processing_metrics.scoped_integrity,
        parallel.processing_metrics.scoped_integrity,
        "scoped integrity metrics differ",
    );

    assert_eq!(sequential.integrity_status, parallel.integrity_status);
    assert_eq!(
        sequential.integrity_evaluation,
        parallel.integrity_evaluation,
    );

    assert_eq!(sequential.events_written, parallel.events_written);
    assert_eq!(sequential.start_timestamp_ns, parallel.start_timestamp_ns,);
    assert_eq!(sequential.end_timestamp_ns, parallel.end_timestamp_ns,);

    // Physical file counts can legitimately differ.
    let (sequential_hash, sequential_rows) = canonical_fingerprint(&sequential_dataset);

    let (parallel_hash, parallel_rows) = canonical_fingerprint(&parallel_dataset);

    assert_eq!(
        sequential_rows, parallel_rows,
        "canonical row counts differ"
    );
    assert_eq!(
        sequential_hash, parallel_hash,
        "canonical row content or ordering differs"
    );

    println!("Sequential records: {}", sequential.events_written);
    println!("Parallel records:   {}", parallel.events_written);
    println!("Canonical rows:     {parallel_rows}");
    println!("Canonical equality: PASS");
    println!("Metrics equality:   PASS");
}

#[test]
fn single_and_parallel_workers_produce_identical_results() {
    verify_parallel_consistency(parallel_fixture(), "parallel_consistency", 8);
}

#[test]
#[cfg(feature = "historical-full-tests")]
fn large_parallel_consistency() {
    let mut batch = crate::support::fixtures::workload().expect("authoritative prepared batch");
    batch.jobs.retain_mut(|job| {
        if job.output.content_type != marketforge_engine::historical::job::ContentType::Trades {
            return false;
        }
        job.tasks.retain(|task| {
            task.exchange == marketforge_engine::shared::domain::canonical::Exchange::Bitget
        });
        !job.tasks.is_empty()
    });
    assert!(
        !batch.jobs.is_empty(),
        "full parallel coverage requires Bitget trades"
    );
    verify_parallel_consistency(batch, "large_parallel_consistency", 2);
}
