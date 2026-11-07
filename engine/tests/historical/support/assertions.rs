use super::{TestResult, runtime::TestRun};
use marketforge_engine::historical::{
    config::PreparedJob,
    normalize::{
        ProcessingResult, ProcessingStatus,
        manifest::{AuxiliaryFileKind, DatasetManifest},
    },
};
use std::{fs, path::Path};
pub fn committed(
    run: &TestRun,
    job: &PreparedJob,
    result: &ProcessingResult,
) -> TestResult<DatasetManifest> {
    if result.status != ProcessingStatus::Complete
        || result.job_id != job.job_id
        || result.dataset_id != job.dataset_id
    {
        return Err(format!("unexpected execution result {result:?}"));
    }
    let manifest_path = Path::new(
        result
            .manifest_path
            .as_deref()
            .ok_or("missing manifest path")?,
    );
    run.owned(manifest_path)?;
    let m: DatasetManifest =
        serde_json::from_slice(&fs::read(manifest_path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    let dataset = manifest_path.parent().unwrap();
    if m.job_id != job.job_id.0
        || m.dataset_id != job.dataset_id.0
        || m.source_tasks != job.tasks.iter().map(|t| t.task_id.0).collect::<Vec<_>>()
        || m.files_written != m.files.len() as u64
        || m.events_written != m.files.iter().map(|f| f.rows).sum::<u64>()
        || m.events_written != result.events_written
    {
        return Err("manifest identity/inventory mismatch".into());
    }
    if m.start_timestamp_ns
        .zip(m.end_timestamp_ns)
        .is_some_and(|(a, b)| a > b)
    {
        return Err("invalid timestamp range".into());
    }
    for f in &m.files {
        let path = dataset.join(&f.path);
        run.owned(&path)?;
        if fs::metadata(&path).map_err(|e| e.to_string())?.len() != f.size_bytes {
            return Err("file size mismatch".into());
        }
        let parquet = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            fs::File::open(&path).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        if parquet.metadata().file_metadata().num_rows()
            != i64::try_from(f.rows).map_err(|e| e.to_string())?
        {
            return Err("Parquet row count mismatch".into());
        }
    }
    for f in &m.auxiliary_files {
        let path = dataset.join(&f.path);
        run.owned(&path)?;
        if fs::metadata(path).map_err(|e| e.to_string())?.len() != f.size_bytes {
            return Err("auxiliary size mismatch".into());
        }
    }
    for name in ["processing-job.json", "processing-job.sha256"] {
        if !dataset.join(name).is_file() {
            return Err(format!("missing {name}"));
        }
    }
    if m.content_type == marketforge_engine::historical::job::ContentType::Depth {
        for kind in [
            AuxiliaryFileKind::DepthEvents,
            AuxiliaryFileKind::DepthSegments,
            AuxiliaryFileKind::DepthBoundary,
        ] {
            if !m.auxiliary_files.iter().any(|f| f.kind == kind) {
                return Err(format!("missing depth auxiliary {kind:?}"));
            }
        }
    }
    Ok(m)
}
