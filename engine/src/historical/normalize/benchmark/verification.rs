use super::{Result, model::*};
use crate::historical::{
    job::ProcessingJob,
    normalize::{
        ProcessingStatus,
        manifest::{DatasetManifest, inspect_auxiliary_files, inspect_parquet_files},
        metrics::IntegrityStatus,
    },
};
use arrow_array::{
    Array, BooleanArray, Decimal256Array, Int64Array, RecordBatch, StringArray, UInt8Array,
    UInt64Array,
};
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    file::reader::{FileReader, SerializedFileReader},
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};
fn checked_add(a: &mut u64, b: u64) -> Result<()> {
    *a = a.checked_add(b).ok_or("benchmark counter overflow")?;
    Ok(())
}
/// Layout-independent typed record digest; explicit null/length boundaries.
fn digest_rows(
    batch: &RecordBatch,
    streams: &mut BTreeMap<String, Sha256>,
    fallback: Option<&str>,
) -> Result<()> {
    let identity = batch
        .column_by_name("stream_id")
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());
    if identity.is_none() && fallback.is_none() {
        return Err("canonical stream_id missing".into());
    }
    for row in 0..batch.num_rows() {
        let key = identity.map(|a| a.value(row)).or(fallback).unwrap();
        let h = streams.entry(key.into()).or_default();
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            let value = if column.is_null(row) {
                None
            } else if let Some(a) = column.as_any().downcast_ref::<StringArray>() {
                Some(a.value(row).to_string())
            } else if let Some(a) = column.as_any().downcast_ref::<Int64Array>() {
                Some(a.value(row).to_string())
            } else if let Some(a) = column.as_any().downcast_ref::<UInt64Array>() {
                Some(a.value(row).to_string())
            } else if let Some(a) = column.as_any().downcast_ref::<UInt8Array>() {
                Some(a.value(row).to_string())
            } else if let Some(a) = column.as_any().downcast_ref::<BooleanArray>() {
                Some(a.value(row).to_string())
            } else if let Some(a) = column.as_any().downcast_ref::<Decimal256Array>() {
                Some(a.value(row).to_string())
            } else {
                return Err(format!(
                    "unsupported canonical digest type {:?}",
                    field.data_type()
                ));
            };
            h.update((field.name().len() as u64).to_le_bytes());
            h.update(field.name().as_bytes());
            h.update(format!("{:?}", field.data_type()).as_bytes());
            match value {
                None => h.update([0]),
                Some(value) => {
                    h.update([1]);
                    h.update((value.len() as u64).to_le_bytes());
                    h.update(value.as_bytes());
                }
            }
        }
    }
    Ok(())
}
pub fn safe_relative(path: &str) -> Result<&Path> {
    let p = Path::new(path);
    if p.as_os_str().is_empty()
        || p.is_absolute()
        || p.components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        return Err(format!("unsafe artifact relative path {path}"));
    }
    Ok(p)
}
pub fn verify(trial: &TrialPlan, execution: &ChildExecution) -> Result<OutputObservation> {
    if execution.schema_version != 1
        || execution.response.schema_version != 1
        || execution.response.kind != "normalization_result"
        || execution.response.status != ProcessingStatus::Complete
        || execution.response.jobs.len() != trial.jobs.len()
    {
        return Err("normalization did not report all requested jobs complete".into());
    }
    let mut out = OutputObservation::default();
    let mut streams = BTreeMap::new();
    for (job, result) in trial.jobs.iter().zip(&execution.response.jobs) {
        if result.status != ProcessingStatus::Complete
            || result.job_id != job.job_id
            || result.dataset_id != job.dataset_id
            || result.failure.is_some()
        {
            return Err("job execution identity/status mismatch".into());
        }
        let manifest_path = job.output.dataset_path.join("manifest.json");
        if result.manifest_path.as_deref() != manifest_path.to_str() {
            return Err("result manifest path mismatch".into());
        }
        let manifest: DatasetManifest =
            serde_json::from_slice(&fs::read(&manifest_path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        verify_manifest(job, &manifest)?;
        if job.output.content_type == crate::historical::job::ContentType::Depth {
            if !manifest.streams.is_empty() {
                verify_stream_depth(job, &manifest, &mut out)?;
            } else {
                for task in &job.tasks {
                    let key = format!("{}.{}", job.job_id.0, task.task_id.0);
                    let prefix = format!("tasks/task-{:06}/depth/", task.task_id.0);
                    let rows = manifest
                        .files
                        .iter()
                        .filter(|f| f.path.starts_with(&prefix))
                        .try_fold(0u64, |n, f| {
                            n.checked_add(f.rows).ok_or("depth row overflow")
                        })?;
                    let directory = job.output.dataset_path.join(prefix);
                    let digest = validate_event_index(&directory, rows, &key)?;
                    out.depth_events_sha256.insert(key.clone(), digest);
                    let boundary: crate::historical::normalize::boundary::DepthBoundaryManifest =
                        serde_json::from_slice(
                            &fs::read(directory.join("boundary.json"))
                                .map_err(|e| e.to_string())?,
                        )
                        .map_err(|e| e.to_string())?;
                    if boundary.version != 1
                        || boundary.exchange != task.exchange
                        || boundary.instrument_id != task.instrument_id
                        || boundary.symbol != task.symbol
                        || boundary.stream_id != task.stream_id.0
                    {
                        return Err("depth boundary identity mismatch".into());
                    }
                    let mut hasher = Sha256::new();
                    for name in ["segments.json", "boundary.json"] {
                        let value: serde_json::Value = serde_json::from_slice(
                            &fs::read(directory.join(name)).map_err(|e| e.to_string())?,
                        )
                        .map_err(|e| e.to_string())?;
                        hasher.update(serde_json::to_vec(&value).map_err(|e| e.to_string())?);
                    }
                    out.reconstruction_sha256
                        .insert(key, super::provenance::hex(hasher.finalize()));
                }
            }
        }
        if result.events_written != manifest.events_written
            || result.files_written != manifest.files_written
            || result.start_timestamp_ns != manifest.start_timestamp_ns
            || result.end_timestamp_ns != manifest.end_timestamp_ns
        {
            return Err("result/manifest counts or timestamps mismatch".into());
        }
        let content = serde_json::to_value(job.output.content_type)
            .map_err(|e| e.to_string())?
            .as_str()
            .unwrap()
            .to_owned();
        let volume = out.work_by_content_type.entry(content).or_default();
        checked_add(
            &mut volume.source_records_read,
            manifest.processing_metrics.counters.records_read,
        )?;
        checked_add(&mut volume.canonical_rows, manifest.events_written)?;
        for task in &manifest.processing_metrics.tasks {
            let volume = out
                .work_by_stream
                .entry(task.stream_id.clone())
                .or_default();
            checked_add(&mut volume.source_records_read, task.counters.records_read)?;
            checked_add(&mut volume.canonical_rows, task.counters.events_written)?;
        }
        checked_add(
            &mut out.source_records_read,
            manifest.processing_metrics.counters.records_read,
        )?;
        for f in &manifest.files {
            let path = job.output.dataset_path.join(safe_relative(&f.path)?);
            let reader =
                SerializedFileReader::new(fs::File::open(&path).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            let meta = reader.metadata();
            let groups = meta.num_row_groups() as u64;
            let mut compressed = 0;
            let mut uncompressed = 0;
            for rg in meta.row_groups() {
                for column in rg.columns() {
                    checked_add(
                        &mut compressed,
                        u64::try_from(column.compressed_size()).map_err(|e| e.to_string())?,
                    )?;
                    checked_add(
                        &mut uncompressed,
                        u64::try_from(column.uncompressed_size()).map_err(|e| e.to_string())?,
                    )?;
                }
            }
            let rows = ParquetRecordBatchReaderBuilder::try_new(
                fs::File::open(&path).map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?
            .with_batch_size(8192)
            .build()
            .map_err(|e| e.to_string())?;
            let mut read = 0;
            for batch in rows {
                let batch = batch.map_err(|e| e.to_string())?;
                checked_add(&mut read, batch.num_rows() as u64)?;
                let expected =
                    if job.output.content_type == crate::historical::job::ContentType::Trades {
                        crate::shared::persistence::parquet::trade_schema()
                    } else {
                        crate::shared::persistence::parquet::depth_schema()
                    };
                if batch.schema().fields() != expected.fields() {
                    return Err("canonical Arrow schema mismatch".into());
                }
                digest_rows(&batch, &mut streams, None)?;
            }
            if read != f.rows {
                return Err("decoded rows differ from manifest".into());
            }
            checked_add(&mut out.canonical_rows, f.rows)?;
            checked_add(&mut out.parquet_bytes, f.size_bytes)?;
            checked_add(&mut out.parquet_file_count, 1)?;
            checked_add(&mut out.parquet_row_group_count, groups)?;
            out.files.push(FileObservation {
                job_id: job.job_id.0,
                dataset_id: job.dataset_id.0,
                path: f.path.clone(),
                size_bytes: f.size_bytes,
                rows: f.rows,
                row_groups: groups,
                compressed_column_bytes: compressed,
                uncompressed_column_bytes: uncompressed,
                start_timestamp_ns: f.start_timestamp_ns,
                end_timestamp_ns: f.end_timestamp_ns,
            });
        }
    }
    out.semantic_stream_sha256 = streams
        .into_iter()
        .map(|(k, v)| (k, super::provenance::hex(v.finalize())))
        .collect();
    Ok(out)
}
pub fn verify_manifest(job: &ProcessingJob, m: &DatasetManifest) -> Result<()> {
    if m.protocol_version != crate::historical::normalize::manifest::DATASET_MANIFEST_VERSION
        || m.job_id != job.job_id.0
        || m.dataset_id != job.dataset_id.0
        || m.content_type != job.output.content_type
        || m.integrity_status == IntegrityStatus::Failed
    {
        return Err("manifest identity/version/integrity mismatch".into());
    }
    let expected: BTreeSet<_> = job.tasks.iter().map(|t| t.task_id.0).collect();
    let actual: BTreeSet<_> = m.source_tasks.iter().copied().collect();
    if actual != expected || actual.len() != m.source_tasks.len() {
        return Err("source-task inventory mismatch".into());
    }
    let files = inspect_parquet_files(&job.output.dataset_path).map_err(|e| e.to_string())?;
    let auxiliary = inspect_auxiliary_files(&job.output.dataset_path).map_err(|e| e.to_string())?;
    if files != m.files || auxiliary != m.auxiliary_files {
        return Err("manifest files differ from finalized output".into());
    }
    let mut rows = 0;
    for f in &files {
        checked_add(&mut rows, f.rows)?;
    }
    if rows != m.events_written
        || m.files_written != files.len() as u64
        || m.processing_metrics.counters.events_written != rows
    {
        return Err("manifest aggregate count mismatch".into());
    }
    if m.start_timestamp_ns != files.iter().filter_map(|f| f.start_timestamp_ns).min()
        || m.end_timestamp_ns != files.iter().filter_map(|f| f.end_timestamp_ns).max()
    {
        return Err("manifest timestamp aggregate mismatch".into());
    }
    let bytes =
        fs::read(job.output.dataset_path.join("processing-job.json")).map_err(|e| e.to_string())?;
    let hash = super::provenance::hex(Sha256::digest(&bytes));
    if fs::read_to_string(job.output.dataset_path.join("processing-job.sha256"))
        .map_err(|e| e.to_string())?
        != format!("{hash}  processing-job.json\n")
    {
        return Err("resolved job checksum mismatch".into());
    }
    let resolved: ProcessingJob = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    if resolved.job_id != job.job_id
        || resolved.dataset_id != job.dataset_id
        || resolved.tasks != job.tasks
        || resolved.resources != job.resources
        || resolved.integrity_policy != job.integrity_policy
    {
        return Err("resolved job provenance mismatch".into());
    }
    for task in &job.tasks {
        if job.output.content_type == crate::historical::job::ContentType::Depth
            && m.streams.is_empty()
        {
            let prefix = format!("tasks/task-{:06}/depth/", task.task_id.0);
            for name in ["events.parquet", "segments.json", "boundary.json"] {
                if !auxiliary
                    .iter()
                    .any(|f| f.path == format!("{prefix}{name}"))
                {
                    return Err(format!("required depth auxiliary missing {prefix}{name}"));
                }
            }
            let segments = crate::shared::persistence::parquet::read_depth_segments(
                &job.output.dataset_path.join(&prefix),
            )
            .map_err(|e| e.to_string())?;
            let rows: u64 = files
                .iter()
                .filter(|f| f.path.starts_with(&prefix))
                .map(|f| f.rows)
                .sum();
            if segments.total_levels != rows {
                return Err("reconstruction row totals mismatch".into());
            }
        }
    }
    if job.output.content_type == crate::historical::job::ContentType::Depth {
        for stream in &m.streams {
            super::super::streams_depth::validate(
                &job.output.dataset_path.join(&stream.directory),
                stream,
                stream.sources.last().map_or(0, |s| s.end_row),
            )
            .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

fn verify_stream_depth(
    job: &ProcessingJob,
    manifest: &DatasetManifest,
    out: &mut OutputObservation,
) -> Result<()> {
    use crate::{
        historical::merge::{DepthEventCursor, ResolvedMergeInput, build_depth_inventory},
        shared::persistence::parquet::{DepthEventBatchBuilder, read_depth_stream_boundaries},
    };
    let input = ResolvedMergeInput {
        dataset_id: job.dataset_id.0,
        dataset_path: job.output.dataset_path.clone(),
        manifest: manifest.clone(),
        canonical_files: vec![],
        auxiliary_files: vec![],
    };
    let inventory = build_depth_inventory(&[input]).map_err(|e| e.to_string())?;
    for task in inventory {
        let key = format!("{}.{}", job.job_id.0, task.task_id);
        let mut cursor = DepthEventCursor::for_task(&task, None).map_err(|e| e.to_string())?;
        let mut builder = DepthEventBatchBuilder::with_capacity(8192);
        let mut digests = BTreeMap::new();
        while let Some(event) = cursor.next_event().map_err(|e| e.to_string())? {
            builder.push(event);
            if builder.len() >= 8192 {
                digest_rows(
                    &builder.finish().map_err(|e| e.to_string())?.unwrap(),
                    &mut digests,
                    Some(&key),
                )?;
            }
        }
        if let Some(batch) = builder.finish().map_err(|e| e.to_string())? {
            digest_rows(&batch, &mut digests, Some(&key))?;
        }
        out.depth_events_sha256.insert(
            key.clone(),
            super::provenance::hex(digests.remove(&key).unwrap_or_default().finalize()),
        );
        let boundaries = read_depth_stream_boundaries(task.boundary_path.parent().unwrap())
            .map_err(|e| e.to_string())?;
        let boundary = &boundaries
            .sources
            .iter()
            .find(|s| s.task_id == task.task_id)
            .ok_or("missing source boundary")?
            .boundary;
        let mut hasher = Sha256::new();
        hasher.update(
            serde_json::to_vec(&serde_json::to_value(&task.segments).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?,
        );
        hasher.update(
            serde_json::to_vec(&serde_json::to_value(boundary).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?,
        );
        out.reconstruction_sha256
            .insert(key, super::provenance::hex(hasher.finalize()));
    }
    Ok(())
}

fn validate_event_index(directory: &Path, rows: u64, key: &str) -> Result<String> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        fs::File::open(directory.join("events.parquet")).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?
    .with_batch_size(8192)
    .build()
    .map_err(|e| e.to_string())?;
    let mut digests = BTreeMap::new();
    let mut expected = 0u64;
    let mut previous = None;
    for batch in reader {
        let batch = batch.map_err(|e| e.to_string())?;
        if batch.schema().fields()
            != crate::shared::persistence::parquet::depth_event_schema().fields()
        {
            return Err("depth event schema mismatch".into());
        }
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
                .ok_or("depth event index column missing")
        };
        let ordinal = column("event_ordinal")?;
        let offset = column("canonical_row_offset")?;
        let count = column("canonical_row_count")?;
        for row in 0..batch.num_rows() {
            if previous.is_some_and(|v| v >= ordinal.value(row)) || offset.value(row) != expected {
                return Err("depth source-event ordering/offset mismatch".into());
            }
            previous = Some(ordinal.value(row));
            expected = expected
                .checked_add(count.value(row))
                .ok_or("depth event row overflow")?;
            if expected > rows {
                return Err("depth event index exceeds canonical rows".into());
            }
        }
        digest_rows(&batch, &mut digests, Some(key))?;
    }
    if expected != rows {
        return Err("depth source-event index does not cover canonical rows".into());
    }
    Ok(super::provenance::hex(
        digests.remove(key).unwrap_or_default().finalize(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ArrayRef;
    use std::sync::Arc;
    #[test]
    fn semantic_digest_ignores_batch_boundaries_and_preserves_row_order() {
        let batch = RecordBatch::try_from_iter([
            (
                "stream_id",
                Arc::new(StringArray::from(vec!["a", "a"])) as ArrayRef,
            ),
            (
                "event_timestamp_ns",
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut a = BTreeMap::new();
        digest_rows(&batch, &mut a, None).unwrap();
        let mut b = BTreeMap::new();
        digest_rows(&batch.slice(0, 1), &mut b, None).unwrap();
        digest_rows(&batch.slice(1, 1), &mut b, None).unwrap();
        assert_eq!(
            super::super::provenance::hex(a.remove("a").unwrap().finalize()),
            super::super::provenance::hex(b.remove("a").unwrap().finalize())
        );
        let mut reverse = BTreeMap::new();
        digest_rows(&batch.slice(1, 1), &mut reverse, None).unwrap();
        digest_rows(&batch.slice(0, 1), &mut reverse, None).unwrap();
        let mut original = BTreeMap::new();
        digest_rows(&batch, &mut original, None).unwrap();
        assert_ne!(
            super::super::provenance::hex(reverse.remove("a").unwrap().finalize()),
            super::super::provenance::hex(original.remove("a").unwrap().finalize())
        );
    }
    #[test]
    fn depth_event_index_counts_zero_row_events_and_validates_offsets() {
        let temp = tempfile::tempdir().unwrap();
        let schema = crate::shared::persistence::parquet::depth_event_schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![0, 1])),
            Arc::new(UInt64Array::from(vec![0, 2])),
            Arc::new(UInt64Array::from(vec![2, 0])),
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![None, None])),
            Arc::new(UInt64Array::from(vec![Some(1), Some(2)])),
            Arc::new(UInt64Array::from(vec![Some(1), Some(2)])),
            Arc::new(UInt8Array::from(vec![1, 0])),
            Arc::new(UInt8Array::from(vec![0, 1])),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            fs::File::create(temp.path().join("events.parquet")).unwrap(),
            schema,
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        assert_eq!(
            validate_event_index(temp.path(), 2, "test").unwrap().len(),
            64
        );
        assert!(validate_event_index(temp.path(), 1, "test").is_err());
        assert!(validate_event_index(temp.path(), 3, "test").is_err());
    }
}
