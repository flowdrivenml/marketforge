use super::DatasetManifest;
use crate::{
    historical::job::ContentType,
    shared::error::{MarketForgeError, Result},
};
use std::collections::HashSet;
impl<M: std::borrow::Borrow<crate::historical::normalize::metrics::ProcessingMetricsReport>>
    DatasetManifest<M>
{
    /// Validate authoritative source spans before readers join stream metadata paths.
    pub fn validate_streams(&self) -> Result<()> {
        let processing_metrics = self.processing_metrics.borrow();
        let reports: std::collections::HashMap<_, _> = processing_metrics
            .tasks
            .iter()
            .map(|task| (task.task_id, task))
            .collect();
        if reports.len() != processing_metrics.tasks.len() {
            return Err(MarketForgeError::InvalidConfiguration(
                "duplicate source metrics".into(),
            ));
        }
        let invalid = |s: &str| MarketForgeError::InvalidConfiguration(s.into());
        if self.streams.is_empty() {
            if self.files.iter().any(|f| f.path.starts_with("streams/")) {
                return Err(invalid("stream files require ownership metadata"));
            }
            return Ok(());
        }
        if !matches!(self.protocol_version, 3..=5) {
            return Err(invalid("stream ownership requires manifest3"));
        }
        let kind = match self.content_type {
            ContentType::Trades => "trades",
            ContentType::Depth => "depth",
            _ => return Err(invalid("unsupported stream content")),
        };
        let mut directories = HashSet::new();
        let mut tasks = HashSet::new();
        let mut owned = HashSet::new();
        let mut contributions = std::collections::HashMap::<u64, u64>::new();
        let mut task_directories = std::collections::HashMap::new();
        for stream in &self.streams {
            let parts = stream.directory.split('/').collect::<Vec<_>>();
            if parts.len() != 3
                || parts[0] != "streams"
                || parts[2] != kind
                || parts[1].len() != 64
                || !parts[1]
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                || !directories.insert(&stream.directory)
            {
                return Err(invalid("unsafe or duplicate stream directory"));
            }
            let mut row_ranges = Vec::new();
            for source in &stream.sources {
                let metrics = reports
                    .get(&source.task_id)
                    .ok_or_else(|| invalid("missing source metrics"))?;
                let first = tasks.insert(source.task_id);
                if task_directories
                    .insert(source.task_id, &stream.directory)
                    .is_some_and(|old| old != &stream.directory)
                {
                    return Err(invalid(
                        "one source task cannot own different concrete stream directories",
                    ));
                }
                let contribution = contributions.entry(source.task_id).or_default();
                *contribution = contribution
                    .checked_add(source.row_count()?)
                    .ok_or_else(|| invalid("source contribution overflow"))?;
                if (!first && self.protocol_version < 5)
                    || *contribution > metrics.counters.events_written
                    || metrics.stream_id != stream.stream_id
                    || (self.protocol_version == 3 && !source.ranges.is_empty())
                {
                    return Err(invalid("invalid stream source attribution"));
                }
                if source.ranges.is_empty() {
                    if source.start_row < source.end_row {
                        row_ranges.push((source.start_row, source.end_row));
                    }
                } else {
                    if source.ranges.first().map(|r| r.start_row) != Some(source.start_row)
                        || source.ranges.last().map(|r| r.end_row) != Some(source.end_row)
                    {
                        return Err(invalid("source bounding range disagrees with row ranges"));
                    }
                    for pair in source.ranges.windows(2) {
                        if pair[0].end_row >= pair[1].start_row {
                            return Err(invalid(
                                "source row ranges must be disjoint and coalesced",
                            ));
                        }
                    }
                    for range in &source.ranges {
                        if range.start_row >= range.end_row {
                            return Err(invalid("empty/inverted source row range"));
                        }
                        row_ranges.push((range.start_row, range.end_row));
                    }
                }
            }
            row_ranges.sort_unstable();
            let mut offset = 0u64;
            for (start, end) in row_ranges {
                if start != offset {
                    return Err(invalid("source row ownership overlaps or leaves gaps"));
                }
                offset = end;
            }
            let prefix = format!("{}/", stream.directory);
            let mut rows = 0u64;
            for file in self.files.iter().filter(|f| f.path.starts_with(&prefix)) {
                let name = &file.path[prefix.len()..];
                if !name.starts_with("part-")
                    || !name.ends_with(".parquet")
                    || name.contains('/')
                    || !owned.insert(&file.path)
                {
                    return Err(invalid("invalid or duplicate stream part"));
                }
                rows = rows
                    .checked_add(file.rows)
                    .ok_or_else(|| invalid("stream row overflow"))?;
            }
            if rows != offset {
                return Err(invalid("stream source/physical row mismatch"));
            }
        }
        for (id, rows) in contributions {
            if rows != reports[&id].counters.events_written {
                return Err(invalid("source row contributions do not match staged rows"));
            }
        }
        let mut accounted = tasks.clone();
        if self.protocol_version >= 4 {
            for task in &processing_metrics.tasks {
                if task.counters.events_written == 0
                    && task.source.as_ref().is_some_and(|source| {
                        source.family_candidate && source.observed_selected_records == 0
                    })
                {
                    accounted.insert(task.task_id);
                }
            }
        }
        if accounted != self.source_tasks.iter().copied().collect()
            || accounted.len() != self.source_tasks.len()
            || owned.len() != self.files.len()
        {
            return Err(invalid("incomplete stream ownership"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod reconciliation_tests {
    use super::*;
    use crate::historical::normalize::{
        metrics::*,
        streams::{RowRange, SourceSpan, StreamOutput},
    };
    fn manifest() -> DatasetManifest {
        let directory = format!("streams/{}/trades", "0".repeat(64));
        let mut report = ProcessingMetricsReport::default();
        for (task_id, rows) in [(11, 2), (12, 1)] {
            report
                .add_task(TaskMetricsReport {
                    failure: None,
                    source: None,
                    accepted_depth_events: None,
                    task_id,
                    stream_id: "synthetic".into(),
                    counters: ProcessingCounters {
                        records_read: rows,
                        records_matched: rows,
                        records_processed: rows,
                        events_normalized: rows,
                        events_written: rows,
                        ..Default::default()
                    },
                    integrity: Default::default(),
                    scoped_integrity: Default::default(),
                    start_timestamp_ns: Some(0),
                    end_timestamp_ns: Some(2),
                })
                .unwrap();
        }
        DatasetManifest {
            protocol_version: 4,
            job_id: 1,
            dataset_id: 1,
            content_type: ContentType::Trades,
            processing_quality: None,
            events_written: 3,
            files_written: 1,
            start_timestamp_ns: Some(0),
            end_timestamp_ns: Some(2),
            source_tasks: vec![11, 12],
            instrument_namespace: None,
            resolved_instruments: vec![],
            streams: vec![StreamOutput {
                stream_id: "synthetic".into(),
                directory: directory.clone(),
                sources: vec![
                    SourceSpan {
                        task_id: 11,
                        start_row: 0,
                        end_row: 3,
                        ranges: vec![
                            RowRange {
                                start_row: 0,
                                end_row: 1,
                            },
                            RowRange {
                                start_row: 2,
                                end_row: 3,
                            },
                        ],
                    },
                    SourceSpan {
                        task_id: 12,
                        start_row: 1,
                        end_row: 2,
                        ranges: vec![RowRange {
                            start_row: 1,
                            end_row: 2,
                        }],
                    },
                ],
            }],
            files: vec![crate::historical::normalize::manifest::DatasetFile {
                path: format!("{directory}/part-000000.parquet"),
                rows: 3,
                size_bytes: 1,
                start_timestamp_ns: Some(0),
                end_timestamp_ns: Some(2),
            }],
            auxiliary_files: vec![],
            integrity_status: IntegrityStatus::Clean,
            integrity_evaluation: IntegrityEvaluation {
                status: IntegrityStatus::Clean,
                violations: vec![],
                total_policy_violations: 0,
            },
            processing_metrics: report,
        }
    }
    #[test]
    fn interleaved_ownership_requires_version_four_and_complete_disjoint_coverage() {
        let mut manifest = manifest();
        manifest.validate_streams().unwrap();
        manifest.protocol_version = 3;
        assert!(manifest.validate_streams().is_err());
        manifest.protocol_version = 4;
        manifest.streams[0].sources[1].ranges[0].start_row = 0;
        assert!(manifest.validate_streams().is_err());
    }
    #[test]
    fn legacy_contiguous_spans_still_validate_without_new_fields() {
        let mut manifest = manifest();
        manifest.protocol_version = 3;
        manifest.streams[0].sources[0] = SourceSpan {
            task_id: 11,
            start_row: 0,
            end_row: 2,
            ranges: vec![],
        };
        manifest.streams[0].sources[1] = SourceSpan {
            task_id: 12,
            start_row: 2,
            end_row: 3,
            ranges: vec![],
        };
        let value = serde_json::to_value(&manifest).unwrap();
        assert!(value["streams"][0]["sources"][0].get("ranges").is_none());
        serde_json::from_value::<DatasetManifest>(value)
            .unwrap()
            .validate_streams()
            .unwrap();
    }
    #[test]
    fn unobserved_family_candidates_need_no_empty_output_stream() {
        let mut manifest = manifest();
        let mut absent = manifest.processing_metrics.tasks[0].clone();
        absent.task_id = 13;
        absent.counters = Default::default();
        absent.start_timestamp_ns = None;
        absent.end_timestamp_ns = None;
        absent.source = Some(SourceObservation {
            family_candidate: true,
            ..Default::default()
        });
        manifest.processing_metrics.add_task(absent).unwrap();
        manifest.source_tasks.push(13);
        manifest.validate_streams().unwrap();
        manifest
            .processing_metrics
            .tasks
            .last_mut()
            .unwrap()
            .source
            .as_mut()
            .unwrap()
            .observed_selected_records = 1;
        assert!(manifest.validate_streams().is_err());
    }
    #[test]
    fn suppression_and_unprocessed_rows_are_not_parse_failures() {
        let counters = ProcessingCounters {
            records_read: 3,
            records_matched: 3,
            records_processed: 2,
            records_unprocessed: 1,
            events_normalized: 2,
            events_written: 1,
            rows_suppressed: 1,
            ..Default::default()
        };
        counters.validate().unwrap();
        assert_eq!(counters.records_rejected, 0);
        serde_json::from_value::<ProcessingCounters>(serde_json::to_value(counters).unwrap())
            .unwrap();
    }
}
