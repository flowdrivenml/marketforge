//! One reservation, bounded disk-backed reconciliation, sequential writers.
use super::reconcile::spool::{Spool, SpoolWriter, Workspace};
use super::{
    config::ProcessingConfig,
    metrics::{ScopedIntegrityMetrics, TaskMetrics},
    progress::{ProgressReporter, ProgressTaskId},
    reconcile::{self, Quota, SourceObservation, TradeEntry},
    streams::{RowRange, SourceSpan, StreamOutput, StreamUnit},
    task::TaskExecution,
    worker::{DepthSink, TradeSink},
};
use crate::{
    historical::job::WorkTask,
    shared::{
        domain::book::boundary::DepthBoundaryManifest,
        domain::canonical::{DepthProcessingOutcome, Trade},
        error::{MarketForgeError, Result},
        normalization::TargetSchema,
        persistence::parquet::{ParquetDepthWriter, ParquetTradeWriter},
    },
};
use std::collections::BTreeMap;
fn invalid(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}
struct Trades {
    reporter: ProgressReporter,
    progress: ProgressTaskId,
    spool: Spool,
    writer: Option<SpoolWriter>,
    source: usize,
    priority: u64,
    ordinal: u64,
    observation: SourceObservation,
}
impl TradeSink for Trades {
    fn write_trade(&mut self, trade: Trade) -> Result<()> {
        self.write_trade_at(
            trade,
            self.ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("source ordinal overflow"))?,
        )
    }
    fn write_trade_at(&mut self, trade: Trade, source_ordinal: u64) -> Result<()> {
        self.ordinal = source_ordinal;
        let timestamp = trade.envelope.event_timestamp_ns;
        self.writer
            .as_mut()
            .ok_or_else(|| invalid("trade spool closed"))?
            .push_trade(&TradeEntry {
                source: self.source,
                priority: self.priority,
                ordinal: self.ordinal,
                trade,
            })?;
        if self.spool.count().is_multiple_of(1024) {
            self.reporter
                .source_spooled(self.progress, self.spool.count());
        }
        self.observation.observe(timestamp);
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        self.reporter
            .source_spooled(self.progress, self.spool.count());
        Ok(())
    }
}
struct Depth {
    reporter: ProgressReporter,
    progress: ProgressTaskId,
    entries: Spool,
    writer: Option<SpoolWriter>,
    boundary: Option<Spool>,
    workspace: Workspace,
    observation: SourceObservation,
}
impl DepthSink for Depth {
    fn write_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()> {
        let bytes = outcome
            .events
            .iter()
            .try_fold(1024u64, |n, level| {
                n.checked_add(512)?
                    .checked_add(level.envelope.symbol.capacity() as u64)?
                    .checked_add(level.envelope.stream_id.capacity() as u64)
            })
            .ok_or_else(|| invalid("depth outcome memory overflow"))?;
        let _memory = self
            .workspace
            .quota()
            .reserve(bytes, "depth event reconciliation buffer")?;
        self.writer
            .as_mut()
            .ok_or_else(|| invalid("depth spool closed"))?
            .push(&outcome)?;
        if self.entries.count().is_multiple_of(1024) {
            self.reporter
                .source_spooled(self.progress, self.entries.count());
        }
        self.observation.observe(outcome.source.event_timestamp_ns);
        Ok(())
    }
    fn write_boundary(&mut self, boundary: DepthBoundaryManifest) -> Result<()> {
        let spool = self.workspace.create()?;
        let mut writer = spool.writer()?;
        writer.push(&boundary)?;
        writer.finish()?;
        self.boundary = Some(spool);
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        self.reporter
            .source_spooled(self.progress, self.entries.count());
        Ok(())
    }
}
fn observation(index: usize, task: &WorkTask) -> Result<SourceObservation> {
    Ok(SourceObservation {
        trade_identity: task
            .raw_schema
            .get("marketforge_trade_identity")
            .map(|v| {
                serde_json::from_value(v.clone())
                    .map_err(|e| invalid(format!("invalid trade identity contract: {e}")))
            })
            .transpose()?,
        family_candidate: super::streams::is_family(task),
        original_task_id: task.raw_schema["marketforge_source"]["task_id"]
            .as_u64()
            .unwrap_or(task.task_id.0),
        source_priority: super::streams::priority(index, task),
        archive_member: task.archive_member.clone(),
        ..Default::default()
    })
}
pub(super) fn equivalent_source(task: &WorkTask) -> Result<String> {
    let physical =
        std::fs::canonicalize(&task.input_path).unwrap_or_else(|_| task.input_path.clone());
    let physical_identity = physical_identity(&physical);
    let mut scope = task.raw_schema["marketforge_source"]["scope"].clone();
    if let Some(members) = scope.get_mut("members").and_then(|v| v.as_array_mut()) {
        members.sort_by_key(|m| {
            (
                m["instrument_id"].as_i64(),
                m["archive_member"].as_str().map(str::to_owned),
            )
        });
    }
    let mut selection = task.raw_schema["marketforge_source"]["instrument_selection"].clone();
    if let Some(symbols) = selection.get_mut("symbols").and_then(|v| v.as_array_mut()) {
        symbols.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
    }
    let mut raw = task.raw_schema.clone();
    if let Some(map) = raw.as_object_mut() {
        map.remove("marketforge_source");
    }
    serde_json::to_string(&serde_json::json!([
        physical_identity,
        task.archive_member,
        task.source_compression,
        task.stream_id,
        task.instrument_id,
        task.instrument,
        raw,
        task.normalizations,
        scope,
        selection,
        task.raw_schema["marketforge_source"]["source_options"],
        task.raw_schema["marketforge_source"]["instrument_namespace"]
    ]))
    .map_err(|e| invalid(e.to_string()))
}
fn physical_identity(path: &std::path::Path) -> serde_json::Value {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Ok(metadata) = std::fs::metadata(path) {
            return serde_json::json!({"device":metadata.dev(),"inode":metadata.ino()});
        }
    }
    serde_json::json!({"canonical_path":path})
}
fn pending(metrics: &mut TaskMetrics) {
    metrics.counters.rows_unpublished = metrics.counters.events_normalized;
    metrics.counters.events_written = 0;
    metrics.start_timestamp_ns = None;
    metrics.end_timestamp_ns = None;
}
fn write_row(metrics: &mut TaskMetrics, count: u64) -> Result<()> {
    metrics.counters.rows_unpublished = metrics
        .counters
        .rows_unpublished
        .checked_sub(count)
        .ok_or_else(|| invalid("pending row underflow"))?;
    metrics.counters.events_written = metrics
        .counters
        .events_written
        .checked_add(count)
        .ok_or_else(|| invalid("written row overflow"))?;
    Ok(())
}
fn fail(executions: &mut [(usize, TaskExecution)], error: MarketForgeError) {
    if let Some((_, execution)) = executions.iter_mut().find(|(_, e)| e.error.is_some()) {
        // Preserve a source failure already recorded during collection.
        let _ = execution;
        return;
    }
    if let Some((_, execution)) = executions.first_mut() {
        execution.error = Some(error);
        execution.metrics.counters.tasks_completed = 0;
        execution.metrics.counters.tasks_failed = 1;
    }
}
pub(crate) fn execute(
    unit: &StreamUnit<'_>,
    config: &ProcessingConfig,
    reporter: &ProgressReporter,
) -> Result<Vec<(usize, TaskExecution)>> {
    let normalization_started = std::time::Instant::now();
    let reference_key = super::streams::stream_key(unit.tasks[0].1)?;
    for (_, task) in &unit.tasks {
        if let Some(value) = task.raw_schema.get("marketforge_trade_identity") {
            let contract: crate::shared::formats::trades::TradeIdentityContract =
                serde_json::from_value(value.clone())
                    .map_err(|e| invalid(format!("invalid trade identity contract: {e}")))?;
            contract.validate().map_err(invalid)?;
        }
        if task.raw_schema.get("marketforge_trade_identity")
            != unit.tasks[0].1.raw_schema.get("marketforge_trade_identity")
        {
            return Err(invalid(
                "inconsistent native trade identity contracts within a compatible source group",
            ));
        }
        if super::streams::stream_key(task)? != reference_key {
            // Family routing has independent concrete-instrument groups; validation
            // of those groups occurs after routing below.
            if !unit.tasks.iter().all(|(_, t)| super::streams::is_family(t)) {
                return Err(invalid("incompatible canonical trade stream grouping"));
            }
        }
    }
    let quota = Quota::new(config.resources.normalization_limits.reconciliation_bytes);
    let mut executions: Vec<(usize, TaskExecution)> = Vec::new();
    let mut observations = BTreeMap::new();
    let workspace = Workspace::new(
        &config.resources.scratch_path,
        config.resources.scratch_budget_bytes,
        quota.clone(),
    )?;
    workspace.attach_diagnostics(
        reporter,
        format!("job={}:stream={}", unit.job.job_id.0, unit.key),
    );
    workspace.configure_raw(
        config.scratch_codec == crate::shared::historical::config::ScratchCodec::Uncompressed,
    );
    workspace.configure_typed_trades(
        config.scratch_format == crate::shared::historical::config::ScratchFormat::TypedTrades,
    );
    workspace.configure_merge(
        config
            .merge_fan_in
            .min(
                config
                    .resources
                    .normalization_limits
                    .open_files
                    .saturating_sub(6) as usize,
            )
            .max(2),
    );
    let mut trades = BTreeMap::new();
    let mut depths = BTreeMap::<usize, Depth>::new();
    let mut seen = BTreeMap::new();
    let policy = super::task::worker_integrity_policy(&config.integrity_policy);
    let is_family = unit.tasks.iter().any(|(_, t)| super::streams::is_family(t));
    reporter.stream_phase(unit.job.job_id.0, &unit.key, "family routing");
    let mut routed = if is_family {
        Some(super::family::route_with_progress(
            unit, &quota, config, reporter,
        ))
    } else {
        None
    };
    if let Some(routing) = routed.as_mut()
        && let Some((responsible, error)) = routing.failure.take()
    {
        let mut failure = Some(error);
        for (index, task) in &unit.tasks {
            let mut metrics = TaskMetrics::default();
            let mut observed = observation(*index, task)?;
            if let Some(stats) = routing.stats.get(index) {
                observed.physical_records_read = stats.physical;
                observed.observed_selected_records = stats.selected;
                observed.filtered_records = stats.filtered;
                observed.member_ordinal = Some(stats.member_ordinal);
                observed.outside_scope_records = stats.outside_scope;
                observed.selection_filtered_records = stats.filtered - stats.outside_scope;
                observed.missing_metadata = stats.missing_metadata;
                observed.unsupported_instruments = stats.unsupported;
                metrics.counters.records_read =
                    stats.selected + stats.filtered + stats.parse_rejected;
                metrics.counters.records_rejected = stats.parse_rejected;
                metrics.counters.records_rejected_unmatched = stats.parse_rejected;
                metrics.scoped_integrity = stats.filtered_integrity.clone();
                metrics.integrity = stats.diagnostics.clone();
                metrics.counters.records_matched = stats.selected;
                metrics.counters.records_unprocessed = stats.selected;
                metrics.counters.records_skipped_instrument = stats.filtered;
            }
            if *index != responsible {
                observed.skipped_stage = Some("normalization".into());
            }
            metrics.source = Some(observed);
            let error = if *index == responsible {
                metrics.counters.tasks_failed = 1;
                failure.take()
            } else {
                Some(MarketForgeError::NotAttempted {
                    reason: "normalization not attempted: family source routing failed".into(),
                })
            };
            executions.push((
                *index,
                TaskExecution {
                    task_id: task.task_id.0,
                    output_paths: vec![],
                    metrics,
                    error,
                },
            ));
        }
        // Physical records without a routable selected authority are kept
        // distinct from malformed/invalid market records.
        let physical = routing.stats.values().map(|s| s.physical).sum::<u64>();
        let classified = executions
            .iter()
            .map(|(_, e)| e.metrics.counters.records_read)
            .sum::<u64>();
        if physical > classified {
            let (_, owner) = executions
                .iter_mut()
                .find(|(i, _)| *i == responsible)
                .unwrap();
            owner.metrics.counters.records_read += physical - classified;
            owner.metrics.counters.records_unrouted += physical - classified;
        }
        return Ok(executions);
    }

    for (index, task) in &unit.tasks {
        let progress = ProgressTaskId::new(unit.job.job_id.0, task.task_id.0);
        if super::streams::stopped(&executions) {
            reporter.task_skipped(
                progress,
                "Source reconciliation stopped after fatal failure".into(),
            );
            let mut metrics = TaskMetrics::default();
            let mut source = observation(*index, task)?;
            if let Some(stats) = routed.as_ref().and_then(|r| r.stats.get(index)) {
                metrics.counters.records_read =
                    stats.selected + stats.filtered + stats.parse_rejected;
                metrics.counters.records_matched = stats.selected;
                metrics.counters.records_unprocessed = stats.selected;
                metrics.counters.records_skipped_instrument = stats.filtered;
                metrics.counters.records_rejected = stats.parse_rejected;
                metrics.counters.records_rejected_unmatched = stats.parse_rejected;
                metrics.scoped_integrity = stats.filtered_integrity.clone();
                metrics.integrity = stats.diagnostics.clone();
                source.physical_records_read = stats.physical;
                source.observed_selected_records = stats.selected;
                source.filtered_records = stats.filtered;
                source.outside_scope_records = stats.outside_scope;
                source.selection_filtered_records = stats.filtered - stats.outside_scope;
            }
            source.skipped_stage = Some("normalization".into());
            metrics.source = Some(source);
            executions.push((
                *index,
                TaskExecution {
                    task_id: task.task_id.0,
                    output_paths: vec![],
                    metrics,
                    error: Some(MarketForgeError::NotAttempted {
                        reason: "normalization not attempted: preceding reconciliation failure"
                            .into(),
                    }),
                },
            ));
            continue;
        }
        let mut metrics = TaskMetrics::default();
        let mut observed = observation(*index, task)?;
        if let Some(original) = routed
            .as_ref()
            .and_then(|r| r.stats.get(index))
            .and_then(|s| s.redundant_of)
        {
            observed.redundant_of_task = Some(original);
            metrics.source = Some(observed.clone());
            metrics.counters.tasks_completed = 1;
            observations.insert(*index, observed);
            executions.push((
                *index,
                TaskExecution {
                    task_id: task.task_id.0,
                    output_paths: vec![],
                    metrics,
                    error: None,
                },
            ));
            continue;
        }
        let key = match equivalent_source(task) {
            Ok(key) => key,
            Err(error) => {
                metrics.source = Some(observed);
                executions.push((
                    *index,
                    TaskExecution {
                        task_id: task.task_id.0,
                        output_paths: vec![],
                        metrics,
                        error: Some(error),
                    },
                ));
                continue;
            }
        };
        if let Some(first) = seen.get(&key) {
            observed.redundant_of_task = Some(*first);
            metrics.source = Some(observed.clone());
            metrics.counters.tasks_completed = 1;
            observations.insert(*index, observed);
            reporter.task_skipped(
                progress,
                format!("Equivalent source selection already handled by task {first}"),
            );
            executions.push((
                *index,
                TaskExecution {
                    task_id: task.task_id.0,
                    output_paths: vec![],
                    metrics,
                    error: None,
                },
            ));
            continue;
        }
        if let Err(error) = quota.charge(
            (key.capacity() as u64)
                .checked_mul(2)
                .and_then(|v| v.checked_add(128))
                .ok_or_else(|| invalid("source key overflow"))?,
            "source equivalence inventory",
        ) {
            metrics.source = Some(observed);
            executions.push((
                *index,
                TaskExecution {
                    task_id: task.task_id.0,
                    output_paths: vec![],
                    metrics,
                    error: Some(error),
                },
            ));
            continue;
        }
        seen.insert(key, task.task_id.0);
        reporter.task_started(
            progress,
            format!("{:?}", task.exchange),
            task.symbol.clone(),
            format!("{:?}", task.normalizations[0].target_schema),
            task.input_path.display().to_string(),
        );
        let input = routed.as_mut().and_then(|r| r.inputs.remove(index));
        let mut worker_progress = super::progress::WorkerProgress::new(reporter, progress);
        reporter.phase(
            progress,
            &unit.key,
            "source read/decode/normalize and canonical spool writing",
        );
        let mut run = || {
            if task.normalizations[0].target_schema == TargetSchema::Trade {
                let spool = workspace.create()?;
                let mut sink = Trades {
                    reporter: reporter.clone(),
                    progress,
                    writer: Some(spool.writer()?),
                    spool,
                    source: *index,
                    priority: observed.source_priority,
                    ordinal: 0,
                    observation: observed.clone(),
                };
                let result = super::worker::process_trade_task_with_metrics_and_progress(
                    task,
                    &mut sink,
                    &mut metrics,
                    &policy,
                    &ScopedIntegrityMetrics::default(),
                    &mut worker_progress,
                );
                observed = sink.observation;
                let finished = sink.writer.take().unwrap().finish();
                reporter.source_spooled(progress, sink.spool.count());
                trades.insert(*index, sink.spool);
                result.and(finished)
            } else {
                let entries = workspace.create()?;
                let mut sink = Depth {
                    reporter: reporter.clone(),
                    progress,
                    writer: Some(entries.writer()?),
                    entries,
                    boundary: None,
                    workspace: workspace.clone(),
                    observation: observed.clone(),
                };
                let result = super::resolve_sequence_policy(task).and_then(|sequence| {
                    super::worker::process_depth_task_with_metrics_and_progress(
                        task,
                        &mut sink,
                        &mut metrics,
                        &policy,
                        &ScopedIntegrityMetrics::default(),
                        sequence,
                        &mut worker_progress,
                    )
                });
                observed = sink.observation.clone();
                let finished = sink.writer.take().unwrap().finish();
                reporter.source_spooled(progress, sink.entries.count());
                depths.insert(*index, sink);
                result.and(finished)
            }
        };
        let mut result = if let Some(input) = input {
            super::worker::decoded::with_decoded(input, run)
        } else {
            run()
        };
        worker_progress.finish(&metrics.counters);
        reporter.phase(
            progress,
            &unit.key,
            if result.is_ok() {
                "source normalization complete"
            } else {
                "source normalization failed"
            },
        );
        observed.physical_records_read = metrics.counters.records_read;
        observed.observed_selected_records = metrics.counters.records_matched;
        observed.filtered_records = metrics.counters.records_skipped_instrument;
        if let Some(stats) = routed.as_mut().and_then(|r| r.stats.remove(index)) {
            let unprocessed = stats
                .selected
                .checked_sub(metrics.counters.records_read)
                .ok_or_else(|| invalid("decoded routing/worker record count mismatch"))?;
            metrics.counters.records_read = metrics
                .counters
                .records_read
                .checked_add(unprocessed)
                .ok_or_else(|| invalid("unprocessed record count overflow"))?;
            metrics.counters.records_matched = metrics
                .counters
                .records_matched
                .checked_add(unprocessed)
                .ok_or_else(|| invalid("unprocessed match count overflow"))?;
            metrics.counters.records_unprocessed = metrics
                .counters
                .records_unprocessed
                .checked_add(unprocessed)
                .ok_or_else(|| invalid("unprocessed record count overflow"))?;
            metrics.counters.records_read = metrics
                .counters
                .records_read
                .checked_add(stats.filtered)
                .and_then(|n| n.checked_add(stats.parse_rejected))
                .ok_or_else(|| invalid("filtered source count overflow"))?;
            metrics.counters.records_skipped_instrument = metrics
                .counters
                .records_skipped_instrument
                .checked_add(stats.filtered)
                .ok_or_else(|| invalid("filtered source count overflow"))?;
            observed.physical_records_read = stats.physical;
            observed.observed_selected_records = stats.selected;
            observed.filtered_records = stats.filtered;
            observed.member_ordinal = Some(stats.member_ordinal);
            observed.outside_scope_records = stats.outside_scope;
            observed.selection_filtered_records = stats.filtered - stats.outside_scope;
            observed.missing_metadata = stats.missing_metadata;
            observed.unsupported_instruments = stats.unsupported;
            metrics.counters.records_rejected = metrics
                .counters
                .records_rejected
                .checked_add(stats.parse_rejected)
                .ok_or_else(|| invalid("parse count overflow"))?;
            metrics.counters.records_rejected_unmatched = metrics
                .counters
                .records_rejected_unmatched
                .checked_add(stats.parse_rejected)
                .ok_or_else(|| invalid("parse count overflow"))?;
            metrics.integrity.merge(&stats.diagnostics)?;
            let combined =
                metrics.scoped_integrity.entry_count() + stats.filtered_integrity.entry_count();
            let merged = crate::shared::historical::limits::check(
                "task metric entries",
                config.resources.normalization_limits.metric_entries,
                combined as u64,
                "family filtered observations",
            )
            .and_then(|()| metrics.scoped_integrity.merge(&stats.filtered_integrity));
            if result.is_ok() {
                result = merged;
            }
        }
        observations.insert(*index, observed.clone());
        metrics.source = Some(observed);
        pending(&mut metrics);
        reporter.phase(
            progress,
            &unit.key,
            "persisting source integrity observations",
        );
        if let Err(spill) =
            metrics
                .scoped_integrity
                .spill(unit.job.job_id.0, task.task_id.0, config)
        {
            result = Err(match result {
                Ok(()) => spill,
                Err(original) => super::failure::report_persistence_error(original, spill),
            });
        }
        reporter.phase(
            progress,
            &unit.key,
            if result.is_ok() {
                "source observations retained; awaiting reconciliation"
            } else {
                "source normalization/report persistence failed"
            },
        );
        if let Err(error) = &result {
            metrics.counters.tasks_completed = 0;
            metrics.counters.tasks_failed = 1;
            reporter.task_failed(progress, error.to_string());
        }
        executions.push((
            *index,
            TaskExecution {
                task_id: task.task_id.0,
                output_paths: vec![],
                metrics,
                error: result.err(),
            },
        ));
    }
    if super::streams::stopped(&executions) {
        for (_, execution) in &mut executions {
            if execution
                .error
                .as_ref()
                .is_some_and(|e| !matches!(e, MarketForgeError::NotAttempted { .. }))
            {
                execution.metrics.counters.tasks_completed = 0;
                execution.metrics.counters.tasks_failed = 1;
            }
        }
        return Ok(executions);
    }
    // Concrete streams retain independent identity, ordering and book state.
    // Writers are opened and finalized one at a time, never one per symbol at once.
    let mut groups = BTreeMap::<String, Vec<(usize, &WorkTask)>>::new();
    for &(index, task) in &unit.tasks {
        groups
            .entry(super::streams::stream_key(task)?)
            .or_default()
            .push((index, task));
    }
    let mut published = std::collections::BTreeSet::new();
    for (key, tasks) in groups {
        let concrete = StreamUnit {
            job: unit.job,
            tasks,
            key: key.clone(),
        };
        let indices = concrete
            .tasks
            .iter()
            .map(|(i, _)| *i)
            .collect::<std::collections::BTreeSet<_>>();
        if concrete
            .tasks
            .iter()
            .all(|(_, t)| super::streams::is_family(t))
            && indices.iter().all(|index| {
                observations
                    .get(index)
                    .is_none_or(|o| o.observed_selected_records == 0)
            })
        {
            for index in &indices {
                if let Some(source) = observations.get_mut(index) {
                    source.skipped_stage = Some("no selected records observed".into());
                }
            }
            published.extend(indices.iter().copied());
            continue;
        }
        let mut observed = observations
            .iter()
            .filter(|(i, _)| indices.contains(i))
            .map(|(i, o)| (*i, o.clone()))
            .collect();
        let mut selected = Vec::new();
        // Move executions rather than clone their potentially large metrics maps.
        let mut remaining = Vec::new();
        for execution in executions {
            if indices.contains(&execution.0) {
                selected.push(execution);
            } else {
                remaining.push(execution);
            }
        }
        let result = if concrete.tasks[0].1.normalizations[0].target_schema == TargetSchema::Trade {
            publish_trades(
                &concrete,
                config,
                indices
                    .iter()
                    .filter_map(|i| trades.remove(i).map(|s| (*i, s)))
                    .collect(),
                &mut observed,
                &mut selected,
                &quota,
                &workspace,
                reporter,
                normalization_started,
            )
        } else {
            let sources = indices
                .iter()
                .filter_map(|i| depths.remove(i).map(|d| (*i, d)))
                .collect();
            reporter.stream_phase(
                unit.job.job_id.0,
                &unit.key,
                "depth replay and Parquet writing/finalization",
            );
            publish_depth(
                &concrete,
                config,
                sources,
                &observed,
                &mut selected,
                reporter,
            )
        };
        let stopped = if let Err(error) = result {
            fail(
                &mut selected,
                MarketForgeError::StreamFailure {
                    stream: key,
                    stage: "source reconciliation / writer finalization",
                    source: Box::new(error),
                },
            );
            true
        } else {
            published.extend(indices.iter().copied());
            false
        };
        observations.extend(observed);
        remaining.extend(selected);
        executions = remaining;
        if stopped {
            break;
        }
    }
    let failed = super::streams::stopped(&executions);
    for (index, execution) in &mut executions {
        if failed && !published.contains(index) && execution.error.is_none() {
            execution.error = Some(MarketForgeError::NotAttempted {
                reason:
                    "remaining canonical submission stopped after reconciliation/writer failure"
                        .into(),
            });
            execution.metrics.counters.tasks_completed = 0;
            if let Some(source) = observations.get_mut(index) {
                source.skipped_stage = Some("remaining canonical submission".into());
            }
        }
        execution.metrics.source = observations.remove(index);
    }
    let (memory, scratch, handles) = workspace.peaks();
    crate::historical::normalize::progress::log::diagnostic(format_args!(
        "reconciliation accounted peak: job={} stream={} resident_bytes={} scratch_bytes={} scratch_handles={}",
        unit.job.job_id.0, unit.key, memory, scratch, handles
    ));
    workspace.report();
    reporter.resource_snapshot(&format!("job={}:stream={}:outcome",unit.job.job_id.0,unit.key),serde_json::json!({
        "source_normalization_finished":true,"reconciliation_finished":!failed,"committed":false,
        "staged_output_rows":executions.iter().map(|(_,e)|e.metrics.counters.events_written).sum::<u64>(),
        "suppressed_rows":executions.iter().filter_map(|(_,e)|e.metrics.source.as_ref()).map(|s|s.duplicates+s.conflicts).sum::<u64>()}));
    reporter.stream_phase(unit.job.job_id.0, &unit.key, "scratch cleanup");
    drop(trades);
    drop(depths);
    drop(routed);
    drop(workspace);
    reporter.stream_phase(
        unit.job.job_id.0,
        &unit.key,
        if failed {
            "reconciliation failed"
        } else {
            "reconciliation complete"
        },
    );
    Ok(executions)
}
fn publish_trades(
    unit: &StreamUnit<'_>,
    config: &ProcessingConfig,
    inputs: Vec<(usize, Spool)>,
    observations: &mut BTreeMap<usize, SourceObservation>,
    executions: &mut [(usize, TaskExecution)],
    quota: &Quota,
    workspace: &Workspace,
    reporter: &ProgressReporter,
    normalization_started: std::time::Instant,
) -> Result<()> {
    let _inventory = quota.reserve(
        (observations.len() as u64)
            .checked_mul(256)
            .ok_or_else(|| invalid("chronological source inventory overflow"))?,
        "trade source append inventory",
    )?;
    let started = std::time::Instant::now();
    // All source workers and spool completion markers succeeded before this point.
    // A profile contract, not the shape of an ID, authorizes source-local append.
    let order = reconcile::chronological_trade_order(observations)?;
    let fast = order.is_some();
    let reason = if fast {
        "explicit source-local identities and validated strictly disjoint accepted intervals"
    } else {
        if observations
            .values()
            .filter(|o| o.redundant_of_task.is_none())
            .all(|o| {
                o.trade_identity.as_ref().is_some_and(|c| {
                    c.scope == crate::shared::formats::trades::TradeIdScope::SourceLocal
                })
            })
        {
            "source-local accepted intervals overlap or share a boundary; chronological append is prohibited"
        } else {
            "global, scoped or unestablished identity domains require exact cross-source identity reconciliation"
        }
    };
    crate::historical::normalize::progress::log::diagnostic(format_args!(
        "trade_execution_path={} reason={reason} sources={}",
        if fast {
            "chronological_append"
        } else {
            "identity_reconciliation"
        },
        inputs.len()
    ));
    let boundaries:Vec<_>=observations.iter().map(|(source,o)|serde_json::json!({"source_index":source,"source_task":o.original_task_id,"minimum_timestamp_ns":o.minimum_accepted_timestamp_ns,"maximum_timestamp_ns":o.maximum_accepted_timestamp_ns,"ordering_validated":o.ordering_validated,"identity_contract":o.trade_identity})).collect();
    reporter.resource_snapshot(&format!("job={}:stream={}:trade_execution",unit.job.job_id.0,unit.key),serde_json::json!({"trade_execution_path":if fast {"chronological_append"} else {"identity_reconciliation"},"reason":reason,"source_count":inputs.len(),"boundaries":boundaries,"source_order":order,"committed":false}));
    let id = ProgressTaskId::new(unit.job.job_id.0, 0);
    let merged = if let Some(order) = order {
        let mut by_source: BTreeMap<_, _> = inputs.into_iter().collect();
        let mut ordered = Vec::with_capacity(by_source.len());
        for source in order {
            let spool = by_source
                .remove(&source)
                .ok_or_else(|| invalid("missing chronological source spool"))?;
            let observed = &observations[&source];
            if (spool.count() == 0) != observed.minimum_accepted_timestamp_ns.is_none() {
                return Err(invalid(
                    "source spool count disagrees with timestamp evidence",
                ));
            }
            ordered.push((source, spool));
        }
        if !by_source.is_empty() {
            return Err(invalid("unaccounted chronological source spool"));
        }
        reconcile::external::SortedRows::Append(ordered)
    } else {
        // With cross-source reconciliation, rejected preferred events cannot be
        // replaced silently by lower-priority rows. Keep the original rule.
        for (index, e) in executions.iter() {
            if e.metrics.counters.records_rejected > 0
                && observations.iter().any(|(other, o)| {
                    other != index
                        && o.redundant_of_task.is_none()
                        && (o.minimum_accepted_timestamp_ns.is_some()
                            || o.observed_selected_records > 0)
                })
            {
                return Err(invalid(
                    "cross-source reconciliation cannot establish preferred identities after rejected records; no automatic lower-priority fallback is permitted",
                ));
            }
        }
        reconcile::external::trades_with_progress(
            workspace,
            inputs.into_iter().map(|(_, spool)| spool).collect(),
            observations,
            reporter,
            id,
            &unit.key,
        )?
    };
    reporter.stream_phase(
        id.job_id,
        &unit.key,
        if fast {
            "chronological append into Parquet writer"
        } else {
            "chronological merging into Parquet writer"
        },
    );
    // Identity decisions are definitive before canonical publication. Preserve
    // them even if a downstream writer fails partway through replay.
    for (index, execution) in executions.iter_mut() {
        let observed = observations
            .get(index)
            .ok_or_else(|| invalid("missing source observation"))?;
        let suppressed = observed
            .duplicates
            .checked_add(observed.conflicts)
            .ok_or_else(|| invalid("suppression counter overflow"))?;
        execution.metrics.counters.rows_suppressed = suppressed;
        execution.metrics.counters.rows_unpublished = execution
            .metrics
            .counters
            .rows_unpublished
            .checked_sub(suppressed)
            .ok_or_else(|| invalid("suppression exceeds pending rows"))?;
    }

    let source_positions: BTreeMap<usize, usize> = executions
        .iter()
        .enumerate()
        .map(|(position, (source, _))| (*source, position))
        .collect();
    let mut ranges = BTreeMap::<usize, Vec<RowRange>>::new();
    let mut ranges_count = 0u64;
    let directory = format!("streams/{}/trades", unit.key);
    let path = unit.job.output.staging_path.join(&directory);
    let mut writer = ParquetTradeWriter::new(&path, config.resources.parquet.clone())?;
    {
        let mut reader = merged.rows()?;
        let mut row = 0u64;
        while let Some(entry) = reader.next()? {
            let end = row
                .checked_add(1)
                .ok_or_else(|| invalid("canonical row overflow"))?;
            let spans = ranges.entry(entry.value.source).or_default();
            if let Some(last) = spans.last_mut().filter(|span| span.end_row == row) {
                last.end_row = end;
            } else {
                ranges_count = ranges_count
                    .checked_add(1)
                    .ok_or_else(|| invalid("row attribution overflow"))?;
                crate::shared::historical::limits::check(
                    "stream source row ranges",
                    config.resources.normalization_limits.metric_entries,
                    ranges_count,
                    "trade source attribution",
                )?;
                quota.charge(128, "trade source row attribution")?;
                spans.push(RowRange {
                    start_row: row,
                    end_row: end,
                });
            }
            let position = *source_positions
                .get(&entry.value.source)
                .ok_or_else(|| invalid("missing source execution"))?;
            let (_, execution) = &mut executions[position];
            let timestamp = entry.value.trade.envelope.event_timestamp_ns;
            writer.write_trade(entry.value.trade)?;
            write_row(&mut execution.metrics, 1)?;
            execution.metrics.record_timestamp(timestamp);
            row = row
                .checked_add(1)
                .ok_or_else(|| invalid("canonical row overflow"))?;
            if row.is_multiple_of(8192) {
                reporter.stage_count(unit.job.job_id.0, &unit.key, row, "staged canonical rows");
                reporter.resource_snapshot(
                    &format!("job={}:stream={}:staged", unit.job.job_id.0, unit.key),
                    serde_json::json!({"staged_output_rows":row,"committed":false}),
                );
            }
        }
        reader.finish()?;
    }
    let interleaved = ranges.values().any(|v| v.len() > 1);
    let mut sources = Vec::new();
    let total = executions.iter().try_fold(0u64, |n, (_, e)| {
        n.checked_add(e.metrics.counters.events_written)
            .ok_or_else(|| invalid("stream total rows overflow"))
    })?;
    for (index, execution) in executions.iter_mut() {
        if execution.metrics.counters.rows_unpublished != 0 {
            return Err(invalid("surviving trade rows remain unpublished"));
        }
        let spans = ranges.remove(index).unwrap_or_default();
        sources.push(SourceSpan {
            task_id: execution.task_id,
            start_row: spans.first().map_or(total, |s| s.start_row),
            end_row: spans.last().map_or(total, |s| s.end_row),
            ranges: if interleaved { spans } else { vec![] },
        });
        execution.output_paths.push(path.clone());
        execution.metrics.counters.validate()?;
    }
    sources.sort_by_key(|s| (s.start_row, s.end_row, s.task_id));
    writer.finish()?;
    let source_records = executions
        .iter()
        .map(|(_, e)| e.metrics.counters.records_read)
        .sum::<u64>();
    let records_processed = executions
        .iter()
        .map(|(_, e)| e.metrics.counters.records_processed)
        .sum::<u64>();
    let rejected = executions
        .iter()
        .map(|(_, e)| e.metrics.counters.records_rejected)
        .sum::<u64>();
    let suppressed = executions
        .iter()
        .map(|(_, e)| e.metrics.counters.rows_suppressed)
        .sum::<u64>();
    let (memory, scratch, _) = workspace.peaks();
    let (scratch_read, scratch_written) = workspace.scratch_io();
    reporter.resource_snapshot(&format!("job={}:stream={}:trade_performance",unit.job.job_id.0,unit.key),serde_json::json!({"trade_execution_path":if fast {"chronological_append"} else {"identity_reconciliation"},"source_records":source_records,"records_processed":records_processed,"elapsed_scope":"stream normalization through Parquet finalization; excludes admission, manifest validation and commit","canonical_rows_written":total,"rows_rejected":rejected,"rows_suppressed":suppressed,"scratch_workspace_peak_bytes":scratch,"scratch_bytes_read":scratch_read,"scratch_bytes_written":scratch_written,"peak_accounted_memory_bytes":memory,"elapsed_seconds":normalization_started.elapsed().as_secs_f64(),"processing_records_per_second":records_processed as f64/normalization_started.elapsed().as_secs_f64(),"publication_elapsed_seconds":started.elapsed().as_secs_f64(),"publication_rows_per_second":total as f64/started.elapsed().as_secs_f64(),"parquet_files":writer.metrics().files_written,"parquet_row_groups":writer.metrics().row_groups_written,"committed":false}));
    super::streams::write_output(
        &path,
        &StreamOutput {
            stream_id: unit.tasks[0].1.stream_id.0.clone(),
            directory,
            sources,
        },
    )
}
fn publish_depth(
    unit: &StreamUnit<'_>,
    config: &ProcessingConfig,
    mut depths: BTreeMap<usize, Depth>,
    observations: &BTreeMap<usize, SourceObservation>,
    executions: &mut [(usize, TaskExecution)],
    reporter: &ProgressReporter,
) -> Result<()> {
    let order = reconcile::depth_source_order(observations)?;
    let mut replayed = 0u64;
    for source in depths.values() {
        if let Some(spool) = &source.boundary {
            let mut reader = spool.reader::<DepthBoundaryManifest>()?;
            let boundary = reader
                .next()?
                .ok_or_else(|| invalid("source boundary missing"))?;
            reconcile::validate_depth_recovery(&boundary.value)?;
            if reader.next()?.is_some() {
                return Err(invalid("multiple source boundaries"));
            }
        }
    }
    let directory = format!("streams/{}/depth", unit.key);
    let path = unit.job.output.staging_path.join(&directory);
    let mut writer = ParquetDepthWriter::new(&path, config.resources.parquet.clone())?;
    writer.enable_sources()?;
    let mut sources = Vec::new();
    for index in order {
        let (_, execution) = executions
            .iter_mut()
            .find(|(i, _)| *i == index)
            .ok_or_else(|| invalid("missing depth source"))?;
        let start = writer.metrics().levels_written;
        // Redundant selections have no source state; record an empty boundary.
        if let Some(mut source) = depths.remove(&index) {
            writer.begin_source(execution.task_id)?;
            {
                let mut reader = source.entries.reader::<DepthProcessingOutcome>()?;
                while let Some(outcome) = reader.next()? {
                    let count = outcome.value.events.len() as u64;
                    let timestamp = outcome.value.source.event_timestamp_ns;
                    writer.write_outcome(outcome.value)?;
                    write_row(&mut execution.metrics, count)?;
                    replayed = replayed
                        .checked_add(1)
                        .ok_or_else(|| invalid("depth replay counter overflow"))?;
                    if replayed.is_multiple_of(1024) {
                        reporter.stage_count(
                            unit.job.job_id.0,
                            &unit.key,
                            replayed,
                            "depth source messages replayed",
                        );
                    }
                    if count > 0 {
                        execution.metrics.record_timestamp(timestamp);
                    }
                }
            }
            source.entries.delete()?;
            let boundary = source
                .boundary
                .take()
                .ok_or_else(|| invalid("source reconstruction boundary missing"))?;
            {
                let mut reader = boundary.reader::<DepthBoundaryManifest>()?;
                let record = reader
                    .next()?
                    .ok_or_else(|| invalid("source reconstruction boundary missing"))?;
                writer.write_boundary(record.value)?;
                if reader.next()?.is_some() {
                    return Err(invalid("multiple source boundaries"));
                }
            }
            boundary.delete()?;
        } else {
            // Keep existing boundary/cardinality requirements for redundant tasks.
            let task = unit.tasks.iter().find(|(i, _)| *i == index).unwrap().1;
            writer.begin_source(execution.task_id)?;
            writer.write_boundary(DepthBoundaryManifest::new(
                task.exchange,
                task.instrument_id,
                task.symbol.clone(),
                task.stream_id.0.clone(),
            ))?;
        }
        sources.push(SourceSpan {
            task_id: execution.task_id,
            start_row: start,
            end_row: writer.metrics().levels_written,
            ranges: vec![],
        });
        execution.output_paths.push(path.clone());
        execution.metrics.counters.validate()?;
    }
    writer.finish()?;
    super::streams::write_output(
        &path,
        &StreamOutput {
            stream_id: unit.tasks[0].1.stream_id.0.clone(),
            directory,
            sources,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::historical::config::selection_test_support as support;
    #[test]
    fn source_equivalence_includes_selection_member_and_interpretation() {
        let job = support::resolved(&support::batch());
        let mut a = job.tasks[0].clone();
        let file = tempfile::NamedTempFile::new().unwrap();
        a.input_path = file.path().into();
        let mut b = a.clone();
        b.task_id = crate::historical::job::TaskId(2000);
        b.raw_schema["marketforge_source"]["source_priority"] = serde_json::json!(99);
        assert_eq!(
            equivalent_source(&a).unwrap(),
            equivalent_source(&b).unwrap()
        );
        b.archive_member = Some("different-member".into());
        assert_ne!(
            equivalent_source(&a).unwrap(),
            equivalent_source(&b).unwrap()
        );
        b.archive_member = None;
        b.raw_schema["marketforge_source"]["instrument_selection"] =
            serde_json::json!({"mode":"include","symbols":[a.symbol]});
        assert_ne!(
            equivalent_source(&a).unwrap(),
            equivalent_source(&b).unwrap()
        );
        b = a.clone();
        b.instrument.tick_size *= rust_decimal::Decimal::from(2);
        assert_ne!(
            equivalent_source(&a).unwrap(),
            equivalent_source(&b).unwrap()
        );
        let other = tempfile::NamedTempFile::new().unwrap();
        b = a.clone();
        b.input_path = other.path().into();
        assert_ne!(
            equivalent_source(&a).unwrap(),
            equivalent_source(&b).unwrap()
        );
    }
    #[test]
    fn snapshot_outcomes_are_atomic_and_checked_before_buffer_growth() {
        use crate::shared::domain::canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate};
        use rust_decimal::Decimal;
        let envelope = EventEnvelope {
            event_timestamp_ns: 1,
            system_timestamp_ns: None,
            exchange: Exchange::Okx,
            instrument_id: 1,
            symbol: "SYNTHETIC".into(),
            stream_id: "synthetic".into(),
        };
        let make = |side| L2LevelUpdate {
            envelope: envelope.clone(),
            side,
            price: Decimal::ONE,
            quantity_base: Some(Decimal::ONE),
            quantity_quote: Some(Decimal::ONE),
            quantity_contracts: None,
            order_count: None,
        };
        let temp = tempfile::tempdir().unwrap();
        let quota = Quota::new(4 << 20);
        let workspace = Workspace::new(temp.path(), 1 << 20, quota.clone()).unwrap();
        let entries = workspace.create().unwrap();
        let mut sink = Depth {
            reporter: ProgressReporter::disabled(),
            progress: ProgressTaskId::new(0, 0),
            writer: Some(entries.writer().unwrap()),
            entries,
            boundary: None,
            workspace,
            observation: Default::default(),
        };
        let held = quota
            .reserve(quota.maximum() - quota.used() - 1, "test retained state")
            .unwrap();
        let result = sink.write_outcome(DepthProcessingOutcome::initialization(vec![
            make(BookSide::Bid),
            make(BookSide::Ask),
        ]));
        assert!(matches!(result, Err(MarketForgeError::ResourceLimit(_))));
        assert_eq!(sink.entries.count(), 0);
        assert_eq!(sink.observation.first_accepted_timestamp_ns, None);
        drop(held);
        sink.write_outcome(DepthProcessingOutcome::initialization(vec![
            make(BookSide::Bid),
            make(BookSide::Ask),
        ]))
        .unwrap();
        sink.writer.take().unwrap().finish().unwrap();
        let mut reader = sink.entries.reader::<DepthProcessingOutcome>().unwrap();
        assert_eq!(reader.next().unwrap().unwrap().value.events.len(), 2);
        assert!(reader.next().unwrap().is_none());
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    #[test]
    fn earlier_skipped_member_does_not_replace_responsible_resource_failure() {
        use crate::historical::normalize::executor::{ExecutionContext, finalize_processing_job};
        let job = support_job();
        let config = crate::historical::config::selection_test_support::runtime();
        let executions = job
            .tasks
            .iter()
            .enumerate()
            .map(|(index, task)| TaskExecution {
                task_id: task.task_id.0,
                output_paths: vec![],
                metrics: Default::default(),
                error: Some(if index == 1 {
                    crate::shared::historical::limits::error(
                        "memory bytes",
                        1,
                        2,
                        "known family member",
                    )
                    .into()
                } else {
                    MarketForgeError::NotAttempted {
                        reason: "normalization after routing failure".into(),
                    }
                }),
            })
            .collect();
        let mut context = ExecutionContext::default();
        let error = finalize_processing_job(&job, &config, executions, &mut context).unwrap_err();
        assert!(
            matches!(error,MarketForgeError::ResourceLimit(ref e) if e.stage=="known family member")
        );
        assert_eq!(context.failed_task_id, Some(job.tasks[1].task_id.0));
        assert_eq!(context.report.tasks.len(), job.tasks.len());
    }
    fn support_job() -> crate::historical::job::ProcessingJob {
        let batch = crate::historical::config::selection_test_support::batch();
        crate::historical::config::selection_test_support::resolved(&batch)
    }
}
