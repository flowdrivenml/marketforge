//! One forward decode per family source/member. Native identifiers are filtered
//! before consulting concrete normalization metadata. Routed payloads live in owned
//! scratch spools; only descriptors, bounded diagnostics and one row stay resident.
use super::{
    reconcile::Quota,
    resources::{SourceFormat, source_format},
    streams::StreamUnit,
    worker::decoded::DecodedSource,
};
use crate::{
    historical::{
        config::{InstrumentSelection, family_of},
        job::WorkTask,
    },
    shared::{
        error::{MarketForgeError, Result},
        io::{
            decode::CsvDecoder,
            source::{JsonlDecoder, JsonlError, with_validated_source_reader},
        },
        normalization::TargetSchema,
    },
};
use std::collections::BTreeMap;
fn invalid(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}
#[derive(Default)]
pub(crate) struct RoutingStats {
    pub physical: u64,
    pub filtered: u64,
    pub selected: u64,
    pub outside_scope: u64,
    pub missing_metadata: u64,
    pub unsupported: u64,
    pub member_ordinal: u64,
    pub redundant_of: Option<u64>,
    pub filtered_integrity: super::metrics::ScopedIntegrityMetrics,
    pub parse_rejected: u64,
    pub diagnostics: super::metrics::IntegrityMetrics,
}
pub(crate) struct Routed {
    pub inputs: BTreeMap<usize, DecodedSource>,
    pub stats: BTreeMap<usize, RoutingStats>,
    pub failure: Option<(usize, MarketForgeError)>,
}
fn selection(task: &WorkTask) -> Result<InstrumentSelection> {
    serde_json::from_value(task.raw_schema["marketforge_source"]["instrument_selection"].clone())
        .map_err(|e| invalid(format!("family selection missing/invalid: {e}")))
}
fn identify(
    symbol: &str,
    tasks: &[(usize, &WorkTask)],
    stats: &mut BTreeMap<usize, RoutingStats>,
    selection: &InstrumentSelection,
) -> Result<Option<usize>> {
    let (owner, first) = tasks[0];
    let scope = first.raw_schema["marketforge_source"]["scope"]["identifier"]
        .as_str()
        .ok_or_else(|| invalid("family identifier missing"))?;
    if symbol.trim().is_empty() {
        return Err(invalid(
            "family record has an empty native symbol; cannot route corrupted identity",
        ));
    }
    let actual =
        family_of(first.exchange, first.instrument.instrument_kind, symbol).ok_or_else(|| {
            invalid(format!(
                "unrecognizable native family symbol {symbol}; cannot safely route this record"
            ))
        })?;
    if actual != scope {
        let stats = stats.get_mut(&owner).unwrap();
        stats.outside_scope += 1;
        stats.filtered += 1;
        return Ok(None);
    }
    if !selection.selects(symbol) {
        stats.get_mut(&owner).unwrap().filtered += 1;
        return Ok(None);
    }
    let (index,task)=tasks.get(tasks.partition_point(|(_,t)|t.symbol.as_str()<symbol)).filter(|(_,t)|t.symbol==symbol).ok_or_else(||{stats.get_mut(&owner).unwrap().missing_metadata+=1;invalid(format!("task {} source {}: encountered selected symbol {symbol} within {scope}, but authoritative offline instrument metadata is missing",first.raw_schema["marketforge_source"]["task_id"],first.input_path.display()))})?;
    stats.get_mut(index).unwrap().selected += 1;
    if let Some(error) = task.raw_schema["marketforge_source"]["deferred_instrument_error"].as_str()
    {
        stats.get_mut(index).unwrap().unsupported += 1;
        return Err(invalid(format!(
            "encountered selected symbol {symbol} has missing metadata or unsupported normalization: {error}"
        )));
    }
    Ok(Some(*index))
}
fn filtered_observation(
    stats: &mut RoutingStats,
    scope: &super::metrics::IntegrityScope,
    timestamp: Option<i64>,
) -> Result<()> {
    stats.filtered_integrity.record(
        scope,
        super::metrics::IntegrityCategory::ParseFailure,
        false,
        timestamp,
    )
}
#[cfg(test)]
pub(crate) fn route(
    unit: &StreamUnit<'_>,
    quota: &Quota,
    config: &super::ProcessingConfig,
) -> Routed {
    route_with_progress(
        unit,
        quota,
        config,
        &super::progress::ProgressReporter::disabled(),
    )
}
pub(crate) fn route_with_progress(
    unit: &StreamUnit<'_>,
    quota: &Quota,
    config: &super::ProcessingConfig,
    reporter: &super::progress::ProgressReporter,
) -> Routed {
    let _limits = crate::shared::historical::limits::LimitScope::enter(
        config.resources.normalization_limits,
        format!("job={}, family={}", unit.job.job_id.0, unit.key),
    );
    let workspace = match super::reconcile::spool::Workspace::new(
        &config.resources.scratch_path,
        config.resources.scratch_budget_bytes,
        quota.clone(),
    ) {
        Ok(w) => w,
        Err(e) => {
            return Routed {
                inputs: BTreeMap::new(),
                stats: BTreeMap::new(),
                failure: Some((unit.tasks[0].0, e)),
            };
        }
    };
    workspace.configure_raw(
        config.scratch_codec == crate::shared::historical::config::ScratchCodec::Uncompressed,
    );
    workspace.attach_diagnostics(
        reporter,
        format!("job={}:family={}", unit.job.job_id.0, unit.key),
    );
    let mut routed = Routed {
        inputs: BTreeMap::new(),
        stats: BTreeMap::new(),
        failure: None,
    };
    // Original priority is shared by derived concrete tasks; member order is
    // deterministic and is not inferred from file traversal or worker completion.
    let mut sources = BTreeMap::new();
    for &(index, task) in &unit.tasks {
        sources
            .entry((
                super::streams::priority(index, task),
                task.archive_member.clone(),
            ))
            .or_insert_with(Vec::new)
            .push((index, task));
    }
    let mut seen = BTreeMap::new();
    let mut previous_priority = None;
    let mut member_ordinal = 0u64;
    for ((priority, _), mut tasks) in sources {
        if previous_priority == Some(priority) {
            let Some(next) = member_ordinal.checked_add(1) else {
                routed.failure = Some((tasks[0].0, invalid("archive member ordinal overflow")));
                break;
            };
            member_ordinal = next;
        } else {
            member_ordinal = 0;
            previous_priority = Some(priority);
        }
        tasks.sort_by(|a, b| a.1.symbol.cmp(&b.1.symbol));
        let (owner, first) = tasks[0];
        let routing_id = super::progress::ProgressTaskId::new(unit.job.job_id.0, first.task_id.0);
        if reporter.is_enabled() {
            for (_, task) in &tasks {
                reporter.phase(
                    super::progress::ProgressTaskId::new(unit.job.job_id.0, task.task_id.0),
                    &unit.key,
                    "family archive routing",
                );
                reporter.task_started(
                    super::progress::ProgressTaskId::new(unit.job.job_id.0, task.task_id.0),
                    format!("{:?}", task.exchange),
                    task.symbol.clone(),
                    format!("{:?}", task.normalizations[0].target_schema),
                    task.input_path.display().to_string(),
                );
            }
        }
        for (index, _) in &tasks {
            routed.stats.insert(
                *index,
                RoutingStats {
                    member_ordinal,
                    ..Default::default()
                },
            );
        }
        let result = (|| -> Result<()> {
            let _scope = super::worker::resource_scope(first);
            let key = super::reconciled::equivalent_source(first)?;
            if let Some(original) = seen.get(&key) {
                for (index, _) in &tasks {
                    routed.stats.get_mut(index).unwrap().redundant_of = Some(*original);
                }
                return Ok(());
            }
            quota.charge(
                (key.capacity() as u64)
                    .checked_mul(2)
                    .and_then(|v| v.checked_add(128))
                    .ok_or_else(|| invalid("source key overflow"))?,
                "family source equivalence inventory",
            )?;
            seen.insert(
                key,
                first.raw_schema["marketforge_source"]["task_id"]
                    .as_u64()
                    .unwrap_or(first.task_id.0),
            );
            let prepared_selection = selection(first)?;
            let filtered_scope = super::metrics::IntegrityScope {
                task_id: first.task_id.0,
                stream_id: String::new(),
                source_file: first.input_path.to_string_lossy().into_owned(),
            };
            let format = source_format(first)?;
            if format == SourceFormat::Xlsx {
                return Err(invalid(
                    "family routing needs native symbols in CSV/JSONL; XLSX profiles do not expose a supported mixed-instrument identity",
                ));
            }
            let mut writers = super::reconcile::spool::AppendCache::new();
            with_validated_source_reader(
                &first.input_path,
                first.source_compression,
                first.archive_member.as_deref(),
                |reader| {
                    match format {
                        SourceFormat::Csv => {
                            let has_headers = first.normalizations[0].target_schema
                                == TargetSchema::Trade
                                && first.raw_schema["header"].as_bool() == Some(true);
                            let mut decoder = CsvDecoder::with_headers(reader, has_headers);
                            let actual = if has_headers {
                                Some(decoder.headers().map_err(|e| {
                                    super::worker::integrity::csv_source_error(first, e)
                                })?)
                            } else {
                                None
                            };
                            let headers = crate::shared::formats::trades::resolve_trade_headers(
                                &first.raw_schema,
                                actual.as_ref(),
                            )?;
                            let identity = if first.normalizations[0].target_schema
                                == TargetSchema::Trade
                            {
                                crate::shared::formats::trades::resolve_identity_column(&headers)?
                            } else {
                                first.normalizations[0].rules["instrument"]["source"].as_str().and_then(|name|headers.iter().position(|h|h==name.as_bytes())).ok_or_else(||invalid("family CSV depth profile has no supported native instrument identity column"))?
                            };
                            for (index, _) in &tasks {
                                quota.charge(
                                    headers.as_slice().len() as u64 + 256,
                                    "family headers",
                                )?;
                                routed.inputs.insert(
                                    *index,
                                    DecodedSource::DiskCsv {
                                        headers: actual.clone(),
                                        spool: workspace.create()?,
                                    },
                                );
                            }
                            let timestamp_column = first.normalizations[0].rules["event_timestamp"]
                                ["source"]
                                .as_str()
                                .and_then(|name| headers.iter().position(|h| h == name.as_bytes()));
                            for (record, row) in decoder.records().enumerate() {
                                let row = match row {
                                    Ok(row) => row,
                                    Err(error) => {
                                        if !error.is_io_error() {
                                            let stats = routed.stats.get_mut(&owner).unwrap();
                                            stats.physical += 1;
                                            stats.parse_rejected += 1;
                                            stats.filtered_integrity.record(
                                                &super::metrics::IntegrityScope {
                                                    task_id: first.task_id.0,
                                                    stream_id: String::new(),
                                                    source_file: first
                                                        .input_path
                                                        .to_string_lossy()
                                                        .into_owned(),
                                                },
                                                super::metrics::IntegrityCategory::ParseFailure,
                                                true,
                                                None,
                                            )?;
                                            stats
                                                .diagnostics
                                                .record(super::metrics::IntegrityDiagnostic {
                                                category:
                                                    super::metrics::IntegrityCategory::ParseFailure,
                                                task_id: first.task_id.0,
                                                source_record: Some(record as u64 + 1),
                                                event_timestamp_ns: None,
                                                message: error.to_string(),
                                            })?;
                                            let policy = super::task::worker_integrity_policy(
                                                &config.integrity_policy,
                                            );
                                            if first.normalizations[0].target_schema
                                                == TargetSchema::Trade
                                                && !super::metrics::enforce_immediate_integrity(
                                                    &policy,
                                                    &stats.filtered_integrity,
                                                    super::metrics::IntegrityCategory::ParseFailure,
                                                )?
                                                .is_failed()
                                            {
                                                continue;
                                            }
                                        }
                                        return Err(super::worker::integrity::csv_source_error(
                                            first, error,
                                        ));
                                    }
                                };
                                let physical = &mut routed.stats.get_mut(&owner).unwrap().physical;
                                *physical += 1;
                                if (*physical).is_multiple_of(8192) {
                                    reporter.routing_records(routing_id, *physical);
                                }
                                let symbol =
                                    std::str::from_utf8(row.get(identity).ok_or_else(|| {
                                        invalid("family record lacks its symbol field")
                                    })?)
                                    .map_err(|e| {
                                        invalid(format!("invalid native symbol encoding: {e}"))
                                    })?;
                                if let Some(index) = identify(
                                    symbol,
                                    &tasks,
                                    &mut routed.stats,
                                    &prepared_selection,
                                )? {
                                    let Some(DecodedSource::DiskCsv { spool, .. }) =
                                        routed.inputs.get(&index)
                                    else {
                                        return Err(invalid("CSV route disappeared"));
                                    };
                                    // One bounded row; byte fields are lossless (including non-UTF8).
                                    let bytes = (row.as_slice().len() as u64)
                                        .checked_mul(2)
                                        .and_then(|n| {
                                            n.checked_add((row.len() as u64).checked_mul(64)?)
                                        })
                                        .ok_or_else(|| invalid("routing row footprint overflow"))?;
                                    let _row_memory = quota.reserve(bytes, "family routing row")?;
                                    let fields = row.iter().map(|f| f.to_vec()).collect::<Vec<_>>();
                                    writers.push(spool, &(record as u64 + 1, fields))?;
                                } else {
                                    let timestamp =
                                        timestamp_column.and_then(|i| row.get(i)).and_then(|raw| {
                                            crate::shared::normalization::parse_timestamp_ns(
                                                raw,
                                                first.normalizations[0].timestamp_encoding,
                                            )
                                            .ok()
                                        });
                                    filtered_observation(
                                        routed.stats.get_mut(&owner).unwrap(),
                                        &filtered_scope,
                                        timestamp,
                                    )?;
                                }
                            }
                        }
                        SourceFormat::Jsonl => {
                            let native=first.normalizations.iter().find_map(|n|n.rules.get("instrument").and_then(|i|i.get("source")).and_then(|v|v.as_str())).ok_or_else(||invalid("family JSONL profile does not declare native instrument identity"))?;
                            for (index, _) in &tasks {
                                quota.charge(128, "family JSON queues")?;
                                routed
                                    .inputs
                                    .insert(*index, DecodedSource::DiskJson(workspace.create()?));
                            }
                            let mut decoder = JsonlDecoder::new(reader);
                            let mut ordinal = 0u64;
                            loop {
                                let value = match decoder.next_json() {
                                    Ok(Some(value)) => value,
                                    Ok(None) => break,
                                    Err(JsonlError::Source(e)) => {
                                        return Err(
                                            crate::shared::historical::limits::source_error(
                                                first.input_path.clone(),
                                                e,
                                            ),
                                        );
                                    }
                                    Err(JsonlError::Record(e)) => {
                                        let stats = routed.stats.get_mut(&owner).unwrap();
                                        stats.physical += 1;
                                        stats.parse_rejected += 1;
                                        stats.filtered_integrity.record(
                                            &super::metrics::IntegrityScope {
                                                task_id: first.task_id.0,
                                                stream_id: String::new(),
                                                source_file: first
                                                    .input_path
                                                    .to_string_lossy()
                                                    .into_owned(),
                                            },
                                            super::metrics::IntegrityCategory::ParseFailure,
                                            true,
                                            None,
                                        )?;
                                        stats.diagnostics.record(
                                            super::metrics::IntegrityDiagnostic {
                                                category:
                                                    super::metrics::IntegrityCategory::ParseFailure,
                                                task_id: first.task_id.0,
                                                source_record: Some(ordinal + 1),
                                                event_timestamp_ns: None,
                                                message: e.to_string(),
                                            },
                                        )?;
                                        return Err(MarketForgeError::RecordIntegrity{category:crate::shared::validation::IntegrityCategory::ParseFailure,message:format!("malformed family JSONL record cannot be attributed to an instrument for safe depth recovery: {e}")});
                                    }
                                };
                                ordinal = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| invalid("family record ordinal overflow"))?;
                                let physical = &mut routed.stats.get_mut(&owner).unwrap().physical;
                                *physical += 1;
                                if (*physical).is_multiple_of(8192) {
                                    reporter.routing_records(routing_id, *physical);
                                }
                                let symbol=crate::shared::formats::depth::resolve_json_path(&value,native).and_then(|v|v.as_str()).ok_or_else(||invalid("family JSONL record lacks a valid native instrument symbol; safe per-instrument recovery is unproven"))?;
                                if let Some(index) = identify(
                                    symbol,
                                    &tasks,
                                    &mut routed.stats,
                                    &prepared_selection,
                                )? {
                                    let Some(DecodedSource::DiskJson(spool)) =
                                        routed.inputs.get(&index)
                                    else {
                                        return Err(invalid("JSON route disappeared"));
                                    };
                                    writers.push(spool, &(ordinal, value))?;
                                } else {
                                    let timestamp =
                                        first.normalizations[0].rules["event_timestamp"]["source"]
                                            .as_str()
                                            .and_then(|path| {
                                                crate::shared::formats::depth::extract_timestamp_ns(
                                                    &value,
                                                    path,
                                                    first.normalizations[0].timestamp_encoding,
                                                )
                                                .ok()
                                            });
                                    filtered_observation(
                                        routed.stats.get_mut(&owner).unwrap(),
                                        &filtered_scope,
                                        timestamp,
                                    )?;
                                }
                            }
                        }
                        SourceFormat::Xlsx => unreachable!(),
                    }
                    Ok(())
                },
            )?;
            writers.finish()?;
            for (index, _) in &tasks {
                match routed.inputs.get(index) {
                    Some(DecodedSource::DiskCsv { spool, .. })
                    | Some(DecodedSource::DiskJson(spool)) => spool.seal()?,
                    _ => {}
                }
            }
            Ok(())
        })();
        reporter.routing_records(
            routing_id,
            routed.stats.get(&owner).map_or(0, |s| s.physical),
        );
        let result = result.and_then(|()| {
            reporter.phase(
                routing_id,
                &unit.key,
                "persisting family routing observations",
            );
            for (index, task) in &tasks {
                if let Some(stats) = routed.stats.get_mut(index) {
                    stats
                        .filtered_integrity
                        .spill(unit.job.job_id.0, task.task_id.0, config)?;
                }
            }
            Ok(())
        });
        if let Err(error) = result {
            let responsible = tasks
                .iter()
                .find(|(index, _)| routed.stats.get(index).is_some_and(|s| s.unsupported > 0))
                .map_or(owner, |(index, _)| *index);
            routed.failure = Some((responsible, error));
            break;
        }
    }
    routed
}

/// Fulfillment is checked across applicable source selections, never per month.
/// Observed-but-invalid selected records are present, not absent.
pub(crate) fn validate_selection(
    job: &crate::historical::job::ProcessingJob,
    report: &super::metrics::ProcessingMetricsReport,
) -> Result<()> {
    for task in &job.tasks {
        let source = &task.raw_schema["marketforge_source"];
        if source["prepared_schema_version"].as_u64() != Some(4) {
            continue;
        }
        let Some(value) = source.get("instrument_selection").filter(|v| !v.is_null()) else {
            continue;
        };
        let selection: InstrumentSelection =
            serde_json::from_value(value.clone()).map_err(|e| invalid(e.to_string()))?;
        if let InstrumentSelection::Include { symbols } = selection {
            for symbol in symbols {
                let satisfied = job
                    .tasks
                    .iter()
                    .filter(|other| {
                        other.symbol == symbol
                            && other.exchange == task.exchange
                            && other.instrument.instrument_kind == task.instrument.instrument_kind
                            && other.raw_schema["marketforge_source"]["scope"]["identifier"]
                                == source["scope"]["identifier"]
                            && other.raw_schema["marketforge_source"]["stream_id"]
                                == source["stream_id"]
                            && other.raw_schema["marketforge_source"]["source_options"]
                                == source["source_options"]
                    })
                    .any(|other| {
                        report
                            .tasks
                            .iter()
                            .find(|r| r.task_id == other.task_id.0)
                            .is_some_and(|metrics| {
                                metrics
                                    .source
                                    .as_ref()
                                    .map_or(metrics.counters.records_matched > 0, |o| {
                                        o.observed_selected_records > 0
                                    })
                            })
                    });
                if !satisfied {
                    return Err(invalid(format!(
                        "unsatisfied instrument_selection: exact symbol {symbol} was never observed in any applicable selected source; absence in an individual source is allowed"
                    )));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "family_tests.rs"]
mod tests;
