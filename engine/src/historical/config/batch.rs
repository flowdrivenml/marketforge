//! Production schema-v3 prepared batches. No database, archive reads, or writes.
use super::{
    FormatCatalog, MarketCategory,
    catalog::{fingerprint, invalid},
};
use crate::{
    historical::job::*,
    shared::{
        domain::canonical::Exchange,
        error::Result,
        historical::{ProcessingConfig, project_root, resolve_path, validate_processing_config},
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedBatch {
    pub schema_version: u32,
    pub kind: String,
    pub status: String,
    pub run_id: String,
    pub run_directory: PathBuf,
    pub batch_path: PathBuf,
    pub ranking_snapshot: Option<Value>,
    pub ranking_policy: String,
    pub requested_interval: RequestedInterval,
    /// Persisted authority: append IDs, never renumber or reuse them within a namespace.
    pub instrument_namespace: String,
    pub instruments: Vec<OfflineInstrument>,
    pub jobs: Vec<PreparedJob>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RequestedInterval {
    pub start: String,
    pub end: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentMetadata {
    pub tick_size: Decimal,
    pub contract_value: Option<Decimal>,
    pub contract_value_asset: Option<String>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OfflineInstrument {
    pub instrument_id: i64,
    pub exchange: Exchange,
    pub instrument_type: InstrumentKind,
    pub market_category: MarketCategory,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub instrument: InstrumentMetadata,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedJob {
    pub operation: super::NormalizationOperation,
    pub job_id: JobId,
    pub dataset_id: DatasetId,
    pub tasks: Vec<PreparedTask>,
    #[serde(deserialize_with = "super::job::v3_ordering")]
    pub ordering: OrderingConfig,
    #[serde(deserialize_with = "super::job::v3_output")]
    pub output: OutputConfig,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedTask {
    pub task_id: TaskId,
    pub input_path: PathBuf,
    pub exchange: Exchange,
    pub instrument_type: InstrumentKind,
    pub market_category: MarketCategory,
    pub base_asset: Option<String>,
    pub quote_asset: Option<String>,
    pub symbol: Option<String>,
    pub instrument: Option<InstrumentMetadata>,
    pub instrument_id: Option<i64>,
    pub stream_id: StreamId,
    pub stream_rank: u32,
    pub scope: Option<FamilyScope>,
    pub source_options: Option<SourceOptions>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyScope {
    pub kind: String,
    pub identifier: String,
    /// Explicit concrete members. Metadata belongs to the batch authority.
    #[serde(default)]
    pub members: Vec<FamilyMember>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyMember {
    pub instrument_id: i64,
    pub archive_member: Option<String>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceOptions {
    pub feed: Feed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Feed {
    Snapshot,
    Incremental,
}
#[derive(Debug)]
pub struct ResolvedBatch {
    pub prepared: PreparedBatch,
    pub jobs: Vec<ProcessingJob>,
}

pub fn read_prepared_batch(
    path: impl AsRef<Path>,
    root: impl AsRef<Path>,
) -> Result<PreparedBatch> {
    let root = project_root(root)?;
    let path = resolve_path(&root, path.as_ref());
    let result = (|| {
        let bytes = std::fs::read(&path).map_err(|e| invalid(e.to_string()))?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|e| invalid(e.to_string()))?;
        // Shared legacy output/order structs intentionally remain permissive elsewhere.
        for job in value["jobs"].as_array().into_iter().flatten() {
            for (field, allowed) in [
                (
                    "output",
                    &["content_type", "staging_path", "dataset_path"][..],
                ),
                ("ordering", &["primary", "tie_break"][..]),
            ] {
                if job[field]
                    .as_object()
                    .is_some_and(|o| o.keys().any(|k| !allowed.contains(&k.as_str())))
                {
                    return Err(invalid(format!("unknown job {field} field")));
                }
            }
        }
        let batch: PreparedBatch =
            serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
        batch.validate(&root)?;
        Ok(batch)
    })();
    result.map_err(|e| invalid(format!("prepared batch {}: {e}", path.display())))
}
pub fn load_normalization_batch(
    path: impl AsRef<Path>,
    catalog: &FormatCatalog,
    runtime: &ProcessingConfig,
    root: impl AsRef<Path>,
) -> Result<ResolvedBatch> {
    let root = project_root(root)?;
    let prepared = read_prepared_batch(path, &root)?;
    let jobs = prepared
        .jobs
        .iter()
        .map(|job| prepared.resolve_job(job, catalog, runtime, &root))
        .collect::<Result<_>>()?;
    Ok(ResolvedBatch { prepared, jobs })
}
impl PreparedBatch {
    pub fn validate(&self, root: &Path) -> Result<()> {
        if self.schema_version != 3
            || self.kind != "normalization_batch"
            || self.status != "prepared"
        {
            return Err(invalid(
                "expected schema_version=3, kind=normalization_batch, status=prepared",
            ));
        }
        if self.run_id.trim().is_empty()
            || self.run_directory.as_os_str().is_empty()
            || self.batch_path.as_os_str().is_empty()
            || self.ranking_policy.trim().is_empty()
            || self.instrument_namespace.trim().is_empty()
        {
            return Err(invalid("batch identity/paths/namespace must not be empty"));
        }
        if self.requested_interval.start.is_empty()
            || self.requested_interval.end.is_empty()
            || self.requested_interval.start >= self.requested_interval.end
        {
            return Err(invalid("invalid requested_interval"));
        }
        if self.jobs.is_empty() {
            return Err(invalid("batch contains no jobs"));
        }
        let mut ids = BTreeSet::new();
        let mut identities = BTreeSet::new();
        for i in &self.instruments {
            if i.instrument_id <= 0
                || !ids.insert(i.instrument_id)
                || !identities.insert(identity(i)?)
            {
                return Err(invalid("duplicate or invalid instrument identity/ID"));
            }
            if i.symbol.trim().is_empty()
                || i.base_asset.trim().is_empty()
                || i.quote_asset.trim().is_empty()
                || i.base_asset == i.quote_asset
                || i.instrument.tick_size <= Decimal::ZERO
                || i.instrument
                    .contract_value
                    .is_some_and(|v| v <= Decimal::ZERO)
                || i.instrument.contract_value.is_some()
                    != i.instrument.contract_value_asset.is_some()
            {
                return Err(invalid("invalid instrument metadata"));
            }
            match (i.instrument_type, i.market_category) {
                (InstrumentKind::Spot, MarketCategory::Spot)
                | (
                    InstrumentKind::Perpetual | InstrumentKind::Future,
                    MarketCategory::Linear | MarketCategory::Inverse,
                ) => (),
                _ => return Err(invalid("unsupported instrument type/market category")),
            }
        }
        let mut jobs = BTreeSet::new();
        let mut datasets = BTreeSet::new();
        let mut paths: Vec<PathBuf> = vec![];
        for j in &self.jobs {
            if j.job_id.0 == 0
                || j.dataset_id.0 == 0
                || !jobs.insert(j.job_id.0)
                || !datasets.insert(j.dataset_id.0)
            {
                return Err(invalid("duplicate or invalid job/dataset ID"));
            }
            for p in [&j.output.staging_path, &j.output.dataset_path] {
                if p.as_os_str().is_empty()
                    || p.components()
                        .any(|c| matches!(c, std::path::Component::ParentDir))
                {
                    return Err(invalid("invalid output path"));
                }
                let p = resolve_path(root, p);
                if p == root
                    || paths
                        .iter()
                        .any(|other| p.starts_with(other) || other.starts_with(&p))
                {
                    return Err(invalid("conflicting output destinations"));
                }
                paths.push(p);
            }
            if j.output.content_type == ContentType::Combined || j.tasks.is_empty() {
                return Err(invalid("job requires tasks and trades or depth content"));
            }
            let mut tasks = BTreeSet::new();
            let mut streams = BTreeMap::new();
            let mut ranks = BTreeMap::new();
            for t in &j.tasks {
                if t.task_id.0 == 0
                    || !tasks.insert(t.task_id.0)
                    || t.input_path.as_os_str().is_empty()
                    || t.stream_id.0.trim().is_empty()
                {
                    return Err(invalid("invalid or duplicate task identity/path"));
                }
                if streams
                    .insert(&t.stream_id.0, t.stream_rank)
                    .is_some_and(|r| r != t.stream_rank)
                    || ranks
                        .insert(t.stream_rank, &t.stream_id.0)
                        .is_some_and(|s| s != &t.stream_id.0)
                {
                    return Err(invalid("inconsistent stream ranks"));
                }
                if let Some(scope) = &t.scope {
                    if scope.kind != "family"
                        || scope.identifier.trim().is_empty()
                        || t.symbol.is_some()
                        || t.instrument.is_some()
                        || t.instrument_id.is_some()
                    {
                        return Err(invalid(
                            "family task must declare family scope and null concrete identity",
                        ));
                    }
                    let mut members = BTreeSet::new();
                    for member in &scope.members {
                        if !members.insert(member.instrument_id)
                            || !ids.contains(&member.instrument_id)
                        {
                            return Err(invalid("duplicate or unknown family instrument"));
                        }
                    }
                } else if t.symbol.as_ref().is_none_or(|s| s.trim().is_empty())
                    || t.instrument.is_none()
                {
                    return Err(invalid("concrete task requires symbol and instrument"));
                }
            }
        }
        Ok(())
    }
    pub fn resolve_job(
        &self,
        job: &PreparedJob,
        catalog: &FormatCatalog,
        runtime: &ProcessingConfig,
        root: &Path,
    ) -> Result<ProcessingJob> {
        validate_processing_config(runtime)?;
        let mut tasks = vec![];
        let mut stream_keys = BTreeMap::new();
        let mut next_id = job.tasks.iter().map(|t| t.task_id.0).max().unwrap_or(0);
        for t in &job.tasks {
            let data_type = match job.output.content_type {
                ContentType::Trades => "trades",
                ContentType::Depth => "depth",
                _ => return Err(invalid("unsupported content type")),
            };
            let id = format!(
                "{}.{}.{}.{}",
                token(&t.exchange)?,
                token(&t.instrument_type)?,
                token(&t.market_category)?,
                data_type
            );
            let profile = catalog
                .get(&id)
                .map_err(|e| invalid(format!("task {} expected profile {id}: {e}", t.task_id.0)))?;
            let bindings = if let Some(scope) = &t.scope {
                if scope.members.is_empty() {
                    return Err(invalid(format!(
                        "task {} family {} requires authoritative concrete scope.members and metadata",
                        t.task_id.0, scope.identifier
                    )));
                }
                scope
                    .members
                    .iter()
                    .map(|m| {
                        self.instruments
                            .iter()
                            .find(|i| i.instrument_id == m.instrument_id)
                            .map(|i| (i, m.archive_member.clone()))
                            .ok_or_else(|| invalid("unknown family member"))
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                let matches: Vec<_> = self
                    .instruments
                    .iter()
                    .filter(|i| {
                        i.exchange == t.exchange
                            && i.instrument_type == t.instrument_type
                            && i.market_category == t.market_category
                            && Some(&i.symbol) == t.symbol.as_ref()
                    })
                    .collect();
                let [i] = matches.as_slice() else {
                    return Err(invalid(format!(
                        "task {} requires one authoritative instrument mapping",
                        t.task_id.0
                    )));
                };
                if t.instrument_id.is_some_and(|id| id != i.instrument_id)
                    || t.instrument.as_ref() != Some(&i.instrument)
                    || t.base_asset.as_ref() != Some(&i.base_asset)
                    || t.quote_asset.as_ref() != Some(&i.quote_asset)
                {
                    return Err(invalid(
                        "task metadata conflicts with offline instrument authority",
                    ));
                }
                vec![(*i, None)]
            };
            let family_symbols: Vec<_> = bindings.iter().map(|(i, _)| i.symbol.clone()).collect();
            for (index, (i, member)) in bindings.into_iter().enumerate() {
                if i.exchange != t.exchange
                    || i.instrument_type != t.instrument_type
                    || i.market_category != t.market_category
                    || t.base_asset.as_ref().is_some_and(|a| a != &i.base_asset)
                    || t.quote_asset.as_ref().is_some_and(|a| a != &i.quote_asset)
                {
                    return Err(invalid("incompatible family instrument identity/assets"));
                }
                if let Some(scope) = &t.scope {
                    if !i.symbol.starts_with(&format!("{}-", scope.identifier)) {
                        return Err(invalid("family member symbol does not belong to scope"));
                    }
                    if data_type == "depth" && member.is_none() {
                        return Err(invalid(
                            "family depth requires explicit archive_member per concrete instrument",
                        ));
                    }
                }
                let task_id = if index == 0 {
                    t.task_id
                } else {
                    next_id = next_id
                        .checked_add(1)
                        .ok_or_else(|| invalid("expanded task ID overflow"))?;
                    TaskId(next_id)
                };
                let stream_id = if t.scope.is_some() {
                    StreamId(format!("{}.instrument:{}", t.stream_id.0, i.symbol))
                } else {
                    t.stream_id.clone()
                };
                stream_keys.insert(stream_id.0.clone(), (t.stream_rank, i.symbol.clone()));
                let mut task = WorkTask {
                    task_id,
                    stream_id,
                    input_path: resolve_path(root, &t.input_path),
                    format_code: FormatCode(profile.definition.format_code.clone()),
                    exchange: i.exchange,
                    instrument_id: i.instrument_id,
                    symbol: i.symbol.clone(),
                    instrument: InstrumentSpec {
                        instrument_kind: i.instrument_type,
                        contract_kind: match i.market_category {
                            MarketCategory::Linear => Some(ContractKind::Linear),
                            MarketCategory::Inverse => Some(ContractKind::Inverse),
                            _ => None,
                        },
                        tick_size: i.instrument.tick_size,
                        contract_value: i.instrument.contract_value,
                        contract_value_asset: i.instrument.contract_value_asset.clone(),
                    },
                    raw_schema: profile.physical.raw_schema.clone(),
                    normalizations: profile.normalizations.clone(),
                    source_ordering: profile
                        .definition
                        .source_ordering
                        .ok_or_else(|| invalid("missing source ordering"))?,
                    source_compression: profile.physical.supported_containers[0],
                    archive_member: member,
                };
                if let Some(options) = &t.source_options {
                    if t.exchange != Exchange::GateIo || data_type != "depth" {
                        return Err(invalid("source_options.feed requires Gate.io depth"));
                    }
                    let feed = token(&options.feed)?;
                    if let Some(variant) = task.raw_schema["marketforge_feeds"].get(&feed).cloned()
                    {
                        let physical: super::PhysicalFormat =
                            serde_json::from_value(variant["physical"].clone())
                                .map_err(|e| invalid(e.to_string()))?;
                        task.source_compression = physical.supported_containers[0];
                        task.raw_schema = physical.raw_schema;
                        task.source_ordering = serde_json::from_value(variant["ordering"].clone())
                            .map_err(|e| invalid(e.to_string()))?;
                        task.normalizations =
                            serde_json::from_value(variant["normalizations"].clone())
                                .map_err(|e| invalid(e.to_string()))?;
                    } else {
                        task.normalizations.retain(|n| {
                            n.rules.get("snapshot").is_some() == (options.feed == Feed::Snapshot)
                        });
                    }
                    if task.normalizations.is_empty() {
                        return Err(invalid("profile does not support requested source feed"));
                    }
                }
                catalog.check_task(&task, profile)?;
                task.raw_schema["marketforge_profile"] =
                    json!({"id":id,"sha256":profile.fingerprint});
                task.raw_schema["marketforge_source"] = json!({"task_id":t.task_id,"stream_id":t.stream_id,"stream_rank":t.stream_rank,"scope":t.scope,"source_options":t.source_options,"family_symbols":if t.scope.is_some(){Some(&family_symbols)}else{None},"instrument_namespace":self.instrument_namespace,"instrument":i,"instrument_catalog_sha256":fingerprint(&self.instruments)?});
                tasks.push(task);
            }
        }
        let expanded = job.tasks.iter().any(|t| t.scope.is_some());
        let mut keys: Vec<_> = stream_keys.into_iter().collect();
        keys.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        let streams = keys
            .into_iter()
            .enumerate()
            .map(|(index, (id, (rank, _)))| {
                Ok(StreamConfig {
                    stream_id: StreamId(id),
                    stream_rank: if expanded {
                        u32::try_from(index + 1).map_err(|_| invalid("stream rank overflow"))?
                    } else {
                        rank
                    },
                })
            })
            .collect::<Result<_>>()?;
        let resolved = ProcessingJob {
            protocol_version: PROCESSING_PROTOCOL_VERSION,
            job_id: job.job_id,
            dataset_id: job.dataset_id,
            operation: ProcessingOperation::Process,
            tasks,
            streams,
            ordering: job.ordering.clone(),
            output: OutputConfig {
                content_type: job.output.content_type,
                staging_path: resolve_path(root, &job.output.staging_path),
                dataset_path: resolve_path(root, &job.output.dataset_path),
            },
            resources: runtime.resources.clone(),
            integrity_policy: runtime.integrity_policy.clone(),
        };
        validate_processing_job_structure(&resolved)?;
        Ok(resolved)
    }
}
fn token(v: &impl Serialize) -> Result<String> {
    serde_json::to_value(v)
        .map_err(|e| invalid(e.to_string()))?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid("invalid identity token"))
}
fn identity(i: &OfflineInstrument) -> Result<String> {
    Ok(format!(
        "{}.{}.{}.{}",
        token(&i.exchange)?,
        token(&i.instrument_type)?,
        token(&i.market_category)?,
        i.symbol
    ))
}

#[cfg(test)]
mod tests;
