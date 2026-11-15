//! Production schema-v4 prepared batches. No database, archive reads, or writes.
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
    pub instrument_selection: Option<InstrumentSelection>,
}
/// Exact native-symbol selection inside the declared source population.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum InstrumentSelection {
    All,
    Include { symbols: Vec<String> },
}
impl<'de> Deserialize<'de> for InstrumentSelection {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        use serde::de::Error;
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| D::Error::custom("instrument_selection must be an object"))?;
        let selection = match object.get("mode").and_then(Value::as_str) {
            Some("all") if object.len() == 1 => Self::All,
            Some("include") if object.len() == 2 && object.contains_key("symbols") => {
                Self::Include {
                    symbols: serde_json::from_value(object["symbols"].clone())
                        .map_err(D::Error::custom)?,
                }
            }
            _ => {
                return Err(D::Error::custom(
                    "instrument_selection accepts only {mode: all} or {mode: include, symbols: [exact symbols]}",
                ));
            }
        };
        selection.validate().map_err(D::Error::custom)?;
        Ok(selection)
    }
}
impl InstrumentSelection {
    pub fn validate(&self) -> Result<()> {
        if let Self::Include { symbols } = self {
            let mut seen = BTreeSet::new();
            if symbols.is_empty()
                || symbols
                    .iter()
                    .any(|s| s.trim().is_empty() || !seen.insert(s))
            {
                return Err(invalid(
                    "instrument_selection.include requires nonempty unique exact symbols",
                ));
            }
        }
        Ok(())
    }
    pub fn selects(&self, symbol: &str) -> bool {
        match self {
            Self::All => true,
            Self::Include { symbols } => symbols.iter().any(|s| s == symbol),
        }
    }
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
        if self.schema_version != 4
            || self.kind != "normalization_batch"
            || self.status != "prepared"
        {
            return Err(invalid(
                "expected schema_version=4, kind=normalization_batch, status=prepared",
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
                if let Some(selection) = &t.instrument_selection {
                    selection.validate()?;
                    if let Some(symbol) = &t.symbol {
                        if !matches!(selection, InstrumentSelection::Include { symbols } if symbols.as_slice() == [symbol.clone()])
                        {
                            return Err(invalid(
                                "concrete symbol permits only omitted selection or include of that exact symbol; all cannot broaden it",
                            ));
                        }
                    }
                }
                if t.scope.is_some() && t.instrument_selection.is_none() {
                    return Err(invalid(
                        "family task requires explicit instrument_selection",
                    ));
                }
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
        let mut prepared_bytes = 0u64;
        let instrument_catalog_sha256 = fingerprint(&self.instruments)?;
        let mut stream_keys = BTreeMap::new();
        let mut next_id = job.tasks.iter().map(|t| t.task_id.0).max().unwrap_or(0);
        for (source_priority, t) in job.tasks.iter().enumerate() {
            let _limits = crate::shared::historical::limits::LimitScope::enter(
                runtime.resources.normalization_limits,
                format!(
                    "job={}, source task={}, preparation",
                    job.job_id.0, t.task_id.0
                ),
            );
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
                let selection = t
                    .instrument_selection
                    .as_ref()
                    .ok_or_else(|| invalid("family task requires instrument_selection"))?;
                let mut selected = self
                    .instruments
                    .iter()
                    .filter(|i| {
                        i.exchange == t.exchange
                            && i.instrument_type == t.instrument_type
                            && i.market_category == t.market_category
                            && family_of(i.exchange, i.instrument_type, &i.symbol)
                                == Some(scope.identifier.as_str())
                            && selection.selects(&i.symbol)
                    })
                    .collect::<Vec<_>>();
                selected.sort_by(|a, b| {
                    a.symbol
                        .cmp(&b.symbol)
                        .then(a.instrument_id.cmp(&b.instrument_id))
                });
                if let InstrumentSelection::Include { symbols } = selection {
                    for symbol in symbols {
                        if family_of(t.exchange, t.instrument_type, symbol)
                            != Some(scope.identifier.as_str())
                        {
                            return Err(invalid(format!(
                                "selected symbol {symbol} is outside family scope {}",
                                scope.identifier
                            )));
                        }
                        if !selected.iter().any(|i| &i.symbol == symbol) {
                            return Err(invalid(format!(
                                "selected symbol {symbol} requires authoritative offline metadata; no specification may be fabricated"
                            )));
                        }
                    }
                }
                if selected.is_empty() {
                    return Err(invalid(format!(
                        "task {} family {} has no authoritative concrete metadata; all still requires metadata for every encountered eligible symbol",
                        t.task_id.0, scope.identifier
                    )));
                }
                crate::shared::historical::limits::check(
                    "family concrete instruments",
                    runtime.resources.normalization_limits.metric_entries,
                    selected.len() as u64,
                    "family resolution",
                )?;
                selected.into_iter().map(|i| {
                    let member = scope.members.iter().find(|m| m.instrument_id == i.instrument_id).and_then(|m|m.archive_member.clone());
                    if matches!(selection,InstrumentSelection::All) && member.is_some() { return Err(invalid("all cannot be confined to a finite archive-member map; use a native-symbol mixed member or an archive with one data member; full multi-member enumeration is not supported")); }
                    Ok((i,member))
                }).collect::<Result<Vec<_>>>()?
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
                    if family_of(i.exchange, i.instrument_type, &i.symbol)
                        != Some(scope.identifier.as_str())
                    {
                        return Err(invalid("family member symbol does not belong to scope"));
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
                if stream_keys
                    .insert(stream_id.0.clone(), (t.stream_rank, i.symbol.clone()))
                    .is_some_and(|old| old != (t.stream_rank, i.symbol.clone()))
                {
                    return Err(invalid(
                        "derived concrete stream identity collides with another declared stream",
                    ));
                }
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
                let effective_profile =
                    profile.for_feed(t.source_options.as_ref().map(|o| o.feed))?;
                task.source_compression = effective_profile.physical.supported_containers[0];
                task.raw_schema = effective_profile.physical.raw_schema.clone();
                task.source_ordering = effective_profile
                    .definition
                    .source_ordering
                    .ok_or_else(|| invalid("missing source ordering"))?;
                task.normalizations = effective_profile.normalizations.clone();
                let deferred_instrument_error = match catalog.check_task(&task, &effective_profile)
                {
                    Ok(()) => None,
                    Err(error) if t.scope.is_some() => Some(error.to_string()),
                    Err(error) => return Err(error),
                };
                task.raw_schema["marketforge_profile"] =
                    json!({"id":id,"sha256":profile.fingerprint});
                task.raw_schema["marketforge_source"] = json!({"task_id":t.task_id,"stream_id":t.stream_id,"stream_rank":t.stream_rank,"scope":t.scope,"source_options":t.source_options,"prepared_schema_version":4,"source_priority":source_priority,"instrument_selection":t.instrument_selection,"instrument_namespace":self.instrument_namespace,"instrument":i,"deferred_instrument_error":deferred_instrument_error,"instrument_catalog_sha256":instrument_catalog_sha256});
                let footprint =
                    crate::historical::normalize::resources::prepared_task_memory(&task)?;
                prepared_bytes = prepared_bytes
                    .checked_add(
                        footprint
                            .checked_mul(3)
                            .ok_or_else(|| invalid("prepared expansion accounting overflow"))?,
                    )
                    .ok_or_else(|| invalid("prepared expansion accounting overflow"))?;
                crate::shared::historical::limits::check(
                    "prepared expansion bytes",
                    runtime.resources.memory_budget_bytes,
                    prepared_bytes,
                    "family/concrete resolution",
                )?;
                tasks.push(task);
            }
        }
        let explicit = job
            .tasks
            .iter()
            .filter(|t| t.scope.is_none())
            .map(|t| (t.stream_id.0.clone(), t.stream_rank))
            .collect::<BTreeMap<_, _>>();
        let mut occupied = explicit.values().copied().collect::<BTreeSet<_>>();
        let mut next_rank = 0u32;
        let mut keys: Vec<_> = stream_keys.into_iter().collect();
        keys.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        let streams = keys
            .into_iter()
            .map(|(id, _)| {
                let rank = if let Some(rank) = explicit.get(&id) {
                    *rank
                } else {
                    loop {
                        next_rank = next_rank
                            .checked_add(1)
                            .ok_or_else(|| invalid("derived stream rank overflow"))?;
                        if occupied.insert(next_rank) {
                            break next_rank;
                        }
                    }
                };
                Ok(StreamConfig {
                    stream_id: StreamId(id),
                    stream_rank: rank,
                })
            })
            .collect::<Result<Vec<_>>>()?;
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
pub(crate) fn family_of(exchange: Exchange, kind: InstrumentKind, symbol: &str) -> Option<&str> {
    match (exchange, kind) {
        (Exchange::Okx, InstrumentKind::Future) => {
            crate::shared::formats::trades::okx_futures_family(symbol)
        }
        (Exchange::Okx, InstrumentKind::Perpetual) => {
            symbol.strip_suffix("-SWAP").filter(|s| !s.is_empty())
        }
        _ => None,
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

#[cfg(test)]
pub(crate) mod selection_tests;
