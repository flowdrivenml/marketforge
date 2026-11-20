use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use crate::{
    historical::{
        job::{
            ContentType, ConvertJob, ProcessingJob, WorkTask, load_committed_processing_job,
            load_processing_job,
        },
        merge::{
            MergeInputDataset, PhysicalMergeManifest, UnifiedMergeMode, UnifiedMergeStream,
            UnifiedMergeStreamKind,
        },
        normalize::manifest::DatasetManifest,
    },
    shared::{
        domain::instrument::{InstrumentCatalog, InstrumentIdentity, InstrumentMetadata},
        error::{MarketForgeError, Result},
    },
};

pub use crate::shared::conversion::planner::ResolvedConversionInstrument;

#[derive(Debug, Clone)]
pub struct ResolvedConversionCatalog {
    pub sources: Vec<ResolvedConversionInstrument>,
    pub references: Vec<ResolvedConversionInstrument>,
}

struct Provenance {
    job: ProcessingJob,
    manifest: Option<DatasetManifest>,
}

impl Provenance {
    fn load(
        dataset: &MergeInputDataset,
        project_root: &Path,
        normalization_root: Option<&Path>,
    ) -> Result<Self> {
        let path = normalization_root.map_or_else(
            || project_root.join(&dataset.dataset_path),
            |root| root.join(dataset.dataset_id.to_string()),
        );
        let job = if dataset.manifest_version >= 5
            || path.join("processing-job.json").is_file()
            || normalization_root.is_some()
        {
            load_committed_processing_job(&path).map_err(|error| {
                invalid_error(format!(
                    "normalization provenance {}: {error}",
                    path.display()
                ))
            })?
        } else {
            load_legacy_job(&project_root.join("data/.jobs/process"), dataset.dataset_id)?
        };
        if job.dataset_id.0 != dataset.dataset_id {
            return invalid("conversion dataset provenance identity mismatch");
        }
        let manifest = if dataset.manifest_version >= 5 {
            let manifest: DatasetManifest = read_json(&path.join("manifest.json"))?;
            manifest.validate_streams()?;
            if manifest.dataset_id != dataset.dataset_id
                || manifest.job_id != job.job_id.0
                || manifest.protocol_version != dataset.manifest_version
            {
                return invalid("normalization manifest does not match committed processing job");
            }
            Some(manifest)
        } else {
            None
        };
        Ok(Self { job, manifest })
    }

    fn resolve(
        &self,
        stream: &UnifiedMergeStream,
        catalog: &InstrumentCatalog,
    ) -> Result<ResolvedConversionInstrument> {
        let mut tasks = self
            .job
            .tasks
            .iter()
            .filter(|task| task.stream_id.0 == stream.stream_id);
        let task = tasks.next().ok_or_else(|| {
            invalid_error(format!(
                "dataset {} contains no processing tasks for stream {}",
                stream.dataset_id, stream.stream_id
            ))
        })?;
        let namespace = task_namespace(task);
        let kind = match stream.kind {
            UnifiedMergeStreamKind::Trade => ContentType::Trades,
            UnifiedMergeStreamKind::Depth => ContentType::Depth,
        };
        if self.job.output.content_type != kind
            || std::iter::once(task).chain(tasks).any(|candidate| {
                candidate.instrument_id != stream.instrument_id
                    || candidate.symbol != stream.symbol
                    || candidate.exchange.as_str() != stream.exchange
                    || candidate.instrument != task.instrument
                    || task_namespace(candidate) != namespace
            })
        {
            return invalid(format!(
                "dataset {} stream {} has conflicting instrument metadata",
                stream.dataset_id, stream.stream_id
            ));
        }

        let identity = InstrumentIdentity {
            exchange: stream.exchange.clone(),
            symbol: stream.symbol.clone(),
            instrument_kind: task.instrument.instrument_kind,
            contract_kind: task.instrument.contract_kind,
        };
        let metadata = catalog.resolve(&identity).ok_or_else(|| {
            invalid_error(format!(
                "instrument missing from catalog: {} {}",
                identity.exchange, identity.symbol
            ))
        })?;
        if let Some(manifest) = &self.manifest {
            if manifest.content_type != kind
                || manifest.instrument_namespace.as_deref()
                    != namespace.and_then(serde_json::Value::as_str)
                || !manifest
                    .streams
                    .iter()
                    .any(|item| item.stream_id == stream.stream_id)
            {
                return invalid(format!(
                    "normalization provenance does not match conversion stream {}",
                    stream.stream_id
                ));
            }
            verify_catalog_authority(manifest, stream, metadata)?;
        }

        Ok(ResolvedConversionInstrument {
            dataset_id: stream.dataset_id,
            stream_rank: stream.stream_rank,
            stream_id: stream.stream_id.clone(),
            identity,
            base_currency: metadata.base_currency.clone(),
            quote_currency: metadata.quote_currency.clone(),
        })
    }
}

fn task_namespace(task: &WorkTask) -> Option<&serde_json::Value> {
    task.raw_schema
        .pointer("/marketforge_source/instrument_namespace")
}

fn verify_catalog_authority(
    manifest: &DatasetManifest,
    stream: &UnifiedMergeStream,
    metadata: &InstrumentMetadata,
) -> Result<()> {
    if manifest.resolved_instruments.is_empty() {
        return Ok(());
    }
    let mut matching = manifest
        .resolved_instruments
        .iter()
        .filter_map(|value| value.get("instrument"))
        .filter(|value| {
            value
                .get("instrument_id")
                .and_then(serde_json::Value::as_i64)
                == Some(stream.instrument_id)
        })
        .peekable();
    if matching.peek().is_none()
        || matching.any(|value| {
            value.get("exchange").and_then(serde_json::Value::as_str) != Some(&stream.exchange)
                || value.get("symbol").and_then(serde_json::Value::as_str) != Some(&stream.symbol)
                || value.get("base_asset").and_then(serde_json::Value::as_str)
                    != Some(metadata.base_currency.as_str())
                || value.get("quote_asset").and_then(serde_json::Value::as_str)
                    != Some(metadata.quote_currency.as_str())
        })
    {
        return invalid(format!(
            "catalog disagrees with committed normalization authority for {}",
            stream.stream_id
        ));
    }
    Ok(())
}

fn resolve_manifest_instruments(
    manifest: &PhysicalMergeManifest,
    catalog: &InstrumentCatalog,
    project_root: &Path,
    normalization_root: Option<&Path>,
) -> Result<Vec<ResolvedConversionInstrument>> {
    let inputs = manifest
        .input_datasets
        .iter()
        .map(|dataset| {
            Ok((
                dataset.dataset_id,
                Provenance::load(dataset, project_root, normalization_root)?,
            ))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    manifest
        .streams
        .iter()
        .map(|stream| {
            inputs
                .get(&stream.dataset_id)
                .ok_or_else(|| invalid_error("missing conversion input provenance"))?
                .resolve(stream, catalog)
        })
        .collect()
}

pub fn resolve_conversion_catalog(
    job: &ConvertJob,
    manifest: &PhysicalMergeManifest,
    catalog: &InstrumentCatalog,
    project_root: &Path,
) -> Result<ResolvedConversionCatalog> {
    resolve_conversion_catalog_with_normalization_root(job, manifest, catalog, project_root, None)
}

pub fn resolve_conversion_catalog_with_normalization_root(
    job: &ConvertJob,
    manifest: &PhysicalMergeManifest,
    catalog: &InstrumentCatalog,
    project_root: &Path,
    normalization_root: Option<&Path>,
) -> Result<ResolvedConversionCatalog> {
    job.validate()?;
    manifest.validate()?;
    if manifest.mode == UnifiedMergeMode::Trades && job.conversion.reference_merge.is_some() {
        return invalid("trade-only conversion uses internal reference trades");
    }
    let sources =
        resolve_manifest_instruments(manifest, catalog, project_root, normalization_root)?;
    let external = job
        .conversion
        .reference_merge
        .as_ref()
        .map(|directory| {
            let external: PhysicalMergeManifest = read_json(&directory.join("manifest.json"))?;
            external.validate()?;
            if external.mode != UnifiedMergeMode::Trades {
                return invalid(
                    "external last-trade references require a trade-only physical merge",
                );
            }
            let instruments =
                resolve_manifest_instruments(&external, catalog, project_root, normalization_root)?;
            Ok((external, instruments))
        })
        .transpose()?;

    let mut references = Vec::new();
    for requested in &job.conversion.references {
        let before = references.len();
        if manifest.mode == UnifiedMergeMode::Combined || external.is_none() {
            references.extend(matching_trades(manifest, &sources, requested).cloned());
        }
        if references.len() == before
            && let Some((external, instruments)) = &external
        {
            references.extend(matching_trades(external, instruments, requested).cloned());
        }
        if references.len() == before {
            return invalid(format!(
                "reference instrument not present in internal or external merge: {} {}",
                requested.exchange, requested.symbol,
            ));
        }
    }

    let mut seen = HashSet::new();
    for reference in &references {
        if !seen.insert(&reference.stream_id) {
            return invalid(format!(
                "duplicate conversion reference stream: {}",
                reference.stream_id
            ));
        }
    }
    Ok(ResolvedConversionCatalog {
        sources,
        references,
    })
}

fn matching_trades<'a>(
    manifest: &'a PhysicalMergeManifest,
    instruments: &'a [ResolvedConversionInstrument],
    identity: &'a InstrumentIdentity,
) -> impl Iterator<Item = &'a ResolvedConversionInstrument> {
    manifest
        .streams
        .iter()
        .zip(instruments)
        .filter_map(move |(stream, resolved)| {
            (stream.kind == UnifiedMergeStreamKind::Trade && &resolved.identity == identity)
                .then_some(resolved)
        })
}

/// Pre-stream-layout publications may lack a committed processing snapshot.
fn load_legacy_job(directory: &Path, dataset_id: u64) -> Result<ProcessingJob> {
    let missing = || {
        invalid_error(format!(
            "no committed or legacy processing job for dataset {dataset_id}"
        ))
    };
    if !directory.is_dir() {
        return Err(missing());
    }
    let suffix = format!("-d{dataset_id}.json");
    let mut paths = Vec::<PathBuf>::new();
    for entry in fs::read_dir(directory).map_err(|error| invalid_error(error.to_string()))? {
        let entry = entry.map_err(|error| invalid_error(error.to_string()))?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.ends_with(&suffix))
        {
            paths.push(entry.path());
        }
    }
    match paths.as_slice() {
        [] => Err(missing()),
        [path] => load_processing_job(path),
        _ => invalid(format!(
            "duplicate legacy processing jobs for dataset {dataset_id}"
        )),
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = fs::read(path)
        .map_err(|error| invalid_error(format!("cannot read {}: {error}", path.display())))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| invalid_error(format!("invalid {}: {error}", path.display())))
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
