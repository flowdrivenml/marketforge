use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use {
    crate::historical::job::ConvertJob, crate::historical::job::load_processing_job,
    crate::historical::merge::PhysicalMergeManifest, crate::historical::merge::UnifiedMergeMode,
    crate::historical::merge::UnifiedMergeStreamKind,
    crate::shared::domain::instrument::InstrumentCatalog,
    crate::shared::domain::instrument::InstrumentIdentity, crate::shared::error::MarketForgeError,
    crate::shared::error::Result,
};

// -----------------------------------------------------------------------------
// Resolved instruments
// -----------------------------------------------------------------------------

pub use crate::shared::conversion::planner::ResolvedConversionInstrument;

#[derive(Debug, Clone)]
pub struct ResolvedConversionCatalog {
    pub sources: Vec<ResolvedConversionInstrument>,
    pub references: Vec<ResolvedConversionInstrument>,
}

// -----------------------------------------------------------------------------
// Resolver
// -----------------------------------------------------------------------------
fn resolve_manifest_instruments(
    manifest: &PhysicalMergeManifest,
    catalog: &InstrumentCatalog,
    processing_jobs: &HashMap<u64, PathBuf>,
) -> Result<Vec<ResolvedConversionInstrument>> {
    let mut instruments = Vec::with_capacity(manifest.streams.len());

    for stream in &manifest.streams {
        let job_path = processing_jobs.get(&stream.dataset_id).ok_or_else(|| {
            invalid_error(format!(
                "processing job not found for dataset {}",
                stream.dataset_id
            ))
        })?;

        let processing_job = load_processing_job(job_path)?;

        let task = processing_job.tasks.first().ok_or_else(|| {
            invalid_error(format!(
                "dataset {} contains no processing tasks",
                stream.dataset_id
            ))
        })?;

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

        if task.instrument_id != stream.instrument_id
            || task.symbol != stream.symbol
            || task.stream_id.0 != stream.stream_id
        {
            return invalid(format!(
                "dataset {} instrument identity mismatch",
                stream.dataset_id
            ));
        }

        instruments.push(ResolvedConversionInstrument {
            dataset_id: stream.dataset_id,
            stream_rank: stream.stream_rank,
            stream_id: stream.stream_id.clone(),
            identity,
            base_currency: metadata.base_currency.clone(),
            quote_currency: metadata.quote_currency.clone(),
        });
    }

    Ok(instruments)
}

pub fn resolve_conversion_catalog(
    job: &ConvertJob,
    manifest: &PhysicalMergeManifest,
    catalog: &InstrumentCatalog,
    project_root: &Path,
) -> Result<ResolvedConversionCatalog> {
    job.validate()?;
    manifest.validate()?;

    let processing_jobs = discover_processing_jobs(&project_root.join("data/.jobs/process"))?;

    // -------------------------------------------------------------------------
    // Resolve conversion sources
    // -------------------------------------------------------------------------

    let sources = resolve_manifest_instruments(manifest, catalog, &processing_jobs)?;

    // -------------------------------------------------------------------------
    // Resolve reference manifest
    // -------------------------------------------------------------------------

    let reference_manifest = if let Some(reference_path) = &job.conversion.reference_merge {
        let path = reference_path.join("manifest.json");

        let bytes = fs::read(&path).map_err(|error| {
            invalid_error(format!(
                "failed to read reference merge manifest {}: {error}",
                path.display()
            ))
        })?;

        let reference_manifest: PhysicalMergeManifest = serde_json::from_slice(&bytes)
            .map_err(|error| invalid_error(format!("invalid reference merge manifest: {error}")))?;

        reference_manifest.validate()?;
        if job.conversion.reference_merge.is_some()
            && !reference_manifest
                .streams
                .iter()
                .any(|stream| stream.kind == UnifiedMergeStreamKind::Trade)
        {
            return invalid("external conversion reference merge contains no trade streams");
        }

        reference_manifest
    } else {
        manifest.clone()
    };

    // -------------------------------------------------------------------------
    // Resolve available reference instruments
    // -------------------------------------------------------------------------

    let available_references =
        resolve_manifest_instruments(&reference_manifest, catalog, &processing_jobs)?;

    let combined = manifest.mode == UnifiedMergeMode::Combined;

    let mut references = Vec::new();

    for requested in &job.conversion.references {
        let mut found = false;

        // -------------------------------------------------------------
        // Combined mode: prefer internal trade references
        // -------------------------------------------------------------

        if combined {
            for (stream, resolved) in manifest.streams.iter().zip(&sources) {
                if &resolved.identity != requested {
                    continue;
                }

                if stream.kind != UnifiedMergeStreamKind::Trade {
                    return invalid(format!(
                        "conversion reference must be a trade stream: {}",
                        resolved.stream_id
                    ));
                }

                references.push(resolved.clone());
                found = true;
            }
        }

        if found {
            continue;
        }

        // -------------------------------------------------------------
        // Resolve missing references from external merge
        // -------------------------------------------------------------

        for (stream, resolved) in reference_manifest.streams.iter().zip(&available_references) {
            if &resolved.identity != requested {
                continue;
            }

            if stream.kind != UnifiedMergeStreamKind::Trade {
                return invalid(format!(
                    "conversion reference must be a trade stream: {}",
                    resolved.stream_id
                ));
            }

            references.push(resolved.clone());
            found = true;
        }

        if !found {
            return invalid(format!(
                "reference instrument not present in internal or external merge: {} {}",
                requested.exchange, requested.symbol,
            ));
        }
    }

    // -------------------------------------------------------------------------
    // Reject duplicate reference streams
    // -------------------------------------------------------------------------

    let mut seen = HashSet::new();

    for reference in &references {
        if !seen.insert((reference.dataset_id, reference.stream_id.clone())) {
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
// -----------------------------------------------------------------------------
// Processing job discovery
// -----------------------------------------------------------------------------

fn discover_processing_jobs(directory: &Path) -> Result<HashMap<u64, PathBuf>> {
    let mut jobs = HashMap::new();

    for entry in fs::read_dir(directory).map_err(|error| {
        invalid_error(format!(
            "failed to read processing jobs {}: {error}",
            directory.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            invalid_error(format!("failed to read processing job entry: {error}"))
        })?;

        let path = entry.path();

        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }

        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };

        let Some(stem) = filename.strip_suffix(".json") else {
            continue;
        };

        let Some((_, suffix)) = stem.rsplit_once("-d") else {
            continue;
        };

        let Ok(dataset_id) = suffix.parse::<u64>() else {
            continue;
        };

        if jobs.insert(dataset_id, path.clone()).is_some() {
            return invalid(format!("duplicate processing job for dataset {dataset_id}"));
        }
    }

    Ok(jobs)
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
