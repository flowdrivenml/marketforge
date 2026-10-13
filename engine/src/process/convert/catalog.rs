use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use crate::{
    error::{MarketForgeError, Result},
    instrument::{Currency, InstrumentCatalog, InstrumentIdentity},
    job::{ConvertJob, load_processing_job},
    process::merge::PhysicalMergeManifest,
};

// -----------------------------------------------------------------------------
// Resolved instruments
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ResolvedConversionInstrument {
    pub dataset_id: u64,
    pub stream_rank: u32,
    pub stream_id: String,

    pub identity: InstrumentIdentity,

    pub base_currency: Currency,
    pub quote_currency: Currency,
}

#[derive(Debug, Clone)]
pub struct ResolvedConversionCatalog {
    pub sources: Vec<ResolvedConversionInstrument>,
    pub references: Vec<ResolvedConversionInstrument>,
}

// -----------------------------------------------------------------------------
// Resolver
// -----------------------------------------------------------------------------

pub fn resolve_conversion_catalog(
    job: &ConvertJob,
    manifest: &PhysicalMergeManifest,
    catalog: &InstrumentCatalog,
    project_root: &Path,
) -> Result<ResolvedConversionCatalog> {
    let processing_jobs = discover_processing_jobs(&project_root.join("data/.jobs/process"))?;

    let mut sources = Vec::with_capacity(manifest.streams.len());

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

        sources.push(ResolvedConversionInstrument {
            dataset_id: stream.dataset_id,
            stream_rank: stream.stream_rank,
            stream_id: stream.stream_id.clone(),

            identity,
            base_currency: metadata.base_currency.clone(),
            quote_currency: metadata.quote_currency.clone(),
        });
    }

    // -------------------------------------------------------------------------
    // Resolve configured references
    // -------------------------------------------------------------------------

    let mut references = Vec::new();
    let mut matched = HashSet::new();

    for requested in &job.conversion.references {
        let mut found = false;

        for source in &sources {
            if &source.identity == requested {
                references.push(source.clone());
                found = true;
            }
        }

        if found {
            matched.insert(requested.clone());
        }
    }

    for requested in &job.conversion.references {
        if !matched.contains(requested) {
            return invalid(format!(
                "reference instrument not present in source merge: {} {}",
                requested.exchange, requested.symbol
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
