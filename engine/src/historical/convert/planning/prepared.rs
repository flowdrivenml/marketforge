//! Runtime binding for a prepared conversion job and its physical publications.
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::Path,
};

use crate::{
    historical::{
        convert::{
            ConversionPlan, ResolvedConversionInstrument,
            execution::{CombinedReferenceSelection, validate_conversion_input},
            plan_conversion,
        },
        job::{ConversionStreamKind, ConvertJob, PreparedConversionPublication},
        merge::{PhysicalMergeManifest, UnifiedMergeMode, UnifiedMergeStreamKind},
    },
    shared::error::{MarketForgeError, Result},
};
use sha2::{Digest, Sha256};

pub struct PreparedConversion {
    pub source: PhysicalMergeManifest,
    pub external: Option<PhysicalMergeManifest>,
    pub plan: ConversionPlan,
    source_checksums: BTreeMap<String, String>,
    external_checksums: Option<BTreeMap<String, String>>,
}

impl PreparedConversion {
    /// The CLI hashes the declared contents before it executes or publishes.
    /// Bounded readers can validate the same bindings without scanning payloads.
    pub fn verify_content(
        &self,
        job: &ConvertJob,
        reporter: &crate::historical::convert::progress::ConversionProgressReporter,
    ) -> Result<()> {
        let mut inputs = vec![(&job.input_merge, &self.source, &self.source_checksums)];
        if let (Some(path), Some(manifest), Some(checksums)) = (
            &job.conversion.reference_merge,
            &self.external,
            &self.external_checksums,
        ) {
            inputs.push((path, manifest, checksums));
        }
        for (directory, manifest, checksums) in inputs {
            for file in &manifest.files {
                reporter.check_interrupted()?;
                let path = directory.join(&file.path);
                let observed = crate::historical::normalize::manifest::file_sha256(&path).map_err(
                    |error| {
                        invalid(format!(
                            "cannot verify conversion input {}: {error}",
                            path.display()
                        ))
                    },
                )?;
                if checksums.get(&file.path) != Some(&observed) {
                    return Err(invalid(format!(
                        "conversion input content checksum mismatch at {}",
                        path.display()
                    )));
                }
            }
        }
        reporter.check_interrupted()
    }
}

fn read(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).map_err(|error| invalid(format!("cannot read {}: {error}", path.display())))
}

fn hash(bytes: &[u8]) -> String {
    crate::historical::normalize::hex_digest(Sha256::digest(bytes))
}

fn invalid(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

struct BoundPublication {
    manifest: PhysicalMergeManifest,
    streams: Vec<ResolvedConversionInstrument>,
    checksums: BTreeMap<String, String>,
}

fn bind(directory: &Path, prepared: &PreparedConversionPublication) -> Result<BoundPublication> {
    let manifest = read(&directory.join("manifest.json"))?;
    let inventory = read(&directory.join("publication-checksums.json"))?;
    if hash(&manifest) != prepared.manifest_sha256
        || hash(&inventory) != prepared.publication_checksums_sha256
    {
        return Err(invalid(format!(
            "prepared conversion publication pin mismatch at {}",
            directory.display()
        )));
    }
    let manifest: PhysicalMergeManifest = serde_json::from_slice(&manifest)
        .map_err(|error| invalid(format!("invalid physical merge manifest: {error}")))?;
    let inventory: BTreeMap<String, String> =
        serde_json::from_slice(&inventory).map_err(|error| {
            invalid(format!(
                "invalid physical merge checksum inventory: {error}"
            ))
        })?;
    if manifest.job_id != prepared.merge_job_id
        || inventory.get("manifest.json") != Some(&prepared.manifest_sha256)
        || manifest
            .files
            .iter()
            .any(|file| !inventory.contains_key(&file.path))
    {
        return Err(invalid(
            "prepared merge publication identity or checksum inventory mismatch",
        ));
    }
    validate_conversion_input(directory, &manifest)?;

    let evidence: HashMap<_, _> = prepared
        .normalized_inputs
        .iter()
        .map(|input| (input.dataset_id, input))
        .collect();
    if evidence.len() != manifest.input_datasets.len() {
        return Err(invalid(
            "prepared normalization inventory does not match merge inputs",
        ));
    }
    let expected_pins = manifest
        .input_datasets
        .iter()
        .map(|input| {
            let evidence = evidence
                .get(&input.dataset_id)
                .ok_or_else(|| invalid("missing prepared evidence for normalized dataset"))?;
            if input.manifest_version >= 5 && evidence.manifest_sha256.is_none() {
                return Err(invalid("missing committed normalized manifest pin"));
            }
            Ok(evidence.manifest_sha256.as_deref())
        })
        .collect::<Result<Vec<_>>>()?;
    if let Some(pinned) = inventory.get("merge-input-checksums.json") {
        let bytes = read(&directory.join("merge-input-checksums.json"))?;
        let observed: Vec<String> = serde_json::from_slice(&bytes)
            .map_err(|error| invalid(format!("invalid merge input checksums: {error}")))?;
        if hash(&bytes) != *pinned
            || observed.len() != expected_pins.len()
            || observed
                .iter()
                .zip(expected_pins)
                .any(|(value, expected)| expected.is_some_and(|expected| value != expected))
        {
            return Err(invalid(
                "prepared normalization pins disagree with merge input checksums",
            ));
        }
    } else if manifest
        .input_datasets
        .iter()
        .any(|input| input.manifest_version >= 5)
    {
        return Err(invalid(
            "committed merge is missing its normalized input checksums",
        ));
    }

    let streams: HashMap<_, _> = prepared
        .streams
        .iter()
        .map(|stream| (stream.stream_rank, stream))
        .collect();
    if streams.len() != manifest.streams.len() {
        return Err(invalid(
            "prepared stream bindings do not cover the physical inventory",
        ));
    }
    let resolved = manifest
        .streams
        .iter()
        .map(|stream| {
            let binding = streams.get(&stream.stream_rank).ok_or_else(|| {
                invalid(format!(
                    "missing prepared stream binding {}",
                    stream.stream_id
                ))
            })?;
            let kind = match stream.kind {
                UnifiedMergeStreamKind::Trade => ConversionStreamKind::Trade,
                UnifiedMergeStreamKind::Depth => ConversionStreamKind::Depth,
            };
            if binding.kind != kind
                || binding.stream_id != stream.stream_id
                || binding.dataset_id != stream.dataset_id
                || binding.instrument_id != stream.instrument_id
                || binding.identity.exchange != stream.exchange
                || binding.identity.symbol != stream.symbol
                || stream
                    .base_currency
                    .as_ref()
                    .is_some_and(|currency| currency != binding.base_currency.as_str())
                || stream
                    .quote_currency
                    .as_ref()
                    .is_some_and(|currency| currency != binding.quote_currency.as_str())
            {
                return Err(invalid(format!(
                    "prepared stream metadata mismatch for {}",
                    stream.stream_id
                )));
            }
            Ok(ResolvedConversionInstrument {
                dataset_id: binding.dataset_id,
                stream_rank: binding.stream_rank,
                stream_id: binding.stream_id.clone(),
                identity: binding.identity.clone(),
                base_currency: binding.base_currency.clone(),
                quote_currency: binding.quote_currency.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BoundPublication {
        manifest,
        streams: resolved,
        checksums: inventory,
    })
}

pub fn prepare_conversion_execution(job: &ConvertJob) -> Result<PreparedConversion> {
    job.validate()?;
    let source = bind(&job.input_merge, &job.source)?;
    let external = job
        .conversion
        .reference_merge
        .as_ref()
        .zip(job.external_reference.as_ref())
        .map(|(path, pin)| bind(path, pin))
        .transpose()?;
    let external_manifest = external.as_ref().map(|publication| &publication.manifest);
    let catalog = super::catalog::select_prepared_references(
        job,
        &source.manifest,
        source.streams,
        external
            .as_ref()
            .map(|publication| (&publication.manifest, publication.streams.as_slice())),
    )?;
    let plan = plan_conversion(job, &catalog)?;
    match source.manifest.mode {
        UnifiedMergeMode::Trades if plan.unresolved_streams > 0 => {
            return Err(invalid("trade conversion streams have no available path"));
        }
        UnifiedMergeMode::Combined => {
            CombinedReferenceSelection::resolve(&plan, &source.manifest, external_manifest)?;
        }
        UnifiedMergeMode::Depth
            if external.is_none()
                || !plan
                    .streams
                    .iter()
                    .flat_map(|stream| &stream.paths)
                    .any(|path| !path.edges.is_empty()) =>
        {
            return Err(invalid(
                "depth conversion requires an external trade reference used by a conversion path",
            ));
        }
        _ => {}
    }
    let (external, external_checksums) = external.map_or((None, None), |publication| {
        (Some(publication.manifest), Some(publication.checksums))
    });
    Ok(PreparedConversion {
        source: source.manifest,
        source_checksums: source.checksums,
        external,
        external_checksums,
        plan,
    })
}
