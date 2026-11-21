use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::instrument::InstrumentKind;
use super::{MergeParquetConfig, MergeResources};
use crate::{
    shared::domain::instrument::{Currency, InstrumentIdentity},
    shared::error::{MarketForgeError, Result},
};

pub const CONVERT_JOB_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedConversionPublication {
    pub merge_job_id: u64,
    pub manifest_sha256: String,
    pub publication_checksums_sha256: String,
    pub normalized_inputs: Vec<ConversionNormalizationEvidence>,
    pub streams: Vec<PreparedConversionStream>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionNormalizationEvidence {
    pub dataset_id: u64,
    pub manifest_sha256: Option<String>,
    pub processing_job_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversionStreamKind {
    Trade,
    Depth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedConversionStream {
    pub dataset_id: u64,
    pub stream_id: String,
    pub stream_rank: u32,
    pub kind: ConversionStreamKind,
    pub instrument_id: i64,
    pub instrument_namespace: String,
    pub identity: InstrumentIdentity,
    pub base_currency: Currency,
    pub quote_currency: Currency,
}

impl PreparedConversionPublication {
    fn validate(&self) -> Result<()> {
        use std::collections::{HashMap, HashSet};

        if self.merge_job_id == 0 || self.streams.is_empty() {
            return invalid("prepared publication requires an identity and stream bindings");
        }
        validate_digest(&self.manifest_sha256)?;
        validate_digest(&self.publication_checksums_sha256)?;
        let mut inputs = HashSet::new();
        for input in &self.normalized_inputs {
            if input.dataset_id == 0 || !inputs.insert(input.dataset_id) {
                return invalid("duplicate or invalid prepared normalization evidence");
            }
            if let Some(digest) = &input.manifest_sha256 {
                validate_digest(digest)?;
            }
            validate_digest(&input.processing_job_sha256)?;
        }
        let mut ranks = HashSet::new();
        let mut ids = HashSet::new();
        let mut instruments = HashMap::new();
        for stream in &self.streams {
            if !inputs.contains(&stream.dataset_id)
                || stream.instrument_id <= 0
                || stream.instrument_namespace.trim().is_empty()
                || stream.stream_id.trim().is_empty()
                || stream.identity.exchange.trim().is_empty()
                || stream.identity.symbol.trim().is_empty()
                || stream.base_currency == stream.quote_currency
                || !ranks.insert(stream.stream_rank)
                || !ids.insert(&stream.stream_id)
            {
                return invalid("invalid or duplicate prepared conversion stream binding");
            }
            Currency::new(stream.base_currency.as_str())?;
            Currency::new(stream.quote_currency.as_str())?;
            let key = (&stream.instrument_namespace, stream.instrument_id);
            let metadata = (
                &stream.identity,
                &stream.base_currency,
                &stream.quote_currency,
            );
            if instruments
                .insert(key, metadata)
                .is_some_and(|previous| previous != metadata)
            {
                return invalid("conflicting prepared metadata for a scoped instrument");
            }
        }
        Ok(())
    }
}

fn validate_digest(digest: &str) -> Result<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid("prepared conversion evidence requires lowercase SHA-256 digests");
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConvertJob {
    pub schema_version: u32,
    pub source: PreparedConversionPublication,
    pub external_reference: Option<PreparedConversionPublication>,
    pub catalog_sha256: String,
    pub parquet: MergeParquetConfig,
    pub job_id: u64,
    pub operation: ConvertOperation,
    pub input_merge: PathBuf,
    pub conversion: ConversionConfig,
    pub resources: MergeResources,
    pub output: ConvertOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConvertOperation {
    Convert,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionConfig {
    pub target_quote: String,
    pub price_source: ConversionPriceSource,
    pub max_staleness_ns: u64,
    #[serde(default)]
    pub instrument_kinds: Option<Vec<InstrumentKind>>,
    #[serde(default)]
    pub references: Vec<InstrumentIdentity>,
    #[serde(default)]
    pub reference_merge: Option<PathBuf>,
    #[serde(default)]
    pub path_policy: ConversionPathPolicy,
    #[serde(default = "default_max_hops")]
    pub max_hops: usize,
}
impl ConversionConfig {
    pub fn selects_instrument_kind(&self, kind: InstrumentKind) -> bool {
        self.instrument_kinds
            .as_ref()
            .is_none_or(|kinds| kinds.contains(&kind))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversionPriceSource {
    LastTrade,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ConversionPathPolicy {
    #[default]
    All,
    Primary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConvertOutput {
    pub dataset_path: PathBuf,
}

fn default_max_hops() -> usize {
    3
}

impl ConvertJob {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != CONVERT_JOB_SCHEMA_VERSION {
            return invalid(
                "unsupported prepared conversion schema version; re-prepare with Forge",
            );
        }
        if self.job_id == 0 {
            return invalid("prepared conversion job ID must be positive");
        }
        if self.conversion.reference_merge.is_some() != self.external_reference.is_some() {
            return invalid(
                "external reference path and prepared publication binding must both be present",
            );
        }
        self.source.validate()?;
        validate_digest(&self.catalog_sha256)?;
        if let Some(external) = &self.external_reference {
            external.validate()?;
            for binding in &external.streams {
                if self.source.streams.iter().any(|source| {
                    binding.instrument_id == source.instrument_id
                        && binding.instrument_namespace == source.instrument_namespace
                        && (binding.identity != source.identity
                            || binding.base_currency != source.base_currency
                            || binding.quote_currency != source.quote_currency)
                }) {
                    return invalid("conflicting prepared instrument metadata across publications");
                }
            }
        }
        if self.parquet.row_group_target_mb == 0
            || self.parquet.file_target_mb < self.parquet.row_group_target_mb
            || self
                .parquet
                .file_target_mb
                .checked_mul(1024 * 1024)
                .is_none()
            || !(1..=22).contains(&self.parquet.compression_level)
        {
            return invalid("invalid prepared conversion Parquet settings");
        }
        if !self.input_merge.is_absolute() {
            return invalid("conversion input_merge must be absolute");
        }

        if !self.output.dataset_path.is_absolute() {
            return invalid("conversion output dataset_path must be absolute");
        }

        if self.input_merge == self.output.dataset_path {
            return invalid("conversion input and output paths must differ");
        }

        if self.conversion.target_quote.is_empty()
            || !self
                .conversion
                .target_quote
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            return invalid("invalid conversion target currency");
        }

        if self.conversion.max_staleness_ns == 0 {
            return invalid("conversion max_staleness_ns must be positive");
        }

        if !(1..=4).contains(&self.conversion.max_hops) {
            return invalid("conversion max_hops must be between 1 and 4");
        }

        if self.resources.workers == 0
            || self.resources.memory_limit_mb == 0
            || self.resources.max_open_files == 0
        {
            return invalid("invalid conversion resource limits");
        }

        let mut unique = std::collections::HashSet::new();

        for reference in &self.conversion.references {
            if reference.exchange.trim().is_empty() || reference.symbol.trim().is_empty() {
                return invalid("conversion reference contains empty identity fields");
            }

            if !unique.insert(reference) {
                return invalid("duplicate conversion reference instrument");
            }
        }
        if let Some(reference_merge) = &self.conversion.reference_merge {
            if !reference_merge.is_absolute() {
                return invalid("conversion reference_merge must be absolute");
            }

            if reference_merge == &self.output.dataset_path {
                return invalid("conversion reference merge and output paths must differ");
            }
        }
        if let Some(kinds) = &self.conversion.instrument_kinds {
            if kinds.is_empty() {
                return invalid("conversion instrument_kinds must not be empty");
            }

            let mut unique = std::collections::HashSet::new();

            for kind in kinds {
                if !unique.insert(kind) {
                    return invalid("duplicate conversion instrument kind");
                }
            }
        }

        Ok(())
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
