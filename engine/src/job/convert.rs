use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::MergeResources;
use super::instrument::InstrumentKind;
use crate::{
    error::{MarketForgeError, Result},
    instrument::InstrumentIdentity,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConvertJob {
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
