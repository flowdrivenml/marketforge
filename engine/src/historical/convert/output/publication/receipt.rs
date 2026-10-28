use crate::historical::convert::execution::PhysicalCombinedConversionMetrics;
use crate::historical::convert::execution::PhysicalDepthConversionMetrics;
use crate::historical::convert::output::DepthConversionValidationMetrics;
use crate::historical::convert::output::validation::combined::CombinedValidationMetrics;
use crate::historical::convert::output::validation::trades::ConversionValidationMetrics;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::BufWriter,
    path::Path,
};
use {
    crate::historical::convert::execution::PhysicalConversionMetrics,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};

pub const CONVERSION_RECEIPT_FILENAME: &str = "execution.json";
pub const CONVERSION_RECEIPT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionDepthStatistics {
    pub levels_read: u64,
    pub levels_converted: u64,
    pub levels_unavailable: u64,

    #[serde(default, skip_serializing_if = "is_zero")]
    pub levels_skipped: u64,
}

impl ConversionDepthStatistics {
    pub fn validate(&self) -> Result<()> {
        let accounted = self
            .levels_converted
            .checked_add(self.levels_unavailable)
            .and_then(|value| value.checked_add(self.levels_skipped))
            .ok_or_else(|| invalid_error("depth receipt level counter overflow"))?;

        if accounted != self.levels_read {
            return invalid(format!(
                "depth receipt level accounting mismatch: \
                 read={}, converted={}, unavailable={}, skipped={}, accounted={}",
                self.levels_read,
                self.levels_converted,
                self.levels_unavailable,
                self.levels_skipped,
                accounted,
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionExecutionReceipt {
    pub version: u32,
    pub job_id: u64,

    pub events_read: u64,
    pub events_written: u64,

    pub identity_events: u64,
    pub converted_events: u64,
    pub unavailable_events: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub skipped_events: u64,
    pub reference_updates: u64,

    pub files_written: u64,
    pub row_groups_written: u64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<ConversionDepthStatistics>,
}

impl ConversionExecutionReceipt {
    pub fn new(job_id: u64, metrics: &PhysicalConversionMetrics) -> Result<Self> {
        let receipt = Self {
            version: CONVERSION_RECEIPT_VERSION,
            job_id,

            events_read: metrics.events_read,
            events_written: metrics.events_written,

            identity_events: metrics.identity_events,
            converted_events: metrics.converted_events,
            unavailable_events: metrics.unavailable_events,

            reference_updates: metrics.reference_updates,

            files_written: metrics.files_written,
            row_groups_written: metrics.row_groups_written,
            depth: None,
            skipped_events: 0,
        };

        receipt.validate()?;

        Ok(receipt)
    }

    pub fn new_depth(job_id: u64, metrics: &PhysicalDepthConversionMetrics) -> Result<Self> {
        let receipt = Self {
            version: CONVERSION_RECEIPT_VERSION,
            job_id,
            skipped_events: 0,
            events_read: metrics.events_read,
            events_written: metrics.events_written,

            identity_events: metrics.identity_events,
            converted_events: metrics.converted_events,
            unavailable_events: metrics.unavailable_events,

            reference_updates: metrics.reference_updates,

            files_written: metrics.files_written,
            row_groups_written: metrics.row_groups_written,

            depth: Some(ConversionDepthStatistics {
                levels_read: metrics.levels_read,
                levels_converted: metrics.levels_converted,
                levels_unavailable: metrics.levels_unavailable,
                levels_skipped: 0,
            }),
        };

        receipt.validate()?;

        Ok(receipt)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != CONVERSION_RECEIPT_VERSION {
            return invalid("unsupported conversion receipt version");
        }

        if self.events_read != self.events_written {
            return invalid("conversion receipt event count mismatch");
        }

        let accounted = self
            .identity_events
            .checked_add(self.converted_events)
            .and_then(|value| value.checked_add(self.unavailable_events))
            .and_then(|value| value.checked_add(self.skipped_events))
            .ok_or_else(|| invalid_error("conversion receipt counter overflow"))?;

        if accounted != self.events_written {
            return invalid("conversion receipt status accounting mismatch");
        }

        if self.files_written == 0 || self.row_groups_written == 0 {
            return invalid("conversion receipt contains no physical output");
        }
        if let Some(depth) = &self.depth {
            depth.validate()?;
        }

        Ok(())
    }

    pub fn write(&self, directory: &Path) -> Result<()> {
        self.validate()?;

        let path = directory.join(CONVERSION_RECEIPT_FILENAME);

        let file = File::options()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                invalid_error(format!(
                    "cannot create conversion receipt {}: {error}",
                    path.display()
                ))
            })?;

        serde_json::to_writer_pretty(BufWriter::new(file), self)
            .map_err(|error| invalid_error(error.to_string()))
    }

    pub fn load(directory: &Path) -> Result<Self> {
        let path = directory.join(CONVERSION_RECEIPT_FILENAME);

        let bytes = fs::read(&path).map_err(|error| {
            invalid_error(format!(
                "cannot read conversion receipt {}: {error}",
                path.display()
            ))
        })?;

        let receipt: Self =
            serde_json::from_slice(&bytes).map_err(|error| invalid_error(error.to_string()))?;

        receipt.validate()?;

        Ok(receipt)
    }

    pub fn reconcile(&self, job_id: u64, validation: &ConversionValidationMetrics) -> Result<()> {
        self.validate()?;

        if self.job_id != job_id {
            return invalid(format!(
                "conversion receipt job mismatch: expected {}, received {}",
                job_id, self.job_id
            ));
        }

        if self.events_written != validation.events
            || self.identity_events != validation.identity
            || self.converted_events != validation.converted
            || self.unavailable_events != validation.unavailable
            || self.files_written != validation.files
            || self.row_groups_written != validation.row_groups
        {
            return invalid("conversion receipt does not match validated Parquet output");
        }
        if self.skipped_events != 0 {
            return invalid("unexpected skipped events in unfiltered conversion");
        }
        Ok(())
    }
    pub fn reconcile_depth(
        &self,
        job_id: u64,
        validation: &DepthConversionValidationMetrics,
    ) -> Result<()> {
        self.validate()?;

        if self.job_id != job_id {
            return invalid("depth conversion receipt job mismatch");
        }

        let depth = self
            .depth
            .as_ref()
            .ok_or_else(|| invalid_error("depth conversion receipt lacks level statistics"))?;

        if self.events_written != validation.events
            || self.identity_events != validation.identity
            || self.converted_events != validation.converted
            || self.unavailable_events != validation.unavailable
            || self.files_written != validation.files
            || self.row_groups_written != validation.row_groups
            || depth.levels_read != validation.levels
            || depth.levels_converted != validation.levels_converted
            || depth.levels_unavailable != validation.levels_unavailable
        {
            return invalid("depth receipt does not match validated Parquet output");
        }
        if depth.levels_skipped != 0 {
            return invalid("unexpected skipped depth levels");
        }
        Ok(())
    }
    pub fn new_combined(job_id: u64, metrics: &PhysicalCombinedConversionMetrics) -> Result<Self> {
        let receipt = Self {
            version: CONVERSION_RECEIPT_VERSION,
            job_id,

            events_read: metrics.events_read,
            events_written: metrics.events_written,

            identity_events: metrics.identity_events,
            converted_events: metrics.converted_events,
            unavailable_events: metrics.unavailable_events,

            reference_updates: metrics
                .internal_reference_updates
                .checked_add(metrics.external_reference_updates)
                .ok_or_else(|| invalid_error("combined reference counter overflow"))?,
            skipped_events: metrics.skipped_events,
            files_written: metrics.files_written,
            row_groups_written: metrics.row_groups_written,

            depth: Some(ConversionDepthStatistics {
                levels_read: metrics.depth_levels_read,
                levels_converted: metrics.depth_levels_converted,
                levels_unavailable: metrics.depth_levels_unavailable,
                levels_skipped: metrics.depth_levels_skipped,
            }),
        };

        receipt.validate()?;

        Ok(receipt)
    }
    pub fn reconcile_combined(
        &self,
        job_id: u64,
        validation: &CombinedValidationMetrics,
    ) -> Result<()> {
        self.validate()?;

        if self.job_id != job_id {
            return invalid("combined conversion receipt job mismatch");
        }

        let depth = self
            .depth
            .as_ref()
            .ok_or_else(|| invalid_error("combined conversion receipt lacks depth statistics"))?;

        if self.events_written != validation.events
            || self.identity_events != validation.identity
            || self.converted_events != validation.converted
            || self.unavailable_events != validation.unavailable
            || self.files_written != validation.files
            || self.row_groups_written != validation.row_groups
            || depth.levels_read != validation.depth_levels
            || depth.levels_converted != validation.depth_levels_converted
            || depth.levels_unavailable != validation.depth_levels_unavailable
            || self.skipped_events != validation.skipped
            || depth.levels_skipped != validation.depth_levels_skipped
        {
            return invalid("combined receipt does not match validated Parquet output");
        }

        Ok(())
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}
