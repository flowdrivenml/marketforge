use super::catalog::ResolvedConversionCatalog;
use crate::historical::job::ConvertJob;
pub use crate::shared::conversion::planner::{
    ConversionPlan, DEFAULT_MAX_PATHS, PlannedConversionStream,
};
use crate::shared::{domain::instrument::Currency, error::Result};

/// Validate the Historical protocol and translate it into transport-independent inputs.
pub fn plan_conversion(
    job: &ConvertJob,
    resolved: &ResolvedConversionCatalog,
) -> Result<ConversionPlan> {
    job.validate()?;
    crate::shared::conversion::planner::plan_conversion(
        Currency::new(&job.conversion.target_quote)?,
        &resolved.sources,
        &resolved.references,
        |source| {
            job.conversion
                .selects_instrument_kind(source.identity.instrument_kind)
        },
        job.conversion.max_hops,
    )
}
