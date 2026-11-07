use super::WorkTask;
use crate::shared::formats::trades::{IdentityPolicy, resolve_identity_policy};
pub fn identity_policy(task: &WorkTask) -> IdentityPolicy {
    resolve_identity_policy(task.exchange, task.instrument.instrument_kind)
}

#[cfg(feature = "historical")]
impl crate::shared::formats::trades::InstrumentMatcher {
    pub fn from_task(
        headers: &csv::ByteRecord,
        task: &WorkTask,
    ) -> crate::shared::error::Result<Self> {
        let policy = identity_policy(task);

        let matcher = Self::new(headers, task.symbol.clone(), policy)?;
        if let Some(symbols) = task.raw_schema["marketforge_source"]["family_symbols"].as_array() {
            let symbols = symbols
                .iter()
                .map(|v| {
                    v.as_str().map(str::to_owned).ok_or_else(|| {
                        crate::shared::error::MarketForgeError::InvalidConfiguration(
                            "invalid family symbol mapping".into(),
                        )
                    })
                })
                .collect::<crate::shared::error::Result<_>>()?;
            Ok(matcher.with_authoritative_symbols(symbols))
        } else {
            Ok(matcher)
        }
    }
}
