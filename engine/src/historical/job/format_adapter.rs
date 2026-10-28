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

        Self::new(headers, task.symbol.clone(), policy)
    }
}
