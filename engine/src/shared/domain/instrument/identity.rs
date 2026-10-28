use serde::{Deserialize, Serialize};

use crate::shared::domain::instrument::ContractKind;
use crate::shared::domain::instrument::InstrumentKind;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentIdentity {
    pub exchange: String,
    pub symbol: String,
    pub instrument_kind: InstrumentKind,

    #[serde(default)]
    pub contract_kind: Option<ContractKind>,
}
