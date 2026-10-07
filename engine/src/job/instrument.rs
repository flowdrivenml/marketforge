use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    Spot,
    Perpetual,
    Future,
    Option,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractKind {
    Linear,
    Inverse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentSpec {
    pub instrument_kind: InstrumentKind,

    pub contract_kind: Option<ContractKind>,

    pub tick_size: Decimal,

    pub contract_value: Option<Decimal>,
    pub contract_value_asset: Option<String>,
}
