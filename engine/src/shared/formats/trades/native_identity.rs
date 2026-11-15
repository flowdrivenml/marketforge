//! Native identity contracts belong to a profile, never to archive names or dates.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeIdScope {
    Global,
    InstrumentScoped,
    SourceLocal,
    Unreliable,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeIdentityContract {
    pub scope: TradeIdScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_domain: Option<String>,
}
impl TradeIdentityContract {
    pub fn validate(&self) -> std::result::Result<(), &'static str> {
        match (self.scope, self.reset_domain.as_deref()) {
            (TradeIdScope::InstrumentScoped, Some(domain)) if !domain.trim().is_empty() => Ok(()),
            (TradeIdScope::InstrumentScoped, _) => {
                Err("instrument_scoped trade identity requires an established reset_domain")
            }
            (_, None) => Ok(()),
            _ => Err("reset_domain requires instrument_scoped trade identity"),
        }
    }
}
