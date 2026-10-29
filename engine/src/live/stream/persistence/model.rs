use serde::{Deserialize, Serialize};

use crate::live::stream::request::{CreateDerivedStreamRequest, CreateSourceStreamRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(i16)]
pub enum StreamOrigin {
    Source = 1,
    Derived = 2,
}

impl StreamOrigin {
    pub const fn as_db(self) -> i16 {
        self as i16
    }

    pub fn from_db(value: i16) -> Result<Self, String> {
        match value {
            1 => Ok(Self::Source),
            2 => Ok(Self::Derived),
            _ => Err(format!("unknown stream origin: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(i16)]
pub enum ActivationPolicy {
    OnDemand = 1,
    AlwaysOn = 2,
}

impl ActivationPolicy {
    pub const fn as_db(self) -> i16 {
        self as i16
    }

    pub fn from_db(value: i16) -> Result<Self, String> {
        match value {
            1 => Ok(Self::OnDemand),
            2 => Ok(Self::AlwaysOn),
            _ => Err(format!("unknown activation policy: {value}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamDefinition {
    Source(CreateSourceStreamRequest),
    Derived(CreateDerivedStreamRequest),
}

impl StreamDefinition {
    pub fn origin(&self) -> StreamOrigin {
        match self {
            Self::Source(_) => StreamOrigin::Source,
            Self::Derived(_) => StreamOrigin::Derived,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Source(request) => request.validate(),
            Self::Derived(request) => request.validate(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesiredStreamState {
    pub enabled: bool,
    pub activation_policy: ActivationPolicy,
}

impl Default for DesiredStreamState {
    fn default() -> Self {
        Self {
            enabled: true,
            activation_policy: ActivationPolicy::OnDemand,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedStreamDefinition {
    pub stream_id: String,
    pub recording_stream_key: Option<i64>,
    pub definition: StreamDefinition,
    pub desired: DesiredStreamState,
    pub revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDependency {
    pub derived_stream_id: String,
    pub input_stream_id: String,
    pub input_position: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins_match_database_values() {
        assert_eq!(StreamOrigin::Source.as_db(), 1);
        assert_eq!(StreamOrigin::Derived.as_db(), 2);

        assert_eq!(StreamOrigin::from_db(1).unwrap(), StreamOrigin::Source);
        assert_eq!(StreamOrigin::from_db(2).unwrap(), StreamOrigin::Derived);
    }

    #[test]
    fn activation_policies_match_database_values() {
        assert_eq!(ActivationPolicy::OnDemand.as_db(), 1);
        assert_eq!(ActivationPolicy::AlwaysOn.as_db(), 2);

        assert_eq!(
            ActivationPolicy::from_db(1).unwrap(),
            ActivationPolicy::OnDemand
        );
        assert_eq!(
            ActivationPolicy::from_db(2).unwrap(),
            ActivationPolicy::AlwaysOn
        );
    }

    #[test]
    fn unknown_database_values_are_rejected() {
        assert!(StreamOrigin::from_db(3).is_err());
        assert!(ActivationPolicy::from_db(3).is_err());
    }

    #[test]
    fn default_streams_are_enabled_on_demand() {
        assert_eq!(
            DesiredStreamState::default(),
            DesiredStreamState {
                enabled: true,
                activation_policy: ActivationPolicy::OnDemand,
            }
        );
    }

    #[test]
    fn desired_state_serialization_round_trip() {
        let original = DesiredStreamState {
            enabled: false,
            activation_policy: ActivationPolicy::AlwaysOn,
        };

        let json = serde_json::to_string(&original).unwrap();

        let restored: DesiredStreamState = serde_json::from_str(&json).unwrap();

        assert_eq!(original, restored);
    }
}
