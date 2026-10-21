//! Typed strategy descriptors, not exchange-name dispatch or capability claims.
//! WP-12 supplies compiled implementations; activation requires verified facts.
use super::error::ErrorCode;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CapabilityEvidence {
    Verified {
        profile_revision: u64,
        fixture: String,
    },
    Unverified,
    Unsupported,
}
impl CapabilityEvidence {
    pub fn require(&self, revision: u64) -> Result<(), ErrorCode> {
        match self {
            Self::Verified {
                profile_revision,
                fixture,
            } if *profile_revision == revision && !fixture.trim().is_empty() => Ok(()),
            _ => Err(ErrorCode::UnsupportedCapability),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HeartbeatStrategy {
    TransportPing {
        interval_ns: u64,
        timeout_ns: u64,
    },
    Application {
        interval_ns: u64,
        timeout_ns: u64,
        ping: Vec<u8>,
        pong_correlation_required: bool,
    },
    ServerInitiated {
        timeout_ns: u64,
    },
    NoProtocolHeartbeat {
        idle_timeout_ns: u64,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SequenceStrategy {
    RangeBridge,
    PreviousAndCurrent,
    Monotonic,
    NoNativeSequence,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DepthSynchronization {
    RestSnapshotBridge {
        snapshot_endpoint: String,
        max_buffer_events: u32,
        max_buffer_bytes: u64,
    },
    ServerSnapshot {
        reset_starts_new_book: bool,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChecksumStrategy {
    NotProvided,
    Compiled { implementation: String, depth: u32 },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepthStrategyContract {
    pub synchronization: DepthSynchronization,
    pub sequence: SequenceStrategy,
    pub checksum: ChecksumStrategy,
    pub evidence: CapabilityEvidence,
}
impl DepthStrategyContract {
    pub fn validate_for_activation(&self, profile_revision: u64) -> Result<(), ErrorCode> {
        self.evidence.require(profile_revision)?;
        match &self.synchronization {
            DepthSynchronization::RestSnapshotBridge {
                snapshot_endpoint,
                max_buffer_events,
                max_buffer_bytes,
            } if snapshot_endpoint.trim().is_empty()
                || *max_buffer_events == 0
                || *max_buffer_bytes == 0
                || self.sequence != SequenceStrategy::RangeBridge =>
            {
                return Err(ErrorCode::InvalidInput);
            }
            _ => {}
        }
        if let ChecksumStrategy::Compiled {
            implementation,
            depth,
        } = &self.checksum
        {
            if implementation.trim().is_empty() || *depth == 0 {
                return Err(ErrorCode::InvalidInput);
            }
        }
        Ok(())
    }
}
impl HeartbeatStrategy {
    pub fn validate(&self, max_payload: usize) -> Result<(), ErrorCode> {
        let valid = match self {
            Self::TransportPing {
                interval_ns,
                timeout_ns,
            } => *interval_ns > 0 && *timeout_ns > 0,
            Self::Application {
                interval_ns,
                timeout_ns,
                ping,
                ..
            } => {
                *interval_ns > 0 && *timeout_ns > 0 && !ping.is_empty() && ping.len() <= max_payload
            }
            Self::ServerInitiated { timeout_ns } => *timeout_ns > 0,
            Self::NoProtocolHeartbeat { idle_timeout_ns } => *idle_timeout_ns > 0,
        };
        if valid {
            Ok(())
        } else {
            Err(ErrorCode::InvalidInput)
        }
    }
}
