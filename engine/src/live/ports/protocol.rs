//! Exchange profile decoding is separate from transport and canonical acceptance.
use super::transport::ReceivedFrame;
use crate::live::model::{
    configuration::ProfileReference,
    envelope::{EventProvenance, EventTiming, LivePayload},
    error::ControlError,
    identity::SourceStreamKey,
};
#[derive(Debug, Clone)]
pub struct AcquisitionPlan {
    pub profile: ProfileReference,
    pub source: SourceStreamKey,
    pub subscription_payload: Vec<u8>,
    pub unsubscribe_payload: Vec<u8>,
    pub max_message_bytes: usize,
    pub heartbeat: crate::live::model::acquisition::HeartbeatStrategy,
    pub depth: Option<crate::live::model::acquisition::DepthStrategyContract>,
    pub evidence: crate::live::model::acquisition::CapabilityEvidence,
}
#[derive(Debug, Clone)]
pub enum ProtocolMessage {
    Data {
        source: SourceStreamKey,
        payload: Vec<u8>,
        origin: crate::live::model::envelope::EventOrigin,
        received_at_ns: i64,
    },
    SubscriptionConfirmed {
        correlation: String,
    },
    SubscriptionRejected {
        correlation: String,
    },
    Heartbeat,
    ServiceNotice,
}
/// Still unaccepted: only EventAcceptor may assign the output cursor.
#[derive(Debug, Clone)]
pub struct NormalizedEvent {
    pub payload: LivePayload,
    pub provenance: EventProvenance,
    pub timing: EventTiming,
}
pub trait ExchangeProtocolPort: Send + Sync {
    fn compile(&self, source: &SourceStreamKey) -> Result<AcquisitionPlan, ControlError>;
    fn decode(&self, frame: &ReceivedFrame) -> Result<Vec<ProtocolMessage>, ControlError>;
}
pub trait NormalizationPort: Send + Sync {
    fn normalize(&self, message: &ProtocolMessage) -> Result<Vec<NormalizedEvent>, ControlError>;
}

/// Irreducible exchange rules are compiled strategies. Common owners don't
/// switch on venue names. Checksum input preserves native tokens and order.
pub trait ChecksumPort: Send + Sync {
    fn verify(
        &self,
        native_levels: &[u8],
        expected_native_checksum: &[u8],
    ) -> Result<(), ControlError>;
}
pub trait DepthStrategyPort: Send + Sync {
    fn contract(&self) -> &crate::live::model::acquisition::DepthStrategyContract;
    fn validate_progress(
        &self,
        previous: Option<&crate::formats::depth::DepthSourceEventMetadata>,
        next: &crate::formats::depth::DepthSourceEventMetadata,
    ) -> Result<(), ControlError>;
}

pub trait FrameRouterPort: Send + Sync {
    fn route<'a>(
        &'a self,
        context: &'a super::context::CallContext,
        frame: ReceivedFrame,
    ) -> super::routing::RoutingFuture<'a, Result<(), ControlError>>;
}
