//! Raw transport ports own network I/O, not exchange semantics or canonical IDs.
use super::{context::CallContext, routing::RoutingFuture};
use crate::live::model::{
    cursor::ConnectionGeneration, error::ControlError, operation::ConnectionId,
};
#[derive(Debug, Clone)]
pub struct TransportEndpoint {
    pub profile_revision: u64,
    pub endpoint_name: String,
}
#[derive(Debug, Clone)]
pub enum WebSocketFrame {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close { code: Option<u16> },
}
#[derive(Debug, Clone)]
pub struct ReceivedFrame {
    pub connection: ConnectionId,
    pub generation: ConnectionGeneration,
    pub receipt_sequence: u64,
    pub received_at_ns: i64,
    pub frame: WebSocketFrame,
}
pub trait WebSocketSessionPort: Send + Sync {
    fn send<'a>(
        &'a self,
        context: &'a CallContext,
        frame: WebSocketFrame,
    ) -> RoutingFuture<'a, Result<(), ControlError>>;
    fn receive<'a>(
        &'a self,
        context: &'a CallContext,
    ) -> RoutingFuture<'a, Result<ReceivedFrame, ControlError>>;
    fn close<'a>(&'a self, context: &'a CallContext)
    -> RoutingFuture<'a, Result<(), ControlError>>;
}
pub trait WebSocketConnectorPort: Send + Sync {
    fn connect<'a>(
        &'a self,
        context: &'a CallContext,
        descriptor: ConnectionDescriptor,
        generation: ConnectionGeneration,
    ) -> RoutingFuture<'a, Result<std::sync::Arc<dyn WebSocketSessionPort>, ControlError>>;
}
#[derive(Debug, Clone)]
pub struct RestRequest {
    pub endpoint: TransportEndpoint,
    pub parameters: Vec<(String, String)>,
    pub weight: u32,
    pub max_response_bytes: usize,
}
#[derive(Debug, Clone)]
pub struct RestResponse {
    pub status: u16,
    pub received_at_ns: i64,
    pub retry_after_ns: Option<u64>,
    pub body: Vec<u8>,
}
pub trait RestTransportPort: Send + Sync {
    fn execute<'a>(
        &'a self,
        context: &'a CallContext,
        request: RestRequest,
    ) -> RoutingFuture<'a, Result<RestResponse, ControlError>>;
}

/// Unix data sessions carry bounded protocol bytes. The publisher owns the data
/// protocol; the transport does not serialize canonical events.
pub trait DataSessionPort: Send + Sync {
    fn read<'a>(
        &'a self,
        context: &'a CallContext,
        max_bytes: usize,
    ) -> RoutingFuture<'a, Result<Vec<u8>, ControlError>>;
    fn write<'a>(
        &'a self,
        context: &'a CallContext,
        bytes: &'a [u8],
    ) -> RoutingFuture<'a, Result<(), ControlError>>;
    fn close<'a>(&'a self, context: &'a CallContext)
    -> RoutingFuture<'a, Result<(), ControlError>>;
}
pub trait DataListenerPort: Send + Sync {
    fn accept<'a>(
        &'a self,
        context: &'a CallContext,
    ) -> RoutingFuture<'a, Result<std::sync::Arc<dyn DataSessionPort>, ControlError>>;
}

/// References identify externally owned authentication/handshake settings.
/// Secret material is neither part of pool equality nor a diagnostic payload;
/// changing a referenced revision requires a different pool key/replacement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VersionedReference {
    pub name: String,
    pub revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionPoolKey {
    pub exchange: crate::live::model::identity::ExchangeId,
    pub endpoint: VersionedReference,
    pub protocol_version: String,
    pub authentication: Option<VersionedReference>,
    pub handshake: VersionedReference,
    pub heartbeat_compatibility: VersionedReference,
    pub connection_class: Option<String>,
}
#[derive(Debug, Clone)]
pub struct ConnectionDescriptor {
    pub id: ConnectionId,
    pub pool: ConnectionPoolKey,
    pub heartbeat: crate::live::model::acquisition::HeartbeatStrategy,
    pub max_frame_bytes: usize,
    pub max_inbound_events: usize,
    pub max_inbound_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestRatePolicy {
    pub max_in_flight: std::num::NonZeroU32,
    pub weight_per_window: std::num::NonZeroU32,
    pub window_ns: std::num::NonZeroU64,
}
