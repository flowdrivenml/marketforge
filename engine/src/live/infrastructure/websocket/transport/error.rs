use thiserror::Error;

use tokio_tungstenite::tungstenite;

use crate::live::infrastructure::limiter::{
    ConnectionLimiterError, RateLimitManagerError, factory::WebSocketLimiterCompileError,
};

#[derive(Debug, Error)]
pub enum WebSocketTransportError {
    #[error("invalid WebSocket configuration: {0}")]
    Configuration(String),

    #[error("WebSocket connection deadline exceeded")]
    ConnectTimeout,

    #[error("WebSocket operation deadline exceeded")]
    OperationTimeout,

    #[error("WebSocket connection closed")]
    Closed,

    #[error(transparent)]
    Protocol(#[from] tungstenite::Error),

    #[error(transparent)]
    SharedLimiter(#[from] RateLimitManagerError),

    #[error(transparent)]
    ConnectionLimiter(#[from] ConnectionLimiterError),

    #[error(transparent)]
    LimiterConfiguration(#[from] WebSocketLimiterCompileError),
    #[error("WebSocket heartbeat response deadline exceeded")]
    HeartbeatTimeout,

    #[error("WebSocket inbound queue exhausted")]
    InboundBackpressure,

    #[error("WebSocket outbound queue is closed")]
    OutboundClosed,
    #[error("WebSocket application heartbeat response deadline exceeded")]
    ApplicationHeartbeatTimeout,
    #[error("WebSocket connection session failed: {0}")]
    Session(String),
}
