// src/live/infrastructure/websocket/heartbeat.rs

use std::time::Duration;

use crate::live::config::exchange::{HeartbeatConfig, HeartbeatStrategy};

use crate::live::infrastructure::websocket::transport::error::WebSocketTransportError;

/// Transport-level heartbeat policy.
///
/// A Ping carries a unique token. Only a Pong echoing
/// the token satisfies the pending heartbeat.
///
/// Exchange application heartbeats are configured and
/// executed separately.
#[derive(Debug, Clone, Copy)]
pub struct HeartbeatPolicy {
    pub interval: Duration,
    pub response_timeout: Duration,
    pub send_timeout: Duration,
}

impl HeartbeatPolicy {
    pub fn new(
        interval: Duration,
        response_timeout: Duration,
        send_timeout: Duration,
    ) -> Result<Self, WebSocketTransportError> {
        if interval.is_zero() || response_timeout.is_zero() || send_timeout.is_zero() {
            return Err(WebSocketTransportError::Configuration(
                "heartbeat durations must be positive".into(),
            ));
        }

        Ok(Self {
            interval,
            response_timeout,
            send_timeout,
        })
    }

    /// Compile only an explicitly configured client-initiated
    /// control-frame heartbeat.
    ///
    /// Server-initiated ping policies and exchange-native
    /// application heartbeats require their own strategies.
    pub fn from_config(config: &HeartbeatConfig) -> Result<Self, WebSocketTransportError> {
        if config.enabled == Some(false) {
            return Err(WebSocketTransportError::Configuration(
                "configured heartbeat is disabled".into(),
            ));
        }

        if config.strategy != HeartbeatStrategy::WebsocketControlFrames {
            return Err(WebSocketTransportError::Configuration(
                "application heartbeat strategy requires its protocol adapter".into(),
            ));
        }

        let interval = config.interval_seconds.ok_or_else(|| {
            WebSocketTransportError::Configuration(
                "client control-frame heartbeat requires interval_seconds".into(),
            )
        })?;

        let response_timeout = config
            .response_timeout_seconds
            .or(config.pong_deadline_seconds)
            .or(config.timeout_seconds)
            .ok_or_else(|| {
                WebSocketTransportError::Configuration(
                    "control-frame heartbeat requires a pong deadline".into(),
                )
            })?;

        Self::new(
            Duration::from_secs(interval),
            Duration::from_secs(response_timeout),
            Duration::from_secs(response_timeout),
        )
    }
}
