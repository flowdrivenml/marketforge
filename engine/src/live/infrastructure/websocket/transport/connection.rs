use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::live::infrastructure::limiter::{ConnectionRateLimiter, RequestPriority};

use super::error::WebSocketTransportError;

/// One physical WebSocket connection.
///
/// Exactly one owner controls socket reads and writes.
/// The connection owns its independent outbound limiter.
///
/// Heartbeat supervision and reconnection are separate layers.
pub struct WebSocketConnection {
    connection_id: u64,
    pub(in crate::live::infrastructure::websocket) socket:
        WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub(in crate::live::infrastructure::websocket) limiter: ConnectionRateLimiter,
}

impl WebSocketConnection {
    pub(in crate::live::infrastructure::websocket) fn new(
        connection_id: u64,
        socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
        limiter: ConnectionRateLimiter,
    ) -> Self {
        Self {
            connection_id,
            socket,
            limiter,
        }
    }

    pub fn connection_id(&self) -> u64 {
        self.connection_id
    }

    pub fn limiter(&self) -> &ConnectionRateLimiter {
        &self.limiter
    }

    /// Sends one ordinary application text message.
    ///
    /// Limiter admission and socket transmission share
    /// one operation deadline.
    pub async fn send_text(
        &mut self,
        text: impl Into<String>,
        timeout: Duration,
    ) -> Result<(), WebSocketTransportError> {
        if timeout.is_zero() {
            return Err(WebSocketTransportError::Configuration(
                "send timeout must be positive".into(),
            ));
        }

        let text = text.into();

        tokio::time::timeout(timeout, async {
            self.limiter
                .acquire_outbound(RequestPriority::Normal)
                .await?;

            self.socket.send(Message::Text(text.into())).await?;

            Ok::<(), WebSocketTransportError>(())
        })
        .await
        .map_err(|_| WebSocketTransportError::OperationTimeout)?
    }

    /// Sends one subscription-control message.
    ///
    /// The subscription limiter already includes the
    /// ordinary outbound-message budget when applicable.
    /// Never charge both operations for the same message.
    pub async fn send_subscription(
        &mut self,
        text: impl Into<String>,
        timeout: Duration,
    ) -> Result<(), WebSocketTransportError> {
        if timeout.is_zero() {
            return Err(WebSocketTransportError::Configuration(
                "subscription timeout must be positive".into(),
            ));
        }

        let text = text.into();

        tokio::time::timeout(timeout, async {
            self.limiter
                .acquire_subscription(RequestPriority::Normal)
                .await?;

            self.socket.send(Message::Text(text.into())).await?;

            Ok::<(), WebSocketTransportError>(())
        })
        .await
        .map_err(|_| WebSocketTransportError::OperationTimeout)?
    }

    /// Receives one WebSocket message.
    ///
    /// W1 returns protocol frames without interpreting
    /// exchange-specific application messages.
    pub async fn receive(&mut self, timeout: Duration) -> Result<Message, WebSocketTransportError> {
        if timeout.is_zero() {
            return Err(WebSocketTransportError::Configuration(
                "receive timeout must be positive".into(),
            ));
        }

        tokio::time::timeout(timeout, async {
            match self.socket.next().await {
                Some(Ok(message)) => Ok(message),
                Some(Err(error)) => Err(error.into()),
                None => Err(WebSocketTransportError::Closed),
            }
        })
        .await
        .map_err(|_| WebSocketTransportError::OperationTimeout)?
    }

    /// Begins a graceful WebSocket close handshake.
    ///
    /// W2/W3 will own the complete supervised shutdown
    /// lifecycle and connection-state transitions.
    pub async fn close(&mut self, timeout: Duration) -> Result<(), WebSocketTransportError> {
        if timeout.is_zero() {
            return Err(WebSocketTransportError::Configuration(
                "close timeout must be positive".into(),
            ));
        }

        tokio::time::timeout(timeout, self.socket.close(None))
            .await
            .map_err(|_| WebSocketTransportError::OperationTimeout)??;

        Ok(())
    }
}
