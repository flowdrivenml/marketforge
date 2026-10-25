use std::time::Duration;

use tokio_tungstenite::connect_async;
use url::Url;

use crate::live::infrastructure::limiter::{
    ConnectionRateLimiter,
    RateLimitManager,
    factory::CompiledWebSocketLimiters,
};

use super::{
    WebSocketConnection,
    WebSocketTransportError,
};

/// Establishes physical WebSocket connections.
///
/// Uses the existing global limiter for exchange-shared
/// connection-attempt budgets and creates one independent
/// outbound limiter for each physical connection.
pub struct WebSocketConnector;

impl WebSocketConnector {
    pub async fn connect(
        url: &str,
        connection_id: u64,
        compiled: &CompiledWebSocketLimiters,
        shared_limiter: &RateLimitManager,
        timeout: Duration,
    ) -> Result<WebSocketConnection, WebSocketTransportError> {
        if connection_id == 0 {
            return Err(WebSocketTransportError::Configuration(
                "connection ID must be positive".into(),
            ));
        }

        if timeout.is_zero() {
            return Err(WebSocketTransportError::Configuration(
                "connection timeout must be positive".into(),
            ));
        }

        let parsed = Url::parse(url).map_err(|error| {
            WebSocketTransportError::Configuration(
                error.to_string(),
            )
        })?;

        let secure = parsed.scheme() == "wss";

        let local_test = parsed.scheme() == "ws"
            && matches!(
                parsed.host_str(),
                Some("localhost" | "127.0.0.1" | "::1")
            );

        if !secure && !local_test {
            return Err(WebSocketTransportError::Configuration(
                "only wss:// or local-test ws:// URLs are allowed".into(),
            ));
        }

        if parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(WebSocketTransportError::Configuration(
                "invalid WebSocket endpoint identity".into(),
            ));
        }

        // Construct the connection-specific limiter before
        // attempting network access.
        let local_limiter = ConnectionRateLimiter::new(
            &compiled.connection,
            connection_id,
        )?;

        // One deadline covers shared limiter admission
        // and the complete WebSocket handshake.
        let socket = tokio::time::timeout(timeout, async {
            if let Some(operation) = compiled
                .shared
                .connection_attempt_operation()?
            {
                shared_limiter.acquire(&operation).await?;
            }

            let (socket, _) = connect_async(url).await?;

            Ok::<_, WebSocketTransportError>(socket)
        })
        .await
        .map_err(|_| WebSocketTransportError::ConnectTimeout)??;

        Ok(WebSocketConnection::new(
            connection_id,
            socket,
            local_limiter,
        ))
    }
}
