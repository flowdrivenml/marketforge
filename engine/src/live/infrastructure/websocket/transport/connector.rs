use std::time::Duration;

use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderMap},
};
use url::Url;

use crate::live::infrastructure::limiter::{
    ConnectionRateLimiter, RateLimitManager, factory::CompiledWebSocketLimiters,
};

use super::connection::WebSocketConnection;
use super::error::WebSocketTransportError;

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
        Self::connect_with_headers(
            url,
            &HeaderMap::new(),
            connection_id,
            compiled,
            shared_limiter,
            timeout,
        )
        .await
    }

    pub(crate) async fn connect_with_headers(
        url: &str,
        headers: &HeaderMap,
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

        let parsed = Url::parse(url)
            .map_err(|error| WebSocketTransportError::Configuration(error.to_string()))?;

        let secure = parsed.scheme() == "wss";

        let local_test = parsed.scheme() == "ws"
            && matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "::1"));

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

        // Tungstenite generates the upgrade fields and a fresh key for each attempt.
        let mut request = url.into_client_request().map_err(|_| {
            WebSocketTransportError::Configuration("invalid WebSocket upgrade request URL".into())
        })?;
        request.headers_mut().extend(headers.clone());

        // Construct the connection-specific limiter before
        // attempting network access.
        let local_limiter = ConnectionRateLimiter::new(&compiled.connection, connection_id)?;

        // One deadline covers shared limiter admission
        // and the complete WebSocket handshake.
        let socket = tokio::time::timeout(timeout, async {
            if let Some(operation) = compiled.shared.connection_attempt_operation()? {
                shared_limiter.acquire(&operation).await?;
            }

            let (socket, _) = connect_async(request).await?;

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

#[cfg(test)]
mod tests {
    use crate::live::config::exchange::WebSocketConfig;

    #[test]
    fn compiles_public_headers_without_case_sensitive_lookup() {
        let websocket: WebSocketConfig =
            toml::from_str("[headers]\nX-Gate-Size-Decimal = '1'").unwrap();
        assert_eq!(
            websocket.handshake_headers().unwrap()["x-gate-size-decimal"],
            "1"
        );
    }

    #[test]
    fn rejects_invalid_or_reserved_upgrade_headers_without_disclosing_values() {
        for (name, value) in [
            ("bad name", "sensitive-value"),
            ("x-test", "sensitive-value\r\nX-Injected: yes"),
            ("x-test", "sensitive-value\x00"),
            ("x-test", "sensitive-value\x7f"),
            ("Host", "sensitive-value"),
            ("ConneCtion", "sensitive-value"),
            ("upgrade", "sensitive-value"),
            ("SEC-WebSocket-Key", "sensitive-value"),
            ("Sec-WebSocket-Version", "sensitive-value"),
            ("Sec-WebSocket-Protocol", "sensitive-value"),
            ("Sec-WebSocket-Extensions", "sensitive-value"),
            ("Content-Length", "sensitive-value"),
            ("Transfer-Encoding", "sensitive-value"),
            ("Authorization", "sensitive-value"),
            ("Proxy-Authorization", "sensitive-value"),
            ("Cookie", "sensitive-value"),
        ] {
            let mut websocket: WebSocketConfig = toml::from_str("").unwrap();
            websocket.headers.insert(name.to_owned(), value.to_owned());
            let error = websocket.handshake_headers().unwrap_err();
            assert!(error.contains("websocket.headers"), "{name}: {error}");
            assert!(!error.contains("sensitive-value"), "{name}: {error}");
        }
    }

    #[test]
    fn rejects_case_insensitive_duplicates() {
        let websocket: WebSocketConfig =
            toml::from_str("[headers]\nX-Example = '1'\nx-example = '2'").unwrap();
        assert!(
            websocket
                .handshake_headers()
                .unwrap_err()
                .contains("duplicate")
        );
    }
}
