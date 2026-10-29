use serde_json::Value;
use thiserror::Error;

use crate::live::infrastructure::websocket::subscriptions::restoration::DesiredSubscription;

use crate::live::infrastructure::websocket::routing::message_router::{
    NativeMessageRouterError, NativeMessageSelector,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum StreamSourceError {
    #[error("invalid exchange profile")]
    InvalidProfile,

    #[error(transparent)]
    Routing(#[from] NativeMessageRouterError),
}

// ============================================================
// SOURCE STREAM
// ============================================================

/// Transport-facing description of one source stream.
///
/// The authoritative stream registry supplies this value.
/// Stream consumers do not construct WebSocket connections.
///
/// Profile selects an already validated ExchangeConfig.
/// Subscription uses the existing MarketForge demand format.
///
/// Routing predicates are derived from verified
/// exchange-native message contracts upstream.
///
/// No connection, limiter, heartbeat, or recovery state
/// is carried by this object.
#[derive(Debug, Clone)]
pub struct StreamSource {
    profile: String,

    subscription: DesiredSubscription,

    routing: NativeMessageSelector,
}

impl StreamSource {
    pub fn new(
        profile: impl Into<String>,
        subscription: DesiredSubscription,
        routing_fields: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<Self, StreamSourceError> {
        let profile = profile.into();

        if profile.is_empty() || profile.trim() != profile {
            return Err(StreamSourceError::InvalidProfile);
        }

        let routing = NativeMessageSelector::new(routing_fields)?;

        Ok(Self {
            profile,
            subscription,
            routing,
        })
    }

    pub(crate) fn with_payload(
        mut self,
        payload: crate::live::infrastructure::websocket::routing::message_router::NativePayloadSelector,
    ) -> Result<Self, StreamSourceError> {
        self.routing = self.routing.with_payload(payload)?;
        Ok(self)
    }

    pub fn profile(&self) -> &str {
        &self.profile
    }

    pub fn stream_id(&self) -> &str {
        &self.subscription.stream_id
    }

    pub fn subscription(&self) -> &DesiredSubscription {
        &self.subscription
    }

    pub(crate) fn routing(&self) -> &NativeMessageSelector {
        &self.routing
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn source() -> StreamSource {
        let subscription =
            DesiredSubscription::new("binance-linear-btc-trades", "trades", "BTCUSDT").unwrap();

        StreamSource::new(
            "binance_linear",
            subscription,
            vec![("/stream".to_owned(), json!("btcusdt@trade"))],
        )
        .unwrap()
    }

    #[test]
    fn preserves_existing_subscription_identity() {
        let source = source();

        assert_eq!(source.profile(), "binance_linear");

        assert_eq!(source.stream_id(), "binance-linear-btc-trades");

        assert_eq!(source.subscription().channel_name, "trades");

        assert_eq!(source.subscription().native_symbol, "BTCUSDT");
    }

    #[test]
    fn routing_matches_the_expected_native_stream() {
        let source = source();

        assert!(source.routing().matches(&json!({
            "stream": "btcusdt@trade",
            "data": {}
        })));

        assert!(!source.routing().matches(&json!({
            "stream": "ethusdt@trade",
            "data": {}
        })));
    }

    #[test]
    fn rejects_empty_profile() {
        let subscription = DesiredSubscription::new("btc", "trades", "BTCUSDT").unwrap();

        let result = StreamSource::new(
            "",
            subscription,
            vec![("/stream".into(), json!("btcusdt@trade"))],
        );

        assert!(matches!(result, Err(StreamSourceError::InvalidProfile)));
    }

    #[test]
    fn rejects_empty_routing_selector() {
        let subscription = DesiredSubscription::new("btc", "trades", "BTCUSDT").unwrap();

        let result = StreamSource::new(
            "binance_linear",
            subscription,
            Vec::<(String, Value)>::new(),
        );

        assert!(matches!(
            result,
            Err(StreamSourceError::Routing(
                NativeMessageRouterError::EmptySelector
            ))
        ));
    }
}
