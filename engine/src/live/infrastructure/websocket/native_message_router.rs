use std::collections::BTreeMap;

use serde_json::Value;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum NativeMessageRouterError {
    #[error("invalid stream identifier")]
    InvalidStreamId,

    #[error("stream already registered: {0}")]
    DuplicateStream(String),

    #[error("unknown stream: {0}")]
    UnknownStream(String),

    #[error("routing selector must contain at least one field")]
    EmptySelector,

    #[error("invalid JSON pointer: {0}")]
    InvalidPath(String),

    #[error("duplicate routing selector")]
    DuplicateSelector,

    #[error("malformed native JSON message")]
    InvalidJson,

    #[error("native frame cannot be classified as JSON")]
    UnsupportedFrame,

    #[error("native message matches multiple streams")]
    AmbiguousMessage,

    #[error("native message matches no registered stream")]
    UnknownMessage,
}

// ============================================================
// ROUTING SELECTOR
// ============================================================

/// Exact field/value predicates identifying one logical
/// source stream in an exchange-native JSON message.
///
/// All fields must match.
///
/// Paths use RFC 6901 JSON Pointer syntax.
///
/// Examples:
///
/// Binance combined stream:
///     /stream = "btcusdt@trade"
///
/// Bybit:
///     /topic = "publicTrade.BTCUSDT"
///
/// OKX:
///     /arg/channel = "books"
///     /arg/instId  = "BTC-USDT-SWAP"
///
/// Selectors are supplied by the manager from verified
/// exchange-native routing contracts.
///
/// The router itself contains no exchange-specific logic.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NativeMessageSelector {
    fields: BTreeMap<String, Value>,
}

impl NativeMessageSelector {
    pub fn new(
        fields: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<Self, NativeMessageRouterError> {
        let mut resolved = BTreeMap::new();

        for (path, expected) in fields {
            if !path.starts_with('/') || path == "/" {
                return Err(NativeMessageRouterError::InvalidPath(path));
            }

            // Reject malformed JSON Pointer escapes.
            let bytes = path.as_bytes();

            let mut index = 0;

            while index < bytes.len() {
                if bytes[index] == b'~' {
                    if index + 1 >= bytes.len() || !matches!(bytes[index + 1], b'0' | b'1') {
                        return Err(NativeMessageRouterError::InvalidPath(path));
                    }

                    index += 2;
                } else {
                    index += 1;
                }
            }

            if resolved.insert(path.clone(), expected).is_some() {
                return Err(NativeMessageRouterError::DuplicateSelector);
            }
        }

        if resolved.is_empty() {
            return Err(NativeMessageRouterError::EmptySelector);
        }

        Ok(Self { fields: resolved })
    }

    pub fn matches(&self, value: &Value) -> bool {
        self.fields
            .iter()
            .all(|(path, expected)| value.pointer(path) == Some(expected))
    }
}

// ============================================================
// ROUTING RESULT
// ============================================================

/// Identifies a logical stream without performing delivery.
///
/// The caller retains ownership of the original native frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeMessageDestination {
    pub stream_id: String,
}

// ============================================================
// NATIVE MESSAGE ROUTER
// ============================================================

/// Classifies exchange-native market-data messages.
///
/// Does not:
/// - Own physical connections.
/// - Own authoritative stream demand.
/// - Perform normalization.
/// - Deliver messages.
/// - Check market-data sequences.
/// - Manage connection recovery.
///
/// The manager owns one router per connection slot.
///
/// A router must contain only subscriptions assigned to
/// that slot.
///
/// Classification is fail-closed: unknown or ambiguous
/// messages never receive an arbitrary destination.
#[derive(Debug, Default)]
pub(crate) struct NativeMessageRouter {
    selectors: BTreeMap<String, NativeMessageSelector>,
}

impl NativeMessageRouter {
    pub fn new() -> Self {
        Self::default()
    }

    // ========================================================
    // REGISTER
    // ========================================================

    pub fn register(
        &mut self,
        stream_id: impl Into<String>,
        selector: NativeMessageSelector,
    ) -> Result<(), NativeMessageRouterError> {
        let stream_id = stream_id.into();

        if stream_id.is_empty() || stream_id.trim() != stream_id {
            return Err(NativeMessageRouterError::InvalidStreamId);
        }

        if self.selectors.contains_key(&stream_id) {
            return Err(NativeMessageRouterError::DuplicateStream(stream_id));
        }

        // Two streams may not declare identical identities.
        //
        // Overlapping but nonidentical selectors are detected
        // when classifying actual messages.
        if self.selectors.values().any(|other| other == &selector) {
            return Err(NativeMessageRouterError::DuplicateSelector);
        }

        self.selectors.insert(stream_id, selector);

        Ok(())
    }
    /// Atomically add routes without changing or removing any
    /// existing routing identity.
    ///
    /// Repeating an identical entry is idempotent.
    ///
    /// This operation is safe to perform before increasing
    /// subscription demand on an existing connection.
    pub fn extend_all(
        &mut self,
        entries: impl IntoIterator<Item = (String, NativeMessageSelector)>,
    ) -> Result<(), NativeMessageRouterError> {
        let mut replacement = Self {
            selectors: self.selectors.clone(),
        };

        for (stream_id, selector) in entries {
            if let Some(existing) = replacement.selectors.get(&stream_id) {
                if existing == &selector {
                    continue;
                }

                return Err(NativeMessageRouterError::DuplicateStream(stream_id));
            }

            replacement.register(stream_id, selector)?;
        }

        self.selectors = replacement.selectors;

        Ok(())
    }
    /// Atomically replace the complete native-message routing table.
    ///
    /// Every selector is validated before any existing routing
    /// state is modified.
    ///
    /// On failure, the previous table remains unchanged.
    ///
    /// The caller must provide the complete desired routing set,
    /// not merely the newly attached streams.
    ///
    /// This method performs no network I/O.
    pub fn replace_all(
        &mut self,
        entries: impl IntoIterator<Item = (String, NativeMessageSelector)>,
    ) -> Result<(), NativeMessageRouterError> {
        let mut replacement = NativeMessageRouter::new();

        for (stream_id, selector) in entries {
            replacement.register(stream_id, selector)?;
        }

        self.selectors = replacement.selectors;

        Ok(())
    }
    // ========================================================
    // UNREGISTER
    // ========================================================

    pub fn unregister(&mut self, stream_id: &str) -> Result<(), NativeMessageRouterError> {
        self.selectors
            .remove(stream_id)
            .ok_or_else(|| NativeMessageRouterError::UnknownStream(stream_id.to_owned()))?;

        Ok(())
    }

    // ========================================================
    // CLASSIFY
    // ========================================================

    pub fn classify(
        &self,
        message: &Message,
    ) -> Result<NativeMessageDestination, NativeMessageRouterError> {
        let native: Value = match message {
            Message::Text(text) => serde_json::from_str(text.as_str())
                .map_err(|_| NativeMessageRouterError::InvalidJson)?,

            Message::Binary(bytes) => {
                // Binary JSON is supported.
                //
                // Exchange-specific compressed or binary
                // protocols require a separate validated
                // decoding step before classification.
                serde_json::from_slice(bytes.as_ref())
                    .map_err(|_| NativeMessageRouterError::InvalidJson)?
            }

            _ => {
                return Err(NativeMessageRouterError::UnsupportedFrame);
            }
        };

        self.classify_value(&native)
    }

    pub fn classify_value(
        &self,
        native: &Value,
    ) -> Result<NativeMessageDestination, NativeMessageRouterError> {
        let mut matched = None;

        for (stream_id, selector) in &self.selectors {
            if !selector.matches(native) {
                continue;
            }

            if matched.is_some() {
                return Err(NativeMessageRouterError::AmbiguousMessage);
            }

            matched = Some(stream_id.clone());
        }

        let stream_id = matched.ok_or(NativeMessageRouterError::UnknownMessage)?;

        Ok(NativeMessageDestination { stream_id })
    }

    pub fn stream_count(&self) -> usize {
        self.selectors.len()
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn selector(fields: Vec<(&str, Value)>) -> NativeMessageSelector {
        NativeMessageSelector::new(
            fields
                .into_iter()
                .map(|(path, value)| (path.to_owned(), value)),
        )
        .unwrap()
    }

    #[test]
    fn routes_binance_combined_streams() {
        let mut router = NativeMessageRouter::new();

        router
            .register(
                "btc-trades",
                selector(vec![("/stream", json!("btcusdt@trade"))]),
            )
            .unwrap();

        router
            .register(
                "eth-trades",
                selector(vec![("/stream", json!("ethusdt@trade"))]),
            )
            .unwrap();

        let message = Message::Text(
            json!({
                "stream": "btcusdt@trade",
                "data": {
                    "p": "80000",
                    "q": "0.1"
                }
            })
            .to_string()
            .into(),
        );

        let destination = router.classify(&message).unwrap();

        assert_eq!(destination.stream_id, "btc-trades");
    }

    #[test]
    fn routes_bybit_topics() {
        let mut router = NativeMessageRouter::new();

        router
            .register(
                "btc-trades",
                selector(vec![("/topic", json!("publicTrade.BTCUSDT"))]),
            )
            .unwrap();

        let message = json!({
            "topic": "publicTrade.BTCUSDT",
            "data": []
        });

        let destination = router.classify_value(&message).unwrap();

        assert_eq!(destination.stream_id, "btc-trades");
    }

    #[test]
    fn routes_okx_using_channel_and_instrument() {
        let mut router = NativeMessageRouter::new();

        router
            .register(
                "btc-depth",
                selector(vec![
                    ("/arg/channel", json!("books")),
                    ("/arg/instId", json!("BTC-USDT-SWAP")),
                ]),
            )
            .unwrap();

        router
            .register(
                "eth-depth",
                selector(vec![
                    ("/arg/channel", json!("books")),
                    ("/arg/instId", json!("ETH-USDT-SWAP")),
                ]),
            )
            .unwrap();

        let message = json!({
            "arg": {
                "channel": "books",
                "instId": "ETH-USDT-SWAP"
            },
            "data": []
        });

        let destination = router.classify_value(&message).unwrap();

        assert_eq!(destination.stream_id, "eth-depth");
    }

    #[test]
    fn distinguishes_same_symbol_across_channels() {
        let mut router = NativeMessageRouter::new();

        router
            .register(
                "btc-trades",
                selector(vec![
                    ("/arg/channel", json!("trades")),
                    ("/arg/instId", json!("BTC-USDT")),
                ]),
            )
            .unwrap();

        router
            .register(
                "btc-depth",
                selector(vec![
                    ("/arg/channel", json!("books")),
                    ("/arg/instId", json!("BTC-USDT")),
                ]),
            )
            .unwrap();

        let message = json!({
            "arg": {
                "channel": "books",
                "instId": "BTC-USDT"
            }
        });

        assert_eq!(
            router.classify_value(&message).unwrap().stream_id,
            "btc-depth"
        );
    }

    #[test]
    fn unknown_message_is_not_guessed() {
        let mut router = NativeMessageRouter::new();

        router
            .register(
                "btc-trades",
                selector(vec![("/stream", json!("btcusdt@trade"))]),
            )
            .unwrap();

        let result = router.classify_value(&json!({
            "stream": "ethusdt@trade"
        }));

        assert_eq!(result, Err(NativeMessageRouterError::UnknownMessage));
    }

    #[test]
    fn overlapping_selectors_are_ambiguous() {
        let mut router = NativeMessageRouter::new();

        router
            .register("generic-btc", selector(vec![("/symbol", json!("BTCUSDT"))]))
            .unwrap();

        router
            .register(
                "btc-trades",
                selector(vec![
                    ("/symbol", json!("BTCUSDT")),
                    ("/channel", json!("trades")),
                ]),
            )
            .unwrap();

        let result = router.classify_value(&json!({
            "symbol": "BTCUSDT",
            "channel": "trades"
        }));

        assert_eq!(result, Err(NativeMessageRouterError::AmbiguousMessage));
    }

    #[test]
    fn duplicate_selector_is_rejected() {
        let mut router = NativeMessageRouter::new();

        let identity = selector(vec![("/topic", json!("publicTrade.BTCUSDT"))]);

        router.register("stream-a", identity.clone()).unwrap();

        assert!(matches!(
            router.register("stream-b", identity),
            Err(NativeMessageRouterError::DuplicateSelector)
        ));
    }

    #[test]
    fn unregister_removes_routing_identity() {
        let mut router = NativeMessageRouter::new();

        router
            .register("btc", selector(vec![("/stream", json!("btcusdt@trade"))]))
            .unwrap();

        router.unregister("btc").unwrap();

        assert_eq!(router.stream_count(), 0);

        assert_eq!(
            router.classify_value(&json!({
                "stream": "btcusdt@trade"
            })),
            Err(NativeMessageRouterError::UnknownMessage)
        );
    }

    #[test]
    fn malformed_json_is_rejected() {
        let router = NativeMessageRouter::new();

        let result = router.classify(&Message::Text("{invalid".into()));

        assert_eq!(result, Err(NativeMessageRouterError::InvalidJson));
    }

    #[test]
    fn invalid_selector_is_rejected() {
        assert!(matches!(
            NativeMessageSelector::new(vec![("topic".into(), json!("trades")),]),
            Err(NativeMessageRouterError::InvalidPath(_))
        ));
    }
    #[test]
    fn replace_all_updates_complete_routing_table() {
        let mut router = NativeMessageRouter::new();

        router
            .register("btc", selector(vec![("/stream", json!("btcusdt@trade"))]))
            .unwrap();

        router
            .replace_all(vec![
                (
                    "eth".to_owned(),
                    selector(vec![("/stream", json!("ethusdt@trade"))]),
                ),
                (
                    "sol".to_owned(),
                    selector(vec![("/stream", json!("solusdt@trade"))]),
                ),
            ])
            .unwrap();

        assert_eq!(router.stream_count(), 2);

        assert_eq!(
            router
                .classify_value(&json!({
                    "stream": "ethusdt@trade"
                }))
                .unwrap()
                .stream_id,
            "eth"
        );

        assert_eq!(
            router
                .classify_value(&json!({
                    "stream": "solusdt@trade"
                }))
                .unwrap()
                .stream_id,
            "sol"
        );

        // BTC was removed.
        assert_eq!(
            router.classify_value(&json!({
                "stream": "btcusdt@trade"
            })),
            Err(NativeMessageRouterError::UnknownMessage)
        );
    }
    #[test]
    fn failed_replace_all_preserves_previous_routes() {
        let mut router = NativeMessageRouter::new();

        router
            .register("btc", selector(vec![("/stream", json!("btcusdt@trade"))]))
            .unwrap();

        let duplicate = selector(vec![("/stream", json!("ethusdt@trade"))]);

        let result = router.replace_all(vec![
            ("eth-a".to_owned(), duplicate.clone()),
            ("eth-b".to_owned(), duplicate),
        ]);

        assert_eq!(result, Err(NativeMessageRouterError::DuplicateSelector));

        // The old routing table must remain intact.
        assert_eq!(router.stream_count(), 1);

        assert_eq!(
            router
                .classify_value(&json!({
                    "stream": "btcusdt@trade"
                }))
                .unwrap()
                .stream_id,
            "btc"
        );

        // Neither replacement entry was committed.
        assert_eq!(
            router.classify_value(&json!({
                "stream": "ethusdt@trade"
            })),
            Err(NativeMessageRouterError::UnknownMessage)
        );
    }
    #[test]
    fn replace_all_accepts_empty_routing_table() {
        let mut router = NativeMessageRouter::new();

        router
            .register("btc", selector(vec![("/stream", json!("btcusdt@trade"))]))
            .unwrap();

        router
            .replace_all(Vec::<(String, NativeMessageSelector)>::new())
            .unwrap();

        assert_eq!(router.stream_count(), 0);

        assert_eq!(
            router.classify_value(&json!({
                "stream": "btcusdt@trade"
            })),
            Err(NativeMessageRouterError::UnknownMessage)
        );
    }
    #[test]
    fn extend_all_is_atomic_and_preserves_existing_routes() {
        let mut router = NativeMessageRouter::new();

        router
            .register("btc", selector(vec![("/stream", json!("btcusdt@trade"))]))
            .unwrap();

        let eth = selector(vec![("/stream", json!("ethusdt@trade"))]);

        router
            .extend_all(vec![("eth".into(), eth.clone())])
            .unwrap();

        assert_eq!(router.stream_count(), 2);

        // Identical entries are idempotent.
        router
            .extend_all(vec![("eth".into(), eth.clone())])
            .unwrap();

        assert_eq!(router.stream_count(), 2);

        // Try adding SOL together with a conflicting ETH identity.
        let result = router.extend_all(vec![
            (
                "sol".into(),
                selector(vec![("/stream", json!("solusdt@trade"))]),
            ),
            (
                "eth".into(),
                selector(vec![("/stream", json!("wrong@trade"))]),
            ),
        ]);

        assert!(matches!(
            result,
            Err(NativeMessageRouterError::DuplicateStream(_))
        ));

        // Failed extension did not partially add SOL.
        assert_eq!(router.stream_count(), 2);

        assert_eq!(
            router
                .classify_value(&json!({"stream": "btcusdt@trade"}))
                .unwrap()
                .stream_id,
            "btc"
        );

        assert_eq!(
            router
                .classify_value(&json!({"stream": "ethusdt@trade"}))
                .unwrap()
                .stream_id,
            "eth"
        );

        assert_eq!(
            router.classify_value(&json!({"stream": "solusdt@trade"})),
            Err(NativeMessageRouterError::UnknownMessage)
        );
    }
}
