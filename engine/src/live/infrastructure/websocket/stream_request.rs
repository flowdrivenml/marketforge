use std::collections::BTreeMap;

use thiserror::Error;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StreamRequestError {
    #[error("invalid exchange profile")]
    InvalidProfile,

    #[error("invalid native symbol")]
    InvalidSymbol,

    #[error("invalid channel")]
    InvalidChannel,

    #[error("invalid subscription parameter name")]
    InvalidParameterName,

    #[error("invalid subscription parameter value")]
    InvalidParameterValue,

    #[error("duplicate subscription parameter: {0}")]
    DuplicateParameter(String),
}

// ============================================================
// DECLARATIVE STREAM REQUEST
// ============================================================

/// What a runtime consumer wants to receive.
///
/// Contains no exchange-native subscription messages,
/// JSON routing selectors, connection configuration,
/// rate limiters, or transport lifecycle state.
///
/// The WebSocketManager resolves this request using
/// already-loaded exchange configuration.
///
/// `native_symbol` must come from instrument metadata.
/// This type does not guess exchange symbol formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRequest {
    profile: String,
    native_symbol: String,
    channel: String,

    parameters: BTreeMap<String, String>,
}

impl StreamRequest {
    pub fn new(
        profile: impl Into<String>,
        native_symbol: impl Into<String>,
        channel: impl Into<String>,
    ) -> Result<Self, StreamRequestError> {
        let profile = profile.into();
        let native_symbol = native_symbol.into();
        let channel = channel.into();

        if !valid(&profile) {
            return Err(StreamRequestError::InvalidProfile);
        }

        if !valid(&native_symbol) {
            return Err(StreamRequestError::InvalidSymbol);
        }

        if !valid(&channel) {
            return Err(StreamRequestError::InvalidChannel);
        }

        Ok(Self {
            profile,
            native_symbol,
            channel,
            parameters: BTreeMap::new(),
        })
    }

    /// Add an explicitly selected subscription parameter.
    ///
    /// Examples include depth, interval, and native
    /// instrument type.
    ///
    /// Reserved symbol variables are generated internally
    /// by SubscriptionResolutionContext.
    pub fn with_parameter(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, StreamRequestError> {
        let name = name.into();
        let value = value.into();

        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            || matches!(name.as_str(), "symbol" | "symbol_lower" | "symbol_upper")
        {
            return Err(StreamRequestError::InvalidParameterName);
        }

        if !valid(&value) {
            return Err(StreamRequestError::InvalidParameterValue);
        }

        if self.parameters.contains_key(&name) {
            return Err(StreamRequestError::DuplicateParameter(name));
        }

        self.parameters.insert(name, value);

        Ok(self)
    }

    pub fn profile(&self) -> &str {
        &self.profile
    }

    pub fn native_symbol(&self) -> &str {
        &self.native_symbol
    }

    pub fn channel(&self) -> &str {
        &self.channel
    }

    pub fn parameters(&self) -> &BTreeMap<String, String> {
        &self.parameters
    }
}

fn valid(value: &str) -> bool {
    !value.is_empty() && value.trim() == value && !value.chars().any(char::is_whitespace)
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_trade_request_has_no_native_protocol_details() {
        let request = StreamRequest::new("binance_linear", "BTCUSDT", "trades").unwrap();

        assert_eq!(request.profile(), "binance_linear");
        assert_eq!(request.native_symbol(), "BTCUSDT");
        assert_eq!(request.channel(), "trades");

        assert!(request.parameters().is_empty());
    }

    #[test]
    fn supports_exchange_native_instrument_symbols() {
        let request = StreamRequest::new("okx_linear", "BTC-USDT-SWAP", "depth").unwrap();

        assert_eq!(request.native_symbol(), "BTC-USDT-SWAP");
    }

    #[test]
    fn supports_explicit_subscription_parameters() {
        let request = StreamRequest::new("binance_linear", "BTCUSDT", "depth")
            .unwrap()
            .with_parameter("depth", "20")
            .unwrap()
            .with_parameter("interval", "100ms")
            .unwrap();

        assert_eq!(request.parameters().get("depth"), Some(&"20".to_owned()));

        assert_eq!(
            request.parameters().get("interval"),
            Some(&"100ms".to_owned())
        );
    }

    #[test]
    fn rejects_invalid_source_identity() {
        assert!(matches!(
            StreamRequest::new("", "BTCUSDT", "trades"),
            Err(StreamRequestError::InvalidProfile)
        ));

        assert!(matches!(
            StreamRequest::new("binance_linear", "", "trades"),
            Err(StreamRequestError::InvalidSymbol)
        ));

        assert!(matches!(
            StreamRequest::new("binance_linear", "BTCUSDT", ""),
            Err(StreamRequestError::InvalidChannel)
        ));
    }

    #[test]
    fn rejects_reserved_symbol_parameters() {
        let request = StreamRequest::new("binance_linear", "BTCUSDT", "trades").unwrap();

        assert!(matches!(
            request.with_parameter("symbol_lower", "ethusdt"),
            Err(StreamRequestError::InvalidParameterName)
        ));
    }

    #[test]
    fn rejects_duplicate_parameters() {
        let request = StreamRequest::new("binance_linear", "BTCUSDT", "depth")
            .unwrap()
            .with_parameter("depth", "20")
            .unwrap();

        assert!(matches!(
            request.with_parameter("depth", "50"),
            Err(StreamRequestError::DuplicateParameter(_))
        ));
    }
}
