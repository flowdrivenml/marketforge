use std::sync::Arc;

use thiserror::Error;

use crate::live::{
    infrastructure::normalization::{NormalizerError, NormalizerFactory},
    metrics::SourceMetrics,
    stream::request::CreateSourceStreamRequest,
};

use super::instance::SourceStreamInstance;

#[derive(Debug, Error)]
pub enum SourceStreamFactoryError {
    #[error("invalid source stream identity: {0}")]
    InvalidIdentity(String),

    #[error("invalid source stream request: {0}")]
    InvalidRequest(String),

    #[error(transparent)]
    Normalization(#[from] NormalizerError),
}

/// Assembles one source stream from existing,
/// already-initialized infrastructure.
///
/// No WebSocket connection is opened here.
/// No recovery request is scheduled here.
/// No runtime lifecycle transition is performed here.
pub struct SourceStreamFactory {
    normalizers: Arc<NormalizerFactory>,
}

impl SourceStreamFactory {
    pub fn new(normalizers: Arc<NormalizerFactory>) -> Self {
        Self { normalizers }
    }

    pub fn create(
        &self,
        stream_id: impl Into<String>,
        request: CreateSourceStreamRequest,
    ) -> Result<SourceStreamInstance, SourceStreamFactoryError> {
        let stream_id = stream_id.into();

        if stream_id.is_empty()
            || stream_id.trim() != stream_id
            || stream_id.chars().any(char::is_control)
        {
            return Err(SourceStreamFactoryError::InvalidIdentity(stream_id));
        }

        request
            .validate()
            .map_err(SourceStreamFactoryError::InvalidRequest)?;

        // Reuse the existing compiled normalization facade.
        let normalizer = self.normalizers.create(stream_id.clone(), &request)?;

        // Defensive identity verification.
        if normalizer.stream_id() != stream_id {
            return Err(SourceStreamFactoryError::InvalidIdentity(
                "normalizer stream identity mismatch".into(),
            ));
        }

        // Source metrics are owned by this logical instance.
        //
        // The central metrics collector may inspect them
        // through the shared Arc.
        let metrics = Arc::new(SourceMetrics::new());

        Ok(SourceStreamInstance::new(
            stream_id, request, normalizer, metrics,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::{
        config::{ExchangeConfigStore, FormatConfigStore},
        infrastructure::normalization::NormalizerLimits,
        stream::request::SourceChannel,
    };

    use serde_json::json;
    use std::path::Path;

    fn factory() -> SourceStreamFactory {
        let root = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../data/.config/live"));

        let exchanges = Arc::new(ExchangeConfigStore::load_dir(root.join("exchanges")).unwrap());

        let formats = Arc::new(FormatConfigStore::load_dir(root.join("formats")).unwrap());

        let normalizers =
            NormalizerFactory::new(exchanges, formats, NormalizerLimits::default()).unwrap();

        SourceStreamFactory::new(Arc::new(normalizers))
    }

    fn request(channel: SourceChannel) -> CreateSourceStreamRequest {
        let mut request: CreateSourceStreamRequest = serde_json::from_value(json!({
            "instrument_id": 15,
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "market_category": "spot",
            "instrument_type": "spot",
            "base_asset": "BTC",
            "quote_asset": "USDT",
            "settlement_asset": null,
            "quantity_type": "base",
            "contract_value": null,
            "contract_value_asset": null,
            "channel": "trades"
        }))
        .unwrap();

        request.channel = channel;
        request
    }

    #[test]
    fn creates_trade_source() {
        let instance = factory()
            .create("binance-btc-trades", request(SourceChannel::Trades))
            .unwrap();

        assert_eq!(instance.stream_id(), "binance-btc-trades");

        assert_eq!(instance.request().channel, SourceChannel::Trades);

        assert_eq!(instance.normalizer().stream_id(), instance.stream_id());

        assert_eq!(instance.metrics().messages_received_total(), 0);
    }

    #[test]
    fn creates_depth_source() {
        let instance = factory()
            .create("binance-btc-depth", request(SourceChannel::Depth))
            .unwrap();

        assert_eq!(instance.request().channel, SourceChannel::Depth);

        assert_eq!(instance.normalizer().stream_id(), "binance-btc-depth");
    }

    #[test]
    fn rejects_invalid_stream_identity() {
        let result = factory().create(" invalid ", request(SourceChannel::Trades));

        assert!(matches!(
            result,
            Err(SourceStreamFactoryError::InvalidIdentity(_))
        ));
    }

    #[test]
    fn rejects_invalid_request() {
        let mut request = request(SourceChannel::Trades);
        request.instrument_id = 0;

        let result = factory().create("invalid", request);

        assert!(matches!(
            result,
            Err(SourceStreamFactoryError::InvalidRequest(_))
        ));
    }

    #[test]
    fn instances_have_independent_metrics() {
        let factory = factory();

        let first = factory
            .create("stream-a", request(SourceChannel::Trades))
            .unwrap();

        let second = factory
            .create("stream-b", request(SourceChannel::Trades))
            .unwrap();

        first.metrics().record_message_received(100);

        assert_eq!(first.metrics().messages_received_total(), 1);

        assert_eq!(second.metrics().messages_received_total(), 0);
    }

    #[test]
    fn metrics_handle_shares_instance_counters() {
        let instance = factory()
            .create("stream-a", request(SourceChannel::Trades))
            .unwrap();

        let metrics = instance.metrics_handle();

        metrics.record_message_received(100);

        assert_eq!(instance.metrics().messages_received_total(), 1);
    }
}
