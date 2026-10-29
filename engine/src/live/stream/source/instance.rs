use std::sync::Arc;

use crate::live::{
    infrastructure::normalization::Normalizer, metrics::SourceMetrics,
    stream::request::CreateSourceStreamRequest,
};

/// Prepared source-stream processing resources.
///
/// This is not yet an attached or running source.
///
/// Runtime lifecycle authority belongs to
/// StreamRuntimeRegistry.
///
/// Physical WebSocket ownership belongs to
/// WebSocketManager.
///
/// Integrity and publication are installed during
/// the following implementation stage.
pub struct SourceStreamInstance {
    stream_id: String,
    request: CreateSourceStreamRequest,
    normalizer: Normalizer,
    metrics: Arc<SourceMetrics>,
}

impl SourceStreamInstance {
    pub(crate) fn new(
        stream_id: String,
        request: CreateSourceStreamRequest,
        normalizer: Normalizer,
        metrics: Arc<SourceMetrics>,
    ) -> Self {
        Self {
            stream_id,
            request,
            normalizer,
            metrics,
        }
    }

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn request(&self) -> &CreateSourceStreamRequest {
        &self.request
    }

    pub fn normalizer(&self) -> &Normalizer {
        &self.normalizer
    }

    pub fn metrics(&self) -> &SourceMetrics {
        &self.metrics
    }

    pub fn metrics_handle(&self) -> Arc<SourceMetrics> {
        Arc::clone(&self.metrics)
    }
}
