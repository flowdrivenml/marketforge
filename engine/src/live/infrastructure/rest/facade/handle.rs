use crate::live::{
    infrastructure::rest::ExchangeRestService, stream::request::CreateSourceStreamRequest,
};

use super::StreamRestCapabilities;

// ============================================================
// STREAM REST HANDLE
// ============================================================

/// Immutable REST access context bound to one source stream.
///
/// Multiple handles may borrow the same ExchangeRestService.
///
/// All HTTP requests will therefore use the existing shared
/// transport, rate limiter, and configuration.
pub struct StreamRestHandle<'service, 'config> {
    service: &'service ExchangeRestService<'config>,

    profile: String,

    request: CreateSourceStreamRequest,

    capabilities: StreamRestCapabilities,
}

impl<'service, 'config> StreamRestHandle<'service, 'config> {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub(super) fn new(
        service: &'service ExchangeRestService<'config>,
        profile: String,
        request: CreateSourceStreamRequest,
        capabilities: StreamRestCapabilities,
    ) -> Self {
        Self {
            service,
            profile,
            request,
            capabilities,
        }
    }

    // ========================================================
    // IDENTITY
    // ========================================================

    pub fn profile(&self) -> &str {
        &self.profile
    }

    pub fn symbol(&self) -> &str {
        &self.request.symbol
    }

    pub fn instrument_id(&self) -> i64 {
        self.request.instrument_id
    }

    pub fn stream_request(&self) -> &CreateSourceStreamRequest {
        &self.request
    }

    // ========================================================
    // CAPABILITIES
    // ========================================================

    pub fn capabilities(&self) -> &StreamRestCapabilities {
        &self.capabilities
    }

    // ========================================================
    // SHARED SERVICE
    // ========================================================

    pub fn service(&self) -> &ExchangeRestService<'config> {
        self.service
    }
}
