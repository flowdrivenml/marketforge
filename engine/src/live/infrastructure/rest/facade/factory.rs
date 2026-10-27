use super::{StreamRestCapabilities, handle::StreamRestHandle};

use crate::live::{
    infrastructure::rest::ExchangeRestService, stream::request::CreateSourceStreamRequest,
};

// ============================================================
// REST FACADE FACTORY
// ============================================================

/// Constructs stream-bound REST handles.
///
/// The factory borrows the existing REST service.
///
/// It does not construct:
/// - HTTP clients.
/// - Rate limiters.
/// - Exchange configurations.
/// - Background tasks.
pub struct RestFacadeFactory<'service, 'config> {
    service: &'service ExchangeRestService<'config>,
}

impl<'service, 'config> RestFacadeFactory<'service, 'config> {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(service: &'service ExchangeRestService<'config>) -> Self {
        Self { service }
    }

    // ========================================================
    // STREAM HANDLE CREATION
    // ========================================================

    pub fn create(
        &self,
        request: &CreateSourceStreamRequest,
    ) -> Result<StreamRestHandle<'service, 'config>, RestFacadeError> {
        request
            .validate()
            .map_err(RestFacadeError::InvalidStreamRequest)?;

        let profile = format!(
            "{}_{}",
            request.exchange.as_str(),
            request.market_category.as_str(),
        );

        let config = self
            .service
            .configs()
            .get(&profile)
            .ok_or_else(|| RestFacadeError::UnknownProfile(profile.clone()))?;

        // Defensive identity verification.
        //
        // A profile must not be bound to a different exchange
        // or market category.
        if config.exchange.id != request.exchange
            || config.exchange.market != request.market_category
        {
            return Err(RestFacadeError::ProfileIdentityMismatch { profile });
        }

        // Compile REST operations from the actual configuration.
        //
        // No exchange-specific operation table is hardcoded.
        let capabilities = StreamRestCapabilities::compile(config);

        Ok(StreamRestHandle::new(
            self.service,
            profile,
            request.clone(),
            capabilities,
        ))
    }

    // ========================================================
    // SERVICE ACCESS
    // ========================================================

    pub fn service(&self) -> &ExchangeRestService<'config> {
        self.service
    }
}

// ============================================================
// FACADE ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestFacadeError {
    InvalidStreamRequest(String),

    UnknownProfile(String),

    ProfileIdentityMismatch {
        profile: String,
    },

    UnsupportedOperation {
        profile: String,
        operation: &'static str,
    },

    InvalidParameter {
        name: &'static str,
        reason: String,
    },
}

impl std::fmt::Display for RestFacadeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidStreamRequest(reason) => {
                write!(formatter, "invalid source-stream request: {reason}")
            }

            Self::UnknownProfile(profile) => {
                write!(formatter, "unknown REST exchange profile: {profile}")
            }

            Self::ProfileIdentityMismatch { profile } => {
                write!(formatter, "REST profile identity mismatch: {profile}")
            }

            Self::UnsupportedOperation { profile, operation } => {
                write!(
                    formatter,
                    "REST operation '{operation}' is unsupported for '{profile}'"
                )
            }

            Self::InvalidParameter { name, reason } => {
                write!(formatter, "invalid REST parameter '{name}': {reason}")
            }
        }
    }
}

impl std::error::Error for RestFacadeError {}
