use crate::live::{
    config::{exchange::ExchangeConfig, format::DepthFormatConfig},
    integrity::{
        BoundDepthIngress, BoundTradeIngress, CompiledDepthContinuityPlan,
        CompiledTradeIntegrityPlan, DepthBufferLimits, DepthResourceLimits, DepthSnapshotSource,
        DepthSourceBinding, TradeIdentityLimits, TradeSourceBinding,
    },
};

use super::handle::StreamIntegrityHandle;

// ============================================================
// DEPTH CONFIGURATION
// ============================================================

/// Immutable inputs required to construct one depth
/// integrity handle.
///
/// Plans and bindings are already compiled/resolved
/// by upstream configuration and stream infrastructure.
pub struct CreateDepthIntegrity<'a> {
    pub binding: DepthSourceBinding,
    pub plan: CompiledDepthContinuityPlan,
    pub buffer_limits: DepthBufferLimits,
    pub resource_limits: DepthResourceLimits,

    /// Required only for REST-synchronized depth.
    ///
    /// This configures snapshot admissibility.
    /// It does not create an HTTP client.
    pub rest_endpoint: Option<RestDepthEndpointConfig<'a>>,
}

/// Existing exchange configuration used to validate
/// the snapshot endpoint.
#[derive(Clone, Copy)]
pub struct RestDepthEndpointConfig<'a> {
    pub exchange: &'a ExchangeConfig,
    pub format: &'a DepthFormatConfig,
}

// ============================================================
// TRADE CONFIGURATION
// ============================================================

/// Immutable inputs required to construct one trade
/// integrity handle.
pub struct CreateTradeIntegrity {
    pub binding: TradeSourceBinding,
    pub plan: CompiledTradeIntegrityPlan,
    pub identity_limits: TradeIdentityLimits,
}

// ============================================================
// FACTORY ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityFactoryError {
    MissingRestEndpoint,

    UnexpectedRestEndpoint,

    InvalidDepthConfiguration(String),

    InvalidTradeConfiguration(String),

    InvalidRestEndpoint(String),
}

// ============================================================
// FACTORY
// ============================================================

/// Constructs source-bound Integrity handles.
///
/// This factory does not own:
/// - WebSocket connections.
/// - REST services.
/// - Recovery workers.
/// - Publication infrastructure.
///
/// It only assembles the existing stateful integrity
/// components.
pub struct IntegrityFacadeFactory;

impl IntegrityFacadeFactory {
    // ========================================================
    // DEPTH
    // ========================================================

    pub fn depth(
        request: CreateDepthIntegrity<'_>,
    ) -> Result<StreamIntegrityHandle, IntegrityFactoryError> {
        let CreateDepthIntegrity {
            binding,
            plan,
            buffer_limits,
            resource_limits,
            rest_endpoint,
        } = request;

        // Prevent partially configured REST-backed ingress,
        // and reject irrelevant REST configuration for
        // WebSocket-snapshot sources.
        match (plan.snapshot_source, rest_endpoint.is_some()) {
            (DepthSnapshotSource::Rest, false) => {
                return Err(IntegrityFactoryError::MissingRestEndpoint);
            }

            (DepthSnapshotSource::WebSocket, true) => {
                return Err(IntegrityFactoryError::UnexpectedRestEndpoint);
            }

            _ => {}
        }

        let mut ingress = BoundDepthIngress::new(binding, plan, buffer_limits, resource_limits)
            .map_err(IntegrityFactoryError::InvalidDepthConfiguration)?;

        if let Some(endpoint) = rest_endpoint {
            ingress
                .configure_rest_endpoint(endpoint.exchange, endpoint.format)
                .map_err(IntegrityFactoryError::InvalidRestEndpoint)?;
        }

        Ok(StreamIntegrityHandle::depth(ingress))
    }

    // ========================================================
    // TRADES
    // ========================================================

    pub fn trades(
        request: CreateTradeIntegrity,
    ) -> Result<StreamIntegrityHandle, IntegrityFactoryError> {
        let ingress =
            BoundTradeIngress::new(request.binding, request.identity_limits, request.plan)
                .map_err(IntegrityFactoryError::InvalidTradeConfiguration)?;

        Ok(StreamIntegrityHandle::trades(ingress))
    }
}
