use std::{collections::BTreeMap, time::Duration};

use thiserror::Error;

use crate::live::config::exchange::ExchangeConfig;

use super::connection::WebSocketConnection;
use super::session::ConnectionSessionInputs;
use crate::live::infrastructure::websocket::heartbeat::plan::CompiledHeartbeatPlan;
use crate::live::infrastructure::websocket::lifecycle::fence::{ConnectionFence, ConnectionStamp};
use crate::live::infrastructure::websocket::lifecycle::state::ConnectionState;
use crate::live::infrastructure::websocket::streams::shared_recovery::{
    SharedRecoveryError, SharedStreamRecovery,
};
use crate::live::infrastructure::websocket::subscriptions::ack::{
    CompiledSubscriptionAck, SubscriptionAckCompiler, SubscriptionAckError,
};
use crate::live::infrastructure::websocket::subscriptions::ack_binding::CompiledSubscriptionAckBinding;
use crate::live::infrastructure::websocket::subscriptions::command_factory::{
    SubscriptionCommandFactory, SubscriptionCommandFactoryError,
};
use crate::live::infrastructure::websocket::subscriptions::driver::SubscriptionCommand;
use crate::live::infrastructure::websocket::subscriptions::restoration::{
    DesiredSubscriptionSnapshot, SubscriptionRestorationError, SubscriptionRestorationPlanner,
};
use crate::live::infrastructure::websocket::subscriptions::restoration_session::{
    RestorationPreparationError, SubscriptionRestorationSession,
};
use crate::live::infrastructure::websocket::subscriptions::session::{
    SubscriptionSession, SubscriptionSessionError,
};

// ============================================================
// LIMITS
// ============================================================

/// Subscription transaction limits for one physical
/// WebSocket generation.
///
/// Queue capacities and driver timeouts remain owned by
/// ConnectionSessionLimits.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionBootstrapLimits {
    pub maximum_pending: usize,
    pub acknowledgement_timeout: Duration,
}

impl ConnectionBootstrapLimits {
    pub fn validate(&self) -> Result<(), ConnectionBootstrapError> {
        if self.maximum_pending == 0 || self.acknowledgement_timeout.is_zero() {
            return Err(ConnectionBootstrapError::InvalidLimits);
        }

        Ok(())
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum ConnectionBootstrapError {
    #[error("invalid bootstrap limits")]
    InvalidLimits,

    #[error("physical connection generation is stale")]
    StaleGeneration,

    #[error("desired demand revision changed")]
    StaleDemand,

    #[error(transparent)]
    Planning(#[from] SubscriptionRestorationError),

    #[error(transparent)]
    Preparation(#[from] RestorationPreparationError),

    #[error(transparent)]
    Commands(#[from] SubscriptionCommandFactoryError),

    #[error(transparent)]
    Session(#[from] SubscriptionSessionError),

    #[error(transparent)]
    Acknowledgements(#[from] SubscriptionAckError),

    #[error(transparent)]
    Recovery(#[from] SharedRecoveryError),
}

// ============================================================
// PREPARED PHYSICAL GENERATION
// ============================================================

/// A generation-specific restoration bootstrap.
///
/// This object owns the new SubscriptionSession and its
/// compiled commands.
///
/// It references persistent recovery authority through
/// SharedStreamRecovery.
///
/// No WebSocket connection or asynchronous task is created
/// during preparation.
pub struct PreparedConnectionGeneration {
    stamp: ConnectionStamp,
    demand_revision: u64,

    subscription_session: SubscriptionSession,
    recovery: SharedStreamRecovery,

    restoration_commands: Vec<SubscriptionCommand>,
}

impl PreparedConnectionGeneration {
    pub fn stamp(&self) -> &ConnectionStamp {
        &self.stamp
    }

    pub fn demand_revision(&self) -> u64 {
        self.demand_revision
    }

    pub fn command_count(&self) -> usize {
        self.restoration_commands.len()
    }

    pub fn recovery(&self) -> &SharedStreamRecovery {
        &self.recovery
    }

    /// Transfer generation-specific resources into the
    /// existing physical connection session.
    ///
    /// This consumes the bootstrap so the same commands
    /// cannot be submitted twice through this object.
    pub fn into_session_inputs(
        self,
        connection: WebSocketConnection,
        heartbeat: CompiledHeartbeatPlan,
        acknowledgements: CompiledSubscriptionAck,
    ) -> ConnectionSessionInputs {
        ConnectionSessionInputs {
            stamp: self.stamp,
            connection,
            heartbeat,
            acknowledgements,
            subscription_session: self.subscription_session,
            recovery: self.recovery,
            restoration_commands: self.restoration_commands,
            demand_revision: self.demand_revision,
        }
    }
}

// ============================================================
// BOOTSTRAP
// ============================================================

pub struct ConnectionGenerationBootstrap;

impl ConnectionGenerationBootstrap {
    /// Prepare one new physical WebSocket generation from
    /// a complete, authoritative demand snapshot.
    ///
    /// The snapshot must contain every currently desired
    /// source stream assigned to this physical connection
    /// slot and route.
    ///
    /// This method must be serialized with demand mutation
    /// by the logical connection-slot owner.
    ///
    /// If preparation succeeds, the resulting generation
    /// must either be executed or explicitly abandoned.
    ///
    /// A failure after recovery rebinding requires
    /// generation cleanup; it does not authorize publication.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare(
        exchange: &ExchangeConfig,
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
        connection_route: Option<&str>,
        desired: &DesiredSubscriptionSnapshot,
        current_demand_revision: u64,
        recovery: &SharedStreamRecovery,
        ack_bindings: BTreeMap<String, CompiledSubscriptionAckBinding>,
        unix_seconds: u64,
        limits: ConnectionBootstrapLimits,
    ) -> Result<PreparedConnectionGeneration, ConnectionBootstrapError> {
        limits.validate()?;

        // ----------------------------------------------------
        // GENERATION VALIDATION
        // ----------------------------------------------------

        if fence.current_stamp().as_ref() != Some(stamp)
            || !matches!(
                fence.state(),
                ConnectionState::Connected | ConnectionState::Ready
            )
        {
            return Err(ConnectionBootstrapError::StaleGeneration);
        }

        // ----------------------------------------------------
        // DEMAND VALIDATION
        // ----------------------------------------------------

        if desired.revision() != current_demand_revision {
            return Err(ConnectionBootstrapError::StaleDemand);
        }

        // ----------------------------------------------------
        // COMPILE ACK PROTOCOL
        // ----------------------------------------------------

        // Fail on invalid ACK configuration before touching
        // stream recovery state.
        let _acknowledgements = SubscriptionAckCompiler::compile(exchange)?;

        // ----------------------------------------------------
        // RESTORATION PLANNING
        // ----------------------------------------------------

        let plan = SubscriptionRestorationPlanner::plan(exchange, fence, stamp, desired)?;

        // ----------------------------------------------------
        // RESTORATION PREPARATION
        // ----------------------------------------------------

        // Use the existing persistent coordinator.
        //
        // All mutations are synchronous. The mutex guard
        // is released before constructing any async work.
        let prepared = {
            let mut guard = recovery.lock()?;

            SubscriptionRestorationSession::prepare(
                plan,
                fence,
                current_demand_revision,
                connection_route,
                &mut guard,
            )?
        };

        // ----------------------------------------------------
        // CONNECTION-SCOPED COMMAND FACTORY
        // ----------------------------------------------------

        // This factory is constructed exactly once for
        // the initial restoration batch.
        //
        // Incremental subscriptions must eventually share
        // its request-ID allocation state.
        let mut factory = SubscriptionCommandFactory::new(
            stamp.clone(),
            connection_route.map(str::to_owned),
            ack_bindings,
        );

        let commands = factory.compile(
            exchange,
            fence,
            current_demand_revision,
            prepared,
            unix_seconds,
        )?;

        // ----------------------------------------------------
        // SUBSCRIPTION TRANSACTION SESSION
        // ----------------------------------------------------

        let subscription_session = SubscriptionSession::new(
            fence.clone(),
            stamp.clone(),
            limits.maximum_pending,
            limits.acknowledgement_timeout,
        )?;

        Ok(PreparedConnectionGeneration {
            stamp: stamp.clone(),
            demand_revision: current_demand_revision,
            subscription_session,
            recovery: recovery.clone(),
            restoration_commands: commands,
        })
    }
}
