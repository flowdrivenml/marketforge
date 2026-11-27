use std::collections::BTreeMap;

use serde_json::Value;
use thiserror::Error;

use super::ack::{ExpectedSubscriptionAck, SubscriptionAction};
use super::ack_binding::{CompiledSubscriptionAckBinding, SubscriptionAckBindingError};
use super::driver::SubscriptionCommand;
use super::protocol::{SubscriptionProtocolError, SubscriptionStrategy};
use super::resolver::ResolvedSubscription;
use super::restoration_session::PreparedRestoration;
use crate::live::config::exchange::{ExchangeConfig, ExchangeId, SubscriptionConfig};
use crate::live::infrastructure::websocket::lifecycle::fence::{ConnectionFence, ConnectionStamp};
use crate::live::infrastructure::websocket::streams::recovery::StreamRecoveryToken;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum SubscriptionCommandFactoryError {
    #[error("restoration generation is stale")]
    StaleGeneration,

    #[error("restoration demand revision is stale")]
    StaleDemand,

    #[error("restoration route does not match this connection")]
    WrongRoute,

    #[error("missing WebSocket subscription configuration")]
    MissingConfiguration,

    #[error("request-ID sequence exhausted")]
    RequestIdExhausted,

    #[error("unsupported native request-ID type: {0}")]
    UnsupportedRequestIdType(String),

    #[error("ID-less subscription '{0}' requires verified ACK identity binding")]
    MissingAckBinding(String),

    #[error("subscription '{0}' has no reliable ACK correlation strategy")]
    UncorrelatableSubscription(String),

    #[error(transparent)]
    AckBinding(#[from] SubscriptionAckBindingError),

    #[error(transparent)]
    Protocol(#[from] SubscriptionProtocolError),
}

// ============================================================
// CONNECTION-SCOPED COMMAND FACTORY
// ============================================================

/// Compiles restoration entries into native subscription
/// commands for one physical WebSocket generation.
///
/// The factory owns request-ID allocation for this
/// connection generation.
///
/// Do not recreate this factory while the physical
/// connection remains active.
///
/// ACK identity bindings must come from verified native
/// protocol contracts, not inferred response paths.
///
/// This factory does not perform socket I/O.
pub struct SubscriptionCommandFactory {
    stamp: ConnectionStamp,

    route: Option<String>,

    next_request_id: u64,

    /// Verified native ACK identity layouts,
    /// indexed by logical channel name.
    ack_bindings: BTreeMap<String, CompiledSubscriptionAckBinding>,
}

impl SubscriptionCommandFactory {
    pub fn new(
        stamp: ConnectionStamp,
        route: Option<String>,
        ack_bindings: BTreeMap<String, CompiledSubscriptionAckBinding>,
    ) -> Self {
        Self {
            stamp,
            route,
            next_request_id: 0,
            ack_bindings,
        }
    }

    pub fn last_request_id(&self) -> u64 {
        self.next_request_id
    }

    // ========================================================
    // COMPILE COMPLETE RESTORATION BATCH
    // ========================================================

    /// Compile every prepared subscription before handing
    /// any command to the asynchronous subscription driver.
    ///
    /// On failure:
    /// - No commands are returned.
    /// - The request-ID counter remains unchanged.
    /// - No recovery state is mutated.
    /// - No network transmission occurs.
    ///
    /// `unix_seconds` is supplied by the connection runtime.
    /// The production caller must obtain it from an
    /// authoritative current clock.
    pub fn compile(
        &mut self,
        exchange: &ExchangeConfig,
        fence: &ConnectionFence,
        current_demand_revision: u64,
        prepared: PreparedRestoration,
        unix_seconds: u64,
    ) -> Result<Vec<SubscriptionCommand>, SubscriptionCommandFactoryError> {
        // ----------------------------------------------------
        // GENERATION AUTHORITY
        // ----------------------------------------------------

        if fence.current_stamp().as_ref() != Some(&self.stamp) || prepared.stamp() != &self.stamp {
            return Err(SubscriptionCommandFactoryError::StaleGeneration);
        }

        if !matches!(
            fence.state(),
            crate::live::infrastructure::websocket::lifecycle::state::ConnectionState::Connected
                | crate::live::infrastructure::websocket::lifecycle::state::ConnectionState::Ready
        ) {
            return Err(SubscriptionCommandFactoryError::StaleGeneration);
        }

        // ----------------------------------------------------
        // DEMAND AUTHORITY
        // ----------------------------------------------------

        if prepared.demand_revision() != current_demand_revision {
            return Err(SubscriptionCommandFactoryError::StaleDemand);
        }

        // ----------------------------------------------------
        // CONNECTION ROUTE
        // ----------------------------------------------------

        if prepared.route() != self.route.as_deref() {
            return Err(SubscriptionCommandFactoryError::WrongRoute);
        }

        let mut next_id = self.next_request_id;
        let mut commands = Vec::with_capacity(prepared.entries().len());
        for entry in prepared.into_entries() {
            commands.push(self.compile_operation(
                exchange,
                entry.subscription.resolved,
                entry.token,
                SubscriptionAction::Subscribe,
                unix_seconds,
                &mut next_id,
            )?);
        }
        self.next_request_id = next_id;
        Ok(commands)
    }

    pub(crate) fn operation(
        &mut self,
        exchange: &ExchangeConfig,
        resolved: ResolvedSubscription,
        token: StreamRecoveryToken,
        action: SubscriptionAction,
        unix_seconds: u64,
    ) -> Result<SubscriptionCommand, SubscriptionCommandFactoryError> {
        let mut next_id = self.next_request_id;
        let command = self.compile_operation(
            exchange,
            resolved,
            token,
            action,
            unix_seconds,
            &mut next_id,
        )?;
        self.next_request_id = next_id;
        Ok(command)
    }

    fn compile_operation(
        &self,
        exchange: &ExchangeConfig,
        mut resolved: ResolvedSubscription,
        token: StreamRecoveryToken,
        action: SubscriptionAction,
        unix_seconds: u64,
        next_id: &mut u64,
    ) -> Result<SubscriptionCommand, SubscriptionCommandFactoryError> {
        if token.connection() != &self.stamp {
            return Err(SubscriptionCommandFactoryError::StaleGeneration);
        }
        if resolved.route != self.route {
            return Err(SubscriptionCommandFactoryError::WrongRoute);
        }
        let subscription = exchange
            .websocket
            .subscription
            .as_ref()
            .ok_or(SubscriptionCommandFactoryError::MissingConfiguration)?;
        let request_id = Self::allocate_native_id(
            subscription,
            &self.stamp,
            next_id,
            exchange.exchange.id == ExchangeId::Okx,
        )?;
        resolved.request.request_id = request_id.clone();
        if resolved.protocol.strategy() == SubscriptionStrategy::GateV4ChannelEvent {
            resolved.request.unix_seconds = Some(unix_seconds);
        }
        let wire_text = match action {
            SubscriptionAction::Subscribe => resolved.protocol.subscribe(&resolved.request)?,
            SubscriptionAction::Unsubscribe => resolved.protocol.unsubscribe(&resolved.request)?,
        };
        let expected = match self.ack_bindings.get(&resolved.channel_name) {
            Some(binding) => match action {
                SubscriptionAction::Subscribe => binding.expected_subscribe(&resolved)?,
                SubscriptionAction::Unsubscribe => binding.expected_unsubscribe(&resolved)?,
            },
            None if request_id.is_some() => match action {
                SubscriptionAction::Subscribe => ExpectedSubscriptionAck::subscribe(request_id),
                SubscriptionAction::Unsubscribe => ExpectedSubscriptionAck::unsubscribe(request_id),
            },
            None => {
                return Err(SubscriptionCommandFactoryError::MissingAckBinding(
                    resolved.channel_name,
                ));
            }
        };
        Ok(SubscriptionCommand {
            token,
            expected,
            wire_text,
        })
    }

    // ========================================================
    // NATIVE REQUEST-ID ALLOCATION
    // ========================================================

    fn allocate_native_id(
        config: &SubscriptionConfig,
        stamp: &ConnectionStamp,
        next_id: &mut u64,
        okx_id_rules: bool,
    ) -> Result<Option<Value>, SubscriptionCommandFactoryError> {
        if config.request_id_field.is_none() {
            return Ok(None);
        }

        let next = next_id
            .checked_add(1)
            .ok_or(SubscriptionCommandFactoryError::RequestIdExhausted)?;

        let native = match config.request_id_type.as_deref() {
            Some("string") => {
                let id = if okx_id_rules {
                    // OKX: alphanumeric only, maximum 32 characters.
                    format!("mf{}r{}", stamp.generation().get(), next)
                } else {
                    format!("mf-{}-{}", stamp.generation().get(), next)
                };

                if okx_id_rules && id.len() > 32 {
                    return Err(SubscriptionCommandFactoryError::RequestIdExhausted);
                }

                Value::String(id)
            }

            Some("unsigned_integer") => Value::from(next),

            Some("signed_integer") => {
                let signed = i64::try_from(next)
                    .map_err(|_| SubscriptionCommandFactoryError::RequestIdExhausted)?;

                Value::from(signed)
            }

            Some(other) => {
                return Err(SubscriptionCommandFactoryError::UnsupportedRequestIdType(
                    other.to_owned(),
                ));
            }

            None => {
                // The existing protocol compiler permits
                // an unspecified request-ID type.
                //
                // Use the integer representation rather
                // than inventing a string requirement.
                Value::from(next)
            }
        };

        *next_id = next;

        Ok(Some(native))
    }
}
