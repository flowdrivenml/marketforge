use std::collections::BTreeMap;

use serde_json::Value;
use thiserror::Error;

use crate::live::config::exchange::{ExchangeConfig, SubscriptionConfig};

use super::{
    CompiledSubscriptionAckBinding, ConnectionFence, ConnectionStamp, ExpectedSubscriptionAck,
    PreparedRestoration, SubscriptionAckBindingError, SubscriptionCommand,
    SubscriptionProtocolError, SubscriptionStrategy,
};

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
            super::ConnectionState::Connected | super::ConnectionState::Ready
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

        // ----------------------------------------------------
        // NATIVE SUBSCRIPTION CONFIGURATION
        // ----------------------------------------------------

        let subscription = exchange
            .websocket
            .subscription
            .as_ref()
            .ok_or(SubscriptionCommandFactoryError::MissingConfiguration)?;

        // ----------------------------------------------------
        // PREFLIGHT ALL COMMANDS
        // ----------------------------------------------------

        let mut commands = Vec::with_capacity(prepared.entries().len());

        let mut next_id = self.next_request_id;

        for entry in prepared.into_entries() {
            let mut resolved = entry.subscription.resolved;

            // -----------------------------------------------
            // GENERATE REQUEST ID
            // -----------------------------------------------

            let request_id = Self::allocate_native_id(subscription, &self.stamp, &mut next_id)?;

            resolved.request.request_id = request_id.clone();

            if matches!(
                resolved.protocol.strategy(),
                SubscriptionStrategy::GateV4ChannelEvent
            ) {
                resolved.request.unix_seconds = Some(unix_seconds);
            }

            // -----------------------------------------------
            // SERIALIZE NATIVE REQUEST
            // -----------------------------------------------

            let wire_text = resolved.protocol.subscribe(&resolved.request)?;

            // -----------------------------------------------
            // BUILD EXPECTED ACK
            // -----------------------------------------------

            let expected = match self.ack_bindings.get(&resolved.channel_name) {
                Some(binding) => binding.expected_subscribe(&resolved)?,

                None if request_id.is_some() => {
                    ExpectedSubscriptionAck::subscribe(request_id.clone())
                }

                None => {
                    return Err(SubscriptionCommandFactoryError::MissingAckBinding(
                        resolved.channel_name.clone(),
                    ));
                }
            };

            // -----------------------------------------------
            // CORRELATION SAFETY
            // -----------------------------------------------

            if expected.request_id.is_none() && expected.identity.is_none() {
                return Err(SubscriptionCommandFactoryError::UncorrelatableSubscription(
                    resolved.channel_name.clone(),
                ));
            }

            commands.push(SubscriptionCommand {
                token: entry.token,
                expected,
                wire_text,
            });
        }

        // ----------------------------------------------------
        // COMMIT REQUEST-ID ALLOCATION
        // ----------------------------------------------------

        // No counter changes occur until the entire batch
        // has been successfully compiled.
        self.next_request_id = next_id;

        Ok(commands)
    }

    // ========================================================
    // NATIVE REQUEST-ID ALLOCATION
    // ========================================================

    fn allocate_native_id(
        config: &SubscriptionConfig,
        stamp: &ConnectionStamp,
        next_id: &mut u64,
    ) -> Result<Option<Value>, SubscriptionCommandFactoryError> {
        if config.request_id_field.is_none() {
            return Ok(None);
        }

        let next = next_id
            .checked_add(1)
            .ok_or(SubscriptionCommandFactoryError::RequestIdExhausted)?;

        let native = match config.request_id_type.as_deref() {
            Some("string") => Value::String(format!("mf-{}-{}", stamp.generation().get(), next,)),

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
