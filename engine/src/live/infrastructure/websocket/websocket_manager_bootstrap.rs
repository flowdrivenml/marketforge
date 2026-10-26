use std::{collections::BTreeMap, sync::Arc};

use crate::live::{
    config::exchange::ExchangeConfig,
    infrastructure::limiter::{RateLimitManager, factory::CompiledWebSocketLimiters},
};

use super::{
    CompiledHeartbeatPlan, CompiledSubscriptionAckBinding, ConnectionBootstrapLimits,
    ConnectionSessionLimits, DesiredSubscriptionSnapshot, ReconnectPolicy,
};

use super::connection_slot_task::ConnectionSlotTaskConfig;
use super::connection_slots::{SlotId, SlotKey};
use super::websocket_manager::{WebSocketManager, WebSocketManagerError};

// ============================================================
// COMPILED PROFILE RUNTIME
// ============================================================

/// Prepared runtime dependencies for one exchange profile.
///
/// The bootstrap layer resolves endpoints, compiles protocol
/// configuration, and supplies existing limiter resources.
///
/// Stream instances never construct or access this type.
pub struct WebSocketProfileRuntime {
    pub exchange: Arc<ExchangeConfig>,

    /// Resolved connection URLs indexed by native route.
    ///
    /// None represents the profile's default route.
    ///
    /// All URLs must be resolved and validated before the
    /// WebSocketManager starts accepting attachments.
    pub urls: BTreeMap<Option<String>, String>,

    pub compiled_limiters: CompiledWebSocketLimiters,

    pub heartbeat_plan: CompiledHeartbeatPlan,

    pub ack_bindings: BTreeMap<String, CompiledSubscriptionAckBinding>,
}

// ============================================================
// MANAGER BOOTSTRAP
// ============================================================

/// Complete prepared infrastructure for a WebSocketManager.
///
/// This is a startup configuration, not stream-facing API.
///
/// All exchange-specific compilation is performed before
/// constructing this object.
pub struct WebSocketManagerRuntime {
    pub runtime_id: String,

    pub profiles: Vec<WebSocketProfileRuntime>,

    pub shared_limiter: RateLimitManager,

    pub reconnect_policy: ReconnectPolicy,

    pub bootstrap_limits: ConnectionBootstrapLimits,

    pub session_limits: ConnectionSessionLimits,

    pub event_capacity: usize,

    pub maximum_early_frames: usize,
}

// ============================================================
// INTERNAL RESOLVED PROFILE
// ============================================================

struct ResolvedProfile {
    exchange: Arc<ExchangeConfig>,

    urls: BTreeMap<Option<String>, String>,

    compiled_limiters: CompiledWebSocketLimiters,

    heartbeat_plan: CompiledHeartbeatPlan,

    ack_bindings: BTreeMap<String, CompiledSubscriptionAckBinding>,
}

impl From<WebSocketProfileRuntime> for ResolvedProfile {
    fn from(profile: WebSocketProfileRuntime) -> Self {
        Self {
            exchange: profile.exchange,
            urls: profile.urls,
            compiled_limiters: profile.compiled_limiters,
            heartbeat_plan: profile.heartbeat_plan,
            ack_bindings: profile.ack_bindings,
        }
    }
}

// ============================================================
// PUBLIC BOOTSTRAP
// ============================================================

impl WebSocketManager {
    /// Construct a WebSocketManager from compiled runtime
    /// dependencies.
    ///
    /// This is the supported constructor for the Live
    /// infrastructure bootstrap and external integration tests.
    ///
    /// No network connection is opened here.
    ///
    /// Connections start only when the first source stream
    /// is attached.
    pub fn from_runtime(runtime: WebSocketManagerRuntime) -> Result<Self, WebSocketManagerError> {
        if runtime.runtime_id.trim().is_empty() {
            return Err(WebSocketManagerError::Initialization(
                "runtime identifier must not be empty".into(),
            ));
        }

        if runtime.profiles.is_empty() {
            return Err(WebSocketManagerError::Initialization(
                "at least one exchange profile is required".into(),
            ));
        }

        let mut resolved = BTreeMap::<String, Arc<ResolvedProfile>>::new();

        let mut exchanges = Vec::new();

        // ====================================================
        // VALIDATE PROFILE RESOURCES
        // ====================================================

        for profile in runtime.profiles {
            let name = profile.exchange.profile().to_owned();

            if resolved.contains_key(&name) {
                return Err(WebSocketManagerError::Initialization(format!(
                    "duplicate exchange profile: {name}"
                )));
            }

            if profile.urls.is_empty() {
                return Err(WebSocketManagerError::Initialization(format!(
                    "profile {name} has no resolved WebSocket URLs"
                )));
            }

            for (route, url) in &profile.urls {
                if url.trim().is_empty() {
                    return Err(WebSocketManagerError::Initialization(format!(
                        "profile {name} has an empty URL \
                                 for route {route:?}"
                    )));
                }

                if let Some(route) = route {
                    if profile.exchange.route(route).is_none() {
                        return Err(WebSocketManagerError::Initialization(format!(
                            "profile {name} references \
                                     unknown route {route}"
                        )));
                    }
                }
            }

            exchanges.push(profile.exchange.clone());

            resolved.insert(name, Arc::new(profile.into()));
        }

        // ====================================================
        // PRESERVE SHARED RUNTIME RESOURCES
        // ====================================================

        let runtime_id = runtime.runtime_id;

        let shared_limiter = runtime.shared_limiter;

        let reconnect_policy = runtime.reconnect_policy;

        let bootstrap_limits = runtime.bootstrap_limits;

        let session_limits = runtime.session_limits;

        // ====================================================
        // PRIVATE CONNECTION-SLOT FACTORY
        // ====================================================

        Self::new(
            exchanges,
            move |slot_id: SlotId, key: &SlotKey, desired: DesiredSubscriptionSnapshot| {
                let profile = resolved.get(&key.profile).ok_or_else(|| {
                    format!("unknown resolved WebSocket profile: {}", key.profile)
                })?;

                let url = profile
                    .urls
                    .get(&key.route)
                    .ok_or_else(|| {
                        format!(
                            "missing resolved WebSocket URL: \
                             profile={} route={:?}",
                            key.profile, key.route,
                        )
                    })?
                    .clone();

                Ok(ConnectionSlotTaskConfig {
                    runtime_id: runtime_id.clone(),

                    slot_id: format!("websocket-slot-{}", slot_id.get()),

                    url,

                    exchange: profile.exchange.clone(),

                    connection_route: key.route.clone(),

                    compiled_limiters: profile.compiled_limiters.clone(),

                    shared_limiter: shared_limiter.clone(),

                    reconnect_policy: reconnect_policy.clone(),

                    heartbeat_plan: profile.heartbeat_plan.clone(),

                    ack_bindings: profile.ack_bindings.clone(),

                    bootstrap_limits: bootstrap_limits.clone(),

                    session_limits: session_limits.clone(),

                    initial_demand: desired,
                })
            },
            runtime.event_capacity,
            runtime.maximum_early_frames,
        )
    }
}
