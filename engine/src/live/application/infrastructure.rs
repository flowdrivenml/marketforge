use crate::live::{
    config::{ExchangeConfigStore, general::RecoveryDefaultsConfig},
    infrastructure::{
        limiter::{factory::*, *},
        websocket::*,
    },
};
use std::{collections::BTreeMap, time::Duration};
/// Resolve explicit egress/quota ownership once, shared by REST and WebSocket.
/// Weighted capacities are operator-supplied verified observations, never guessed.
pub fn compile_limiters(
    configs: &ExchangeConfigStore,
    egress: &str,
    weights: &BTreeMap<String, (u64, u64)>,
) -> Result<ExchangeLimiters, String> {
    if egress.is_empty() || egress.len() > 256 {
        return Err("egress identity must contain 1..256 bytes".into());
    }
    let mut bindings = vec![];
    for name in configs.names() {
        let exchange = configs.get(name).unwrap();
        let scope = BudgetScope::Egress(egress.into());
        let mut rest = RestLimiterBindings::default();
        let binding = RestBudgetBinding::new(
            format!("{}_rest", exchange.exchange.id.as_str()),
            scope.clone(),
        );
        let rate = exchange.rest.rate_limiting.as_ref();
        if rate.is_some_and(|r| r.ip_max_requests.is_some()) {
            rest.ip = Some(binding.clone());
        }
        if rate.is_some_and(|r| r.global_max_requests.is_some()) {
            rest.global = Some(binding.clone());
        }
        for policy in exchange.rest.rate_limit_policies.keys() {
            rest.endpoint_policies
                .insert(policy.clone(), binding.clone());
        }
        if rate.is_some_and(|r| r.strategy == "weighted_shared_pool") {
            let (capacity, window) = weights.get(name).copied().ok_or_else(|| {
                format!("{name}: verified REST shared-weight capacity/window required")
            })?;
            rest.shared_weight = Some(ResolvedRestBudget {
                id: BudgetId::new(
                    exchange.exchange.id,
                    format!("{name}_rest"),
                    "shared_weight",
                    scope.clone(),
                )
                .map_err(|e| e.to_string())?,
                quota: WindowQuota::new(capacity, Duration::from_secs(window))
                    .map_err(|e| e.to_string())?,
            });
        }
        let ws_binding = WebSocketBudgetBinding::new(
            format!("{}_websocket", exchange.exchange.id.as_str()),
            scope,
        );
        bindings.push((
            name.to_string(),
            rest,
            WebSocketLimiterBindings {
                connection_attempts: Some(ws_binding.clone()),
                connection_attempts_per_second: Some(ws_binding),
                connection_service: Some(format!("{name}_websocket")),
            },
        ));
    }
    let inputs = bindings
        .iter()
        .map(|(name, r, w)| LimiterProfileInput::new(configs.get(name).unwrap(), r, w))
        .collect::<Vec<_>>();
    ExchangeLimiterFactory::build(&inputs, 1024).map_err(|e| format!("limiter compilation: {e:?}"))
}
/// Pure configuration compilation. Endpoint overrides are explicit embedding seams
/// for local exchanges; no connection is opened here.
pub fn compile_websocket(
    configs: &ExchangeConfigStore,
    limiters: &ExchangeLimiters,
    recovery: &RecoveryDefaultsConfig,
    overrides: &BTreeMap<String, String>,
) -> Result<WebSocketManagerRuntime, String> {
    let mut profiles = vec![];
    for name in configs.names() {
        let exchange = configs.get_shared(name).unwrap();
        let ws = &exchange.websocket;
        let default = overrides
            .get(name)
            .cloned()
            .or_else(|| ws.url.clone())
            .or_else(|| {
                ws.base_url
                    .as_ref()
                    .map(|base| format!("{}{}", base, ws.raw_path.as_deref().unwrap_or("")))
            });
        let mut urls = BTreeMap::new();
        if let Some(url) = default {
            urls.insert(None, url);
        }
        for (route, endpoint) in &ws.routes {
            let url = overrides
                .get(name)
                .cloned()
                .or_else(|| endpoint.url.clone())
                .or_else(|| endpoint.base_url.clone())
                .ok_or_else(|| format!("{name}: unresolved WebSocket route {route}"))?;
            urls.insert(Some(route.clone()), url);
        }
        let mut ack_bindings = BTreeMap::new();
        if let Some(routing) = ws.routing.as_ref().filter(|r| r.mode == "arg") {
            let fields = [
                routing.channel_field.as_ref(),
                routing.instrument_field.as_ref(),
                routing.instrument_type_field.as_ref(),
            ]
            .into_iter()
            .flatten()
            .map(|path| {
                let field = path
                    .strip_prefix("arg.")
                    .ok_or_else(|| format!("{name}: unsupported argument ACK path"))?;
                Ok(SubscriptionAckFieldBinding::object(0, field, path))
            })
            .collect::<Result<Vec<_>, String>>()?;
            let binding = CompiledSubscriptionAckBinding::new(fields).map_err(|e| e.to_string())?;
            for channel in exchange.channels.keys() {
                ack_bindings.insert(channel.clone(), binding.clone());
            }
        }
        profiles.push(WebSocketProfileRuntime {
            heartbeat_plan: HeartbeatPlanCompiler::compile(&exchange).map_err(|e| e.to_string())?,
            exchange,
            urls,
            compiled_limiters: limiters
                .profile(name)
                .ok_or("missing limiter profile")?
                .websocket
                .clone(),
            ack_bindings,
        });
    }
    Ok(WebSocketManagerRuntime {
        runtime_id: format!("live-{}", std::process::id()),
        profiles,
        shared_limiter: limiters.manager(),
        reconnect_policy: ReconnectPolicy::new(
            Duration::from_millis(recovery.initial_backoff_ms),
            Duration::from_secs(recovery.max_backoff_seconds),
            Duration::from_secs(60),
            Duration::from_secs(10),
            None,
        )
        .map_err(|e| e.to_string())?,
        bootstrap_limits: ConnectionBootstrapLimits {
            maximum_pending: 128,
            acknowledgement_timeout: Duration::from_secs(10),
        },
        session_limits: ConnectionSessionLimits {
            inbound_capacity: 256,
            outbound_capacity: 128,
            command_capacity: 128,
            event_capacity: 128,
            dispatch_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(5),
            driver: SubscriptionDriverLimits {
                queue_timeout: Duration::from_secs(5),
                receipt_timeout: Duration::from_secs(5),
                maximum_early_frames: 256,
            },
        },
        event_capacity: 1024,
        maximum_early_frames: 256,
    })
}
