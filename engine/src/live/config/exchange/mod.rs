// ---------------------------------------------------------
// MODULES
// ---------------------------------------------------------

mod channel;
mod identity;
mod market;
mod rest;
mod validation;
mod websocket;

// ---------------------------------------------------------
// PUBLIC EXPORTS
// ---------------------------------------------------------

pub use channel::{ChannelConfig, DepthValue, QuantityConfig, SynchronizationConfig};

pub use identity::{ExchangeId, ExchangeIdentity, MarketType};

pub use market::{MarketMapping, NativeMarketType};

pub use rest::{
    RateLimitErrorCode, RateLimitPolicy, RestApplicationResponseConfig,
    RestApplicationResponseStrategy, RestConfig, RestEndpoint, RestRateLimiting, WeightPolicy,
    WeightTier,
};

pub use websocket::{
    ApplicationHeartbeatConfig, ConnectionConfig, HeartbeatConfig, HeartbeatStrategy,
    LifecycleConfig, RoutingConfig, SubscriptionConfig, WebSocketConfig, WebSocketLimits,
    WebSocketRoute,
};

mod model;
pub use model::ExchangeConfig;
