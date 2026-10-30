mod policy;
mod spec;
mod transforms;

#[cfg(all(feature = "historical", test))]
mod csv;

#[cfg(feature = "historical")]
mod headers;

#[cfg(feature = "historical")]
mod identity;

#[cfg(feature = "historical")]
mod processor;

// ---- Unconditional (usable by live path) ----

pub use spec::{
    BoolSpec, BoolTransform, FieldSpec, OptionalTradeFields, QuantitySpec, QuantityTransform,
    SideSpec, SideTransform, TradeSpec,
};

pub use policy::{IdentityDecision, IdentityMetrics, IdentityPolicy, resolve_identity_policy};

pub use transforms::{transform_bool, transform_quantity, transform_side};

// ---- Historical (CSV archive processing only) ----

#[cfg(feature = "historical")]
pub use identity::InstrumentMatcher;

#[cfg(feature = "historical")]
pub use headers::resolve_trade_headers;

#[cfg(feature = "historical")]
pub use processor::{TradeContext, TradeProcessor};
