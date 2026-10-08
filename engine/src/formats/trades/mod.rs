mod headers;
mod identity;
mod processor;
mod spec;
mod transforms;

pub use headers::resolve_trade_headers;

pub use processor::{TradeContext, TradeProcessor};

pub use spec::{
    BoolSpec, BoolTransform, FieldSpec, OptionalTradeFields, QuantitySpec, QuantityTransform,
    SideSpec, SideTransform, TradeSpec,
};

pub use identity::{
    IdentityDecision, IdentityMetrics, IdentityPolicy, InstrumentMatcher, identity_policy,
};

pub use transforms::{transform_bool, transform_quantity, transform_side};
