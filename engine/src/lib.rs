pub mod historical;
#[cfg(all(test, feature = "historical", feature = "monitor-tui"))]
pub(crate) use historical::config::normalization_fixture;
pub mod shared;
pub use historical::job;
pub use shared::domain::book;
pub use shared::domain::canonical;
pub use shared::domain::instrument;
pub use shared::error;
#[cfg(feature = "historical")]
pub use shared::io::decode;
#[cfg(feature = "historical")]
pub use shared::io::source;
pub use shared::normalization as normalize;
pub use shared::validation as validate;
pub mod formats {
    pub use crate::shared::formats::depth;
    pub mod trades {
        pub use crate::historical::job::identity_policy;
        pub use crate::shared::formats::trades::{
            BoolSpec, BoolTransform, FieldSpec, IdentityDecision, IdentityMetrics, IdentityPolicy,
            OptionalTradeFields, QuantitySpec, QuantityTransform, SideSpec, SideTransform,
            TradeSpec, resolve_identity_policy, transform_bool, transform_quantity, transform_side,
        };
        #[cfg(feature = "historical")]
        pub use crate::shared::formats::trades::{
            InstrumentMatcher, TradeContext, TradeProcessor, resolve_trade_headers,
        };
    }
}
#[cfg(feature = "monitor-tui")]
pub mod tui;
#[cfg(feature = "historical")]
pub use historical::normalize as process;
pub mod live;
#[cfg(any(feature = "live", feature = "historical", feature = "parquet"))]
/// Legacy public persistence paths; implementations live under Shared.
pub mod storage {
    #[cfg(feature = "parquet")]
    pub use crate::shared::persistence::parquet;
    #[cfg(feature = "parquet")]
    pub mod historical_compat {
        pub use crate::shared::persistence::parquet::{
            DECIMAL_PRECISION, DECIMAL_SCALE, decimal_to_i256, depth_event_schema, depth_schema,
            i256_to_decimal, trade_schema,
        };
    }
}

// Original public client and protocol paths.
#[cfg(any(feature = "stream-client", feature = "management-client"))]
pub mod client {
    #[cfg(feature = "management-client")]
    pub use crate::live::management::client as management;
    #[cfg(all(unix, feature = "stream-client"))]
    pub use crate::live::streaming::client as streaming;
}
pub use live::management::protocol as management_protocol;
pub use live::streaming::protocol as stream_protocol;
