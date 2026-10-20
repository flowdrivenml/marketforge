//! Live domain models for MarketForge Live.
//!
//! Live-specific models extend existing canonical semantics without duplicating
//! their definitions.

pub mod configuration;
pub mod coverage;
pub mod cursor;
pub mod delivery;
pub mod demand;
pub mod derived;
pub mod envelope;
pub mod error;
pub mod generation;
pub mod identity;
pub mod lifecycle;
pub mod operation;
pub mod protocol;
pub mod snapshot;

pub use cursor::{
    BookGeneration, ConnectionGeneration, DerivedGeneration, LogEpoch, StreamCursor,
    SubscriptionGeneration,
};
pub use demand::{DemandId, DemandOwner, RetentionSessionId, StreamHandle, SubscriberId};
pub use identity::{
    ChannelKind, ChannelSpec, DerivedStreamId, ExchangeId, MarketKind, SourceStreamKey, StreamId,
    ValidationIssue, ValidationIssueKind, ValidationIssues,
};
pub use lifecycle::{
    ConnectionState, CoverageState, IntegrityFailure, IntegrityState, RecoveryState,
    SourceStreamState, StreamLifecycle, SubscriptionState,
};
