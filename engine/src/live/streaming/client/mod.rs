//! Bounded, multiplexed live-tail client. Never reconnects or resumes implicitly.
mod connection;
mod subscription;
pub use crate::live::streaming::protocol::{
    PageRequest, StreamDescription, StreamMessage, StreamPage,
};
pub use connection::{ClientError, ClientOptions, MarketForgeStreamClient};
pub use subscription::Subscription;
