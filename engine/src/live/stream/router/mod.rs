//! Bounded volatile canonical routing for direct Rust consumers.
//!
//! Acceptance transfers a complete publication to Router ownership; it is not
//! durability or consumer processing acknowledgment. Saturated consumers fail
//! explicitly. Late depth consumers remain unsynchronized until a new snapshot.
mod event;
mod facade;
mod source;
pub use event::*;
pub use facade::*;
