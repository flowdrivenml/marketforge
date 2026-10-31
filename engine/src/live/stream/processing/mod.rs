//! Stream-facing processing ownership and Router adapters.
pub mod controller;
pub mod lifecycle;

mod pipeline;
mod service;
pub use pipeline::*;
pub use service::*;
