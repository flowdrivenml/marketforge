//! Stream-facing processing ownership and Router adapters.
pub mod controller;
pub mod lifecycle;

mod pipeline;
mod service;
pub use pipeline::*;
pub use service::*;

mod operations;
pub use operations::{DerivedStatus, RetainedProcessorInput};

mod conversion;

pub(crate) fn preflight(graph:&crate::live::stream::registry::StreamGraph,limits:crate::live::config::general::DerivedProcessingConfig)->Result<(),String>{for id in graph.stream_ids(){if matches!(graph.get(id).unwrap().definition,crate::live::stream::persistence::StreamDefinition::Derived(_)){drop(operations::resolve_start(graph,id,limits)?);}}Ok(())}
