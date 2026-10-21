//! Planned module: tests/live/(root).

// Document 17 scaffolding: declarations only; implementations are not complete.
pub(crate) mod config;
pub(crate) mod contracts;
pub(crate) mod control;
pub(crate) mod depth;
pub(crate) mod derived;
pub(crate) mod foundations;
pub(crate) mod integration;
pub(crate) mod metrics;
pub(crate) mod protocol;
pub(crate) mod publication;
pub(crate) mod recording;
pub(crate) mod recovery;
pub(crate) mod registry;
pub mod reporting;
pub mod resolution;
pub mod resolution_claims;
pub mod resolution_concurrency;
pub mod resolution_integration;
pub mod resolution_lifecycle;
pub(crate) mod retention;
pub mod router;
pub mod router_admission;
pub mod router_commitment;
pub mod router_obligations;
pub(crate) mod routing;
pub(crate) mod runtime;
pub(crate) mod subscriptions;
pub(crate) mod support;
pub mod tracking_recovery;
pub(crate) mod trades;
pub(crate) mod transport;
pub(crate) mod tui;

pub(crate) mod wp06_contracts;

pub(crate) mod c06_recording;
pub(crate) mod wp07_contracts;

pub(crate) mod c06_postgres;

pub(crate) mod wp08_foundations;

pub(crate) mod wp09_configuration;

pub(crate) mod destination_worker;
