mod depth_publication;
mod depth_recovery;
#[path = "facade/all_profiles.rs"]
mod facade_all_profiles;
#[path = "facade/depth_lifecycle.rs"]
mod facade_depth_lifecycle;
#[path = "facade/depth_processing.rs"]
mod facade_depth_processing;
#[path = "facade/rest_snapshot_sync.rs"]
mod facade_rest_snapshot_sync;
#[path = "facade/trades.rs"]
mod facade_trades;
mod failure_injection;
mod incident_lifecycle;
mod trade_lifecycle;

#[path = "../../support/mod.rs"]
mod support;
