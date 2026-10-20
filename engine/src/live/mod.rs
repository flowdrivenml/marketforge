pub fn run() {
    println!("marketforge-live");
}

pub mod config;
pub mod control;
pub mod derived;
pub mod exchange;
pub mod foundation;
pub mod ingest;
pub mod metrics;
pub mod model;
pub mod ports;
pub mod publish;
pub mod recording;
pub mod recovery;
pub mod registry;
pub mod retention;
pub mod routing;
pub mod runtime;
pub mod transport;
