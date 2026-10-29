pub mod config;
pub mod pool;
pub mod schema;
pub mod streams;

pub use config::PostgresConfig;
pub use pool::{connect, health_check, shutdown};
pub use schema::ensure_live_schema;
pub use streams::{
    RegisterStreamRequest, RegisteredStream, StreamKind, StreamRepository, StreamRepositoryError,
};
