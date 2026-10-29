use std::{env, num::NonZeroU32, time::Duration};

pub const DATABASE_URL_ENV: &str = "MARKETFORGE_DATABASE_URL";

pub struct PostgresConfig {
    database_url: String,
    max_connections: NonZeroU32,
    acquire_timeout: Duration,
}

impl PostgresConfig {
    pub fn from_env() -> Result<Self, env::VarError> {
        let database_url = env::var(DATABASE_URL_ENV)?;

        Ok(Self::new(database_url))
    }

    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            max_connections: NonZeroU32::new(5).unwrap(),
            acquire_timeout: Duration::from_secs(10),
        }
    }

    pub fn with_max_connections(mut self, max_connections: NonZeroU32) -> Self {
        self.max_connections = max_connections;
        self
    }

    pub fn with_acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = acquire_timeout;
        self
    }

    pub(crate) fn database_url(&self) -> &str {
        &self.database_url
    }

    pub fn max_connections(&self) -> u32 {
        self.max_connections.get()
    }

    pub fn acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_pool_size_is_five() {
        let config = PostgresConfig::new("postgresql://localhost/test");

        assert_eq!(config.max_connections(), 5);
    }

    #[test]
    fn default_acquire_timeout_is_ten_seconds() {
        let config = PostgresConfig::new("postgresql://localhost/test");

        assert_eq!(config.acquire_timeout(), Duration::from_secs(10));
    }

    #[test]
    fn supports_custom_pool_size() {
        let config = PostgresConfig::new("postgresql://localhost/test")
            .with_max_connections(NonZeroU32::new(20).unwrap());

        assert_eq!(config.max_connections(), 20);
    }

    #[test]
    fn supports_custom_timeout() {
        let config = PostgresConfig::new("postgresql://localhost/test")
            .with_acquire_timeout(Duration::from_secs(30));

        assert_eq!(config.acquire_timeout(), Duration::from_secs(30));
    }
}
