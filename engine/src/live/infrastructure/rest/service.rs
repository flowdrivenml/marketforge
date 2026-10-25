use super::{
    ExchangeRestClient, ExchangeRestResponse, RestError, RestErrorKind, RestRequest,
    RestRequestResolver, RestRetryPolicy,
};
use crate::live::config::ExchangeConfigStore;

/// Public execution interface for exchange REST operations.
///
/// The service borrows the immutable exchange configuration
/// store and owns the already-constructed REST client.
///
/// Request resolution remains separate from HTTP execution.
///
/// The client retains its existing shared RateLimitManager.
pub struct ExchangeRestService<'a> {
    configs: &'a ExchangeConfigStore,
    client: ExchangeRestClient,
}

impl<'a> ExchangeRestService<'a> {
    /// Construct a service using an existing REST client.
    ///
    /// Does not rebuild limiters or reload configuration.
    pub fn new(configs: &'a ExchangeConfigStore, client: ExchangeRestClient) -> Self {
        Self { configs, client }
    }

    /// Execute one logical REST request without retries.
    ///
    /// The HTTP client performs:
    /// - Rate-limit admission.
    /// - HTTP transport.
    /// - Response-size enforcement.
    /// - Throttling feedback.
    /// - HTTP error classification.
    /// - Application-response validation.
    pub async fn execute(&self, request: RestRequest) -> Result<ExchangeRestResponse, RestError> {
        let resolved = self.resolve(&request)?;

        self.client.execute_once(&resolved).await
    }

    /// Execute a logical REST request with a retry policy.
    ///
    /// Resolution runs once. Each HTTP attempt subsequently
    /// passes through the existing REST client's limiter.
    ///
    /// Retries are governed by RestRetryPolicy and the
    /// client's existing error classification.
    pub async fn execute_with_retry(
        &self,
        request: RestRequest,
        policy: RestRetryPolicy,
    ) -> Result<ExchangeRestResponse, RestError> {
        let resolved = self.resolve(&request)?;

        self.client.execute_with_retry(&resolved, policy).await
    }

    /// Resolve a request before any HTTP operation occurs.
    fn resolve(&self, request: &RestRequest) -> Result<super::ResolvedRestRequest, RestError> {
        let resolver = RestRequestResolver::new(self.configs);

        resolver.resolve(request).map_err(|error| {
            RestError::new(
                RestErrorKind::Configuration,
                request.profile(),
                request.endpoint(),
                error.to_string(),
            )
        })
    }

    /// Access immutable exchange configuration.
    pub fn configs(&self) -> &ExchangeConfigStore {
        self.configs
    }

    /// Access the already-constructed REST client.
    pub fn client(&self) -> &ExchangeRestClient {
        &self.client
    }
}
