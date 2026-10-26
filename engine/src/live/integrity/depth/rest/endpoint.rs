use crate::live::{
    config::{
        exchange::{ExchangeConfig, RestEndpoint},
        format::DepthFormatConfig,
    },
    infrastructure::normalization::{DepthMessageKind, NormalizationSource, NormalizedDepth},
};

use crate::live::integrity::depth::binding::DepthSourceBinding;
use crate::live::integrity::depth::rest::request::RestDepthRequestContext;
use crate::live::integrity::source_plan_compatibility::SourcePlanCompatibility;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestDepthEndpointError {
    WrongExchangeProfile,
    WrongMarketProfile,
    WrongFormatProfile,

    WrongEndpoint,
    UnknownEndpoint,
    UnsupportedHttpMethod,
    MissingResponseMapping,

    MissingLimitParameter,
    MissingMaximumLimit,
    InvalidRequestedLimit,
    RequestedLimitExceeded { requested: u64, maximum: u64 },

    WrongResponseSource,
    WrongResponseKind,
    MissingSequence,
    InvalidResponseDepth,
    ResponseDepthExceeded { declared: u64, requested: u64 },
}

// ============================================================
// COMPILED ENDPOINT POLICY
// ============================================================

/// Immutable, exchange-profile-specific REST depth policy.
///
/// Compile once from validated configuration.
///
/// This policy establishes configured endpoint identity
/// and admissible request/response coverage.
///
/// It does not perform HTTP requests or authenticate
/// received network bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledRestDepthEndpoint {
    endpoint_key: String,
    maximum_limit: u64,
}

impl CompiledRestDepthEndpoint {
    pub fn compile(
        binding: &DepthSourceBinding,
        exchange: &ExchangeConfig,
        format: &DepthFormatConfig,
    ) -> Result<Self, RestDepthEndpointError> {
        // ----------------------------------------------------
        // PROFILE COMPATIBILITY
        // ----------------------------------------------------

        SourcePlanCompatibility::validate(binding.exchange(), binding.market(), exchange.profile())
            .map_err(|_| RestDepthEndpointError::WrongExchangeProfile)?;

        if exchange.market().as_str() != binding.market() {
            return Err(RestDepthEndpointError::WrongMarketProfile);
        }

        SourcePlanCompatibility::validate(binding.exchange(), binding.market(), &format.format.id)
            .map_err(|_| RestDepthEndpointError::WrongFormatProfile)?;

        if format.synchronization.snapshot_source != "rest" {
            return Err(RestDepthEndpointError::WrongEndpoint);
        }

        // ----------------------------------------------------
        // ENDPOINT IDENTITY
        // ----------------------------------------------------

        let key = format
            .synchronization
            .snapshot_endpoint
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(RestDepthEndpointError::WrongEndpoint)?;

        let endpoint = exchange
            .endpoint(key)
            .ok_or(RestDepthEndpointError::UnknownEndpoint)?;

        let maximum_limit = validate_endpoint(endpoint)?;

        Ok(Self {
            endpoint_key: key.to_owned(),
            maximum_limit,
        })
    }

    pub fn endpoint_key(&self) -> &str {
        &self.endpoint_key
    }

    pub fn maximum_limit(&self) -> u64 {
        self.maximum_limit
    }

    // ========================================================
    // REQUEST VALIDATION
    // ========================================================

    pub fn validate_request(
        &self,
        request: &RestDepthRequestContext,
    ) -> Result<(), RestDepthEndpointError> {
        if request.endpoint_key != self.endpoint_key {
            return Err(RestDepthEndpointError::WrongEndpoint);
        }

        if request.requested_limit == 0 {
            return Err(RestDepthEndpointError::InvalidRequestedLimit);
        }

        if request.requested_limit > self.maximum_limit {
            return Err(RestDepthEndpointError::RequestedLimitExceeded {
                requested: request.requested_limit,
                maximum: self.maximum_limit,
            });
        }

        Ok(())
    }

    // ========================================================
    // RESPONSE VALIDATION
    // ========================================================

    pub fn validate_response(
        &self,
        request: &RestDepthRequestContext,
        response: &NormalizedDepth,
    ) -> Result<(), RestDepthEndpointError> {
        self.validate_request(request)?;

        if response.source() != NormalizationSource::Rest {
            return Err(RestDepthEndpointError::WrongResponseSource);
        }

        if response.kind() != DepthMessageKind::Snapshot {
            return Err(RestDepthEndpointError::WrongResponseKind);
        }

        if response.sequence().sequence_last.is_none() {
            return Err(RestDepthEndpointError::MissingSequence);
        }

        // The normalization contract does not promise that
        // declared_depth is the number of returned records.
        //
        // When present, we interpret it as the declared
        // requested/available depth ceiling.
        if let Some(declared) = response.declared_depth() {
            if declared == 0 {
                return Err(RestDepthEndpointError::InvalidResponseDepth);
            }

            if declared > request.requested_limit {
                return Err(RestDepthEndpointError::ResponseDepthExceeded {
                    declared,
                    requested: request.requested_limit,
                });
            }
        }

        Ok(())
    }
}

// ============================================================
// CONFIGURED ENDPOINT VALIDATION
// ============================================================

fn validate_endpoint(endpoint: &RestEndpoint) -> Result<u64, RestDepthEndpointError> {
    // --------------------------------------------------------
    // HTTP METHOD
    // --------------------------------------------------------

    if !endpoint.method.eq_ignore_ascii_case("GET") {
        return Err(RestDepthEndpointError::UnsupportedHttpMethod);
    }

    // --------------------------------------------------------
    // AUTHORITATIVE SNAPSHOT SEQUENCE
    // --------------------------------------------------------

    // The REST endpoint configuration identifies the native
    // sequence field.
    //
    // Bid/ask extraction belongs to the depth normalization
    // configuration, not the transport endpoint contract.
    let has_sequence_mapping = endpoint
        .response_sequence_field
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        || endpoint
            .response_update_id_field
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());

    if !has_sequence_mapping {
        return Err(RestDepthEndpointError::MissingResponseMapping);
    }

    // --------------------------------------------------------
    // REQUEST LIMIT PARAMETER
    // --------------------------------------------------------

    let has_limit_parameter = match endpoint.limit_parameter.as_deref() {
        Some(name) => endpoint
            .parameters
            .iter()
            .any(|parameter| parameter == name),

        None => endpoint
            .parameters
            .iter()
            .any(|parameter| parameter == "limit"),
    };

    if !has_limit_parameter {
        return Err(RestDepthEndpointError::MissingLimitParameter);
    }

    // --------------------------------------------------------
    // MAXIMUM REQUEST LIMIT
    // --------------------------------------------------------

    let maximum = endpoint
        .maximum_limit
        .ok_or(RestDepthEndpointError::MissingMaximumLimit)?;

    if maximum == 0 {
        return Err(RestDepthEndpointError::InvalidRequestedLimit);
    }

    Ok(maximum)
}
#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::config::exchange::RestEndpoint;

    fn endpoint() -> RestEndpoint {
        toml::from_str(
            r#"
            method = "GET"
            path = "/api/v3/depth"

            parameters = ["symbol", "limit"]
            limit_parameter = "limit"

            default_limit = 100
            maximum_limit = 5000

            response_update_id_field = "lastUpdateId"
            response_bids_field = "bids"
            response_asks_field = "asks"
            "#,
        )
        .unwrap()
    }

    fn policy() -> CompiledRestDepthEndpoint {
        CompiledRestDepthEndpoint {
            endpoint_key: "order_book_snapshot".into(),
            maximum_limit: 5000,
        }
    }

    fn request(limit: u64) -> RestDepthRequestContext {
        RestDepthRequestContext {
            source_epoch: crate::live::integrity::depth::binding::DepthSourceEpoch {
                stream_generation: 10,
                connection_generation: 20,
                subscription_generation: 30,
            },

            sync_token: crate::live::integrity::depth::buffer::DepthSyncToken {
                stream_generation: 10,
                attempt: 1,
            },

            endpoint_key: "order_book_snapshot".into(),
            requested_limit: limit,
        }
    }

    #[test]
    fn accepts_valid_endpoint_configuration() {
        assert_eq!(validate_endpoint(&endpoint()), Ok(5000),);
    }

    #[test]
    fn rejects_non_get_endpoint() {
        let mut endpoint = endpoint();
        endpoint.method = "POST".into();

        assert_eq!(
            validate_endpoint(&endpoint),
            Err(RestDepthEndpointError::UnsupportedHttpMethod),
        );
    }

    #[test]
    fn rejects_missing_sequence_mapping() {
        let mut endpoint = endpoint();
        endpoint.response_update_id_field = None;
        endpoint.response_sequence_field = None;

        assert_eq!(
            validate_endpoint(&endpoint),
            Err(RestDepthEndpointError::MissingResponseMapping),
        );
    }

    #[test]
    fn rejects_missing_maximum_limit() {
        let mut endpoint = endpoint();
        endpoint.maximum_limit = None;

        assert_eq!(
            validate_endpoint(&endpoint),
            Err(RestDepthEndpointError::MissingMaximumLimit),
        );
    }

    #[test]
    fn accepts_limit_at_exact_maximum() {
        assert_eq!(policy().validate_request(&request(5000)), Ok(()),);
    }

    #[test]
    fn rejects_requested_limit_above_maximum() {
        assert_eq!(
            policy().validate_request(&request(5001)),
            Err(RestDepthEndpointError::RequestedLimitExceeded {
                requested: 5001,
                maximum: 5000,
            }),
        );
    }

    #[test]
    fn rejects_wrong_endpoint_key() {
        let mut request = request(100);
        request.endpoint_key = "recent_trades".into();

        assert_eq!(
            policy().validate_request(&request),
            Err(RestDepthEndpointError::WrongEndpoint),
        );
    }

    #[test]
    fn rejects_zero_requested_limit() {
        assert_eq!(
            policy().validate_request(&request(0)),
            Err(RestDepthEndpointError::InvalidRequestedLimit),
        );
    }
    #[test]
    fn real_binance_spot_endpoint_has_valid_snapshot_contract() {
        use std::path::PathBuf;

        use crate::live::config::exchange::ExchangeConfig;

        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/exchanges/binance_spot.toml");

        let config = ExchangeConfig::load(path).unwrap();

        let endpoint = config
            .endpoint("depth_snapshot")
            .expect("Binance Spot depth snapshot endpoint exists");

        let maximum =
            validate_endpoint(endpoint).expect("Binance Spot REST snapshot endpoint must validate");

        assert_eq!(maximum, 5000);
    }
}
