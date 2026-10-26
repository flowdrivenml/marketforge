use crate::live::integrity::depth::binding::DepthSourceEpoch;
use crate::live::integrity::depth::buffer::DepthSyncToken;

/// Trusted metadata attached to one REST snapshot request.
///
/// This context is created by the source runtime when
/// initiating the HTTP operation.
///
/// It must accompany the completion of that same request.
///
/// `requested_limit` is the endpoint's requested book depth,
/// not the WebSocket subscription depth.
///
/// The exact meaning of response-declared coverage remains
/// exchange/endpoint-specific.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestDepthRequestContext {
    pub source_epoch: DepthSourceEpoch,
    pub sync_token: DepthSyncToken,

    /// Configured REST endpoint identity, not a network URL.
    ///
    /// For example, a key identifying the configured
    /// exchange-native order-book snapshot operation.
    pub endpoint_key: String,

    pub requested_limit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestDepthRequestError {
    EmptyEndpoint,
    InvalidRequestedLimit,
    EpochMismatch,
    AttemptMismatch,
}

impl RestDepthRequestContext {
    pub fn validate(&self, epoch: DepthSourceEpoch) -> Result<(), RestDepthRequestError> {
        if self.endpoint_key.trim().is_empty() {
            return Err(RestDepthRequestError::EmptyEndpoint);
        }

        if self.requested_limit == 0 {
            return Err(RestDepthRequestError::InvalidRequestedLimit);
        }

        if self.source_epoch != epoch {
            return Err(RestDepthRequestError::EpochMismatch);
        }

        if self.sync_token.stream_generation != epoch.stream_generation {
            return Err(RestDepthRequestError::AttemptMismatch);
        }

        if self.sync_token.attempt == 0 {
            return Err(RestDepthRequestError::AttemptMismatch);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch() -> DepthSourceEpoch {
        DepthSourceEpoch {
            stream_generation: 10,
            connection_generation: 20,
            subscription_generation: 30,
        }
    }

    fn request() -> RestDepthRequestContext {
        RestDepthRequestContext {
            source_epoch: epoch(),
            sync_token: DepthSyncToken {
                stream_generation: 10,
                attempt: 1,
            },
            endpoint_key: "orderbook_snapshot".into(),
            requested_limit: 1000,
        }
    }

    #[test]
    fn accepts_valid_request() {
        assert_eq!(request().validate(epoch()), Ok(()));
    }

    #[test]
    fn rejects_wrong_source_epoch() {
        let different = DepthSourceEpoch {
            connection_generation: 21,
            ..epoch()
        };

        assert_eq!(
            request().validate(different),
            Err(RestDepthRequestError::EpochMismatch),
        );
    }

    #[test]
    fn rejects_missing_endpoint() {
        let mut request = request();
        request.endpoint_key.clear();

        assert_eq!(
            request.validate(epoch()),
            Err(RestDepthRequestError::EmptyEndpoint),
        );
    }

    #[test]
    fn rejects_zero_requested_limit() {
        let mut request = request();
        request.requested_limit = 0;

        assert_eq!(
            request.validate(epoch()),
            Err(RestDepthRequestError::InvalidRequestedLimit),
        );
    }

    #[test]
    fn rejects_wrong_stream_generation() {
        let mut request = request();
        request.sync_token.stream_generation = 9;

        assert_eq!(
            request.validate(epoch()),
            Err(RestDepthRequestError::AttemptMismatch),
        );
    }
}
