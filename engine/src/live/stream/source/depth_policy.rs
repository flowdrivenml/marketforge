use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDepthRecoveryPolicy {
    pub endpoint_key: String,
    pub requested_limit: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SourceDepthRecoveryPolicyError {
    #[error("REST endpoint key cannot be empty")]
    EmptyEndpoint,

    #[error("REST requested depth must be positive")]
    InvalidLimit,
}

impl SourceDepthRecoveryPolicy {
    pub fn new(
        endpoint_key: impl Into<String>,
        requested_limit: u64,
    ) -> Result<Self, SourceDepthRecoveryPolicyError> {
        let endpoint_key = endpoint_key.into();

        if endpoint_key.trim().is_empty() {
            return Err(SourceDepthRecoveryPolicyError::EmptyEndpoint);
        }

        if requested_limit == 0 {
            return Err(SourceDepthRecoveryPolicyError::InvalidLimit);
        }

        Ok(Self {
            endpoint_key,
            requested_limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_rest_policy() {
        let policy = SourceDepthRecoveryPolicy::new("depth_snapshot", 1000).unwrap();

        assert_eq!(policy.endpoint_key, "depth_snapshot");
        assert_eq!(policy.requested_limit, 1000);
    }

    #[test]
    fn rejects_empty_endpoint() {
        assert!(matches!(
            SourceDepthRecoveryPolicy::new("", 100),
            Err(SourceDepthRecoveryPolicyError::EmptyEndpoint)
        ));
    }

    #[test]
    fn rejects_zero_limit() {
        assert!(matches!(
            SourceDepthRecoveryPolicy::new("depth_snapshot", 0),
            Err(SourceDepthRecoveryPolicyError::InvalidLimit)
        ));
    }
}
