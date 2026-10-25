use std::collections::BTreeMap;

use crate::live::config::exchange::WeightPolicy;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WeightError {
    #[error("unsupported weight policy type '{0}'")]
    UnsupportedPolicy(String),

    #[error("weight policy requires a positive fixed cost")]
    InvalidFixedCost,

    #[error("weight policy requires a parameter name")]
    MissingParameterName,

    #[error("missing request parameter '{0}'")]
    MissingParameter(String),

    #[error("weight policy requires at least one tier")]
    MissingTiers,

    #[error("weight tier must define exactly one threshold")]
    InvalidTierThreshold,

    #[error("weight tier threshold must be greater than zero")]
    InvalidTierValue,

    #[error("weight tier cost must be greater than zero")]
    InvalidTierCost,

    #[error("duplicate weight tier threshold {0}")]
    DuplicateTier(u64),

    #[error(
        "no applicable weight tier for parameter '{parameter}' \
         with value {value}"
    )]
    NoMatchingTier { parameter: String, value: u64 },

    #[error("tiered weight costs must not decrease as thresholds increase")]
    NonMonotonicWeights,
}

// ============================================================
// REQUEST PARAMETERS
// ============================================================

/// Numeric REST request parameters used when calculating
/// parameter-dependent request weights.
///
/// Examples:
///
/// - limit = 100
/// - depth = 500
///
/// Parameter names remain exchange-native.
pub type WeightParameters = BTreeMap<String, u64>;

// ============================================================
// COMPILED WEIGHT POLICY
// ============================================================

/// Runtime representation of a validated request-weight
/// calculation strategy.
///
/// The exchange configuration is compiled once during
/// infrastructure initialization.
///
/// REST requests only perform the final cost lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledWeightPolicy {
    /// Fixed weighted cost.
    Fixed { cost: u64 },

    /// Fixed request-count cost.
    RequestCount { cost: u64 },

    /// Exact parameter-value matching.
    ///
    /// A request must match one configured tier.
    Discrete {
        parameter: String,
        tiers: BTreeMap<u64, u64>,
    },

    /// Upper-bound tier matching.
    ///
    /// The smallest threshold >= request value is used.
    Tiered {
        parameter: String,
        tiers: BTreeMap<u64, u64>,
    },
}

// ============================================================
// COMPILATION
// ============================================================

impl CompiledWeightPolicy {
    /// Compile a configured exchange weight policy.
    ///
    /// Compilation validates the strategy and its required
    /// parameters before any REST requests are executed.
    pub fn compile(policy: &WeightPolicy) -> Result<Self, WeightError> {
        match policy.kind.as_str() {
            "fixed" => {
                let cost = policy
                    .weight
                    .filter(|cost| *cost > 0)
                    .ok_or(WeightError::InvalidFixedCost)?;

                Ok(Self::Fixed { cost })
            }

            "request_count" => {
                let cost = policy
                    .cost
                    .filter(|cost| *cost > 0)
                    .ok_or(WeightError::InvalidFixedCost)?;

                Ok(Self::RequestCount { cost })
            }

            "discrete" => {
                let parameter = Self::require_parameter(policy)?;

                let tiers = Self::compile_tiers(policy, false)?;

                Ok(Self::Discrete { parameter, tiers })
            }

            "tiered" => {
                let parameter = Self::require_parameter(policy)?;

                let tiers = Self::compile_tiers(policy, true)?;

                Ok(Self::Tiered { parameter, tiers })
            }

            other => Err(WeightError::UnsupportedPolicy(other.to_owned())),
        }
    }

    // ========================================================
    // PARAMETER VALIDATION
    // ========================================================

    fn require_parameter(policy: &WeightPolicy) -> Result<String, WeightError> {
        policy
            .parameter
            .as_ref()
            .filter(|parameter| !parameter.trim().is_empty())
            .cloned()
            .ok_or(WeightError::MissingParameterName)
    }

    // ========================================================
    // TIER COMPILATION
    // ========================================================

    /// Compile configured tiers into an ordered lookup table.
    ///
    /// Discrete:
    ///     threshold = exact supported parameter value.
    ///
    /// Tiered:
    ///     threshold = inclusive upper bound.
    ///
    /// Each configured tier must declare exactly one of
    /// `limit` or `max`.
    fn compile_tiers(
        policy: &WeightPolicy,
        enforce_monotonic_costs: bool,
    ) -> Result<BTreeMap<u64, u64>, WeightError> {
        if policy.tiers.is_empty() {
            return Err(WeightError::MissingTiers);
        }

        let mut tiers = BTreeMap::new();

        for tier in &policy.tiers {
            let threshold = match (tier.limit, tier.max) {
                (Some(value), None) | (None, Some(value)) => value,

                _ => {
                    return Err(WeightError::InvalidTierThreshold);
                }
            };

            if threshold == 0 {
                return Err(WeightError::InvalidTierValue);
            }

            if tier.weight == 0 {
                return Err(WeightError::InvalidTierCost);
            }

            if tiers.insert(threshold, tier.weight).is_some() {
                return Err(WeightError::DuplicateTier(threshold));
            }
        }

        // For upper-bound tier selection, larger requests
        // must not become cheaper than smaller requests.
        //
        // This is a configuration consistency safeguard.
        if enforce_monotonic_costs {
            let mut previous_cost = 0;

            for &cost in tiers.values() {
                if cost < previous_cost {
                    return Err(WeightError::NonMonotonicWeights);
                }

                previous_cost = cost;
            }
        }

        Ok(tiers)
    }

    // ========================================================
    // COST CALCULATION
    // ========================================================

    /// Calculate the numeric request cost.
    ///
    /// Does not consume any rate-limit capacity.
    pub fn calculate(&self, parameters: &WeightParameters) -> Result<u64, WeightError> {
        match self {
            Self::Fixed { cost } | Self::RequestCount { cost } => Ok(*cost),

            Self::Discrete { parameter, tiers } => {
                let value = Self::parameter_value(parameters, parameter)?;

                tiers
                    .get(&value)
                    .copied()
                    .ok_or_else(|| WeightError::NoMatchingTier {
                        parameter: parameter.clone(),
                        value,
                    })
            }

            Self::Tiered { parameter, tiers } => {
                let value = Self::parameter_value(parameters, parameter)?;

                tiers
                    .range(value..)
                    .next()
                    .map(|(_, cost)| *cost)
                    .ok_or_else(|| WeightError::NoMatchingTier {
                        parameter: parameter.clone(),
                        value,
                    })
            }
        }
    }

    fn parameter_value(parameters: &WeightParameters, name: &str) -> Result<u64, WeightError> {
        parameters
            .get(name)
            .copied()
            .ok_or_else(|| WeightError::MissingParameter(name.to_owned()))
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::config::exchange::WeightTier;

    fn policy(kind: &str, parameter: Option<&str>, tiers: Vec<WeightTier>) -> WeightPolicy {
        WeightPolicy {
            kind: kind.to_owned(),
            weight: None,
            parameter: parameter.map(str::to_owned),
            tiers,
            cost: None,
        }
    }

    fn tier(threshold: u64, weight: u64) -> WeightTier {
        WeightTier {
            limit: Some(threshold),
            max: None,
            weight,
        }
    }

    fn parameters(name: &str, value: u64) -> WeightParameters {
        BTreeMap::from([(name.to_owned(), value)])
    }

    #[test]
    fn calculates_fixed_weight() {
        let mut config = policy("fixed", None, vec![]);
        config.weight = Some(20);

        let compiled = CompiledWeightPolicy::compile(&config).unwrap();

        assert_eq!(compiled.calculate(&WeightParameters::new()).unwrap(), 20);
    }

    #[test]
    fn calculates_request_count_cost() {
        let mut config = policy("request_count", None, vec![]);

        config.cost = Some(1);

        let compiled = CompiledWeightPolicy::compile(&config).unwrap();

        assert_eq!(compiled.calculate(&WeightParameters::new()).unwrap(), 1);
    }

    #[test]
    fn discrete_policy_requires_exact_match() {
        let config = policy(
            "discrete",
            Some("limit"),
            vec![tier(100, 5), tier(500, 10), tier(1000, 20)],
        );

        let compiled = CompiledWeightPolicy::compile(&config).unwrap();

        assert_eq!(compiled.calculate(&parameters("limit", 500)).unwrap(), 10);

        assert!(matches!(
            compiled.calculate(&parameters("limit", 200)),
            Err(WeightError::NoMatchingTier { .. })
        ));
    }

    #[test]
    fn tiered_policy_uses_smallest_upper_bound() {
        let config = policy(
            "tiered",
            Some("limit"),
            vec![tier(100, 5), tier(500, 10), tier(1000, 20)],
        );

        let compiled = CompiledWeightPolicy::compile(&config).unwrap();

        assert_eq!(compiled.calculate(&parameters("limit", 50)).unwrap(), 5);

        assert_eq!(compiled.calculate(&parameters("limit", 200)).unwrap(), 10);

        assert_eq!(compiled.calculate(&parameters("limit", 1000)).unwrap(), 20);
    }

    #[test]
    fn rejects_missing_parameter() {
        let config = policy("tiered", Some("limit"), vec![tier(100, 5)]);

        let compiled = CompiledWeightPolicy::compile(&config).unwrap();

        assert!(matches!(
            compiled.calculate(&WeightParameters::new()),
            Err(WeightError::MissingParameter(_))
        ));
    }

    #[test]
    fn rejects_duplicate_thresholds() {
        let config = policy("discrete", Some("limit"), vec![tier(100, 5), tier(100, 10)]);

        assert!(matches!(
            CompiledWeightPolicy::compile(&config),
            Err(WeightError::DuplicateTier(100))
        ));
    }

    #[test]
    fn rejects_decreasing_tiered_weights() {
        let config = policy("tiered", Some("limit"), vec![tier(100, 10), tier(500, 5)]);

        assert!(matches!(
            CompiledWeightPolicy::compile(&config),
            Err(WeightError::NonMonotonicWeights)
        ));
    }
}
