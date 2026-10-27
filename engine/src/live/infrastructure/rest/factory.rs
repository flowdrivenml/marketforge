use super::application::RestApplicationRule;
use super::{ExchangeRestClient, RestClientConfig, RestClientFeedbackConfig, RestUsageBinding};
use crate::live::config::exchange::{RestApplicationResponseStrategy, RestConfig};
use crate::live::infrastructure::limiter::RateLimitManager;
use crate::live::infrastructure::limiter::factory::CompiledRestLimiters;
use std::collections::BTreeMap;
use std::time::Duration;

// ============================================================
// COMPILED APPLICATION RESPONSE CONFIGURATION
// ============================================================

/// Application-response rules compiled for one exchange
/// profile.
///
/// Only endpoints using the Envelope strategy appear
/// in `rules`.
///
/// Endpoints using Raw are deliberately excluded.
///
/// Compilation nevertheless requires every configured
/// endpoint to declare an explicit response strategy.
#[derive(Debug, Clone)]
pub struct CompiledRestApplicationRules {
    rules: BTreeMap<String, RestApplicationRule>,
    endpoint_count: usize,
    raw_endpoint_count: usize,
}

impl CompiledRestApplicationRules {
    /// Returns an application validator for an endpoint,
    /// if the endpoint uses an application envelope.
    pub fn rule(&self, endpoint: &str) -> Option<&RestApplicationRule> {
        self.rules.get(endpoint)
    }

    /// Returns all compiled envelope rules.
    ///
    /// The returned map can be cloned into the existing
    /// RestClientFeedbackConfig.
    pub fn rules(&self) -> &BTreeMap<String, RestApplicationRule> {
        &self.rules
    }

    /// Number of configured REST endpoints.
    pub fn endpoint_count(&self) -> usize {
        self.endpoint_count
    }

    /// Number of explicitly declared raw endpoints.
    pub fn raw_endpoint_count(&self) -> usize {
        self.raw_endpoint_count
    }

    /// Number of endpoints using application envelopes.
    pub fn envelope_endpoint_count(&self) -> usize {
        self.rules.len()
    }
}

// ============================================================
// REST APPLICATION RESPONSE COMPILER
// ============================================================

/// Compiles declarative exchange response configuration
/// into runtime application-validation rules.
///
/// This is one component of the REST factory.
///
/// It performs no network operations, owns no mutable
/// runtime state, and does not instantiate rate limiters.
pub struct RestApplicationRuleCompiler;

impl RestApplicationRuleCompiler {
    pub fn compile(config: &RestConfig) -> Result<CompiledRestApplicationRules, String> {
        let mut rules = BTreeMap::new();

        let mut raw_endpoint_count = 0usize;

        // ====================================================
        // COMPILE EVERY ENDPOINT
        // ====================================================

        for (endpoint_name, endpoint) in &config.endpoints {
            // =================================================
            // REQUIRE EXPLICIT CONFIGURATION
            // =================================================

            let application = endpoint.application_response.as_ref().ok_or_else(|| {
                format!(
                    "rest.endpoints.{endpoint_name}: \
                         missing application_response configuration"
                )
            })?;

            // =================================================
            // VALIDATE CONFIGURATION
            // =================================================

            application.validate().map_err(|reason| {
                format!(
                    "rest.endpoints.{endpoint_name}.\
                     application_response: {reason}"
                )
            })?;

            // =================================================
            // COMPILE STRATEGY
            // =================================================

            match application.strategy {
                // =============================================
                // RAW JSON
                // =============================================
                RestApplicationResponseStrategy::Raw => {
                    raw_endpoint_count += 1;
                }

                // =============================================
                // APPLICATION ENVELOPE
                // =============================================
                RestApplicationResponseStrategy::Envelope => {
                    let code_field = application.code_field.as_ref().ok_or_else(|| {
                        format!(
                            "rest.endpoints.{endpoint_name}: \
                                 envelope requires code_field"
                        )
                    })?;

                    let rule = RestApplicationRule::new(
                        code_field.clone(),
                        application.success_codes.clone(),
                        application.message_field.clone(),
                        application.data_field.clone(),
                    )
                    .map_err(|error| {
                        format!(
                            "rest.endpoints.{endpoint_name}: \
                             cannot compile application rule: \
                             {error}"
                        )
                    })?;

                    if rules.insert(endpoint_name.clone(), rule).is_some() {
                        return Err(format!(
                            "duplicate application rule for \
                             endpoint '{endpoint_name}'"
                        ));
                    }
                }
            }
        }

        // ====================================================
        // VERIFY COMPILATION COMPLETENESS
        // ====================================================

        let endpoint_count = config.endpoints.len();

        if raw_endpoint_count + rules.len() != endpoint_count {
            return Err("REST application-response compilation \
                 produced incomplete endpoint coverage"
                .into());
        }

        Ok(CompiledRestApplicationRules {
            rules,
            endpoint_count,
            raw_endpoint_count,
        })
    }
}

// ============================================================
// FACTORY FEEDBACK POLICY
// ============================================================

/// Runtime feedback settings for one REST exchange profile.
///
/// Usage-header bindings must be established from verified
/// exchange semantics.
///
/// The factory does not invent usage mappings.
///
/// A positive fallback cooldown is required so that unknown
/// throttling conditions never produce a zero-duration
/// protection period.
#[derive(Debug, Clone)]
pub struct RestFactoryFeedbackPolicy {
    pub usage_bindings: Vec<RestUsageBinding>,

    pub fallback_cooldown: Duration,
}

impl RestFactoryFeedbackPolicy {
    pub fn new(
        usage_bindings: Vec<RestUsageBinding>,
        fallback_cooldown: Duration,
    ) -> Result<Self, String> {
        if fallback_cooldown.is_zero() {
            return Err("REST fallback cooldown must be positive".into());
        }

        Ok(Self {
            usage_bindings,
            fallback_cooldown,
        })
    }
}

// ============================================================
// REST CLIENT FACTORY
// ============================================================

/// Assembles one reusable HTTP client from existing,
/// validated infrastructure.
///
/// Ownership contract:
///
/// - The caller owns exchange configuration loading.
/// - The caller owns compilation of rate-limit policies.
/// - The supplied limiter retains its existing shared state.
/// - The returned client owns its immutable profile settings.
/// - Application-response configuration is compiled here.
///
/// This factory never creates or resets rate-limit budgets.
pub struct ExchangeRestClientFactory;

impl ExchangeRestClientFactory {
    pub fn build(
        limiter: RateLimitManager,
        compiled_profiles: BTreeMap<String, CompiledRestLimiters>,
        rest_profiles: &BTreeMap<String, RestConfig>,
        feedback_policies: BTreeMap<String, RestFactoryFeedbackPolicy>,
        client_config: RestClientConfig,
    ) -> Result<ExchangeRestClient, String> {
        // ============================================================
        // VERIFY COMPLETE PROFILE COVERAGE
        // ============================================================

        validate_profile_coverage(rest_profiles, &compiled_profiles, &feedback_policies)?;

        // ====================================================
        // COMPILE APPLICATION RESPONSE RULES
        // ====================================================

        let mut feedback = BTreeMap::new();

        for (name, config) in rest_profiles {
            // All configured endpoints must explicitly
            // declare an application-response strategy.
            let application = RestApplicationRuleCompiler::compile(config).map_err(|error| {
                format!(
                    "{name}: application-response \
                             compilation failed: {error}"
                )
            })?;

            // A policy is guaranteed by the preceding
            // coverage checks.
            let policy = feedback_policies
                .get(name)
                .ok_or_else(|| format!("{name}: missing REST feedback policy"))?;

            if policy.fallback_cooldown.is_zero() {
                return Err(format!(
                    "{name}: REST fallback cooldown \
                     must be positive"
                ));
            }

            // =================================================
            // ASSEMBLE CLIENT FEEDBACK CONFIGURATION
            // =================================================

            let settings = RestClientFeedbackConfig {
                rate_limiting: config.rate_limiting.clone(),

                usage_bindings: policy.usage_bindings.clone(),

                fallback_cooldown: policy.fallback_cooldown,

                application_rules: application.rules().clone(),
            };

            settings.validate().map_err(|error| {
                format!(
                    "{name}: invalid REST client feedback \
                     configuration: {error}"
                )
            })?;

            feedback.insert(name.clone(), settings);
        }

        // ====================================================
        // CONSTRUCT CLIENT USING EXISTING SHARED LIMITER
        // ====================================================

        // ExchangeRestClient::new performs its own
        // consistency validation.
        //
        // The supplied RateLimitManager shares state with
        // its existing clones; no new limiter is created.

        ExchangeRestClient::new(limiter, compiled_profiles, feedback, client_config)
    }
}

// ============================================================
// REST FACTORY PROFILE COVERAGE VALIDATION
// ============================================================

/// Validate that all REST factory inputs cover exactly
/// the same exchange profiles.
///
/// A profile must exist in:
///
/// - Exchange REST configurations.
/// - Compiled REST rate-limit profiles.
/// - REST feedback policies.
///
/// Missing or additional profiles are rejected.
///
/// This does not yet prove that a compiled limiter was
/// generated from the current version of its configuration.
/// That requires configuration identity validation.
fn validate_profile_coverage(
    rest_profiles: &BTreeMap<String, RestConfig>,
    compiled_profiles: &BTreeMap<String, CompiledRestLimiters>,
    feedback_policies: &BTreeMap<String, RestFactoryFeedbackPolicy>,
) -> Result<(), String> {
    if rest_profiles.is_empty() {
        return Err("REST factory requires at least one profile".into());
    }

    for name in rest_profiles.keys() {
        if !compiled_profiles.contains_key(name) {
            return Err(format!(
                "{name}: missing compiled REST \
                 rate-limit configuration"
            ));
        }

        if !feedback_policies.contains_key(name) {
            return Err(format!("{name}: missing REST feedback policy"));
        }
    }

    for name in compiled_profiles.keys() {
        if !rest_profiles.contains_key(name) {
            return Err(format!(
                "{name}: compiled REST limits have \
                 no matching exchange configuration"
            ));
        }
    }

    for name in feedback_policies.keys() {
        if !rest_profiles.contains_key(name) {
            return Err(format!(
                "{name}: REST feedback policy has \
                 no matching exchange configuration"
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use super::super::application::{RestApplicationOutcome, RestApplicationValidator};

    // ========================================================
    // TEST CONFIGURATION
    // ========================================================

    fn rest_config(response: &str) -> RestConfig {
        let toml_text = format!(
            r#"
base_url = "https://example.invalid"

[rest_unused_placeholder]
"#
        );

        // Build a valid RestConfig through its existing
        // TOML deserialization contract.
        //
        // Endpoint declaration format matches our
        // exchange profiles.
        let toml_text = toml_text.replace(
            "[rest_unused_placeholder]",
            &format!(
                r#"
[endpoints.depth_snapshot]
method = "GET"
path = "/depth"
parameters = ["symbol"]

[endpoints.depth_snapshot.application_response]
{response}
"#
            ),
        );

        toml::from_str(&toml_text).unwrap()
    }

    // ========================================================
    // RAW RESPONSE
    // ========================================================

    #[test]
    fn compiles_raw_response() {
        let config = rest_config(
            r#"
strategy = "raw"
"#,
        );

        let compiled = RestApplicationRuleCompiler::compile(&config).unwrap();

        assert_eq!(compiled.endpoint_count(), 1);

        assert_eq!(compiled.raw_endpoint_count(), 1);

        assert_eq!(compiled.envelope_endpoint_count(), 0);

        assert!(compiled.rule("depth_snapshot").is_none());
    }

    // ========================================================
    // APPLICATION ENVELOPE
    // ========================================================

    #[test]
    fn compiles_bybit_envelope() {
        let config = rest_config(
            r#"
strategy = "envelope"
code_field = "retCode"
success_codes = ["0"]
message_field = "retMsg"
data_field = "result"
"#,
        );

        let compiled = RestApplicationRuleCompiler::compile(&config).unwrap();

        assert_eq!(compiled.endpoint_count(), 1);

        assert_eq!(compiled.raw_endpoint_count(), 0);

        assert_eq!(compiled.envelope_endpoint_count(), 1);

        let rule = compiled.rule("depth_snapshot").unwrap();

        assert_eq!(rule.code_field(), "retCode");

        assert_eq!(rule.success_codes(), &["0".to_owned()]);
    }

    // ========================================================
    // RUNTIME COMPATIBILITY
    // ========================================================

    #[test]
    fn compiled_rule_validates_exchange_response() {
        let config = rest_config(
            r#"
strategy = "envelope"
code_field = "code"
success_codes = ["00000"]
message_field = "msg"
data_field = "data"
"#,
        );

        let compiled = RestApplicationRuleCompiler::compile(&config).unwrap();

        let rule = compiled.rule("depth_snapshot").unwrap();

        let successful = json!({
            "code": "00000",
            "msg": "success",
            "data": []
        });

        assert_eq!(
            RestApplicationValidator::validate(rule, &successful,),
            RestApplicationOutcome::Success,
        );

        let rejected = json!({
            "code": "40017",
            "msg": "Invalid request parameter",
            "data": null
        });

        assert!(matches!(
            RestApplicationValidator::validate(rule, &rejected,),
            RestApplicationOutcome::Failure { .. }
        ));
    }

    // ========================================================
    // MISSING CONFIGURATION
    // ========================================================

    #[test]
    fn rejects_missing_response_strategy() {
        let config: RestConfig = toml::from_str(
            r#"
base_url = "https://example.invalid"

[endpoints.depth_snapshot]
method = "GET"
path = "/depth"
parameters = ["symbol"]
"#,
        )
        .unwrap();

        let error = RestApplicationRuleCompiler::compile(&config).unwrap_err();

        assert!(error.contains("missing application_response"), "{error}");
    }

    // ========================================================
    // INVALID CONFIGURATION
    // ========================================================

    #[test]
    fn rejects_invalid_envelope_configuration() {
        let config = rest_config(
            r#"
strategy = "envelope"
success_codes = ["0"]
"#,
        );

        let error = RestApplicationRuleCompiler::compile(&config).unwrap_err();

        assert!(error.contains("code_field"), "{error}");
    }
    // ============================================================
    // REAL EXCHANGE PROFILE COMPILATION
    // ============================================================

    #[test]
    fn compiles_application_rules_for_all_real_exchange_profiles() {
        use std::fs;
        use std::path::PathBuf;

        use crate::live::config::exchange::{ExchangeConfig, RestApplicationResponseStrategy};

        // ========================================================
        // CONFIGURATION DIRECTORY
        // ========================================================

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/exchanges");

        assert!(
            root.is_dir(),
            "exchange configuration directory missing: {}",
            root.display(),
        );

        // ========================================================
        // DISCOVER PROFILES
        // ========================================================

        let mut paths = fs::read_dir(&root)
            .expect("cannot read exchange configuration directory")
            .map(|entry| entry.expect("cannot read directory entry").path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "toml")
            })
            .collect::<Vec<_>>();

        paths.sort();

        assert_eq!(
            paths.len(),
            15,
            "expected exactly 15 exchange configuration profiles",
        );

        // ========================================================
        // EXPECTED PROFILE NAMES
        // ========================================================

        let exchanges = ["binance", "bybit", "okx", "bitget", "gateio"];

        let markets = ["spot", "linear", "inverse"];

        let mut expected_profiles = std::collections::BTreeSet::new();

        for exchange in exchanges {
            for market in markets {
                expected_profiles.insert(format!("{exchange}_{market}"));
            }
        }

        // ========================================================
        // VALIDATE AND COMPILE
        // ========================================================

        let mut actual_profiles = std::collections::BTreeSet::new();

        let mut total_endpoints = 0usize;
        let mut total_raw = 0usize;
        let mut total_envelope = 0usize;

        for path in paths {
            let profile = path
                .file_stem()
                .expect("missing profile filename")
                .to_string_lossy()
                .into_owned();

            assert!(
                expected_profiles.contains(&profile),
                "unexpected exchange profile: {profile}"
            );

            assert!(
                actual_profiles.insert(profile.clone()),
                "duplicate exchange profile: {profile}"
            );

            // ====================================================
            // LOAD TOML
            // ====================================================

            let content = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("{profile}: cannot read configuration: {error}"));

            let config: ExchangeConfig = toml::from_str(&content)
                .unwrap_or_else(|error| panic!("{profile}: TOML deserialization failed: {error}"));

            // ====================================================
            // COMPLETE EXCHANGE CONFIGURATION VALIDATION
            // ====================================================

            config.validate().unwrap_or_else(|error| {
                panic!("{profile}: exchange configuration invalid: {error}")
            });

            // ====================================================
            // APPLICATION-RESPONSE COMPILATION
            // ====================================================

            let compiled =
                RestApplicationRuleCompiler::compile(&config.rest).unwrap_or_else(|error| {
                    panic!(
                        "{profile}: REST application-rule \
                         compilation failed: {error}"
                    )
                });

            assert_eq!(
                compiled.endpoint_count(),
                config.rest.endpoints.len(),
                "{profile}: endpoint count mismatch",
            );

            assert_eq!(
                compiled.raw_endpoint_count() + compiled.envelope_endpoint_count(),
                compiled.endpoint_count(),
                "{profile}: incomplete response strategy coverage",
            );

            // ====================================================
            // VERIFY EACH ENDPOINT
            // ====================================================

            for (endpoint_name, endpoint) in &config.rest.endpoints {
                let response = endpoint.application_response.as_ref().unwrap_or_else(|| {
                    panic!(
                        "{profile}/{endpoint_name}: \
                         application_response missing"
                    )
                });

                let runtime_rule = compiled.rule(endpoint_name);

                match response.strategy {
                    RestApplicationResponseStrategy::Raw => {
                        assert!(
                            runtime_rule.is_none(),
                            "{profile}/{endpoint_name}: \
                         raw endpoint compiled an envelope rule"
                        );
                    }

                    RestApplicationResponseStrategy::Envelope => {
                        let rule = runtime_rule.unwrap_or_else(|| {
                            panic!(
                                "{profile}/{endpoint_name}: \
                             missing compiled envelope rule"
                            )
                        });

                        assert_eq!(
                            Some(rule.code_field()),
                            response.code_field.as_deref(),
                            "{profile}/{endpoint_name}: \
                         application code field mismatch",
                        );

                        assert_eq!(
                            rule.success_codes(),
                            response.success_codes.as_slice(),
                            "{profile}/{endpoint_name}: \
                         application success codes mismatch",
                        );

                        assert_eq!(
                            rule.message_field(),
                            response.message_field.as_deref(),
                            "{profile}/{endpoint_name}: \
                         message field mismatch",
                        );

                        assert_eq!(
                            rule.data_field(),
                            response.data_field.as_deref(),
                            "{profile}/{endpoint_name}: \
                         data field mismatch",
                        );
                    }
                }
            }

            // ====================================================
            // REPORT PROFILE
            // ====================================================

            println!(
                "{profile:<20} endpoints={:<3} raw={:<3} envelope={:<3}",
                compiled.endpoint_count(),
                compiled.raw_endpoint_count(),
                compiled.envelope_endpoint_count(),
            );

            total_endpoints += compiled.endpoint_count();
            total_raw += compiled.raw_endpoint_count();
            total_envelope += compiled.envelope_endpoint_count();
        }

        // ========================================================
        // COMPLETE PROFILE COVERAGE
        // ========================================================

        assert_eq!(
            actual_profiles, expected_profiles,
            "exchange profile coverage mismatch",
        );

        // Based on the current 15-profile configuration set.
        assert_eq!(total_endpoints, 35, "unexpected REST endpoint count");

        assert_eq!(
            total_raw + total_envelope,
            total_endpoints,
            "incomplete REST response strategy coverage",
        );

        println!();
        println!("REST APPLICATION COMPILATION COMPLETE");
        println!("Profiles:  {}", actual_profiles.len());
        println!("Endpoints: {total_endpoints}");
        println!("Raw:       {total_raw}");
        println!("Envelope:  {total_envelope}");
    }
    // ============================================================
    // FACTORY STARTUP VALIDATION TESTS
    // ============================================================

    #[test]
    fn factory_rejects_empty_profile_set() {
        let rest_profiles = BTreeMap::new();
        let compiled_profiles = BTreeMap::new();
        let feedback_policies = BTreeMap::new();

        let error =
            validate_profile_coverage(&rest_profiles, &compiled_profiles, &feedback_policies)
                .unwrap_err();

        assert!(error.contains("at least one profile"), "{error}");
    }

    #[test]
    fn factory_rejects_missing_compiled_limiter_profile() {
        let mut rest_profiles = BTreeMap::new();

        rest_profiles.insert(
            "binance_spot".into(),
            rest_config(
                r#"
strategy = "raw"
"#,
            ),
        );

        let compiled_profiles = BTreeMap::new();
        let feedback_policies = BTreeMap::new();

        let error =
            validate_profile_coverage(&rest_profiles, &compiled_profiles, &feedback_policies)
                .unwrap_err();

        assert!(error.contains("missing compiled REST"), "{error}");
    }

    #[test]
    fn factory_rejects_zero_fallback_cooldown() {
        let result = RestFactoryFeedbackPolicy::new(Vec::new(), Duration::ZERO);

        assert!(result.is_err());
    }

    #[test]
    fn factory_accepts_positive_fallback_cooldown() {
        let policy = RestFactoryFeedbackPolicy::new(Vec::new(), Duration::from_secs(5)).unwrap();

        assert_eq!(policy.fallback_cooldown, Duration::from_secs(5),);
    }
}
