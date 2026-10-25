use std::collections::{BTreeMap, BTreeSet};

use crate::live::config::ExchangeConfigStore;

use crate::live::infrastructure::limiter::factory::{CompiledRestLimiters, ExchangeLimiters};

use super::{
    ExchangeRestClientFactory, ExchangeRestService, RestClientConfig, RestFactoryFeedbackPolicy,
};

// ============================================================
// PRODUCTION REST BOOTSTRAP
// ============================================================

/// Constructs the REST service using an existing,
/// fully initialized MarketForge limiter subsystem.
///
/// Ownership:
///
/// - ExchangeConfigStore owns immutable configurations.
/// - ExchangeLimiters owns the shared rate-limit manager.
/// - The REST service receives a clone of that manager.
/// - Cloning the manager preserves shared budget state.
/// - REST configuration is not reloaded.
/// - Rate-limit policies are not recompiled.
///
/// Bootstrap performs no network requests.
pub struct ExchangeRestBootstrap;

impl ExchangeRestBootstrap {
    /// Construct a ready-to-use REST service.
    ///
    /// The caller must ensure that `configs` and `limiters`
    /// belong to the same configuration generation.
    ///
    /// Matching profile and endpoint names are checked here.
    /// Exact configuration-generation identity is not yet
    /// represented in the current limiter types.
    pub fn build<'a>(
        configs: &'a ExchangeConfigStore,
        limiters: &ExchangeLimiters,
        feedback_policies: BTreeMap<String, RestFactoryFeedbackPolicy>,
        client_config: RestClientConfig,
    ) -> Result<ExchangeRestService<'a>, String> {
        // ====================================================
        // VALIDATE PROFILE COVERAGE
        // ====================================================

        let config_profiles: BTreeSet<String> = configs.names().map(str::to_owned).collect();

        let limiter_profiles: BTreeSet<String> = limiters
            .profiles()
            .map(|(name, _)| name.to_owned())
            .collect();

        if config_profiles.is_empty() {
            return Err("REST bootstrap requires exchange profiles".into());
        }

        if config_profiles != limiter_profiles {
            let missing_limiters = config_profiles
                .difference(&limiter_profiles)
                .cloned()
                .collect::<Vec<_>>();

            let unexpected_limiters = limiter_profiles
                .difference(&config_profiles)
                .cloned()
                .collect::<Vec<_>>();

            return Err(format!(
                "REST bootstrap profile mismatch: \
                 missing_limiters={missing_limiters:?}, \
                 unexpected_limiters={unexpected_limiters:?}"
            ));
        }

        // ====================================================
        // VALIDATE FEEDBACK POLICY COVERAGE
        // ====================================================

        let feedback_profiles: BTreeSet<String> = feedback_policies.keys().cloned().collect();

        if config_profiles != feedback_profiles {
            let missing_feedback = config_profiles
                .difference(&feedback_profiles)
                .cloned()
                .collect::<Vec<_>>();

            let unexpected_feedback = feedback_profiles
                .difference(&config_profiles)
                .cloned()
                .collect::<Vec<_>>();

            return Err(format!(
                "REST bootstrap feedback mismatch: \
                 missing={missing_feedback:?}, \
                 unexpected={unexpected_feedback:?}"
            ));
        }

        // ====================================================
        // COLLECT VALIDATED REST CONFIGURATIONS
        // ====================================================

        let mut rest_profiles = BTreeMap::new();

        for (name, config) in configs.iter() {
            config.validate().map_err(|error| {
                format!(
                    "{name}: invalid exchange configuration: \
                     {error}"
                )
            })?;

            rest_profiles.insert(name.to_owned(), config.rest.clone());
        }

        // ====================================================
        // EXTRACT EXISTING COMPILED REST LIMITERS
        // ====================================================

        let mut compiled_profiles: BTreeMap<String, CompiledRestLimiters> = BTreeMap::new();

        for (name, validated) in limiters.profiles() {
            // =================================================
            // CHECK PROFILE IDENTITY
            // =================================================

            if validated.profile != name {
                return Err(format!(
                    "{name}: validated limiter profile \
                     identifies itself as '{}'",
                    validated.profile
                ));
            }

            let config = configs
                .get(name)
                .ok_or_else(|| format!("{name}: missing exchange configuration"))?;

            // =================================================
            // CHECK COMPILED ENDPOINT COVERAGE
            // =================================================

            let configured_endpoints: BTreeSet<String> =
                config.rest.endpoints.keys().cloned().collect();

            let compiled_endpoints: BTreeSet<String> = validated
                .rest
                .endpoints()
                .map(|(endpoint, _)| endpoint.to_owned())
                .collect();

            if configured_endpoints != compiled_endpoints {
                let missing = configured_endpoints
                    .difference(&compiled_endpoints)
                    .cloned()
                    .collect::<Vec<_>>();

                let unexpected = compiled_endpoints
                    .difference(&configured_endpoints)
                    .cloned()
                    .collect::<Vec<_>>();

                return Err(format!(
                    "{name}: compiled REST endpoint mismatch: \
                     missing={missing:?}, \
                     unexpected={unexpected:?}"
                ));
            }

            // =================================================
            // REUSE COMPILED REST LIMITER
            // =================================================

            compiled_profiles.insert(name.to_owned(), validated.rest.clone());
        }

        // ====================================================
        // REUSE THE EXISTING SHARED RATE-LIMIT MANAGER
        // ====================================================

        let manager = limiters.manager();

        // ====================================================
        // CONSTRUCT REST CLIENT
        // ====================================================

        let client = ExchangeRestClientFactory::build(
            manager,
            compiled_profiles,
            &rest_profiles,
            feedback_policies,
            client_config,
        )
        .map_err(|error| {
            format!(
                "REST client factory initialization failed: \
                 {error}"
            )
        })?;

        // ====================================================
        // CONSTRUCT PUBLIC REST SERVICE
        // ====================================================

        Ok(ExchangeRestService::new(configs, client))
    }
}
