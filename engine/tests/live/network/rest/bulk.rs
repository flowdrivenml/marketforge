use std::{env, time::Duration};

use crate::capture::Capture;

use marketforge_engine::live::{
    config::ExchangeConfigStore,
    config::exchange::RestEndpoint,
    infrastructure::limiter::factory::{ExchangeLimiters, WeightParameters},
    infrastructure::rest::{
        RestApplicationOutcome, RestApplicationRuleCompiler, RestApplicationValidator,
        RestFeedbackInterpreter, RestRequest, RestRequestResolver, RestResponseHeaders,
    },
};

// ============================================================
// TEST SETTINGS
// ============================================================

const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const REQUEST_TIMEOUT_SECONDS: u64 = 12;
const DELAY_BETWEEN_REQUESTS_MS: u64 = 2000;

// ============================================================
// NATIVE TEST SYMBOLS
// ============================================================

fn native_symbol(profile: &str) -> &'static str {
    match profile {
        "binance_spot" | "binance_linear" => "BTCUSDT",
        "binance_inverse" => "BTCUSD_PERP",

        "bybit_spot" | "bybit_linear" => "BTCUSDT",
        "bybit_inverse" => "BTCUSD",

        "bitget_spot" | "bitget_linear" => "BTCUSDT",
        "bitget_inverse" => "BTCUSD_CM",

        "okx_spot" => "BTC-USDT",
        "okx_linear" => "BTC-USDT-SWAP",
        "okx_inverse" => "BTC-USD-SWAP",

        "gateio_spot" | "gateio_linear" => "BTC_USDT",
        "gateio_inverse" => "BTC_USD",

        other => panic!("unknown profile: {other}"),
    }
}

// ============================================================
// REQUEST CONSTRUCTION
// ============================================================

fn build_request(profile: &str, endpoint_name: &str, endpoint: &RestEndpoint) -> RestRequest {
    let symbol = native_symbol(profile);

    let mut request = RestRequest::new(profile, endpoint_name);

    let needs_symbol = endpoint
        .parameters
        .iter()
        .any(|parameter| parameter == "symbol")
        || endpoint
            .contract
            .as_deref()
            .is_some_and(|value| value.contains("{symbol}"))
        || endpoint
            .currency_pair
            .as_deref()
            .is_some_and(|value| value.contains("{symbol}"));

    if needs_symbol {
        request = request.with_parameter("symbol", symbol);
    }

    if endpoint.parameters.iter().any(|p| p == "instId") {
        request = request.with_parameter("instId", symbol);
    }

    if profile == "bitget_linear" {
        request = request.with_parameter("native_rest_category", "USDT-FUTURES");
    }

    if profile == "gateio_linear" {
        request = request.with_parameter("native_settlement", "usdt");
    }

    request
}

// ============================================================
// READ RESPONSE WITH SIZE LIMIT
// ============================================================

async fn read_bounded(mut response: reqwest::Response) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();

    while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(format!("response exceeds {} bytes", MAX_RESPONSE_BYTES,));
        }

        body.extend_from_slice(&chunk);
    }

    Ok(body)
}

// ============================================================
// ONE ENDPOINT PROBE
// ============================================================

async fn probe_endpoint(
    client: &reqwest::Client,
    resolver: &RestRequestResolver<'_>,
    profile: &str,
    endpoint_name: &str,
    endpoint: &RestEndpoint,
    rules: &marketforge_engine::live::infrastructure::rest::CompiledRestApplicationRules,
    config: &marketforge_engine::live::config::exchange::ExchangeConfig,
    capture: Option<(&Capture, &ExchangeLimiters)>,
    relative: &str,
) -> Result<usize, String> {
    // ========================================================
    // RESOLVE USING MARKETFORGE
    // ========================================================

    let request = build_request(profile, endpoint_name, endpoint);

    let resolved = resolver
        .resolve(&request)
        .map_err(|error| format!("resolution failed: {error}"))?;

    if !resolved.url().starts_with("https://") {
        return Err("refusing non-HTTPS endpoint".into());
    }

    // ========================================================
    // EXECUTE ONE HTTPS REQUEST
    // ========================================================

    if let Some((_, limiters)) = capture {
        let mut weights = WeightParameters::new();
        for (name, value) in resolved.query() {
            if let Ok(number) = value.parse() {
                weights.insert(name.clone(), number);
            }
        }
        let operation = limiters
            .profile(profile)
            .ok_or("missing compiled limiter")?
            .rest
            .operation(endpoint_name, &weights, resolved.priority())
            .map_err(|error| error.to_string())?;
        tokio::time::timeout(resolved.timeout(), limiters.manager().acquire(&operation))
            .await
            .map_err(|_| "REST quota admission timed out")?
            .map_err(|error| error.to_string())?;
    }

    let response = client
        .get(resolved.url())
        .query(resolved.query())
        .timeout(
            resolved
                .timeout()
                .min(Duration::from_secs(REQUEST_TIMEOUT_SECONDS)),
        )
        .send()
        .await
        .map_err(|error| format!("HTTP transport error: {error}"))?;

    let status = response.status();

    let headers = RestResponseHeaders::new();
    let rate_limits = config.rest.rate_limiting.as_ref();
    if RestFeedbackInterpreter::inspect(rate_limits, status.as_u16(), &headers, None)
        .requires_cooldown()
    {
        return Err(format!("exchange throttled (HTTP {})", status.as_u16()));
    }
    if capture.is_some() && matches!(status.as_u16(), 401 | 403) {
        return Err(format!(
            "HTTP {}: public capture denied; import an existing {relative}",
            status.as_u16()
        ));
    }
    let body = read_bounded(response).await?;

    let json: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|error| format!("invalid JSON (HTTP {}): {error}", status.as_u16(),))?;

    // ========================================================
    // VERIFY HTTP STATUS
    // ========================================================

    if RestFeedbackInterpreter::inspect(rate_limits, status.as_u16(), &headers, Some(&json))
        .requires_cooldown()
    {
        return Err(format!("exchange throttled (HTTP {})", status.as_u16()));
    }
    if !status.is_success() {
        return Err(format!("HTTP {}: {}", status.as_u16(), json,));
    }

    // ========================================================
    // VERIFY APPLICATION RESPONSE
    // ========================================================

    if let Some(rule) = rules.rule(endpoint_name) {
        let outcome = RestApplicationValidator::validate(rule, &json);

        if outcome != RestApplicationOutcome::Success {
            return Err(format!("application validation failed: {outcome:?}"));
        }
    }

    // ========================================================
    // SAVE VERIFIED RAW JSON
    // ========================================================

    if let Some((capture, _)) = capture {
        capture
            .save(relative, &body)
            .map_err(|error| error.to_string())?;
    }

    Ok(body.len())
}

// ============================================================
// BULK LIVE TEST
// ============================================================

#[tokio::test]
#[ignore = "explicit real-exchange network test"]
async fn verifies_public_rest_endpoints_in_bulk() {
    // ========================================================
    // CONFIGURATION
    // ========================================================

    let root = crate::support::data(".config/live/exchanges");

    let configs =
        ExchangeConfigStore::load_dir(&root).expect("cannot load exchange configurations");

    run(&configs, None).await;
}

/// Returns whether the next capture family may proceed without ignoring a throttle.
pub(super) async fn run(
    configs: &ExchangeConfigStore,
    capture: Option<(&Capture, &ExchangeLimiters)>,
) -> bool {
    let resolver = RestRequestResolver::new(configs);

    // Optional filter:
    //
    // LIVE_REST_PROFILE=bybit_spot
    //
    // Optional maximum number of public requests:
    //
    // LIVE_REST_MAX=2
    //
    // Both are useful for progressive verification.

    let profile_filter = env::var("LIVE_REST_PROFILE").ok();

    let max_requests = env::var("LIVE_REST_MAX")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("LIVE_REST_MAX must be an integer")
        })
        .unwrap_or(usize::MAX);

    // ========================================================
    // HTTP CLIENT
    // ========================================================

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECONDS))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // ========================================================
    // TEST COUNTERS
    // ========================================================

    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut skipped_auth = 0usize;
    let mut requests_sent = 0usize;

    let mut failures = Vec::<String>::new();

    let mut stop_on_throttle = false;

    println!();
    println!("============================================");
    println!("MARKETFORGE LIVE REST BULK VERIFICATION");
    println!("============================================");

    // ========================================================
    // ITERATE PROFILES
    // ========================================================

    'profiles: for profile in configs.names() {
        if profile_filter
            .as_deref()
            .is_some_and(|filter| filter != profile)
        {
            continue;
        }

        let config = configs
            .get(profile)
            .unwrap_or_else(|| panic!("missing profile: {profile}"));

        let rules = RestApplicationRuleCompiler::compile(&config.rest)
            .unwrap_or_else(|error| panic!("{profile}: cannot compile rules: {error}"));

        println!();
        println!("PROFILE: {profile}");

        // ====================================================
        // ITERATE ENDPOINTS
        // ====================================================

        for (endpoint_name, endpoint) in &config.rest.endpoints {
            let relative = format!("rest-fixtures/{profile}__{endpoint_name}.json");
            if capture.is_some_and(|(capture, _)| !capture.wants(&relative)) {
                continue;
            }
            let authentication = endpoint.authentication.as_deref().unwrap_or("none");

            if authentication != "none" {
                skipped_auth += 1;

                println!("  SKIP_AUTH  {endpoint_name}");

                continue;
            }

            if requests_sent >= max_requests {
                break 'profiles;
            }

            // =================================================
            // PACE REQUESTS
            // =================================================

            if requests_sent > 0 {
                tokio::time::sleep(Duration::from_millis(DELAY_BETWEEN_REQUESTS_MS)).await;
            }

            requests_sent += 1;

            // =================================================
            // PROBE ENDPOINT
            // =================================================

            let result = probe_endpoint(
                &client,
                &resolver,
                profile,
                endpoint_name,
                endpoint,
                &rules,
                config,
                capture,
                &relative,
            )
            .await;

            match result {
                Ok(bytes) => {
                    passed += 1;

                    println!(
                        "  PASS       {endpoint_name:<22} \
                         bytes={bytes}"
                    );
                }

                Err(error) => {
                    failed += 1;

                    println!(
                        "  FAIL       {endpoint_name:<22} \
                         {error}"
                    );

                    failures.push(format!("{profile}/{endpoint_name}: {error}"));

                    // Stop on exchange throttling rather
                    // than continuing to send requests.
                    if error.starts_with("exchange throttled") {
                        stop_on_throttle = true;
                        break 'profiles;
                    }
                }
            }
        }
    }

    // ========================================================
    // FINAL REPORT
    // ========================================================

    println!();
    println!("============================================");
    println!("LIVE REST VERIFICATION REPORT");
    println!("============================================");

    println!("Requests attempted: {requests_sent}");
    println!("Passed:             {passed}");
    println!("Failed:             {failed}");
    println!("Authentication skips: {skipped_auth}");

    if stop_on_throttle {
        println!("STOPPED: exchange throttling detected");
    }

    if !failures.is_empty() {
        println!();
        println!("FAILURE DETAILS:");

        for failure in &failures {
            println!("  {failure}");
        }
    }

    assert_eq!(
        passed + failed,
        requests_sent,
        "request accounting mismatch",
    );

    if capture.is_some() {
        return !stop_on_throttle;
    }

    assert!(
        !stop_on_throttle,
        "bulk verification stopped on exchange throttling"
    );

    assert!(
        failures.is_empty(),
        "{} live REST endpoint(s) failed",
        failures.len(),
    );
    true
}
