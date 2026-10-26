use std::{env, fs, path::PathBuf, time::Duration};

use marketforge_engine::live::{
    config::ExchangeConfigStore,
    config::exchange::RestEndpoint,
    infrastructure::rest::{
        RestApplicationOutcome, RestApplicationRuleCompiler, RestApplicationValidator, RestRequest,
        RestRequestResolver,
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
    fixtures_dir: &PathBuf,
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

    let response = client
        .get(resolved.url())
        .query(resolved.query())
        .send()
        .await
        .map_err(|error| format!("HTTP transport error: {error}"))?;

    let status = response.status();

    let body = read_bounded(response).await?;

    let json: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|error| format!("invalid JSON (HTTP {}): {error}", status.as_u16(),))?;

    // ========================================================
    // VERIFY HTTP STATUS
    // ========================================================

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

    let filename = format!("{profile}__{endpoint_name}.json");

    let path = fixtures_dir.join(filename);

    let pretty = serde_json::to_vec_pretty(&json).map_err(|error| error.to_string())?;

    fs::write(&path, pretty).map_err(|error| format!("cannot save fixture: {error}"))?;

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

    let resolver = RestRequestResolver::new(&configs);

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
    // OUTPUT DIRECTORY
    // ========================================================

    let fixtures_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/live-rest-fixtures");

    fs::create_dir_all(&fixtures_dir).expect("cannot create fixture directory");

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

    let profiles = [
        "binance_spot",
        "binance_linear",
        "binance_inverse",
        "bybit_spot",
        "bybit_linear",
        "bybit_inverse",
        "okx_spot",
        "okx_linear",
        "okx_inverse",
        "bitget_spot",
        "bitget_linear",
        "bitget_inverse",
        "gateio_spot",
        "gateio_linear",
        "gateio_inverse",
    ];

    'profiles: for profile in profiles {
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
                &fixtures_dir,
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
                    if error.starts_with("HTTP 429") {
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
    println!("Fixtures directory: {}", fixtures_dir.display());

    if stop_on_throttle {
        println!("STOPPED: exchange returned HTTP 429");
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

    assert!(
        !stop_on_throttle,
        "bulk verification stopped on exchange throttling"
    );

    assert!(
        failures.is_empty(),
        "{} live REST endpoint(s) failed",
        failures.len(),
    );
}
