use std::collections::BTreeSet;
use std::path::PathBuf;

use marketforge_engine::live::config::ExchangeConfigStore;

use marketforge_engine::live::infrastructure::rest::{RestRequest, RestRequestResolver};

// ============================================================
// TEST MARKET INSTRUMENTS
// ============================================================

/// Representative native BTC instruments.
///
/// These are used only for request construction.
/// No network calls are performed.
fn native_symbol(profile: &str) -> &'static str {
    match profile {
        "binance_spot" | "binance_linear" => "BTCUSDT",
        "binance_inverse" => "BTCUSD_PERP",

        "bybit_spot" | "bybit_linear" => "BTCUSDT",
        "bybit_inverse" => "BTCUSD",

        "okx_spot" => "BTC-USDT",
        "okx_linear" => "BTC-USDT-SWAP",
        "okx_inverse" => "BTC-USD-SWAP",

        "bitget_spot" | "bitget_linear" => "BTCUSDT",
        "bitget_inverse" => "BTCUSD",

        "gateio_spot" | "gateio_linear" => "BTC_USDT",
        "gateio_inverse" => "BTC_USD",

        other => panic!("unknown test profile: {other}"),
    }
}

// ============================================================
// TEST REQUEST BUILDER
// ============================================================

fn make_request(
    profile: &str,
    endpoint_name: &str,
    endpoint: &marketforge_engine::live::config::exchange::RestEndpoint,
) -> RestRequest {
    let symbol = native_symbol(profile);

    let mut request = RestRequest::new(profile, endpoint_name);

    // Supply the canonical symbol when the endpoint accepts
    // it directly, or when a configured template needs it.
    //
    // Examples:
    //
    // Binance:
    //     symbol=BTCUSDT
    //
    // Gate.io:
    //     contract="{symbol}"
    //
    // Gate.io Spot:
    //     currency_pair="{symbol}"

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

    // OKX uses instId instead of symbol.
    if endpoint.parameters.iter().any(|p| p == "instId") {
        request = request.with_parameter("instId", symbol);
    }

    // Bitget Linear obtains its REST category from
    // a native market-category template.
    //
    // Gate.io Linear similarly uses native_settlement
    // in its futures URL.
    if profile == "bitget_linear" {
        request = request.with_parameter("native_rest_category", "USDT-FUTURES");
    }

    if profile == "gateio_linear" {
        request = request.with_parameter("native_settlement", "usdt");
    }

    request
}

// ============================================================
// REAL CONFIGURATION BULK RESOLVER TEST
// ============================================================

#[test]
fn resolves_all_real_exchange_rest_endpoints() {
    // ========================================================
    // LOAD ACTUAL EXCHANGE CONFIGURATIONS
    // ========================================================

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/exchanges");

    let configs =
        ExchangeConfigStore::load_dir(&root).expect("failed to load exchange configurations");

    let resolver = RestRequestResolver::new(&configs);

    // ========================================================
    // EXPECTED PROFILE COVERAGE
    // ========================================================

    let mut profiles = BTreeSet::new();

    for exchange in ["binance", "bybit", "okx", "bitget", "gateio"] {
        for market in ["spot", "linear", "inverse"] {
            profiles.insert(format!("{exchange}_{market}"));
        }
    }

    // ========================================================
    // COUNTERS
    // ========================================================

    let mut total = 0usize;
    let mut resolved = 0usize;
    let mut skipped_auth = 0usize;

    let mut failures = Vec::new();

    // ========================================================
    // RESOLVE EVERY ENDPOINT
    // ========================================================

    for profile in &profiles {
        let config = configs
            .get(profile)
            .unwrap_or_else(|| panic!("missing configuration: {profile}"));

        println!();
        println!("PROFILE: {profile}");

        for (endpoint_name, endpoint) in &config.rest.endpoints {
            total += 1;

            let authentication = endpoint.authentication.as_deref().unwrap_or("none");

            // =================================================
            // AUTHENTICATION
            // =================================================

            if authentication != "none" {
                skipped_auth += 1;

                println!(
                    "  SKIP_AUTH  {endpoint_name:<22} \
                     authentication={authentication}"
                );

                continue;
            }

            // =================================================
            // CONSTRUCT LOGICAL REQUEST
            // =================================================

            let request = make_request(profile, endpoint_name, endpoint);

            // =================================================
            // RESOLVE USING MARKETFORGE
            // =================================================

            match resolver.resolve(&request) {
                Ok(resolved_request) => {
                    // =========================================
                    // VERIFY HTTPS
                    // =========================================

                    let url = resolved_request.url();

                    if !url.starts_with("https://") {
                        failures.push(format!(
                            "{profile}/{endpoint_name}: \
                             non-HTTPS URL: {url}"
                        ));

                        continue;
                    }

                    // =========================================
                    // VERIFY NO UNRESOLVED TEMPLATES
                    // =========================================

                    if url.contains('{') || url.contains('}') {
                        failures.push(format!(
                            "{profile}/{endpoint_name}: \
                             unresolved URL template: {url}"
                        ));

                        continue;
                    }

                    // =========================================
                    // VERIFY REQUIRED QUERY PARAMETERS
                    // =========================================

                    let query = resolved_request.query();

                    let mut missing = Vec::new();

                    for required in &endpoint.required_parameters {
                        if !query.contains_key(required) {
                            missing.push(required.clone());
                        }
                    }

                    if !missing.is_empty() {
                        failures.push(format!(
                            "{profile}/{endpoint_name}: \
                             missing required parameters: \
                             {missing:?}"
                        ));

                        continue;
                    }

                    // =========================================
                    // SUCCESS
                    // =========================================

                    resolved += 1;

                    println!(
                        "  PASS       {endpoint_name:<22} \
                         {}",
                        url
                    );

                    println!("             query={query:?}");
                }

                Err(error) => {
                    failures.push(format!("{profile}/{endpoint_name}: {error}"));

                    println!(
                        "  FAIL       {endpoint_name:<22} \
                         {error}"
                    );
                }
            }
        }
    }

    // ========================================================
    // SUMMARY
    // ========================================================

    println!();
    println!("==========================================");
    println!("MARKETFORGE REST BULK RESOLVER REPORT");
    println!("==========================================");

    println!("Profiles:            {}", profiles.len());
    println!("Endpoints:           {total}");
    println!("Successfully resolved: {resolved}");
    println!("Authentication skips: {skipped_auth}");
    println!("Failures:            {}", failures.len());

    if !failures.is_empty() {
        println!();
        println!("FAILURE DETAILS:");

        for failure in &failures {
            println!("  {failure}");
        }
    }

    assert_eq!(total, 37, "unexpected REST endpoint count",);

    assert_eq!(
        resolved + skipped_auth + failures.len(),
        total,
        "endpoint accounting mismatch",
    );

    assert!(
        failures.is_empty(),
        "REST bulk resolver failed for {} endpoints",
        failures.len(),
    );
}
