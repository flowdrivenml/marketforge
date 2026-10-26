use std::time::Duration;

use marketforge_engine::live::{
    config::ExchangeConfigStore,
    infrastructure::rest::{
        RestApplicationOutcome, RestApplicationRuleCompiler, RestApplicationValidator, RestRequest,
        RestRequestResolver,
    },
};

// ============================================================
// REAL EXCHANGE REST SMOKE TEST
// ============================================================

/// Makes one real HTTPS request to a public exchange.
///
/// Explicitly ignored during ordinary cargo test runs.
///
/// No API keys.
/// No trading.
/// No authenticated operations.
/// No retries.
#[tokio::test]
#[ignore = "requires public internet access"]
async fn bybit_spot_recent_trades_live() {
    // ========================================================
    // LOAD ACTUAL EXCHANGE CONFIGURATIONS
    // ========================================================

    let root = crate::support::data(".config/live/exchanges");

    let configs =
        ExchangeConfigStore::load_dir(&root).expect("failed to load exchange configurations");

    let profile = "bybit_spot";
    let endpoint = "recent_trades";

    let config = configs
        .get(profile)
        .expect("Bybit Spot configuration missing");

    // ========================================================
    // BUILD LOGICAL REQUEST
    // ========================================================

    let request = RestRequest::new(profile, endpoint).with_parameter("symbol", "BTCUSDT");

    // ========================================================
    // RESOLVE THROUGH MARKETFORGE
    // ========================================================

    let resolver = RestRequestResolver::new(&configs);

    let resolved = resolver
        .resolve(&request)
        .expect("REST request resolution failed");

    assert!(
        resolved.url().starts_with("https://"),
        "live requests must use HTTPS"
    );

    println!();
    println!("PROFILE:  {profile}");
    println!("ENDPOINT: {endpoint}");
    println!("URL:      {}", resolved.url());
    println!("QUERY:    {:?}", resolved.query());

    // ========================================================
    // COMPILE APPLICATION RESPONSE RULE
    // ========================================================

    let compiled = RestApplicationRuleCompiler::compile(&config.rest)
        .expect("application rule compilation failed");

    let rule = compiled
        .rule(endpoint)
        .expect("Bybit endpoint must have an envelope rule");

    // ========================================================
    // CONSTRUCT HTTP CLIENT
    // ========================================================

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    // ========================================================
    // EXECUTE ONE REAL HTTPS REQUEST
    // ========================================================

    let mut response = client
        .get(resolved.url())
        .query(resolved.query())
        .send()
        .await
        .expect("live REST request failed");

    let status = response.status();

    println!("HTTP:     {}", status.as_u16());

    // ========================================================
    // BOUNDED RESPONSE READ
    // ========================================================

    const MAX_RESPONSE_BYTES: usize = 512 * 1024;

    let mut bytes = Vec::new();

    while let Some(chunk) = response
        .chunk()
        .await
        .expect("failed to read response body")
    {
        assert!(
            bytes.len() + chunk.len() <= MAX_RESPONSE_BYTES,
            "live response exceeded size limit"
        );

        bytes.extend_from_slice(&chunk);
    }

    println!("BYTES:    {}", bytes.len());

    let json: serde_json::Value =
        serde_json::from_slice(&bytes).expect("exchange returned invalid JSON");

    // ========================================================
    // VERIFY HTTP SUCCESS
    // ========================================================

    assert!(
        status.is_success(),
        "exchange returned HTTP {}: {}",
        status,
        json
    );

    // ========================================================
    // VERIFY APPLICATION SUCCESS
    // ========================================================

    let outcome = RestApplicationValidator::validate(rule, &json);

    assert_eq!(
        outcome,
        RestApplicationOutcome::Success,
        "exchange application response failed"
    );

    // ========================================================
    // VERIFY EXPECTED PAYLOAD
    // ========================================================

    let trades = json
        .get("result")
        .and_then(|result| result.get("list"))
        .and_then(|list| list.as_array())
        .expect("missing result.list array");

    assert!(!trades.is_empty(), "exchange returned no trades");

    println!("TRADES:   {}", trades.len());

    // Show one record to inspect native field names.
    println!(
        "SAMPLE:   {}",
        serde_json::to_string_pretty(&trades[0]).unwrap()
    );

    println!();
    println!("======================================");
    println!("BYBIT SPOT LIVE REST SMOKE TEST");
    println!("======================================");
    println!("Configuration loading: PASS");
    println!("Request resolution:     PASS");
    println!("HTTPS request:          PASS");
    println!("HTTP status:            PASS");
    println!("JSON decoding:          PASS");
    println!("Application validation: PASS");
    println!("Trade payload:          PASS");
}

// ============================================================
// BYBIT SPOT — LIVE L2 SNAPSHOT
// ============================================================

#[tokio::test]
#[ignore = "requires public internet access"]
async fn bybit_spot_depth_snapshot_live() {
    let root = crate::support::data(".config/live/exchanges");

    let configs =
        ExchangeConfigStore::load_dir(&root).expect("failed to load exchange configurations");

    let profile = "bybit_spot";
    let endpoint = "depth_snapshot";

    // ========================================================
    // RESOLVE REQUEST USING MARKETFORGE
    // ========================================================

    let request = RestRequest::new(profile, endpoint).with_parameter("symbol", "BTCUSDT");

    let resolved = RestRequestResolver::new(&configs)
        .resolve(&request)
        .expect("failed to resolve depth request");

    println!("\nPROFILE:  {profile}");
    println!("ENDPOINT: {endpoint}");
    println!("URL:      {}", resolved.url());
    println!("QUERY:    {:?}", resolved.query());

    // ========================================================
    // COMPILE APPLICATION RULE
    // ========================================================

    let config = configs.get(profile).unwrap();

    let rules = RestApplicationRuleCompiler::compile(&config.rest).unwrap();

    let rule = rules.rule(endpoint).unwrap();

    // ========================================================
    // EXECUTE ONE HTTPS REQUEST
    // ========================================================

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let mut response = client
        .get(resolved.url())
        .query(resolved.query())
        .send()
        .await
        .expect("depth request failed");

    let status = response.status();

    const MAX_BYTES: usize = 1024 * 1024;

    let mut bytes = Vec::new();

    while let Some(chunk) = response.chunk().await.expect("failed to read response") {
        assert!(
            bytes.len() + chunk.len() <= MAX_BYTES,
            "depth response exceeded size limit"
        );

        bytes.extend_from_slice(&chunk);
    }

    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("invalid JSON");

    assert!(status.is_success(), "HTTP {}: {}", status, json);

    // ========================================================
    // VALIDATE APPLICATION ENVELOPE
    // ========================================================

    let outcome = RestApplicationValidator::validate(rule, &json);

    assert_eq!(
        outcome,
        RestApplicationOutcome::Success,
        "invalid application response"
    );

    // ========================================================
    // VALIDATE L2 SNAPSHOT STRUCTURE
    // ========================================================

    let result = &json["result"];

    let bids = result["b"].as_array().expect("missing bids array");

    let asks = result["a"].as_array().expect("missing asks array");

    assert!(!bids.is_empty(), "empty bid book");
    assert!(!asks.is_empty(), "empty ask book");

    // Every order-book level must contain a price
    // and quantity, both represented as strings.

    for (side, levels) in [("bid", bids), ("ask", asks)] {
        for (index, level) in levels.iter().enumerate() {
            let values = level
                .as_array()
                .unwrap_or_else(|| panic!("{side}[{index}]: not an array"));

            assert_eq!(
                values.len(),
                2,
                "{side}[{index}]: expected [price, quantity]"
            );

            for value in values {
                assert!(
                    value.as_str().is_some(),
                    "{side}[{index}]: expected string values"
                );
            }
        }
    }

    // ========================================================
    // REPORT NATIVE METADATA
    // ========================================================

    println!("HTTP:       {}", status.as_u16());
    println!("BYTES:      {}", bytes.len());
    println!("BIDS:       {}", bids.len());
    println!("ASKS:       {}", asks.len());

    println!("SYMBOL:     {}", result["s"]);
    println!("UPDATE ID:  {}", result["u"]);
    println!("SEQUENCE:   {}", result["seq"]);
    println!("TIMESTAMP:  {}", json["ts"]);

    println!("BEST BID:   {}", bids[0]);
    println!("BEST ASK:   {}", asks[0]);

    println!();
    println!("======================================");
    println!("BYBIT SPOT LIVE L2 SNAPSHOT");
    println!("======================================");
    println!("Request resolution:     PASS");
    println!("HTTPS execution:        PASS");
    println!("Application validation: PASS");
    println!("L2 structure:           PASS");
}
