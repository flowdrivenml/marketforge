use crate::support::{fixtures::workload, root};
use marketforge_engine::historical::config::FormatCatalog;
use std::collections::BTreeSet;
const DECLARED: &[&str] = &[
    "binance.spot.spot.trades",
    "binance.perpetual.linear.trades",
    "binance.perpetual.inverse.trades",
    "binance.future.linear.trades",
    "binance.future.inverse.trades",
    "bitget.spot.spot.trades",
    "bitget.spot.spot.depth",
    "bitget.perpetual.linear.trades",
    "bitget.perpetual.linear.depth",
    "bybit.spot.spot.trades",
    "bybit.spot.spot.depth",
    "bybit.perpetual.linear.trades",
    "bybit.perpetual.linear.depth",
    "bybit.perpetual.inverse.trades",
    "bybit.perpetual.inverse.depth",
    "bybit.future.linear.trades",
    "bybit.future.linear.depth",
    "gateio.spot.spot.trades",
    "gateio.spot.spot.depth",
    "gateio.perpetual.linear.trades",
    "gateio.perpetual.linear.depth",
    "gateio.perpetual.inverse.trades",
    "gateio.perpetual.inverse.depth",
    "okx.spot.spot.trades",
    "okx.spot.spot.depth",
    "okx.perpetual.linear.trades",
    "okx.perpetual.linear.depth",
    "okx.perpetual.inverse.trades",
    "okx.perpetual.inverse.depth",
    "okx.future.linear.trades",
    "okx.future.linear.depth",
    "okx.future.inverse.trades",
    "okx.future.inverse.depth",
    "xnas.equity.equity.trades",
    "xnas.equity.equity.depth",
];
#[test]
fn every_supported_profile_has_an_identifiable_case() {
    let c = FormatCatalog::load(root().join("data/.config/historical")).unwrap();
    let declared: BTreeSet<_> = DECLARED.iter().copied().collect();
    let supported: BTreeSet<_> = c
        .profiles()
        .filter(|p| p.definition.unsupported_reason.is_none())
        .map(|p| p.definition.id.as_str())
        .collect();
    assert_eq!(
        supported, declared,
        "production profile cases/coverage gaps"
    );
}
#[test]
fn profile_coverage_distinguishes_fixture_and_execution_prerequisites() {
    let c = FormatCatalog::load(root().join("data/.config/historical")).unwrap();
    let b = workload().unwrap();
    let mut coverage = vec![];
    for p in c.profiles() {
        let matches: Vec<_> = b
            .jobs
            .iter()
            .filter(|j| {
                p.definition.id.ends_with(
                    serde_json::to_value(j.output.content_type)
                        .unwrap()
                        .as_str()
                        .unwrap(),
                )
            })
            .flat_map(|j| &j.tasks)
            .filter(|t| {
                t.exchange.as_str() == p.definition.exchange
                    && t.instrument_type == p.definition.instrument_type
                    && t.market_category == p.definition.market_category
            })
            .collect();
        let missing: Vec<_> = matches
            .iter()
            .filter(|t| !t.input_path.exists())
            .map(|t| t.input_path.display().to_string())
            .collect();
        let unresolved_family = matches
            .iter()
            .any(|t| t.scope.as_ref().is_some_and(|s| s.members.is_empty()));
        coverage.push(serde_json::json!({"profile":p.definition.id,"declared_synthetic_case":DECLARED.contains(&p.definition.id.as_str()),"fixture_tasks":matches.len(),"missing_archives":missing,"family_metadata_unresolved":unresolved_family,"real_execution":"not implied by this source-only coverage check","feed_gap":p.definition.id.starts_with("gateio.perpetual")&&p.definition.id.ends_with("depth")}));
    }
    let mut run = crate::support::runtime::TestRun::new("profiles/catalog_coverage").unwrap();
    std::fs::write(
        run.path.join("coverage.json"),
        serde_json::to_vec_pretty(&coverage).unwrap(),
    )
    .unwrap();
    assert!(!coverage.is_empty());
    run.finish(&Ok(())).unwrap();
}

#[test]
fn xnas_equity_trades_profile() {
    crate::support::profile_cases::synthetic(
        "xnas.equity.equity.trades",
        crate::support::profile_cases::Units::Base,
    )
    .unwrap();
}

#[test]
fn xnas_equity_depth_profile() {
    crate::support::profile_cases::synthetic(
        "xnas.equity.equity.depth",
        crate::support::profile_cases::Units::Base,
    )
    .unwrap();
}
