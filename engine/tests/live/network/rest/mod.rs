mod bulk;
pub(crate) mod service;
mod smoke;

#[tokio::test]
#[ignore = "explicit public REST fixture collection via fixtures.py"]
async fn collect_rest_fixtures() {
    let capture = crate::capture::Capture::from_env();
    let store = service::load_configs();
    let selected = store
        .names()
        .filter(|profile| capture.wants_rest_profile(profile))
        .map(str::to_owned)
        .collect();
    let store = store.selected(&selected).unwrap();
    let profiles = store.names().collect::<Vec<_>>();
    let limiters = service::build_capture_limiters(&store, &profiles, &capture.weights);

    // Both capture paths charge the same budgets for this collection run.
    if bulk::run(&store, Some((&capture, &limiters))).await {
        service::run(&store, Some((&capture, &limiters))).await;
    }
}

#[test]
fn fixture_rest_limiters_compile_for_all_profiles_without_network() {
    let store = service::load_configs();
    let profiles = store.names().collect::<Vec<_>>();
    // Synthetic test quantities used solely to compile the local budget plan.
    let weights = profiles
        .iter()
        .filter(|name| name.starts_with("binance_"))
        .map(|name| (name.to_string(), (5000, 60)))
        .collect();
    let limiters = service::build_capture_limiters(&store, &profiles, &weights);
    assert_eq!(limiters.profile_count(), 15);
    for profile in profiles {
        let rest = &limiters.profile(profile).unwrap().rest;
        assert!(rest.endpoint("depth_snapshot").is_some(), "{profile}");
        let trades = if profile.starts_with("gateio_") {
            "historical_trades"
        } else {
            "recent_trades"
        };
        assert!(rest.endpoint(trades).is_some(), "{profile}");
    }
}
