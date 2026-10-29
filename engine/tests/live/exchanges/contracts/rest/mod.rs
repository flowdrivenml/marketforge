mod resolution;

/// Exact public REST contract, including deliberate absence of authenticated
/// Binance futures history. This guards both endpoint removal and accidental additions.
pub(super) fn assert_endpoint_matrix(
    configs: &marketforge_engine::live::config::ExchangeConfigStore,
) -> usize {
    use std::collections::BTreeSet;
    let mut total = 0;
    for (profile, config) in configs.iter() {
        let expected: &[&str] = match profile {
            "binance_inverse" => &["aggregate_trades", "depth_snapshot", "recent_trades"],
            "binance_spot" | "okx_spot" | "okx_linear" | "okx_inverse" => {
                &["depth_snapshot", "historical_trades", "recent_trades"]
            }
            "gateio_spot" | "gateio_linear" | "gateio_inverse" => {
                &["depth_snapshot", "historical_trades"]
            }
            "binance_linear" | "bybit_spot" | "bybit_linear" | "bybit_inverse" | "bitget_spot"
            | "bitget_linear" | "bitget_inverse" => &["depth_snapshot", "recent_trades"],
            other => panic!("unexpected REST profile: {other}"),
        };
        assert_eq!(
            config
                .rest
                .endpoints
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            expected.iter().copied().collect::<BTreeSet<_>>(),
            "{profile}: public endpoint contract changed"
        );
        total += expected.len();
    }
    assert_eq!(configs.len(), 15);
    assert_eq!(total, 35);
    total
}
