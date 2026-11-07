// Binance profile cases are declared below.

use crate::support::profile_cases::{Units, synthetic};

#[test]
fn spot_spot_trades_profile() {
    synthetic("binance.spot.spot.trades", Units::Base).unwrap();
}

#[test]
fn perpetual_linear_trades_profile() {
    synthetic("binance.perpetual.linear.trades", Units::Base).unwrap();
}

#[test]
fn perpetual_inverse_trades_profile() {
    synthetic("binance.perpetual.inverse.trades", Units::InverseHundred).unwrap();
}

#[test]
fn future_linear_trades_profile() {
    synthetic("binance.future.linear.trades", Units::Base).unwrap();
}

#[test]
fn future_inverse_trades_profile() {
    synthetic("binance.future.inverse.trades", Units::InverseHundred).unwrap();
}

#[cfg(feature = "historical-real-tests")]
#[test]
fn perpetual_linear_archive_prefix() {
    crate::support::profile_cases::binance_linear_prefix().unwrap();
}
