use csv::ByteRecord;

use crate::{
    canonical::Exchange,
    error::{MarketForgeError, Result},
    job::{InstrumentKind, WorkTask},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityPolicy {
    /// Archive is expected to contain only the requested instrument.
    Strict,

    /// Archive may contain multiple instruments.
    Filter,

    /// Source does not expose a native instrument identifier.
    TrustSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityDecision {
    Match,
    Skip,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdentityMetrics {
    pub records_read: u64,
    pub records_matched: u64,
    pub records_skipped_instrument: u64,
}

pub struct InstrumentMatcher {
    policy: IdentityPolicy,
    column_index: Option<usize>,
    expected_symbol: String,
    expected_family: Option<String>,
    metrics: IdentityMetrics,
}

impl InstrumentMatcher {
    pub fn new(
        headers: &ByteRecord,
        expected_symbol: impl Into<String>,
        policy: IdentityPolicy,
    ) -> Result<Self> {
        let expected_symbol = expected_symbol.into();

        if expected_symbol.trim().is_empty() {
            return Err(MarketForgeError::InvalidConfiguration(
                "expected instrument symbol cannot be empty".to_owned(),
            ));
        }

        let column_index = match policy {
            IdentityPolicy::TrustSource => None,

            IdentityPolicy::Strict | IdentityPolicy::Filter => {
                Some(resolve_identity_column(headers)?)
            }
        };

        let expected_family = if policy == IdentityPolicy::Filter {
            Some(
                okx_futures_family(&expected_symbol)
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(format!(
                            "invalid OKX futures instrument: {expected_symbol}"
                        ))
                    })?
                    .to_owned(),
            )
        } else {
            None
        };

        Ok(Self {
            policy,
            column_index,
            expected_symbol,
            expected_family,
            metrics: IdentityMetrics::default(),
        })
    }

    pub fn from_task(headers: &ByteRecord, task: &WorkTask) -> Result<Self> {
        let policy = identity_policy(task);

        Self::new(headers, task.symbol.clone(), policy)
    }

    pub fn check(&mut self, record: &ByteRecord) -> Result<IdentityDecision> {
        self.metrics.records_read += 1;

        if self.policy == IdentityPolicy::TrustSource {
            self.metrics.records_matched += 1;

            return Ok(IdentityDecision::Match);
        }

        let index = self.column_index.ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "instrument identity column was not resolved".to_owned(),
            )
        })?;

        let raw = record.get(index).ok_or_else(|| {
            MarketForgeError::InvalidCanonical(
                "missing instrument identity field in record".to_owned(),
            )
        })?;

        let symbol = std::str::from_utf8(raw).map_err(|error| {
            MarketForgeError::InvalidCanonical(format!(
                "invalid UTF-8 in instrument identity: {error}"
            ))
        })?;

        if symbol.is_empty() {
            return Err(MarketForgeError::InvalidCanonical(
                "empty instrument identity in source record".to_owned(),
            ));
        }

        if symbol == self.expected_symbol {
            self.metrics.records_matched += 1;

            return Ok(IdentityDecision::Match);
        }

        match self.policy {
            IdentityPolicy::Strict => Err(MarketForgeError::InvalidCanonical(format!(
                "instrument mismatch: expected {}, received {}",
                self.expected_symbol, symbol,
            ))),

            IdentityPolicy::Filter => {
                let expected_family = self.expected_family.as_deref().ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(
                        "missing expected futures family".to_owned(),
                    )
                })?;

                let actual_family = okx_futures_family(symbol).ok_or_else(|| {
                    MarketForgeError::InvalidCanonical(format!(
                        "invalid futures instrument in source: {symbol}"
                    ))
                })?;

                if actual_family != expected_family {
                    return Err(MarketForgeError::InvalidCanonical(format!(
                        "futures family mismatch: expected {expected_family}, received {actual_family}"
                    )));
                }

                self.metrics.records_skipped_instrument += 1;

                Ok(IdentityDecision::Skip)
            }

            IdentityPolicy::TrustSource => unreachable!(),
        }
    }

    pub fn finish(&self) -> Result<()> {
        // A valid futures-family archive may contain zero trades
        // for the requested contract.
        //
        // Instrument-family consistency is validated in check().
        // Therefore, zero matching records are permitted.

        Ok(())
    }

    pub fn metrics(&self) -> &IdentityMetrics {
        &self.metrics
    }

    pub fn policy(&self) -> IdentityPolicy {
        self.policy
    }
}
fn okx_futures_family(symbol: &str) -> Option<&str> {
    let (family, expiry) = symbol.rsplit_once('-')?;

    if family.is_empty() || expiry.len() != 6 || !expiry.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    Some(family)
}
fn resolve_identity_column(headers: &ByteRecord) -> Result<usize> {
    for name in ["instrument_name", "symbol"] {
        if let Some(index) = headers.iter().position(|header| header == name.as_bytes()) {
            return Ok(index);
        }
    }

    Err(MarketForgeError::InvalidConfiguration(
        "instrument identity policy requires an instrument_name or symbol column".to_owned(),
    ))
}

pub fn identity_policy(task: &WorkTask) -> IdentityPolicy {
    match (task.exchange, task.instrument.instrument_kind) {
        (Exchange::Okx, InstrumentKind::Future) => IdentityPolicy::Filter,

        (Exchange::Okx, _) => IdentityPolicy::Strict,

        (Exchange::Bybit, InstrumentKind::Perpetual) => IdentityPolicy::Strict,

        _ => IdentityPolicy::TrustSource,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers() -> ByteRecord {
        ByteRecord::from(vec!["instrument_name", "trade_id", "side", "price", "size"])
    }

    fn record(symbol: &str) -> ByteRecord {
        ByteRecord::from(vec![symbol, "123", "buy", "78500", "1"])
    }

    #[test]
    fn strict_policy_accepts_matching_instrument() {
        let mut matcher =
            InstrumentMatcher::new(&headers(), "BTC-USD-SWAP", IdentityPolicy::Strict).unwrap();

        assert_eq!(
            matcher.check(&record("BTC-USD-SWAP")).unwrap(),
            IdentityDecision::Match
        );

        matcher.finish().unwrap();

        assert_eq!(matcher.metrics().records_matched, 1);
    }

    #[test]
    fn strict_policy_rejects_mismatched_instrument() {
        let mut matcher =
            InstrumentMatcher::new(&headers(), "BTC-USD-SWAP", IdentityPolicy::Strict).unwrap();

        assert!(matcher.check(&record("ETH-USD-SWAP")).is_err());
    }

    #[test]
    fn filter_policy_skips_other_instruments() {
        let mut matcher =
            InstrumentMatcher::new(&headers(), "BTC-USD-261225", IdentityPolicy::Filter).unwrap();

        assert_eq!(
            matcher.check(&record("BTC-USD-260904")).unwrap(),
            IdentityDecision::Skip
        );

        assert_eq!(
            matcher.check(&record("BTC-USD-261225")).unwrap(),
            IdentityDecision::Match
        );

        matcher.finish().unwrap();

        assert_eq!(matcher.metrics().records_read, 2);
        assert_eq!(matcher.metrics().records_matched, 1);
        assert_eq!(matcher.metrics().records_skipped_instrument, 1);
    }

    #[test]
    fn rejects_missing_identity_column() {
        let headers = ByteRecord::from(vec!["timestamp", "price", "quantity"]);

        assert!(InstrumentMatcher::new(&headers, "BTCUSDT", IdentityPolicy::Strict,).is_err());
    }

    #[test]
    fn rejects_empty_identity() {
        let mut matcher =
            InstrumentMatcher::new(&headers(), "BTC-USD-SWAP", IdentityPolicy::Strict).unwrap();

        assert!(matcher.check(&record("")).is_err());
    }

    #[test]
    fn trust_source_does_not_require_identity_column() {
        let headers = ByteRecord::from(vec!["timestamp", "price", "quantity"]);

        let mut matcher =
            InstrumentMatcher::new(&headers, "BTCUSDT", IdentityPolicy::TrustSource).unwrap();

        let record = ByteRecord::from(vec!["1000", "78500", "0.1"]);

        assert_eq!(matcher.check(&record).unwrap(), IdentityDecision::Match);

        matcher.finish().unwrap();
    }

    #[test]
    fn resolves_bybit_symbol_column() {
        let headers = ByteRecord::from(vec!["timestamp", "symbol", "side", "size", "price"]);

        let mut matcher =
            InstrumentMatcher::new(&headers, "BTCUSDT", IdentityPolicy::Strict).unwrap();

        let record = ByteRecord::from(vec!["1000", "BTCUSDT", "Buy", "0.1", "78500"]);

        assert_eq!(matcher.check(&record).unwrap(), IdentityDecision::Match);
    }

    #[test]
    fn filter_accepts_zero_trades_for_requested_contract() {
        let mut matcher =
            InstrumentMatcher::new(&headers(), "BTC-USD_UM-261225", IdentityPolicy::Filter)
                .unwrap();

        assert_eq!(
            matcher.check(&record("BTC-USD_UM-260925")).unwrap(),
            IdentityDecision::Skip
        );

        assert_eq!(
            matcher.check(&record("BTC-USD_UM-261030")).unwrap(),
            IdentityDecision::Skip
        );

        matcher.finish().unwrap();

        assert_eq!(matcher.metrics().records_read, 2);
        assert_eq!(matcher.metrics().records_matched, 0);
        assert_eq!(matcher.metrics().records_skipped_instrument, 2);
    }

    #[test]
    fn filter_rejects_wrong_futures_family() {
        let mut matcher =
            InstrumentMatcher::new(&headers(), "BTC-USD_UM-261225", IdentityPolicy::Filter)
                .unwrap();

        assert!(matcher.check(&record("BTC-USD-260925")).is_err());
    }

    #[test]
    fn filter_accepts_other_contracts_in_same_family() {
        let mut matcher =
            InstrumentMatcher::new(&headers(), "BTC-USD-261225", IdentityPolicy::Filter).unwrap();

        assert_eq!(
            matcher.check(&record("BTC-USD-260925")).unwrap(),
            IdentityDecision::Skip
        );

        assert_eq!(
            matcher.check(&record("BTC-USD-261225")).unwrap(),
            IdentityDecision::Match
        );

        matcher.finish().unwrap();
    }
}
