use super::SourceTradeRecoveryReview;
use crate::live::infrastructure::rest::{HistoricalTradeCursor, HistoricalTradePage, RestRequest};
use crate::live::{
    config::exchange::{ExchangeId, MarketType},
    recovery::RecoveryOperation,
    stream::request::{CreateSourceStreamRequest, SourceChannel},
};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SourceTradeRecoveryPolicyError {
    #[error("trade recovery requires a trade source")]
    WrongChannel,

    #[error("automatic REST trade recovery is unsupported for this profile")]
    UnsupportedProfile,

    #[error("trade recovery requires an accepted numeric baseline")]
    MissingNumericBaseline,

    #[error("trade recovery review belongs to another logical stream")]
    WrongStream,

    #[error("trade recovery requires coverage observations")]
    MissingCoverageEvidence,

    #[error("numeric trade recovery cursor is exhausted")]
    CursorExhausted,

    #[error("historical trade cursor precedes the recovery baseline")]
    InvalidContinuationCursor,

    #[error("historical trade recovery operation does not match policy")]
    InvalidOperation,
}

/// Permitted REST acquisition strategy for one source.
///
/// This policy does not:
/// - Execute REST requests.
/// - Verify returned historical completeness.
/// - Reconcile trade identities.
/// - Clear integrity coverage holds.
/// - Authorize publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTradeRecoveryPolicy {
    max_records_per_page: u64,
}

impl SourceTradeRecoveryPolicy {
    /// Construct the initial, conservative recovery strategy.
    ///
    /// Only Binance Spot is enabled.
    ///
    /// A recent-trades response may be too short to bridge
    /// the accepted baseline and triggering trades.
    /// Such a response must fail reconciliation rather than
    /// being treated as complete history.
    pub fn for_source(
        source: &CreateSourceStreamRequest,
    ) -> Result<Self, SourceTradeRecoveryPolicyError> {
        use SourceTradeRecoveryPolicyError as Error;

        if source.channel != SourceChannel::Trades {
            return Err(Error::WrongChannel);
        }

        match (source.exchange, source.market_category) {
            (ExchangeId::Binance, MarketType::Spot) => Ok(Self {
                max_records_per_page: 1000,
            }),

            _ => Err(Error::UnsupportedProfile),
        }
    }

    /// Validate the current review before scheduling REST.
    ///
    /// `stream_id` must come from the owning source runtime.
    ///
    /// This method does not establish that the reviewed
    /// discontinuity can be recovered by the selected endpoint.
    pub fn operation_for_review(
        &self,
        stream_id: &str,
        review: &SourceTradeRecoveryReview,
    ) -> Result<RecoveryOperation, SourceTradeRecoveryPolicyError> {
        use SourceTradeRecoveryPolicyError as Error;

        if review.stream_id != stream_id {
            return Err(Error::WrongStream);
        }

        if review.observations.is_empty() {
            return Err(Error::MissingCoverageEvidence);
        }

        let baseline = review
            .baseline
            .numeric_trade_id
            .ok_or(Error::MissingNumericBaseline)?;

        let from_id = baseline.checked_add(1).ok_or(Error::CursorExhausted)?;

        let page = HistoricalTradePage::with_cursor(HistoricalTradeCursor::TradeId { from_id })
            .with_limit(self.max_records_per_page);

        Ok(RecoveryOperation::HistoricalTrades { page })
    }
    /// Construct a historical REST operation for a validated
    /// pagination cursor.
    ///
    /// The initial cursor is baseline + 1.
    /// Continuation cursors must be authorized by the
    /// SourceTradePageCollector before calling this method.
    pub fn operation_for_cursor(
        &self,
        stream_id: &str,
        review: &SourceTradeRecoveryReview,
        from_id: u64,
    ) -> Result<RecoveryOperation, SourceTradeRecoveryPolicyError> {
        use SourceTradeRecoveryPolicyError as Error;

        let initial = self.operation_for_review(stream_id, review)?;

        let RecoveryOperation::HistoricalTrades { page } = initial else {
            return Err(Error::InvalidOperation);
        };

        let HistoricalTradeCursor::TradeId {
            from_id: first_required,
        } = page.cursor
        else {
            return Err(Error::InvalidOperation);
        };

        if from_id < first_required {
            return Err(Error::InvalidContinuationCursor);
        }

        Ok(RecoveryOperation::HistoricalTrades {
            page: HistoricalTradePage::with_cursor(HistoricalTradeCursor::TradeId { from_id })
                .with_limit(self.max_records_per_page),
        })
    }
    /// Construct the logical REST request corresponding
    /// to an explicitly selected recovery operation.
    ///
    /// The operation must be compatible with the source,
    /// review and configured pagination limits.
    pub fn request_for_operation(
        &self,
        source: &CreateSourceStreamRequest,
        review: &SourceTradeRecoveryReview,
        operation: &RecoveryOperation,
    ) -> Result<RestRequest, SourceTradeRecoveryPolicyError> {
        use SourceTradeRecoveryPolicyError as Error;

        if Self::for_source(source)? != *self {
            return Err(Error::UnsupportedProfile);
        }

        let RecoveryOperation::HistoricalTrades { page } = operation else {
            return Err(Error::InvalidOperation);
        };

        let HistoricalTradeCursor::TradeId { from_id } = &page.cursor else {
            return Err(Error::InvalidOperation);
        };

        // Validate that the cursor belongs to the same
        // recovery review and does not precede its baseline.
        self.operation_for_cursor(&review.stream_id, review, *from_id)?;

        if page.limit != Some(self.max_records_per_page) {
            return Err(Error::InvalidOperation);
        }

        Ok(RestRequest::new("binance_spot", "historical_trades")
            .with_parameter("symbol", source.symbol.clone())
            .with_parameter("fromId", from_id.to_string())
            .with_parameter("limit", self.max_records_per_page.to_string()))
    }
    /// Build the logical REST request for a specific
    /// recovery review.
    ///
    /// The operation and request must refer to the
    /// same accepted numeric baseline.
    pub fn request_for_source(
        &self,
        source: &CreateSourceStreamRequest,
        review: &SourceTradeRecoveryReview,
    ) -> Result<RestRequest, SourceTradeRecoveryPolicyError> {
        use SourceTradeRecoveryPolicyError as Error;

        let expected = Self::for_source(source)?;

        if &expected != self {
            return Err(Error::UnsupportedProfile);
        }

        let operation = self.operation_for_review(&review.stream_id, review)?;

        let RecoveryOperation::HistoricalTrades { page } = operation else {
            return Err(Error::UnsupportedProfile);
        };

        let HistoricalTradeCursor::TradeId { from_id } = page.cursor else {
            return Err(Error::UnsupportedProfile);
        };

        Ok(RestRequest::new("binance_spot", "historical_trades")
            .with_parameter("symbol", source.symbol.clone())
            .with_parameter("fromId", from_id.to_string())
            .with_parameter("limit", self.max_records_per_page.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::{
        config::exchange::{ExchangeId, MarketType},
        integrity::{
            TradeIntegrityBaseline, TradeIntegrityObservation, TradeIntegrityObservationKind,
        },
        stream::request::QuantityType,
    };

    fn source() -> CreateSourceStreamRequest {
        CreateSourceStreamRequest {
            instrument_id: 42,
            exchange: ExchangeId::Binance,
            symbol: "BTCUSDT".into(),
            market_category: MarketType::Spot,
            instrument_type: "spot".into(),

            base_asset: Some("BTC".into()),
            quote_asset: Some("USDT".into()),
            settlement_asset: None,

            quantity_type: QuantityType::Base,
            contract_value: None,
            contract_value_asset: None,

            channel: SourceChannel::Trades,
        }
    }

    fn review() -> SourceTradeRecoveryReview {
        SourceTradeRecoveryReview {
            stream_id: "binance:spot:BTCUSDT:trades".into(),
            stream_generation: 10,

            baseline: TradeIntegrityBaseline {
                numeric_trade_id: Some(100),
                cross_sequence: None,
            },

            triggering_records: 1,

            observations: vec![TradeIntegrityObservation {
                index: 0,
                kind: TradeIntegrityObservationKind::NumericIdDiscontinuity,
                previous: 100,
                current: 105,
            }],
        }
    }

    #[test]
    fn rejects_depth_source() {
        let mut request = source();
        request.channel = SourceChannel::Depth;

        assert!(matches!(
            SourceTradeRecoveryPolicy::for_source(&request),
            Err(SourceTradeRecoveryPolicyError::WrongChannel)
        ));
    }

    #[test]
    fn rejects_unsupported_market() {
        let mut request = source();
        request.market_category = MarketType::Linear;

        assert!(matches!(
            SourceTradeRecoveryPolicy::for_source(&request),
            Err(SourceTradeRecoveryPolicyError::UnsupportedProfile)
        ));
    }

    #[test]
    fn rejects_unsupported_exchange() {
        let mut request = source();
        request.exchange = ExchangeId::Bybit;

        assert!(matches!(
            SourceTradeRecoveryPolicy::for_source(&request),
            Err(SourceTradeRecoveryPolicyError::UnsupportedProfile)
        ));
    }

    #[test]
    fn creates_operation_for_matching_review() {
        let policy = SourceTradeRecoveryPolicy::for_source(&source()).unwrap();

        let review = review();

        let operation = policy
            .operation_for_review(&review.stream_id, &review)
            .unwrap();

        let RecoveryOperation::HistoricalTrades { page } = operation else {
            panic!("expected historical trade recovery");
        };

        assert_eq!(page.cursor, HistoricalTradeCursor::TradeId { from_id: 101 });

        assert_eq!(page.limit, Some(1000));
    }

    #[test]
    fn rejects_foreign_review() {
        let policy = SourceTradeRecoveryPolicy::for_source(&source()).unwrap();

        assert!(matches!(
            policy.operation_for_review("another-stream", &review()),
            Err(SourceTradeRecoveryPolicyError::WrongStream)
        ));
    }

    #[test]
    fn rejects_review_without_numeric_baseline() {
        let policy = SourceTradeRecoveryPolicy::for_source(&source()).unwrap();

        let mut review = review();
        review.baseline.numeric_trade_id = None;

        assert!(matches!(
            policy.operation_for_review(&review.stream_id, &review),
            Err(SourceTradeRecoveryPolicyError::MissingNumericBaseline)
        ));
    }

    #[test]
    fn rejects_review_without_evidence() {
        let policy = SourceTradeRecoveryPolicy::for_source(&source()).unwrap();

        let mut review = review();
        review.observations.clear();

        assert!(matches!(
            policy.operation_for_review(&review.stream_id, &review),
            Err(SourceTradeRecoveryPolicyError::MissingCoverageEvidence)
        ));
    }
    #[test]
    fn recovery_request_preserves_native_instrument_binding() {
        let source = source();
        let review = review();

        let policy = SourceTradeRecoveryPolicy::for_source(&source).unwrap();

        let request = policy.request_for_source(&source, &review).unwrap();

        assert_eq!(request.profile(), "binance_spot");
        assert_eq!(request.endpoint(), "historical_trades");

        assert_eq!(
            request.parameters().get("symbol").map(String::as_str),
            Some("BTCUSDT"),
        );

        assert_eq!(
            request.parameters().get("fromId").map(String::as_str),
            Some("101"),
        );

        assert_eq!(
            request.parameters().get("limit").map(String::as_str),
            Some("1000"),
        );
    }
    #[test]
    fn binance_spot_uses_historical_trade_id_cursor() {
        let policy = SourceTradeRecoveryPolicy::for_source(&source()).unwrap();

        let review = review();

        let operation = policy
            .operation_for_review(&review.stream_id, &review)
            .unwrap();

        let RecoveryOperation::HistoricalTrades { page } = operation else {
            panic!("expected historical trade recovery");
        };

        assert_eq!(page.cursor, HistoricalTradeCursor::TradeId { from_id: 101 });

        assert_eq!(page.limit, Some(1000));
    }
    #[test]
    fn rejects_exhausted_numeric_trade_cursor() {
        let policy = SourceTradeRecoveryPolicy::for_source(&source()).unwrap();

        let mut review = review();

        review.baseline.numeric_trade_id = Some(u64::MAX);

        assert!(matches!(
            policy.operation_for_review(&review.stream_id, &review),
            Err(SourceTradeRecoveryPolicyError::CursorExhausted)
        ));
    }
    #[test]
    fn historical_continuation_uses_explicit_cursor() {
        let source = source();
        let review = review();

        let policy = SourceTradeRecoveryPolicy::for_source(&source).unwrap();

        // The first page begins at baseline + 1 = 101.
        //
        // Assume the collector validated the first 100
        // records and returned cursor 201.
        let operation = policy
            .operation_for_cursor(&review.stream_id, &review, 201)
            .unwrap();

        let RecoveryOperation::HistoricalTrades { page } = &operation else {
            panic!("expected historical trade operation");
        };

        assert_eq!(page.cursor, HistoricalTradeCursor::TradeId { from_id: 201 });

        assert_eq!(page.limit, Some(1000));

        let request = policy
            .request_for_operation(&source, &review, &operation)
            .unwrap();

        assert_eq!(request.endpoint(), "historical_trades");

        assert_eq!(
            request.parameters().get("fromId").map(String::as_str),
            Some("201"),
        );

        assert_eq!(
            request.parameters().get("symbol").map(String::as_str),
            Some("BTCUSDT"),
        );
    }
    #[test]
    fn continuation_cannot_move_before_initial_cursor() {
        let policy = SourceTradeRecoveryPolicy::for_source(&source()).unwrap();

        let review = review();

        let result = policy.operation_for_cursor(&review.stream_id, &review, 100);

        assert!(matches!(
            result,
            Err(SourceTradeRecoveryPolicyError::InvalidContinuationCursor)
        ));
    }
}
