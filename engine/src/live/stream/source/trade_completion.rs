use super::{CompletedTradeAttempt, SourceTradeRecoveryReview};
use crate::live::infrastructure::rest::ExchangeRestResponse;
use crate::live::integrity::TradeSourceEpoch;
use crate::live::recovery::RecoveryOperation;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SourceTradeCompletionError {
    #[error("REST completion belongs to an obsolete acquisition")]
    ObsoleteEpoch,

    #[error("REST completion belongs to another stream")]
    WrongStream,

    #[error("trade coverage review changed during REST acquisition")]
    ObsoleteReview,

    #[error("REST completion has an invalid trade recovery operation")]
    WrongOperation,

    #[error("REST response has not been reconciled")]
    ReconciliationPending,

    #[error("REST trade acquisition failed: {0}")]
    Acquisition(String),

    #[error("REST trade response belongs to another exchange profile")]
    WrongProfile,

    #[error("REST trade response belongs to another endpoint")]
    WrongEndpoint,

    #[error("REST trade response returned unsuccessful HTTP status: {0}")]
    HttpStatus(u16),

    #[error("REST trade response body is empty")]
    EmptyResponse,
}

/// Validate an already mailbox-accepted REST trade completion.
///
/// This is an additional source-level authority check.
/// It does not normalize, publish, or commit trades.
pub fn validate_trade_completion(
    completed: &CompletedTradeAttempt,
    stream_id: &str,
    active_epoch: TradeSourceEpoch,
    current_review: &SourceTradeRecoveryReview,
) -> Result<(), SourceTradeCompletionError> {
    use SourceTradeCompletionError as Error;

    let context = &completed.context;

    if context.epoch != active_epoch {
        return Err(Error::ObsoleteEpoch);
    }

    if context.review.stream_id != stream_id {
        return Err(Error::WrongStream);
    }

    if completed.completion.requester.stream_id != stream_id {
        return Err(Error::WrongStream);
    }

    if completed.completion.requester.epoch != context.recovery_epoch()
        || completed.completion.requester.attempt != context.attempt
    {
        return Err(Error::ObsoleteEpoch);
    }

    if context.review.stream_generation != current_review.stream_generation
        || context.review.stream_id != current_review.stream_id
        || context.review.baseline != current_review.baseline
        || context.review.observations != current_review.observations
        || context.review.triggering_records != current_review.triggering_records
    {
        return Err(Error::ObsoleteReview);
    }

    if !matches!(
        context.operation,
        RecoveryOperation::RecentTrades { .. } | RecoveryOperation::HistoricalTrades { .. }
    ) {
        return Err(Error::WrongOperation);
    }

    Ok(())
}
/// authorized trade recovery completion.
///
/// The profile is supplied by the trusted source configuration,
/// never inferred from the response body.
///
/// A successful result does not prove complete historical
/// coverage or authorize trade publication.
pub fn validate_trade_response<'a>(
    completed: &'a CompletedTradeAttempt,
    expected_profile: &str,
) -> Result<&'a ExchangeRestResponse, SourceTradeCompletionError> {
    use SourceTradeCompletionError as Error;

    let expected_endpoint = match &completed.context.operation {
        RecoveryOperation::RecentTrades { .. } => "recent_trades",

        RecoveryOperation::HistoricalTrades { .. } => "historical_trades",

        RecoveryOperation::DepthSnapshot { .. } => {
            return Err(Error::WrongOperation);
        }
    };

    let response = completed
        .completion
        .result
        .as_ref()
        .map_err(|error| Error::Acquisition(error.to_string()))?;

    if response.profile() != expected_profile {
        return Err(Error::WrongProfile);
    }

    if response.endpoint() != expected_endpoint {
        return Err(Error::WrongEndpoint);
    }

    if !(200..300).contains(&response.status()) {
        return Err(Error::HttpStatus(response.status()));
    }

    if response.body_bytes() == 0 {
        return Err(Error::EmptyResponse);
    }

    Ok(response.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::infrastructure::rest::{
        HistoricalTradeCursor, HistoricalTradePage, RestRequest,
    };
    use crate::live::{
        infrastructure::rest::{ExchangeRestResponse, RestResponseHeaders},
        integrity::{
            TradeIntegrityBaseline, TradeIntegrityObservation, TradeIntegrityObservationKind,
        },
        recovery::{RecoveryAttemptId, RecoveryJobId, RecoveryRequestId, RecoveryRequester},
    };
    use serde_json::json;
    use std::{sync::Arc, time::Duration};

    use super::super::SourceTradeAttemptContext;

    const STREAM: &str = "binance:spot:BTCUSDT:trades";

    fn completed(
        profile: &str,
        endpoint: &str,
        status: u16,
        bytes: usize,
    ) -> CompletedTradeAttempt {
        let epoch = TradeSourceEpoch {
            stream_generation: 10,
            connection_generation: 20,
            subscription_generation: 30,
        };

        let context = SourceTradeAttemptContext {
            epoch,
            attempt: RecoveryAttemptId(1),

            review: SourceTradeRecoveryReview {
                stream_id: STREAM.into(),
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
            },

            operation: RecoveryOperation::HistoricalTrades {
                page: HistoricalTradePage::with_cursor(HistoricalTradeCursor::TradeId {
                    from_id: 101,
                })
                .with_limit(1000),
            },

            request: RestRequest::new("binance_spot", "historical_trades")
                .with_parameter("symbol", "BTCUSDT")
                .with_parameter("fromId", "101")
                .with_parameter("limit", "1000"),
        };

        let response = ExchangeRestResponse::new(
            profile,
            endpoint,
            status,
            json!([
                {
                    "id": 101,
                    "price": "80000",
                    "qty": "0.001",
                    "time": 1792806200000_i64,
                    "isBuyerMaker": true
                }
            ]),
            RestResponseHeaders::new(),
            Duration::from_millis(10),
            bytes,
        );

        let completion = crate::live::recovery::RecoveryCompletion {
            request_id: RecoveryRequestId(7),
            job_id: RecoveryJobId(7),

            requester: RecoveryRequester {
                stream_id: STREAM.into(),
                epoch: context.recovery_epoch(),
                attempt: context.attempt,
            },

            result: Ok(Arc::new(response)),
        };

        CompletedTradeAttempt {
            context,
            completion,
        }
    }

    #[test]
    fn accepts_matching_binance_spot_response() {
        let completed = completed("binance_spot", "historical_trades", 200, 100);

        let response = validate_trade_response(&completed, "binance_spot").unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(response.endpoint(), "historical_trades");
    }

    #[test]
    fn rejects_foreign_profile() {
        let completed = completed("binance_linear", "recent_trades", 200, 100);

        assert!(matches!(
            validate_trade_response(&completed, "binance_spot",),
            Err(SourceTradeCompletionError::WrongProfile)
        ));
    }

    #[test]
    fn rejects_foreign_endpoint() {
        let completed = completed("binance_spot", "recent_trades", 200, 100);

        assert!(matches!(
            validate_trade_response(&completed, "binance_spot",),
            Err(SourceTradeCompletionError::WrongEndpoint)
        ));
    }

    #[test]
    fn rejects_http_failure() {
        let completed = completed("binance_spot", "historical_trades", 500, 100);

        assert!(matches!(
            validate_trade_response(&completed, "binance_spot",),
            Err(SourceTradeCompletionError::HttpStatus(500))
        ));
    }

    #[test]
    fn rejects_empty_response_body() {
        let completed = completed("binance_spot", "historical_trades", 200, 0);

        assert!(matches!(
            validate_trade_response(&completed, "binance_spot",),
            Err(SourceTradeCompletionError::EmptyResponse)
        ));
    }
}
