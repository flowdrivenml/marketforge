use thiserror::Error;

use crate::{
    live::infrastructure::normalization::{
        NormalizationSource, NormalizedTrades, TradeRecordSemantics,
    },
    shared::domain::canonical::Trade,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceTradePageLimits {
    pub max_pages: usize,
    pub max_records: usize,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SourceTradePageError {
    #[error("trade pagination limits must be positive")]
    InvalidLimits,

    #[error("REST trade page belongs to another stream")]
    WrongStream,

    #[error("historical pagination requires individual REST trades")]
    WrongSource,

    #[error("REST trade page is empty")]
    EmptyPage,

    #[error("invalid numeric trade identity")]
    InvalidIdentity,

    #[error("REST trade pages are not numerically continuous")]
    Discontinuous,

    #[error("historical pagination exceeded configured limits")]
    CapacityExceeded,

    #[error("numeric trade cursor is exhausted")]
    CursorExhausted,

    #[error("historical recovery has not reached its target")]
    Incomplete,

    #[error("historical recovery already reached its target")]
    AlreadyComplete,

    #[error("cannot construct normalized recovery batch: {0}")]
    InvalidBatch(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceTradePageProgress {
    MoreRequired { from_id: u64 },
    Complete,
}

/// Bounded collector for one numeric-ID historical
/// recovery interval.
///
/// The collector requires REST pages to be continuous
/// from baseline + 1 through the triggering trade.
///
/// This is a strategy-specific condition. It is not
/// a universal guarantee of exchange trade completeness.
#[derive(Debug)]
pub struct SourceTradePageCollector {
    stream_id: String,
    target_id: u64,
    next_id: u64,
    limits: SourceTradePageLimits,
    pages: usize,
    trades: Vec<Trade>,
    complete: bool,
}

impl SourceTradePageCollector {
    pub fn new(
        stream_id: impl Into<String>,
        baseline_id: u64,
        target_id: u64,
        limits: SourceTradePageLimits,
    ) -> Result<Self, SourceTradePageError> {
        if limits.max_pages == 0 || limits.max_records == 0 {
            return Err(SourceTradePageError::InvalidLimits);
        }

        let next_id = baseline_id
            .checked_add(1)
            .ok_or(SourceTradePageError::CursorExhausted)?;

        if target_id < next_id {
            return Err(SourceTradePageError::InvalidLimits);
        }

        Ok(Self {
            stream_id: stream_id.into(),
            target_id,
            next_id,
            limits,
            pages: 0,
            trades: Vec::new(),
            complete: false,
        })
    }

    pub fn next_from_id(&self) -> Option<u64> {
        (!self.complete).then_some(self.next_id)
    }

    pub fn page_count(&self) -> usize {
        self.pages
    }

    pub fn record_count(&self) -> usize {
        self.trades.len()
    }

    pub fn is_complete(&self) -> bool {
        self.complete
    }

    pub fn push_page(
        &mut self,
        page: &NormalizedTrades,
    ) -> Result<SourceTradePageProgress, SourceTradePageError> {
        use SourceTradePageError as Error;

        if self.complete {
            return Err(Error::AlreadyComplete);
        }

        if page.stream_id() != self.stream_id {
            return Err(Error::WrongStream);
        }

        if page.source() != NormalizationSource::Rest
            || page.semantics() != TradeRecordSemantics::Individual
        {
            return Err(Error::WrongSource);
        }

        if page.trades().is_empty() {
            return Err(Error::EmptyPage);
        }

        if self.pages >= self.limits.max_pages {
            return Err(Error::CapacityExceeded);
        }

        let next_count = self
            .trades
            .len()
            .checked_add(page.record_count())
            .ok_or(Error::CapacityExceeded)?;

        if next_count > self.limits.max_records {
            return Err(Error::CapacityExceeded);
        }

        // Validate the entire page before modifying
        // the collector's authoritative state.
        let mut expected = Some(self.next_id);
        let mut reached_target = false;

        for trade in page.trades() {
            if trade.envelope.stream_id != self.stream_id {
                return Err(Error::WrongStream);
            }

            let id = trade
                .trade_id
                .as_deref()
                .and_then(|id| id.parse::<u64>().ok())
                .ok_or(Error::InvalidIdentity)?;

            if Some(id) != expected {
                return Err(Error::Discontinuous);
            }
            if id >= self.target_id {
                reached_target = true;
            }

            // Avoid overflow when processing the last
            // representable numeric trade identity.
            // None represents exhaustion of the numeric ID space.
            //
            // Any subsequent record will fail the continuity check,
            // including a duplicate u64::MAX.
            expected = id.checked_add(1);

            if expected.is_none() && id != self.target_id {
                return Err(Error::CursorExhausted);
            }
        }

        // All validation succeeded. Commit this page.
        self.trades.extend_from_slice(page.trades());
        self.pages += 1;
        self.complete = reached_target;

        if self.complete {
            Ok(SourceTradePageProgress::Complete)
        } else {
            self.next_id = expected.ok_or(Error::CursorExhausted)?;

            Ok(SourceTradePageProgress::MoreRequired {
                from_id: self.next_id,
            })
        }
    }

    /// Consume the complete REST bridge.
    ///
    /// The resulting message remains REST-provenance
    /// normalized data, not integrity-accepted trades.
    pub fn finish(self) -> Result<NormalizedTrades, SourceTradePageError> {
        if !self.complete {
            return Err(SourceTradePageError::Incomplete);
        }

        NormalizedTrades::new(
            self.stream_id,
            NormalizationSource::Rest,
            TradeRecordSemantics::Individual,
            self.trades,
        )
        .map_err(SourceTradePageError::InvalidBatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rust_decimal::Decimal;

    use crate::live::infrastructure::normalization::{
        NormalizationSource, NormalizedTrades, TradeRecordSemantics,
    };

    use crate::shared::domain::canonical::{EventEnvelope, Exchange, Trade, TradeSide};

    const STREAM: &str = "binance:spot:BTCUSDT:trades";

    fn limits() -> SourceTradePageLimits {
        SourceTradePageLimits {
            max_pages: 3,
            max_records: 10,
        }
    }

    fn collector(baseline: u64, target: u64) -> SourceTradePageCollector {
        SourceTradePageCollector::new(STREAM, baseline, target, limits()).unwrap()
    }

    fn trade(id: u64) -> Trade {
        Trade {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange: Exchange::Binance,
                instrument_id: 42,
                symbol: "BTCUSDT".into(),
                stream_id: STREAM.into(),
            },

            trade_id: Some(id.to_string()),
            sequence: None,

            side: TradeSide::Buy,
            price: Decimal::from(80_000),

            quantity_base: Some(Decimal::ONE),
            quantity_quote: None,
            quantity_contracts: None,

            is_rpi: None,
            trade_iv: None,
            mark_iv: None,

            index_price: None,
            mark_price: None,
        }
    }

    fn page(ids: &[u64]) -> NormalizedTrades {
        NormalizedTrades::new(
            STREAM,
            NormalizationSource::Rest,
            TradeRecordSemantics::Individual,
            ids.iter().copied().map(trade).collect(),
        )
        .unwrap()
    }

    #[test]
    fn completes_single_page_recovery() {
        let mut collector = collector(100, 103);

        let progress = collector.push_page(&page(&[101, 102, 103])).unwrap();

        assert_eq!(progress, SourceTradePageProgress::Complete);

        assert!(collector.is_complete());
        assert_eq!(collector.page_count(), 1);
        assert_eq!(collector.record_count(), 3);
        assert_eq!(collector.next_from_id(), None);

        let recovered = collector.finish().unwrap();

        let ids: Vec<_> = recovered
            .trades()
            .iter()
            .map(|trade| trade.trade_id.as_deref().unwrap())
            .collect();

        assert_eq!(ids, vec!["101", "102", "103"]);
    }

    #[test]
    fn continues_across_multiple_pages() {
        let mut collector = collector(100, 105);

        assert_eq!(
            collector.push_page(&page(&[101, 102])).unwrap(),
            SourceTradePageProgress::MoreRequired { from_id: 103 }
        );

        assert_eq!(collector.next_from_id(), Some(103));

        assert_eq!(
            collector.push_page(&page(&[103, 104])).unwrap(),
            SourceTradePageProgress::MoreRequired { from_id: 105 }
        );

        assert_eq!(
            collector.push_page(&page(&[105])).unwrap(),
            SourceTradePageProgress::Complete
        );

        assert_eq!(collector.page_count(), 3);
        assert_eq!(collector.record_count(), 5);

        let recovered = collector.finish().unwrap();

        assert_eq!(recovered.record_count(), 5);
    }

    #[test]
    fn missing_trade_id_rejects_page_atomically() {
        let mut collector = collector(100, 105);

        collector.push_page(&page(&[101, 102])).unwrap();

        let result = collector.push_page(&page(&[103, 105]));

        assert_eq!(result, Err(SourceTradePageError::Discontinuous));

        // The rejected page must not mutate the collector.
        assert_eq!(collector.page_count(), 1);
        assert_eq!(collector.record_count(), 2);
        assert_eq!(collector.next_from_id(), Some(103));
        assert!(!collector.is_complete());
    }

    #[test]
    fn repeated_trade_id_rejects_page_atomically() {
        let mut collector = collector(100, 105);

        collector.push_page(&page(&[101, 102])).unwrap();

        let result = collector.push_page(&page(&[102, 103]));

        assert_eq!(result, Err(SourceTradePageError::Discontinuous));

        assert_eq!(collector.page_count(), 1);
        assert_eq!(collector.record_count(), 2);
        assert_eq!(collector.next_from_id(), Some(103));
    }

    #[test]
    fn page_limit_prevents_unbounded_recovery() {
        let mut collector = SourceTradePageCollector::new(
            STREAM,
            100,
            105,
            SourceTradePageLimits {
                max_pages: 2,
                max_records: 10,
            },
        )
        .unwrap();

        collector.push_page(&page(&[101])).unwrap();
        collector.push_page(&page(&[102])).unwrap();

        assert_eq!(
            collector.push_page(&page(&[103])),
            Err(SourceTradePageError::CapacityExceeded)
        );

        assert_eq!(collector.page_count(), 2);
        assert_eq!(collector.record_count(), 2);
    }

    #[test]
    fn record_limit_prevents_unbounded_recovery() {
        let mut collector = SourceTradePageCollector::new(
            STREAM,
            100,
            105,
            SourceTradePageLimits {
                max_pages: 10,
                max_records: 3,
            },
        )
        .unwrap();

        collector.push_page(&page(&[101, 102])).unwrap();

        assert_eq!(
            collector.push_page(&page(&[103, 104])),
            Err(SourceTradePageError::CapacityExceeded)
        );

        assert_eq!(collector.record_count(), 2);
        assert_eq!(collector.next_from_id(), Some(103));
    }

    #[test]
    fn incomplete_recovery_cannot_be_finished() {
        let mut collector = collector(100, 105);

        collector.push_page(&page(&[101, 102])).unwrap();

        assert!(matches!(
            collector.finish(),
            Err(SourceTradePageError::Incomplete)
        ));
    }

    #[test]
    fn completed_recovery_rejects_additional_pages() {
        let mut collector = collector(100, 102);

        assert_eq!(
            collector.push_page(&page(&[101, 102])),
            Ok(SourceTradePageProgress::Complete)
        );

        assert_eq!(
            collector.push_page(&page(&[103])),
            Err(SourceTradePageError::AlreadyComplete)
        );

        assert_eq!(collector.record_count(), 2);
    }

    #[test]
    fn rejects_exhausted_initial_cursor() {
        assert!(matches!(
            SourceTradePageCollector::new(STREAM, u64::MAX, u64::MAX, limits(),),
            Err(SourceTradePageError::CursorExhausted)
        ));
    }

    #[test]
    fn accepts_maximum_trade_id_as_final_target() {
        let mut collector = collector(u64::MAX - 1, u64::MAX);

        assert_eq!(
            collector.push_page(&page(&[u64::MAX])),
            Ok(SourceTradePageProgress::Complete)
        );

        assert_eq!(collector.next_from_id(), None);
        assert_eq!(collector.record_count(), 1);
    }
    #[test]
    fn rejects_records_after_maximum_numeric_identity() {
        let mut collector = collector(u64::MAX - 2, u64::MAX);

        let result = collector.push_page(&page(&[u64::MAX - 1, u64::MAX, u64::MAX]));

        assert_eq!(result, Err(SourceTradePageError::Discontinuous));

        assert_eq!(collector.page_count(), 0);
        assert_eq!(collector.record_count(), 0);
        assert!(!collector.is_complete());
    }
}
