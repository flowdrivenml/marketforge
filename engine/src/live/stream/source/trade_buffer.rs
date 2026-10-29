use std::collections::VecDeque;

use thiserror::Error;

use crate::live::infrastructure::normalization::{
    NormalizationSource, NormalizedTrades, TradeRecordSemantics,
};
use crate::live::integrity::TradeSourceEpoch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceTradeBufferLimits {
    pub max_batches: usize,
    pub max_records: usize,
    pub max_bytes: usize,
}

impl SourceTradeBufferLimits {
    pub fn validate(self) -> Result<Self, SourceTradeBufferError> {
        if self.max_batches == 0 || self.max_records == 0 || self.max_bytes == 0 {
            return Err(SourceTradeBufferError::InvalidLimits);
        }

        Ok(self)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SourceTradeBufferError {
    #[error("trade buffer limits must be positive")]
    InvalidLimits,

    #[error("trade buffer belongs to another acquisition epoch")]
    ObsoleteEpoch,

    #[error("trade buffer accepts only individual WebSocket trades")]
    InvalidSource,

    #[error("trade buffer belongs to another logical stream")]
    WrongStream,

    #[error("trade recovery buffer capacity exceeded")]
    CapacityExceeded,
}

/// Unaccepted live trades retained during a coverage review.
///
/// This buffer does not perform deduplication, reconciliation,
/// identity commits, or publication.
#[derive(Debug)]
pub struct SourceTradeLiveBuffer {
    stream_id: String,
    epoch: TradeSourceEpoch,
    limits: SourceTradeBufferLimits,
    batches: VecDeque<NormalizedTrades>,
    records: usize,
    bytes: usize,
}

impl SourceTradeLiveBuffer {
    pub fn new(
        stream_id: impl Into<String>,
        epoch: TradeSourceEpoch,
        limits: SourceTradeBufferLimits,
    ) -> Result<Self, SourceTradeBufferError> {
        Ok(Self {
            stream_id: stream_id.into(),
            epoch,
            limits: limits.validate()?,
            batches: VecDeque::new(),
            records: 0,
            bytes: 0,
        })
    }

    pub fn epoch(&self) -> TradeSourceEpoch {
        self.epoch
    }

    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    pub fn record_count(&self) -> usize {
        self.records
    }

    pub fn accounted_bytes(&self) -> usize {
        self.bytes
    }

    pub fn batches(&self) -> impl Iterator<Item = &NormalizedTrades> {
        self.batches.iter()
    }

    pub fn push(
        &mut self,
        epoch: TradeSourceEpoch,
        message: NormalizedTrades,
        accounted_bytes: usize,
    ) -> Result<(), SourceTradeBufferError> {
        if epoch != self.epoch {
            return Err(SourceTradeBufferError::ObsoleteEpoch);
        }

        if message.stream_id() != self.stream_id {
            return Err(SourceTradeBufferError::WrongStream);
        }

        if message.source() != NormalizationSource::WebSocket
            || message.semantics() != TradeRecordSemantics::Individual
        {
            return Err(SourceTradeBufferError::InvalidSource);
        }

        let records = self
            .records
            .checked_add(message.record_count())
            .ok_or(SourceTradeBufferError::CapacityExceeded)?;

        let bytes = self
            .bytes
            .checked_add(accounted_bytes)
            .ok_or(SourceTradeBufferError::CapacityExceeded)?;

        if self.batches.len() >= self.limits.max_batches
            || records > self.limits.max_records
            || bytes > self.limits.max_bytes
        {
            return Err(SourceTradeBufferError::CapacityExceeded);
        }

        // All admission checks have completed.
        // A rejected push leaves the buffer unchanged.
        self.batches.push_back(message);
        self.records = records;
        self.bytes = bytes;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rust_decimal::Decimal;

    use crate::shared::domain::canonical::{EventEnvelope, Exchange, Trade, TradeSide};

    const STREAM: &str = "binance:spot:BTCUSDT:trades";

    fn epoch(generation: u64) -> TradeSourceEpoch {
        TradeSourceEpoch {
            stream_generation: generation,
            connection_generation: 20,
            subscription_generation: 30,
        }
    }

    fn limits() -> SourceTradeBufferLimits {
        SourceTradeBufferLimits {
            max_batches: 2,
            max_records: 3,
            max_bytes: 1000,
        }
    }

    fn message(ids: &[&str]) -> NormalizedTrades {
        let trades = ids
            .iter()
            .map(|id| Trade {
                envelope: EventEnvelope {
                    event_timestamp_ns: 1_000_000_000,
                    system_timestamp_ns: None,
                    exchange: Exchange::Binance,
                    instrument_id: 42,
                    symbol: "BTCUSDT".into(),
                    stream_id: STREAM.into(),
                },
                trade_id: Some((*id).into()),
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
            })
            .collect();

        NormalizedTrades::new(
            STREAM,
            NormalizationSource::WebSocket,
            TradeRecordSemantics::Individual,
            trades,
        )
        .unwrap()
    }

    #[test]
    fn preserves_batches_and_source_order() {
        let mut buffer = SourceTradeLiveBuffer::new(STREAM, epoch(10), limits()).unwrap();

        buffer
            .push(epoch(10), message(&["101", "102"]), 100)
            .unwrap();

        buffer.push(epoch(10), message(&["103"]), 100).unwrap();

        assert_eq!(buffer.batch_count(), 2);
        assert_eq!(buffer.record_count(), 3);
        assert_eq!(buffer.accounted_bytes(), 200);

        let ids: Vec<_> = buffer
            .batches()
            .flat_map(|batch| batch.trades().iter())
            .map(|trade| trade.trade_id.as_deref().unwrap())
            .collect();

        assert_eq!(ids, vec!["101", "102", "103"]);
    }

    #[test]
    fn obsolete_epoch_cannot_enter_buffer() {
        let mut buffer = SourceTradeLiveBuffer::new(STREAM, epoch(10), limits()).unwrap();

        assert_eq!(
            buffer.push(epoch(11), message(&["101"]), 100),
            Err(SourceTradeBufferError::ObsoleteEpoch)
        );

        assert_eq!(buffer.record_count(), 0);
    }

    #[test]
    fn capacity_failure_preserves_existing_batches() {
        let mut buffer = SourceTradeLiveBuffer::new(STREAM, epoch(10), limits()).unwrap();

        buffer
            .push(epoch(10), message(&["101", "102"]), 100)
            .unwrap();

        assert_eq!(
            buffer.push(epoch(10), message(&["103", "104"]), 100),
            Err(SourceTradeBufferError::CapacityExceeded)
        );

        assert_eq!(buffer.batch_count(), 1);
        assert_eq!(buffer.record_count(), 2);
        assert_eq!(buffer.accounted_bytes(), 100);
    }

    #[test]
    fn rejects_oversized_transport_payload() {
        let mut buffer = SourceTradeLiveBuffer::new(STREAM, epoch(10), limits()).unwrap();

        assert_eq!(
            buffer.push(epoch(10), message(&["101"]), 1001),
            Err(SourceTradeBufferError::CapacityExceeded)
        );

        assert_eq!(buffer.batch_count(), 0);
    }

    #[test]
    fn batch_limit_is_independent_of_record_limit() {
        let mut buffer = SourceTradeLiveBuffer::new(STREAM, epoch(10), limits()).unwrap();

        buffer.push(epoch(10), message(&[]), 10).unwrap();
        buffer.push(epoch(10), message(&[]), 10).unwrap();

        assert_eq!(
            buffer.push(epoch(10), message(&[]), 10),
            Err(SourceTradeBufferError::CapacityExceeded)
        );

        assert_eq!(buffer.batch_count(), 2);
        assert_eq!(buffer.record_count(), 0);
    }
}
