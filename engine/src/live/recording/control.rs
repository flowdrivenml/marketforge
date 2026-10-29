use std::{fmt, sync::Arc};

use tokio::sync::{mpsc, oneshot, watch};

use crate::live::config::general::RecordingConfig;

use super::writer::RecordingEvent;

pub(crate) enum RecordingCommand {
    Event(RecordingEvent),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug)]
pub enum RecordingError {
    InvalidConfiguration(String),
    InvalidQueueCapacity,
    Disabled,
    Closed,
    Database(sqlx::Error),
}

impl fmt::Display for RecordingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => {
                write!(f, "invalid recording configuration: {message}")
            }
            Self::InvalidQueueCapacity => {
                write!(f, "recording queue capacity must be positive")
            }
            Self::Disabled => {
                write!(f, "recording is disabled")
            }
            Self::Closed => {
                write!(f, "recording worker is closed")
            }
            Self::Database(error) => {
                write!(f, "PostgreSQL recording error: {error}")
            }
        }
    }
}

impl std::error::Error for RecordingError {}

impl From<sqlx::Error> for RecordingError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

#[derive(Clone)]
pub struct RecordingControl {
    sender: mpsc::Sender<RecordingCommand>,
    config_sender: Arc<watch::Sender<RecordingConfig>>,
}

impl RecordingControl {
    pub(crate) fn new(
        sender: mpsc::Sender<RecordingCommand>,
        config_sender: watch::Sender<RecordingConfig>,
    ) -> Self {
        Self {
            sender,
            config_sender: Arc::new(config_sender),
        }
    }

    /// Queues an event for asynchronous persistence.
    ///
    /// Success means queued, not committed.
    pub async fn submit(&self, event: RecordingEvent) -> Result<(), RecordingError> {
        if !self.config_sender.borrow().enabled {
            return Err(RecordingError::Disabled);
        }

        // Reserving capacity first lets us recheck the configuration
        // before consuming the event.
        let permit = self
            .sender
            .reserve()
            .await
            .map_err(|_| RecordingError::Closed)?;

        if !self.config_sender.borrow().enabled {
            return Err(RecordingError::Disabled);
        }

        permit.send(RecordingCommand::Event(event));

        Ok(())
    }

    /// Updates recording settings without restarting the worker.
    ///
    /// Configuration changes are validated before publication.
    pub fn update_config(&self, config: RecordingConfig) -> Result<(), RecordingError> {
        config
            .validate()
            .map_err(RecordingError::InvalidConfiguration)?;

        if self.sender.is_closed() {
            return Err(RecordingError::Closed);
        }

        self.config_sender.send_replace(config);

        Ok(())
    }

    pub fn config(&self) -> RecordingConfig {
        self.config_sender.borrow().clone()
    }

    /// Requests graceful worker shutdown.
    ///
    /// The worker closes the input channel and drains accepted events.
    /// This acknowledgment does not certify that PostgreSQL committed
    /// those events; the worker JoinHandle must also return Ok.
    pub async fn shutdown(&self) -> Result<(), RecordingError> {
        let (ack_sender, ack_receiver) = oneshot::channel();

        self.sender
            .send(RecordingCommand::Shutdown(ack_sender))
            .await
            .map_err(|_| RecordingError::Closed)?;

        ack_receiver.await.map_err(|_| RecordingError::Closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_runtime_settings() {
        let config = RecordingConfig {
            enabled: true,
            batch_max_records: 0,
            batch_max_delay_ms: 100,
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn accepts_runtime_batch_changes() {
        let config = RecordingConfig {
            enabled: true,
            batch_max_records: 500,
            batch_max_delay_ms: 25,
        };

        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn disabled_control_rejects_events_before_queueing() {
        use crate::shared::domain::canonical::{
            CanonicalEvent, EventEnvelope, Exchange, Trade, TradeSide,
        };
        use rust_decimal::Decimal;

        let (sender, _receiver) = mpsc::channel(1);
        let (config_sender, _) = watch::channel(RecordingConfig {
            enabled: false,
            batch_max_records: 100,
            batch_max_delay_ms: 10,
        });

        let control = RecordingControl::new(sender, config_sender);

        let event = RecordingEvent {
            stream_key: 1,
            log_epoch: 0,
            event_offset: 0,
            event: CanonicalEvent::Trade(Trade {
                envelope: EventEnvelope {
                    event_timestamp_ns: 123,
                    system_timestamp_ns: None,
                    exchange: Exchange::Bybit,
                    instrument_id: 1,
                    symbol: "BTCUSDT".into(),
                    stream_id: "test".into(),
                },
                trade_id: None,
                sequence: None,
                side: TradeSide::Buy,
                price: Decimal::ONE,
                quantity_base: Some(Decimal::ONE),
                quantity_quote: None,
                quantity_contracts: None,
                is_rpi: None,
                trade_iv: None,
                mark_iv: None,
                index_price: None,
                mark_price: None,
            }),
        };

        assert!(matches!(
            control.submit(event).await,
            Err(RecordingError::Disabled)
        ));
    }
}
