use super::SourceProcessingEpoch;
use crate::live::recovery::RecoverySourceEpoch;
use crate::live::{
    infrastructure::websocket::StreamAcquisition,
    integrity::{
        DepthSourceBinding, DepthSourceEpoch, IntegrityLifecycleEvent, TradeSourceBinding,
        TradeSourceEpoch,
    },
};
use thiserror::Error;

pub struct TrustedAcquisitionBinding {
    pub epoch: SourceProcessingEpoch,
    pub lifecycle: Option<IntegrityLifecycleEvent>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SourceAcquisitionError {
    #[error("obsolete or duplicate transport acquisition")]
    ObsoleteAcquisition,

    #[error("integrity generation counter exhausted")]
    GenerationExhausted,

    #[error("invalid depth binding: {0}")]
    DepthBinding(String),

    #[error("invalid trade binding: {0}")]
    TradeBinding(String),
}

#[derive(Debug, Clone)]
enum BindingTemplate {
    Depth(DepthSourceBinding),
    Trades(TradeSourceBinding),
}

/// Trusted, single-owner integrity acquisition allocator.
///
/// One binder belongs to one SourceStreamRunner lifetime.
///
/// It starts from the generation already installed in the
/// integrity handle and advances on each confirmed acquisition.
///
/// The physical connection stamp is preserved independently
/// by StreamAcquisition and SourceTransportGate.
pub struct SourceAcquisitionBinder {
    template: BindingTemplate,

    last_stream_generation: u64,
    last_connection_generation: u64,
    last_subscription_generation: u64,

    last_acquisition: Option<StreamAcquisition>,
}

impl SourceAcquisitionBinder {
    pub fn depth(binding: DepthSourceBinding) -> Self {
        let epoch = binding.epoch();

        Self {
            template: BindingTemplate::Depth(binding),
            last_stream_generation: epoch.stream_generation,
            last_connection_generation: epoch.connection_generation,
            last_subscription_generation: epoch.subscription_generation,
            last_acquisition: None,
        }
    }

    pub fn trades(binding: TradeSourceBinding) -> Self {
        let epoch = binding.epoch();

        Self {
            template: BindingTemplate::Trades(binding),
            last_stream_generation: epoch.stream_generation,
            last_connection_generation: epoch.connection_generation,
            last_subscription_generation: epoch.subscription_generation,
            last_acquisition: None,
        }
    }

    pub fn last_acquisition(&self) -> Option<&StreamAcquisition> {
        self.last_acquisition.as_ref()
    }

    pub fn stream_generation(&self) -> u64 {
        self.last_stream_generation
    }

    /// Produce a trusted successor binding.
    ///
    /// The caller must invoke this only after the transport
    /// gate accepts an ACK-confirmed Connected event.
    ///
    /// Each successful call creates a new integrity generation
    /// and matching trusted lifecycle transition.
    pub fn bind(
        &mut self,
        acquisition: &StreamAcquisition,
    ) -> Result<TrustedAcquisitionBinding, SourceAcquisitionError> {
        if acquisition.generation == 0 {
            return Err(SourceAcquisitionError::ObsoleteAcquisition);
        }

        if let Some(previous) = &self.last_acquisition {
            if acquisition.generation <= previous.generation {
                return Err(SourceAcquisitionError::ObsoleteAcquisition);
            }
        }

        // Compute all successor counters before mutating state.
        // Exhaustion must leave the previous binding authoritative.
        let stream_generation = self
            .last_stream_generation
            .checked_add(1)
            .ok_or(SourceAcquisitionError::GenerationExhausted)?;

        let connection_generation = self
            .last_connection_generation
            .checked_add(1)
            .ok_or(SourceAcquisitionError::GenerationExhausted)?;

        let subscription_generation = self
            .last_subscription_generation
            .checked_add(1)
            .ok_or(SourceAcquisitionError::GenerationExhausted)?;

        let trusted = match &self.template {
            BindingTemplate::Depth(template) => {
                let epoch = DepthSourceEpoch {
                    stream_generation,
                    connection_generation,
                    subscription_generation,
                };

                let binding = DepthSourceBinding::new(
                    template.stream_id(),
                    template.exchange(),
                    template.market(),
                    template.instrument_id(),
                    template.symbol(),
                    template.subscribed_depth(),
                    epoch,
                )
                .map_err(|error| SourceAcquisitionError::DepthBinding(format!("{error:?}")))?;

                TrustedAcquisitionBinding {
                    epoch: SourceProcessingEpoch::Depth(epoch),
                    lifecycle: Some(IntegrityLifecycleEvent::DepthGeneration(binding)),
                }
            }

            BindingTemplate::Trades(template) => {
                let epoch = TradeSourceEpoch {
                    stream_generation,
                    connection_generation,
                    subscription_generation,
                };

                let binding = TradeSourceBinding::new(
                    template.stream_id(),
                    template.exchange(),
                    template.market(),
                    template.instrument_id(),
                    template.symbol(),
                    epoch,
                )
                .map_err(|error| SourceAcquisitionError::TradeBinding(format!("{error:?}")))?;

                TrustedAcquisitionBinding {
                    epoch: SourceProcessingEpoch::Trades(epoch),
                    lifecycle: Some(IntegrityLifecycleEvent::TradeGeneration(binding)),
                }
            }
        };

        // Commit only after successor construction succeeds.
        self.last_stream_generation = stream_generation;
        self.last_connection_generation = connection_generation;
        self.last_subscription_generation = subscription_generation;
        self.last_acquisition = Some(acquisition.clone());

        Ok(trusted)
    }
}

impl SourceProcessingEpoch {
    pub fn recovery_epoch(self) -> RecoverySourceEpoch {
        match self {
            Self::Depth(epoch) => RecoverySourceEpoch {
                stream_generation: epoch.stream_generation,
                connection_generation: epoch.connection_generation,
                subscription_generation: epoch.subscription_generation,
            },

            Self::Trades(epoch) => RecoverySourceEpoch {
                stream_generation: epoch.stream_generation,
                connection_generation: epoch.connection_generation,
                subscription_generation: epoch.subscription_generation,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        live::infrastructure::websocket::ConnectionFence, shared::domain::canonical::Exchange,
    };

    fn acquisition(generation: u64) -> StreamAcquisition {
        let fence = ConnectionFence::new("marketforge-test-runtime", "test-slot").unwrap();

        StreamAcquisition {
            connection: fence.begin_connect().unwrap(),
            generation,
        }
    }

    fn initial_depth_binding() -> DepthSourceBinding {
        DepthSourceBinding::new(
            "btc-depth",
            Exchange::Bybit,
            "spot",
            42,
            "BTCUSDT",
            Some(200),
            DepthSourceEpoch {
                stream_generation: 10,
                connection_generation: 20,
                subscription_generation: 30,
            },
        )
        .unwrap()
    }

    fn initial_trade_binding() -> TradeSourceBinding {
        TradeSourceBinding::new(
            "btc-trades",
            Exchange::Bybit,
            "spot",
            42,
            "BTCUSDT",
            TradeSourceEpoch {
                stream_generation: 10,
                connection_generation: 20,
                subscription_generation: 30,
            },
        )
        .unwrap()
    }

    #[test]
    fn depth_binding_preserves_authoritative_identity() {
        let initial = initial_depth_binding();

        let mut binder = SourceAcquisitionBinder::depth(initial.clone());

        let transport = acquisition(1);

        let trusted = binder.bind(&transport).unwrap();

        match trusted {
            TrustedAcquisitionBinding {
                epoch: SourceProcessingEpoch::Depth(epoch),
                lifecycle: Some(IntegrityLifecycleEvent::DepthGeneration(binding)),
            } => {
                assert_eq!(epoch.stream_generation, 11);
                assert_eq!(epoch.connection_generation, 21);
                assert_eq!(epoch.subscription_generation, 31);

                assert_eq!(binding.epoch(), epoch);

                assert_eq!(binding.stream_id(), initial.stream_id());
                assert_eq!(binding.exchange(), initial.exchange());
                assert_eq!(binding.market(), initial.market());
                assert_eq!(binding.instrument_id(), initial.instrument_id());
                assert_eq!(binding.symbol(), initial.symbol());

                assert_eq!(binding.subscribed_depth(), initial.subscribed_depth(),);
            }

            _ => panic!("expected trusted depth generation"),
        }

        assert_eq!(binder.last_acquisition(), Some(&transport));
    }

    #[test]
    fn trade_binding_preserves_authoritative_identity() {
        let initial = initial_trade_binding();

        let mut binder = SourceAcquisitionBinder::trades(initial.clone());

        let trusted = binder.bind(&acquisition(1)).unwrap();

        match trusted {
            TrustedAcquisitionBinding {
                epoch: SourceProcessingEpoch::Trades(epoch),
                lifecycle: Some(IntegrityLifecycleEvent::TradeGeneration(binding)),
            } => {
                assert_eq!(epoch.stream_generation, 11);
                assert_eq!(binding.epoch(), epoch);

                assert_eq!(binding.stream_id(), initial.stream_id());
                assert_eq!(binding.exchange(), initial.exchange());
                assert_eq!(binding.market(), initial.market());
                assert_eq!(binding.instrument_id(), initial.instrument_id());
                assert_eq!(binding.symbol(), initial.symbol());
            }

            _ => panic!("expected trusted trade generation"),
        }
    }

    #[test]
    fn reconnect_advances_all_integrity_generations() {
        let mut binder = SourceAcquisitionBinder::depth(initial_depth_binding());

        let first = binder.bind(&acquisition(1)).unwrap();
        let second = binder.bind(&acquisition(2)).unwrap();

        let first_epoch = match first.epoch {
            SourceProcessingEpoch::Depth(epoch) => epoch,
            _ => unreachable!(),
        };

        let second_epoch = match second.epoch {
            SourceProcessingEpoch::Depth(epoch) => epoch,
            _ => unreachable!(),
        };

        assert!(second_epoch.stream_generation > first_epoch.stream_generation);

        assert!(second_epoch.connection_generation > first_epoch.connection_generation);

        assert!(second_epoch.subscription_generation > first_epoch.subscription_generation);
    }

    #[test]
    fn duplicate_acquisition_is_rejected_without_mutation() {
        let mut binder = SourceAcquisitionBinder::depth(initial_depth_binding());

        let transport = acquisition(1);

        binder.bind(&transport).unwrap();

        let generation_before = binder.stream_generation();

        assert!(matches!(
            binder.bind(&transport),
            Err(SourceAcquisitionError::ObsoleteAcquisition)
        ));

        assert_eq!(binder.stream_generation(), generation_before,);
    }

    #[test]
    fn obsolete_acquisition_cannot_replace_current_binding() {
        let mut binder = SourceAcquisitionBinder::trades(initial_trade_binding());

        binder.bind(&acquisition(2)).unwrap();

        let before = binder.stream_generation();

        assert!(matches!(
            binder.bind(&acquisition(1)),
            Err(SourceAcquisitionError::ObsoleteAcquisition)
        ));

        assert_eq!(binder.stream_generation(), before);
    }

    #[test]
    fn generation_exhaustion_is_atomic() {
        let initial = DepthSourceBinding::new(
            "btc-depth",
            Exchange::Bybit,
            "spot",
            42,
            "BTCUSDT",
            Some(200),
            DepthSourceEpoch {
                stream_generation: u64::MAX,
                connection_generation: 20,
                subscription_generation: 30,
            },
        )
        .unwrap();

        let mut binder = SourceAcquisitionBinder::depth(initial);

        assert!(matches!(
            binder.bind(&acquisition(1)),
            Err(SourceAcquisitionError::GenerationExhausted)
        ));

        assert_eq!(binder.stream_generation(), u64::MAX);
        assert!(binder.last_acquisition().is_none());
    }
    #[test]
    fn depth_epoch_maps_exactly_to_recovery_epoch() {
        let epoch = SourceProcessingEpoch::Depth(DepthSourceEpoch {
            stream_generation: 11,
            connection_generation: 21,
            subscription_generation: 31,
        });

        assert_eq!(
            epoch.recovery_epoch(),
            RecoverySourceEpoch {
                stream_generation: 11,
                connection_generation: 21,
                subscription_generation: 31,
            }
        );
    }

    #[test]
    fn trade_epoch_maps_exactly_to_recovery_epoch() {
        let epoch = SourceProcessingEpoch::Trades(TradeSourceEpoch {
            stream_generation: 12,
            connection_generation: 22,
            subscription_generation: 32,
        });

        assert_eq!(
            epoch.recovery_epoch(),
            RecoverySourceEpoch {
                stream_generation: 12,
                connection_generation: 22,
                subscription_generation: 32,
            }
        );
    }
}
