use super::{
    model::{ActivationPolicy, PersistedStreamDefinition, StreamDefinition, StreamOrigin},
    repository::{StreamDefinitionError, StreamDefinitionRepository},
};

use crate::live::stream::request::SourceChannel;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveStreamDescriptor {
    pub stream_id: String,
    pub origin: StreamOrigin,

    pub instrument_id: Option<i64>,
    pub channel: Option<SourceChannel>,

    pub enabled: bool,
    pub activation_policy: ActivationPolicy,

    pub revision: i64,
}

impl From<PersistedStreamDefinition> for LiveStreamDescriptor {
    fn from(value: PersistedStreamDefinition) -> Self {
        let (instrument_id, channel) = match &value.definition {
            StreamDefinition::Source(request) => {
                (Some(request.instrument_id), Some(request.channel))
            }

            StreamDefinition::Derived(_) => (None, None),
        };

        Self {
            stream_id: value.stream_id,
            origin: value.definition.origin(),
            instrument_id,
            channel,
            enabled: value.desired.enabled,
            activation_policy: value.desired.activation_policy,
            revision: value.revision,
        }
    }
}

/// Read-only persistent stream discovery.
///
/// This does not describe current connection health,
/// event availability, or continuity.
#[derive(Clone)]
pub struct LiveStreamDiscovery {
    repository: StreamDefinitionRepository,
}

impl LiveStreamDiscovery {
    pub fn new(repository: StreamDefinitionRepository) -> Self {
        Self { repository }
    }

    pub async fn get(
        &self,
        stream_id: &str,
    ) -> Result<Option<LiveStreamDescriptor>, StreamDefinitionError> {
        Ok(self.repository.find(stream_id).await?.map(Into::into))
    }

    pub async fn list(&self) -> Result<Vec<LiveStreamDescriptor>, StreamDefinitionError> {
        Ok(self
            .repository
            .list()
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::{
        config::exchange::{ExchangeId, MarketType},
        stream::request::{CreateSourceStreamRequest, QuantityType},
    };

    #[test]
    fn source_descriptor_preserves_instrument_and_channel() {
        let persisted = PersistedStreamDefinition {
            stream_id: "btc-trades".into(),

            recording_stream_key: Some(10),

            definition: StreamDefinition::Source(CreateSourceStreamRequest {
                instrument_id: 15,
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
            }),

            desired: super::super::model::DesiredStreamState {
                enabled: true,
                activation_policy: ActivationPolicy::AlwaysOn,
            },

            revision: 3,
        };

        let descriptor = LiveStreamDescriptor::from(persisted);

        assert_eq!(descriptor.stream_id, "btc-trades");
        assert_eq!(descriptor.origin, StreamOrigin::Source);
        assert_eq!(descriptor.instrument_id, Some(15));
        assert_eq!(descriptor.channel, Some(SourceChannel::Trades));
        assert_eq!(descriptor.revision, 3);
        assert!(descriptor.enabled);
    }
}
