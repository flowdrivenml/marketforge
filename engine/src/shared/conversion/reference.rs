use std::collections::HashMap;

use crate::shared::conversion::ConversionDecimal;

use crate::shared::error::{MarketForgeError, Result};

pub use crate::shared::merge::MergeKey as ReferenceKey;

#[derive(Debug, Clone, PartialEq)]
pub struct ReferenceObservation {
    pub dataset_id: u64,
    pub stream_id: String,
    pub key: ReferenceKey,
    pub price: ConversionDecimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceUnavailable {
    Missing,
    Future,
    Stale,
}

#[derive(Debug, Clone)]
pub struct ReferenceState {
    latest: HashMap<String, ReferenceObservation>,
}

impl ReferenceState {
    pub fn new() -> Self {
        Self {
            latest: HashMap::new(),
        }
    }

    pub fn update(&mut self, observation: ReferenceObservation) -> Result<()> {
        if !observation.price.is_positive() {
            return invalid("reference price must be positive");
        }

        if let Some(previous) = self.latest.get(&observation.stream_id) {
            if observation.key <= previous.key {
                return invalid(format!(
                    "reference stream {} did not advance monotonically",
                    observation.stream_id
                ));
            }
        }

        self.latest
            .insert(observation.stream_id.clone(), observation);

        Ok(())
    }

    pub fn evaluate(
        &self,
        stream_id: &str,
        source_key: ReferenceKey,
        max_staleness_ns: u64,
    ) -> std::result::Result<&ReferenceObservation, ReferenceUnavailable> {
        let observation = self
            .latest
            .get(stream_id)
            .ok_or(ReferenceUnavailable::Missing)?;

        if observation.key >= source_key {
            return Err(ReferenceUnavailable::Future);
        }

        let age = i128::from(source_key.timestamp_ns) - i128::from(observation.key.timestamp_ns);

        if age < 0 {
            return Err(ReferenceUnavailable::Future);
        }

        if age as u128 > u128::from(max_staleness_ns) {
            return Err(ReferenceUnavailable::Stale);
        }

        Ok(observation)
    }

    pub fn len(&self) -> usize {
        self.latest.len()
    }

    pub fn is_empty(&self) -> bool {
        self.latest.is_empty()
    }
    pub fn snapshot(&self) -> Self {
        self.clone()
    }

    pub fn observations(&self) -> impl Iterator<Item = &ReferenceObservation> {
        self.latest.values()
    }
}

impl Default for ReferenceState {
    fn default() -> Self {
        Self::new()
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
