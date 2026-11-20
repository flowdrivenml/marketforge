use std::collections::{BTreeMap, HashMap};

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
    history: Option<ReferenceHistory>,
}

#[derive(Debug, Clone, Default)]
struct ReferenceHistory {
    streams: HashMap<String, BTreeMap<ReferenceKey, ReferenceObservation>>,
    external_streams: std::collections::HashSet<String>,
    regression_ns: u64,
    high_water: Option<i64>,
}

impl ReferenceState {
    pub fn new() -> Self {
        Self {
            latest: HashMap::new(),
            history: None,
        }
    }

    pub(crate) fn with_regression_window(regression_ns: u64) -> Self {
        Self {
            history: Some(ReferenceHistory {
                regression_ns,
                ..Default::default()
            }),
            ..Self::new()
        }
    }

    pub(crate) fn allows_regression(&self) -> bool {
        self.history.is_some()
    }

    pub(crate) fn advance_watermark(&mut self, timestamp_ns: i64) {
        let Some(history) = &mut self.history else {
            return;
        };
        if history.high_water.is_some_and(|high| timestamp_ns <= high) {
            return;
        }
        history.high_water = Some(timestamp_ns);
        let earliest = timestamp_ns.saturating_sub_unsigned(history.regression_ns);
        let boundary = ReferenceKey::new(earliest, 0, 0, 0);
        for observations in history.streams.values_mut() {
            retain_predecessor(observations, boundary);
        }
    }

    pub(crate) fn update_external(&mut self, observation: ReferenceObservation) -> Result<()> {
        if let Some(history) = &mut self.history {
            history
                .external_streams
                .insert(observation.stream_id.clone());
        }
        self.update(observation)
    }

    pub fn update(&mut self, observation: ReferenceObservation) -> Result<()> {
        if !observation.price.is_positive() {
            return invalid("reference price must be positive");
        }

        if let Some(history) = &mut self.history {
            let observations = history
                .streams
                .entry(observation.stream_id.clone())
                .or_default();
            if observations.contains_key(&observation.key) {
                return invalid("duplicate causal reference observation");
            }
            observations.insert(observation.key, observation.clone());
            if let Some(high) = history.high_water {
                retain_predecessor(
                    observations,
                    ReferenceKey::new(high.saturating_sub_unsigned(history.regression_ns), 0, 0, 0),
                );
            }
            self.latest
                .insert(observation.stream_id.clone(), observation);
            return Ok(());
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
        let observation = if let Some(history) = &self.history {
            let observations = history
                .streams
                .get(stream_id)
                .ok_or(ReferenceUnavailable::Missing)?;
            let cutoff = if history.external_streams.contains(stream_id) {
                ReferenceKey::new(source_key.timestamp_ns, 0, 0, 0)
            } else {
                source_key
            };
            observations
                .range(..cutoff)
                .next_back()
                .map(|(_, observation)| observation)
                .ok_or(ReferenceUnavailable::Future)?
        } else {
            self.latest
                .get(stream_id)
                .ok_or(ReferenceUnavailable::Missing)?
        };

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

// Keep the last observation below the safe event-time watermark for stale diagnostics.
fn retain_predecessor(
    observations: &mut BTreeMap<ReferenceKey, ReferenceObservation>,
    boundary: ReferenceKey,
) {
    while observations.range(..boundary).nth(1).is_some() {
        observations.pop_first();
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
