use std::collections::{BTreeMap, HashMap};

use thiserror::Error;

use super::{DesiredSubscription, DesiredSubscriptionSnapshot, SubscriptionRestorationError};

// ============================================================
// CONNECTION SLOT IDENTITY
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SlotId(u64);

impl SlotId {
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Connections may share subscriptions only when their
/// exchange profile and native WebSocket route are compatible.
///
/// Additional compatibility dimensions must be incorporated
/// here before supporting authenticated or differentiated
/// endpoints.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SlotKey {
    pub profile: String,
    pub route: Option<String>,
}

impl SlotKey {
    pub fn new(profile: impl Into<String>, route: Option<String>) -> Self {
        Self {
            profile: profile.into(),
            route,
        }
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub(crate) enum ConnectionSlotsError {
    #[error("stream already assigned: {0}")]
    AlreadyAssigned(String),

    #[error("unknown stream: {0}")]
    UnknownStream(String),

    #[error("connection slot identifier exhausted")]
    SlotIdExhausted,

    #[error("subscription revision exhausted")]
    RevisionExhausted,

    #[error("connection slot capacity must be positive")]
    InvalidCapacity,

    #[error(transparent)]
    Snapshot(#[from] SubscriptionRestorationError),
}

// ============================================================
// CONNECTION SLOT
// ============================================================

#[derive(Debug)]
struct ConnectionSlot {
    key: SlotKey,
    revision: u64,
    capacity: Option<usize>,
    subscriptions: BTreeMap<String, DesiredSubscription>,
}

impl ConnectionSlot {
    fn has_capacity(&self) -> bool {
        self.capacity
            .is_none_or(|maximum| self.subscriptions.len() < maximum)
    }

    fn snapshot(&self) -> Result<DesiredSubscriptionSnapshot, ConnectionSlotsError> {
        Ok(DesiredSubscriptionSnapshot::new(
            self.revision,
            self.subscriptions.values().cloned(),
        )?)
    }
}

// ============================================================
// ALLOCATION RESULT
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SlotAssignment {
    pub slot_id: SlotId,
    pub revision: u64,
    pub created: bool,
}

// ============================================================
// CONNECTION SLOT REGISTRY
// ============================================================

/// Owns logical WebSocket connection-slot assignments.
///
/// Does not own physical connections, Tokio tasks, or
/// authoritative consumer demand.
///
/// All mutations must be serialized by WebSocketManager.
///
/// This implementation groups by an already-resolved
/// exchange profile and native route.
///
/// Endpoint and authentication compatibility must be
/// incorporated before production allocation.
pub(crate) struct ConnectionSlots {
    next_slot_id: u64,

    slots: BTreeMap<SlotId, ConnectionSlot>,

    stream_slots: HashMap<String, SlotId>,
}

impl ConnectionSlots {
    pub fn new() -> Self {
        Self {
            next_slot_id: 0,
            slots: BTreeMap::new(),
            stream_slots: HashMap::new(),
        }
    }

    // ========================================================
    // ASSIGN STREAM
    // ========================================================

    pub fn assign(
        &mut self,
        key: SlotKey,
        subscription: DesiredSubscription,
        capacity: Option<usize>,
    ) -> Result<SlotAssignment, ConnectionSlotsError> {
        if capacity == Some(0) {
            return Err(ConnectionSlotsError::InvalidCapacity);
        }

        let stream_id = subscription.stream_id.clone();

        if self.stream_slots.contains_key(&stream_id) {
            return Err(ConnectionSlotsError::AlreadyAssigned(stream_id));
        }

        // Find an existing compatible slot with capacity.
        let existing = self
            .slots
            .iter()
            .find(|(_, slot)| slot.key == key && slot.capacity == capacity && slot.has_capacity())
            .map(|(id, _)| *id);

        if let Some(slot_id) = existing {
            let slot = self.slots.get_mut(&slot_id).expect("selected slot exists");

            let next_revision = slot
                .revision
                .checked_add(1)
                .ok_or(ConnectionSlotsError::RevisionExhausted)?;

            slot.subscriptions.insert(stream_id.clone(), subscription);

            slot.revision = next_revision;

            self.stream_slots.insert(stream_id, slot_id);

            return Ok(SlotAssignment {
                slot_id,
                revision: next_revision,
                created: false,
            });
        }

        // Allocate a new logical connection slot.
        let next_id = self
            .next_slot_id
            .checked_add(1)
            .ok_or(ConnectionSlotsError::SlotIdExhausted)?;

        let slot_id = SlotId(next_id);

        let mut subscriptions = BTreeMap::new();
        subscriptions.insert(stream_id.clone(), subscription);

        self.slots.insert(
            slot_id,
            ConnectionSlot {
                key,
                revision: 1,
                capacity,
                subscriptions,
            },
        );

        self.stream_slots.insert(stream_id, slot_id);
        self.next_slot_id = next_id;

        Ok(SlotAssignment {
            slot_id,
            revision: 1,
            created: true,
        })
    }

    // ========================================================
    // REMOVE STREAM
    // ========================================================

    /// Remove an assigned source stream.
    ///
    /// Returns Some(updated assignment) when the connection
    /// slot still has subscriptions.
    ///
    /// Returns None when the slot became empty and was removed.
    ///
    /// WebSocketManager must stop the corresponding runner
    /// before retiring the slot's runtime resources.
    pub fn remove(
        &mut self,
        stream_id: &str,
    ) -> Result<Option<SlotAssignment>, ConnectionSlotsError> {
        let slot_id = *self
            .stream_slots
            .get(stream_id)
            .ok_or_else(|| ConnectionSlotsError::UnknownStream(stream_id.to_owned()))?;

        let slot = self
            .slots
            .get_mut(&slot_id)
            .expect("stream references an existing slot");

        if slot.subscriptions.len() == 1 {
            self.stream_slots.remove(stream_id);
            self.slots.remove(&slot_id);

            return Ok(None);
        }

        let next_revision = slot
            .revision
            .checked_add(1)
            .ok_or(ConnectionSlotsError::RevisionExhausted)?;

        slot.subscriptions.remove(stream_id);
        slot.revision = next_revision;

        self.stream_slots.remove(stream_id);

        Ok(Some(SlotAssignment {
            slot_id,
            revision: next_revision,
            created: false,
        }))
    }

    // ========================================================
    // LOOKUP
    // ========================================================

    pub fn slot_for(&self, stream_id: &str) -> Option<SlotId> {
        self.stream_slots.get(stream_id).copied()
    }

    pub fn snapshot(
        &self,
        slot_id: SlotId,
    ) -> Result<Option<DesiredSubscriptionSnapshot>, ConnectionSlotsError> {
        self.slots
            .get(&slot_id)
            .map(ConnectionSlot::snapshot)
            .transpose()
    }

    pub fn key(&self, slot_id: SlotId) -> Option<&SlotKey> {
        self.slots.get(&slot_id).map(|slot| &slot.key)
    }

    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    pub fn stream_count(&self) -> usize {
        self.stream_slots.len()
    }
}

impl Default for ConnectionSlots {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(id: &str, channel: &str) -> DesiredSubscription {
        DesiredSubscription::new(id, channel, "BTCUSDT").unwrap()
    }

    fn binance() -> SlotKey {
        SlotKey::new("binance_linear", None)
    }

    #[test]
    fn compatible_streams_share_connection_slot() {
        let mut slots = ConnectionSlots::new();

        let first = slots
            .assign(binance(), stream("btc-trades", "trades"), Some(10))
            .unwrap();

        let second = slots
            .assign(binance(), stream("btc-depth", "depth"), Some(10))
            .unwrap();

        assert!(first.created);
        assert!(!second.created);

        assert_eq!(first.slot_id, second.slot_id);
        assert_eq!(slots.slot_count(), 1);
        assert_eq!(slots.stream_count(), 2);

        let snapshot = slots.snapshot(first.slot_id).unwrap().unwrap();

        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.revision(), 2);
    }

    #[test]
    fn different_routes_use_different_slots() {
        let mut slots = ConnectionSlots::new();

        let public = slots
            .assign(
                SlotKey::new("okx_linear", Some("public".into())),
                stream("btc-depth", "depth"),
                Some(10),
            )
            .unwrap();

        let business = slots
            .assign(
                SlotKey::new("okx_linear", Some("business".into())),
                stream("btc-trades", "trades"),
                Some(10),
            )
            .unwrap();

        assert_ne!(public.slot_id, business.slot_id);
        assert_eq!(slots.slot_count(), 2);
    }

    #[test]
    fn capacity_creates_additional_slot() {
        let mut slots = ConnectionSlots::new();

        let first = slots
            .assign(binance(), stream("stream-1", "trades"), Some(1))
            .unwrap();

        let second = slots
            .assign(binance(), stream("stream-2", "trades"), Some(1))
            .unwrap();

        assert_ne!(first.slot_id, second.slot_id);
        assert_eq!(slots.slot_count(), 2);
    }

    #[test]
    fn removing_one_stream_preserves_other_subscriptions() {
        let mut slots = ConnectionSlots::new();

        let first = slots
            .assign(binance(), stream("btc-trades", "trades"), Some(10))
            .unwrap();

        slots
            .assign(binance(), stream("btc-depth", "depth"), Some(10))
            .unwrap();

        let updated = slots.remove("btc-trades").unwrap().unwrap();

        assert_eq!(updated.slot_id, first.slot_id);
        assert_eq!(updated.revision, 3);

        let snapshot = slots.snapshot(first.slot_id).unwrap().unwrap();

        assert_eq!(snapshot.len(), 1);
        assert!(snapshot.get("btc-trades").is_none());
        assert!(snapshot.get("btc-depth").is_some());
    }

    #[test]
    fn removing_last_stream_retires_slot() {
        let mut slots = ConnectionSlots::new();

        let assignment = slots
            .assign(binance(), stream("btc-trades", "trades"), Some(10))
            .unwrap();

        assert!(slots.remove("btc-trades").unwrap().is_none());

        assert_eq!(slots.slot_count(), 0);
        assert_eq!(slots.stream_count(), 0);

        assert!(slots.snapshot(assignment.slot_id).unwrap().is_none());
    }

    #[test]
    fn duplicate_stream_identity_is_rejected() {
        let mut slots = ConnectionSlots::new();

        slots
            .assign(binance(), stream("btc-trades", "trades"), Some(10))
            .unwrap();

        let duplicate = slots.assign(binance(), stream("btc-trades", "trades"), Some(10));

        assert!(matches!(
            duplicate,
            Err(ConnectionSlotsError::AlreadyAssigned(_))
        ));

        assert_eq!(slots.stream_count(), 1);
    }
}
