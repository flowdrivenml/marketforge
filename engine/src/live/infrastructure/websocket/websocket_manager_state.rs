use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use thiserror::Error;
use tokio::sync::mpsc;

use crate::live::config::exchange::ExchangeConfig;

use super::{DesiredSubscriptionSnapshot, StreamBinding, StreamInput};

use super::connection_slots::{ConnectionSlots, ConnectionSlotsError, SlotId, SlotKey};

use super::stream_dispatch::{StreamDispatchError, StreamDispatcher};

use super::stream_source::StreamSource;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub(crate) enum WebSocketManagerStateError {
    #[error("duplicate exchange profile: {0}")]
    DuplicateProfile(String),

    #[error("unknown exchange profile: {0}")]
    UnknownProfile(String),

    #[error("unknown or disabled channel: {profile}/{channel}")]
    UnsupportedChannel { profile: String, channel: String },

    #[error("unknown websocket route: {profile}/{route}")]
    UnknownRoute { profile: String, route: String },

    #[error("unknown attachment binding")]
    UnknownBinding,

    #[error("stream attachment state is inconsistent")]
    InconsistentState,

    #[error(transparent)]
    Slots(#[from] ConnectionSlotsError),

    #[error(transparent)]
    Dispatch(#[from] StreamDispatchError),
}

// ============================================================
// CHANGE RESULTS
// ============================================================

/// Result of a successful local attachment.
///
/// This describes the physical-slot operation that the
/// asynchronous manager must subsequently perform.
pub(crate) struct ManagerAttachment {
    pub binding: StreamBinding,

    pub slot_id: SlotId,

    pub created_slot: bool,

    pub desired: DesiredSubscriptionSnapshot,
}

/// Result of a successful local detachment.
///
/// `desired == None` means the connection slot became empty
/// and must be shut down.
pub(crate) struct ManagerDetachment {
    pub slot_id: SlotId,

    pub desired: Option<DesiredSubscriptionSnapshot>,
}

// ============================================================
// MANAGER OWNERSHIP STATE
// ============================================================

/// Synchronous ownership core of WebSocketManager.
///
/// Owns:
/// - Validated exchange profile references.
/// - Logical connection-slot assignments.
/// - Source routing descriptions.
/// - Stream attachment bindings.
/// - Shared stream destination dispatcher.
///
/// Does not own physical WebSocket tasks.
///
/// The asynchronous WebSocketManager must serialize
/// mutations and coordinate them with ManagedConnectionSlot.
pub(crate) struct WebSocketManagerState {
    profiles: BTreeMap<String, Arc<ExchangeConfig>>,

    slots: ConnectionSlots,

    dispatcher: StreamDispatcher,

    sources: BTreeMap<String, StreamSource>,

    bindings: HashMap<u64, String>,
}

impl WebSocketManagerState {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        profiles: impl IntoIterator<Item = Arc<ExchangeConfig>>,
    ) -> Result<Self, WebSocketManagerStateError> {
        let mut resolved = BTreeMap::new();

        for config in profiles {
            let profile = config.profile().to_owned();

            if resolved.insert(profile.clone(), config).is_some() {
                return Err(WebSocketManagerStateError::DuplicateProfile(profile));
            }
        }

        Ok(Self {
            profiles: resolved,
            slots: ConnectionSlots::new(),
            dispatcher: StreamDispatcher::new(),
            sources: BTreeMap::new(),
            bindings: HashMap::new(),
        })
    }
    /// Look up the logical slot associated with an attachment.
    ///
    /// Returns None when the binding is unknown.
    pub fn slot_for_binding(&self, binding: StreamBinding) -> Option<SlotId> {
        let stream_id = self.bindings.get(&binding.id())?;

        self.slots.slot_for(stream_id)
    }

    /// Number of subscribed streams assigned to one slot.
    pub fn slot_stream_count(&self, slot_id: SlotId) -> Result<usize, WebSocketManagerStateError> {
        let snapshot = self
            .slots
            .snapshot(slot_id)?
            .ok_or(WebSocketManagerStateError::InconsistentState)?;

        Ok(snapshot.len())
    }
    /// Revoke transport availability for every stream remaining
    /// in a stopped connection slot.
    ///
    /// Attempts every notification, even when one fails.
    ///
    /// The caller must retire the slot's attachments if this
    /// method returns an error.
    pub fn disconnect_slot_local(&self, slot_id: SlotId) -> Result<(), WebSocketManagerStateError> {
        let dispatcher = self.dispatcher();

        let mut first_error = None;

        for source in self.sources_for_slot(slot_id) {
            if let Err(error) = dispatcher.disconnect_for_replacement(source.stream_id()) {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        if let Some(error) = first_error {
            return Err(WebSocketManagerStateError::Dispatch(error));
        }

        Ok(())
    }

    /// Retire every attachment belonging to a failed slot.
    ///
    /// Other connection slots are unaffected.
    ///
    /// This is a terminal cleanup operation: the caller must
    /// not subsequently restart this slot using these bindings.
    pub fn retire_slot_local(
        &mut self,
        slot_id: SlotId,
    ) -> Result<usize, WebSocketManagerStateError> {
        let stream_ids = self
            .sources_for_slot(slot_id)
            .into_iter()
            .map(|source| source.stream_id().to_owned())
            .collect::<Vec<_>>();

        let mut retired = 0;

        for stream_id in stream_ids {
            let binding = self
                .dispatcher
                .binding_for(&stream_id)?
                .ok_or(WebSocketManagerStateError::InconsistentState)?;

            self.detach_local(binding)?;

            retired += 1;
        }

        Ok(retired)
    }
    /// Retire every local attachment and slot assignment.
    ///
    /// This operation must only be called by a manager that
    /// is shutting down and will never accept new attachments.
    ///
    /// Connection-slot tasks must already have been stopped
    /// before this operation is called.
    pub fn retire_all_local(&mut self) -> Result<usize, WebSocketManagerStateError> {
        let retired = self.dispatcher.detach_all()?;

        self.slots = ConnectionSlots::new();

        self.sources.clear();

        self.bindings.clear();

        Ok(retired)
    }
    // ========================================================
    // ATTACH
    // ========================================================

    /// Register a source stream with its destination.
    ///
    /// All profile and channel checks happen before mutation.
    ///
    /// If destination registration fails after slot assignment,
    /// the slot assignment is rolled back.
    ///
    /// This is a LOCAL ownership transaction only.
    /// The asynchronous manager must still coordinate
    /// starting or updating the physical connection.
    pub fn attach_local(
        &mut self,
        source: StreamSource,
        destination: mpsc::Sender<StreamInput>,
    ) -> Result<ManagerAttachment, WebSocketManagerStateError> {
        let profile = source.profile();

        let config = self
            .profiles
            .get(profile)
            .ok_or_else(|| WebSocketManagerStateError::UnknownProfile(profile.to_owned()))?;

        let subscription = source.subscription();

        let channel = config
            .channel(&subscription.channel_name)
            .filter(|channel| channel.enabled)
            .ok_or_else(|| WebSocketManagerStateError::UnsupportedChannel {
                profile: profile.to_owned(),
                channel: subscription.channel_name.clone(),
            })?;

        let route = channel.route.clone();

        if let Some(route_name) = &route {
            if !config.websocket.routes.contains_key(route_name) {
                return Err(WebSocketManagerStateError::UnknownRoute {
                    profile: profile.to_owned(),
                    route: route_name.clone(),
                });
            }
        }

        // The exchange configuration has already been
        // validated; duplicate declarations are expected
        // to agree.
        let capacity = config
            .websocket
            .max_subscriptions_per_connection
            .or_else(|| {
                config
                    .websocket
                    .limits
                    .as_ref()
                    .and_then(|limits| limits.max_subscriptions_per_connection)
            })
            .map(|value| usize::try_from(value).unwrap_or(usize::MAX));

        let stream_id = source.stream_id().to_owned();

        let assignment =
            self.slots
                .assign(SlotKey::new(profile, route), subscription.clone(), capacity)?;

        // Attach the stream destination after the slot
        // assignment has succeeded.
        let binding = match self.dispatcher.attach(stream_id.clone(), destination) {
            Ok(binding) => binding,

            Err(error) => {
                // No physical work has begun yet.
                //
                // Roll back the local slot assignment.
                self.slots.remove(&stream_id)?;

                return Err(WebSocketManagerStateError::Dispatch(error));
            }
        };

        let desired = self
            .slots
            .snapshot(assignment.slot_id)?
            .ok_or(WebSocketManagerStateError::InconsistentState)?;

        self.bindings.insert(binding.id(), stream_id.clone());

        self.sources.insert(stream_id, source);

        Ok(ManagerAttachment {
            binding,
            slot_id: assignment.slot_id,
            created_slot: assignment.created,
            desired,
        })
    }

    // ========================================================
    // DETACH
    // ========================================================

    /// Remove one attachment using its opaque binding.
    ///
    /// Stale bindings cannot detach replacement streams.
    ///
    /// Returns the updated slot projection, or None if
    /// the slot was removed.
    pub fn detach_local(
        &mut self,
        binding: StreamBinding,
    ) -> Result<ManagerDetachment, WebSocketManagerStateError> {
        let stream_id = self
            .bindings
            .get(&binding.id())
            .cloned()
            .ok_or(WebSocketManagerStateError::UnknownBinding)?;

        let slot_id = self
            .slots
            .slot_for(&stream_id)
            .ok_or(WebSocketManagerStateError::InconsistentState)?;

        // The dispatcher validates the exact binding.
        self.dispatcher.detach(&stream_id, binding)?;

        // ConnectionSlots owns the subscription projection.
        self.slots.remove(&stream_id)?;

        self.sources.remove(&stream_id);

        self.bindings.remove(&binding.id());

        let desired = self.slots.snapshot(slot_id)?;

        Ok(ManagerDetachment { slot_id, desired })
    }

    // ========================================================
    // LOOKUPS
    // ========================================================

    pub fn profile(&self, name: &str) -> Option<&Arc<ExchangeConfig>> {
        self.profiles.get(name)
    }

    pub fn dispatcher(&self) -> StreamDispatcher {
        self.dispatcher.clone()
    }

    pub fn source(&self, stream_id: &str) -> Option<&StreamSource> {
        self.sources.get(stream_id)
    }

    pub fn sources_for_slot(&self, slot_id: SlotId) -> Vec<StreamSource> {
        self.sources
            .values()
            .filter(|source| self.slots.slot_for(source.stream_id()) == Some(slot_id))
            .cloned()
            .collect()
    }

    pub fn slot_key(&self, slot_id: SlotId) -> Option<&SlotKey> {
        self.slots.key(slot_id)
    }

    pub fn slot_count(&self) -> usize {
        self.slots.slot_count()
    }

    pub fn stream_count(&self) -> usize {
        self.slots.stream_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use super::super::DesiredSubscription;

    fn config() -> Arc<ExchangeConfig> {
        let toml = r#"
schema_version = 1

[exchange]
id = "binance"
market = "linear"
profile = "binance_linear"

[websocket]
base_url = "wss://example.invalid"
max_subscriptions_per_connection = 10

[channels.trades]
enabled = true
format = "trades/binance_linear"

[rest]
base_url = "https://example.invalid"
"#;

        Arc::new(toml::from_str(toml).unwrap())
    }

    fn source(stream_id: &str, symbol: &str) -> StreamSource {
        StreamSource::new(
            "binance_linear",
            DesiredSubscription::new(stream_id, "trades", symbol).unwrap(),
            vec![(
                "/stream".to_owned(),
                json!(format!("{}@trade", symbol.to_ascii_lowercase())),
            )],
        )
        .unwrap()
    }

    #[test]
    fn compatible_streams_share_slot() {
        let mut manager = WebSocketManagerState::new([config()]).unwrap();

        let (btc_tx, _btc_rx) = mpsc::channel(16);
        let (eth_tx, _eth_rx) = mpsc::channel(16);

        let btc = manager
            .attach_local(source("btc-trades", "BTCUSDT"), btc_tx)
            .unwrap();

        assert!(btc.created_slot);
        assert_eq!(btc.desired.len(), 1);

        let eth = manager
            .attach_local(source("eth-trades", "ETHUSDT"), eth_tx)
            .unwrap();

        assert!(!eth.created_slot);

        assert_eq!(btc.slot_id, eth.slot_id);

        assert_eq!(eth.desired.len(), 2);

        assert_eq!(manager.slot_count(), 1);
        assert_eq!(manager.stream_count(), 2);
    }

    #[test]
    fn removing_one_stream_preserves_other_demand() {
        let mut manager = WebSocketManagerState::new([config()]).unwrap();

        let (btc_tx, _btc_rx) = mpsc::channel(16);
        let (eth_tx, _eth_rx) = mpsc::channel(16);

        let btc = manager
            .attach_local(source("btc-trades", "BTCUSDT"), btc_tx)
            .unwrap();

        let eth = manager
            .attach_local(source("eth-trades", "ETHUSDT"), eth_tx)
            .unwrap();

        let removal = manager.detach_local(btc.binding).unwrap();

        assert_eq!(removal.slot_id, eth.slot_id);

        let remaining = removal.desired.unwrap();

        assert_eq!(remaining.len(), 1);
        assert!(remaining.get("eth-trades").is_some());

        assert_eq!(manager.slot_count(), 1);
        assert_eq!(manager.stream_count(), 1);
    }

    #[test]
    fn removing_last_stream_retires_slot() {
        let mut manager = WebSocketManagerState::new([config()]).unwrap();

        let (tx, _rx) = mpsc::channel(16);

        let attached = manager
            .attach_local(source("btc-trades", "BTCUSDT"), tx)
            .unwrap();

        let removal = manager.detach_local(attached.binding).unwrap();

        assert!(removal.desired.is_none());

        assert_eq!(manager.slot_count(), 0);
        assert_eq!(manager.stream_count(), 0);
    }

    #[test]
    fn stale_binding_cannot_detach_replacement() {
        let mut manager = WebSocketManagerState::new([config()]).unwrap();

        let (first_tx, _first_rx) = mpsc::channel(16);

        let first = manager
            .attach_local(source("btc-trades", "BTCUSDT"), first_tx)
            .unwrap();

        manager.detach_local(first.binding).unwrap();

        let (second_tx, _second_rx) = mpsc::channel(16);

        let second = manager
            .attach_local(source("btc-trades", "BTCUSDT"), second_tx)
            .unwrap();

        assert_ne!(first.binding, second.binding);

        assert!(matches!(
            manager.detach_local(first.binding),
            Err(WebSocketManagerStateError::UnknownBinding)
        ));

        assert_eq!(manager.stream_count(), 1);
    }

    #[test]
    fn duplicate_attachment_does_not_change_slot_state() {
        let mut manager = WebSocketManagerState::new([config()]).unwrap();

        let (first_tx, _first_rx) = mpsc::channel(16);

        manager
            .attach_local(source("btc-trades", "BTCUSDT"), first_tx)
            .unwrap();

        let (second_tx, _second_rx) = mpsc::channel(16);

        let result = manager.attach_local(source("btc-trades", "BTCUSDT"), second_tx);

        assert!(matches!(
            result,
            Err(WebSocketManagerStateError::Slots(
                ConnectionSlotsError::AlreadyAssigned(_)
            ))
        ));

        assert_eq!(manager.slot_count(), 1);
        assert_eq!(manager.stream_count(), 1);
    }

    #[test]
    fn rollback_when_destination_registration_fails() {
        let mut manager = WebSocketManagerState::new([config()]).unwrap();

        // Deliberately occupy the destination name without
        // registering a subscription in ConnectionSlots.
        let dispatcher = manager.dispatcher();

        let (existing_tx, _existing_rx) = mpsc::channel(16);

        dispatcher.attach("btc-trades".into(), existing_tx).unwrap();

        let (new_tx, _new_rx) = mpsc::channel(16);

        let result = manager.attach_local(source("btc-trades", "BTCUSDT"), new_tx);

        assert!(matches!(
            result,
            Err(WebSocketManagerStateError::Dispatch(
                StreamDispatchError::AlreadyAttached(_)
            ))
        ));

        // Failed registration must not leave a phantom
        // subscription or empty connection slot.
        assert_eq!(manager.slot_count(), 0);
        assert_eq!(manager.stream_count(), 0);
    }
    #[test]
    fn retiring_all_attachments_closes_destinations() {
        let mut manager = WebSocketManagerState::new([config()]).unwrap();

        let (btc_tx, mut btc_rx) = mpsc::channel(16);
        let (eth_tx, mut eth_rx) = mpsc::channel(16);

        manager
            .attach_local(source("btc-trades", "BTCUSDT"), btc_tx)
            .unwrap();

        manager
            .attach_local(source("eth-trades", "ETHUSDT"), eth_tx)
            .unwrap();

        assert_eq!(manager.slot_count(), 1);
        assert_eq!(manager.stream_count(), 2);

        let retired = manager.retire_all_local().unwrap();

        assert_eq!(retired, 2);
        assert_eq!(manager.slot_count(), 0);
        assert_eq!(manager.stream_count(), 0);

        assert!(matches!(
            btc_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        assert!(matches!(
            eth_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn attachment_lookup_identifies_owning_slot() {
        let mut manager = WebSocketManagerState::new([config()]).unwrap();

        let (tx, _rx) = mpsc::channel(16);

        let attachment = manager
            .attach_local(source("btc-trades", "BTCUSDT"), tx)
            .unwrap();

        assert_eq!(
            manager.slot_for_binding(attachment.binding),
            Some(attachment.slot_id),
        );

        assert_eq!(manager.slot_stream_count(attachment.slot_id).unwrap(), 1,);

        manager.detach_local(attachment.binding).unwrap();

        assert_eq!(manager.slot_for_binding(attachment.binding), None,);
    }
    #[test]
    fn retiring_failed_slot_closes_all_its_attachments() {
        let mut manager = WebSocketManagerState::new([config()]).unwrap();

        let (btc_tx, mut btc_rx) = mpsc::channel(16);
        let (eth_tx, mut eth_rx) = mpsc::channel(16);

        let btc = manager
            .attach_local(source("btc-trades", "BTCUSDT"), btc_tx)
            .unwrap();

        let eth = manager
            .attach_local(source("eth-trades", "ETHUSDT"), eth_tx)
            .unwrap();

        // Both streams share the same slot.
        assert_eq!(btc.slot_id, eth.slot_id);

        assert_eq!(manager.retire_slot_local(btc.slot_id).unwrap(), 2);

        assert_eq!(manager.stream_count(), 0);
        assert_eq!(manager.slot_count(), 0);

        assert!(matches!(
            btc_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));

        assert!(matches!(
            eth_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
    #[test]
    fn retiring_one_slot_preserves_another_slot() {
        let mut manager = WebSocketManagerState::new([config()]).unwrap();

        let (btc_tx, mut btc_rx) = mpsc::channel(16);
        let (eth_tx, mut eth_rx) = mpsc::channel(16);

        let btc = manager
            .attach_local(source("btc-trades", "BTCUSDT"), btc_tx)
            .unwrap();

        // Use a different profile/route to force another slot.
        //
        // In this fixture, the configured slot capacity is 10,
        // so instead create ten additional subscriptions to fill
        // the existing slot before attaching ETH.
        for index in 0..9 {
            let (tx, _rx) = mpsc::channel(16);

            manager
                .attach_local(
                    source(&format!("fill-{index}"), &format!("SYMBOL{index}")),
                    tx,
                )
                .unwrap();
        }

        let eth = manager
            .attach_local(source("eth-trades", "ETHUSDT"), eth_tx)
            .unwrap();

        assert_ne!(btc.slot_id, eth.slot_id);
        assert_eq!(manager.slot_count(), 2);

        assert_eq!(manager.retire_slot_local(btc.slot_id).unwrap(), 10);

        assert_eq!(manager.slot_count(), 1);
        assert_eq!(manager.stream_count(), 1);

        assert!(matches!(
            btc_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));

        // ETH's sender is still owned by the dispatcher.
        assert!(matches!(
            eth_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}
