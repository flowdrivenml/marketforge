use thiserror::Error;
use tokio_tungstenite::tungstenite::Message;

use crate::live::infrastructure::websocket::{StreamAcquisition, StreamInput};

#[derive(Debug)]
pub enum SourceTransportEvent {
    /// A new ACK-confirmed acquisition became active.
    Connected(StreamAcquisition),

    /// The currently active acquisition was interrupted.
    Disconnected(StreamAcquisition),

    /// Data belongs to the currently active acquisition.
    Data {
        acquisition: StreamAcquisition,
        message: Message,
    },

    /// Duplicate or obsolete transport notification.
    Ignored,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SourceTransportGateError {
    #[error("new acquisition arrived before the previous acquisition was disconnected")]
    OverlappingAcquisition,

    #[error("logical acquisition generation exhausted")]
    GenerationExhausted,
}

/// Serial admission boundary between WebSocketManager
/// and the source processing actor.
///
/// One gate belongs to exactly one StreamBinding lifetime.
///
/// It never generates connection or subscription identities.
/// Those originate from the trusted WebSocket dispatcher.
pub struct SourceTransportGate {
    active: Option<StreamAcquisition>,
    highest_generation: u64,
}

impl Default for SourceTransportGate {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceTransportGate {
    pub fn new() -> Self {
        Self {
            active: None,
            highest_generation: 0,
        }
    }

    pub fn active(&self) -> Option<&StreamAcquisition> {
        self.active.as_ref()
    }

    pub fn highest_generation(&self) -> u64 {
        self.highest_generation
    }

    pub fn process(
        &mut self,
        input: StreamInput,
    ) -> Result<SourceTransportEvent, SourceTransportGateError> {
        match input {
            StreamInput::Connected(acquisition) => self.on_connected(acquisition),

            StreamInput::Disconnected(acquisition) => Ok(self.on_disconnected(acquisition)),

            StreamInput::Data {
                acquisition,
                message,
            } => Ok(self.on_data(acquisition, message)),
        }
    }

    fn on_connected(
        &mut self,
        acquisition: StreamAcquisition,
    ) -> Result<SourceTransportEvent, SourceTransportGateError> {
        // Duplicate ACK notification for the active acquisition.
        if self.active.as_ref() == Some(&acquisition) {
            return Ok(SourceTransportEvent::Ignored);
        }

        // Never allow an old acquisition to become active again.
        if acquisition.generation <= self.highest_generation {
            return Ok(SourceTransportEvent::Ignored);
        }

        // A new acquisition cannot silently supersede an
        // active one: integrity must observe interruption first.
        if self.active.is_some() {
            return Err(SourceTransportGateError::OverlappingAcquisition);
        }

        self.highest_generation = acquisition.generation;
        self.active = Some(acquisition.clone());

        Ok(SourceTransportEvent::Connected(acquisition))
    }

    fn on_disconnected(&mut self, acquisition: StreamAcquisition) -> SourceTransportEvent {
        // A delayed disconnection from an obsolete connection
        // must not invalidate the currently active acquisition.
        if self.active.as_ref() != Some(&acquisition) {
            return SourceTransportEvent::Ignored;
        }

        self.active = None;

        SourceTransportEvent::Disconnected(acquisition)
    }

    fn on_data(&self, acquisition: StreamAcquisition, message: Message) -> SourceTransportEvent {
        // Compare the complete identity, including the
        // physical connection stamp and logical generation.
        if self.active.as_ref() != Some(&acquisition) {
            return SourceTransportEvent::Ignored;
        }

        SourceTransportEvent::Data {
            acquisition,
            message,
        }
    }

    /// The transport input channel closed unexpectedly.
    ///
    /// Returns the acquisition that must be interrupted
    /// in the integrity layer, if any.
    pub fn channel_closed(&mut self) -> Option<StreamAcquisition> {
        self.active.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::infrastructure::websocket::ConnectionFence;

    fn acquisition(generation: u64) -> StreamAcquisition {
        let fence = ConnectionFence::new("test-runtime", "test-slot").unwrap();

        let stamp = fence.begin_connect().unwrap();

        StreamAcquisition {
            connection: stamp,
            generation,
        }
    }

    fn connected(
        gate: &mut SourceTransportGate,
        acquisition: &StreamAcquisition,
    ) -> SourceTransportEvent {
        gate.process(StreamInput::Connected(acquisition.clone()))
            .unwrap()
    }

    #[test]
    fn accepts_first_confirmed_acquisition() {
        let mut gate = SourceTransportGate::new();
        let first = acquisition(1);

        assert!(matches!(
            connected(&mut gate, &first),
            SourceTransportEvent::Connected(_)
        ));

        assert_eq!(gate.active(), Some(&first));
        assert_eq!(gate.highest_generation(), 1);
    }

    #[test]
    fn duplicate_connection_is_ignored() {
        let mut gate = SourceTransportGate::new();
        let first = acquisition(1);

        connected(&mut gate, &first);

        assert!(matches!(
            connected(&mut gate, &first),
            SourceTransportEvent::Ignored
        ));

        assert_eq!(gate.highest_generation(), 1);
    }

    #[test]
    fn data_requires_matching_acquisition() {
        let mut gate = SourceTransportGate::new();
        let first = acquisition(1);

        let before_connect = gate
            .process(StreamInput::Data {
                acquisition: first.clone(),
                message: Message::Text("before".into()),
            })
            .unwrap();

        assert!(matches!(before_connect, SourceTransportEvent::Ignored));

        connected(&mut gate, &first);

        let accepted = gate
            .process(StreamInput::Data {
                acquisition: first,
                message: Message::Text("accepted".into()),
            })
            .unwrap();

        assert!(matches!(accepted, SourceTransportEvent::Data { .. }));
    }

    #[test]
    fn stale_disconnect_cannot_interrupt_new_acquisition() {
        let mut gate = SourceTransportGate::new();

        let first = acquisition(1);
        let second = acquisition(2);

        connected(&mut gate, &first);

        assert!(matches!(
            gate.process(StreamInput::Disconnected(first.clone()))
                .unwrap(),
            SourceTransportEvent::Disconnected(_)
        ));

        connected(&mut gate, &second);

        assert!(matches!(
            gate.process(StreamInput::Disconnected(first)).unwrap(),
            SourceTransportEvent::Ignored
        ));

        assert_eq!(gate.active(), Some(&second));
    }

    #[test]
    fn stale_data_cannot_cross_reconnection() {
        let mut gate = SourceTransportGate::new();

        let first = acquisition(1);
        let second = acquisition(2);

        connected(&mut gate, &first);

        gate.process(StreamInput::Disconnected(first.clone()))
            .unwrap();

        connected(&mut gate, &second);

        let stale = gate
            .process(StreamInput::Data {
                acquisition: first,
                message: Message::Text("obsolete".into()),
            })
            .unwrap();

        assert!(matches!(stale, SourceTransportEvent::Ignored));

        let current = gate
            .process(StreamInput::Data {
                acquisition: second,
                message: Message::Text("current".into()),
            })
            .unwrap();

        assert!(matches!(current, SourceTransportEvent::Data { .. }));
    }

    #[test]
    fn overlapping_acquisition_fails_closed() {
        let mut gate = SourceTransportGate::new();

        let first = acquisition(1);
        let second = acquisition(2);

        connected(&mut gate, &first);

        let result = gate.process(StreamInput::Connected(second));

        assert!(matches!(
            result,
            Err(SourceTransportGateError::OverlappingAcquisition)
        ));

        assert_eq!(gate.active(), Some(&first));
        assert_eq!(gate.highest_generation(), 1);
    }

    #[test]
    fn channel_closure_revokes_active_acquisition() {
        let mut gate = SourceTransportGate::new();

        let first = acquisition(1);

        connected(&mut gate, &first);

        assert_eq!(gate.channel_closed(), Some(first));
        assert!(gate.active().is_none());

        // Repeated closure has no additional effect.
        assert!(gate.channel_closed().is_none());
    }

    #[test]
    fn old_generation_cannot_be_reactivated() {
        let mut gate = SourceTransportGate::new();

        let first = acquisition(1);
        let second = acquisition(2);

        connected(&mut gate, &first);

        gate.process(StreamInput::Disconnected(first.clone()))
            .unwrap();

        connected(&mut gate, &second);

        gate.process(StreamInput::Disconnected(second)).unwrap();

        assert!(matches!(
            connected(&mut gate, &first),
            SourceTransportEvent::Ignored
        ));

        assert!(gate.active().is_none());
    }
}
