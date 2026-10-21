//! Bounded process-local duplicate suppression. Reserving happens before effects;
//! an interrupted/pending command is never guessed safe to execute again.
use crate::live::model::{
    error::ErrorCode,
    operation::{RuntimeInstanceId, SessionId},
    protocol::*,
};
use std::collections::HashMap;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    runtime: RuntimeInstanceId,
    uid: u32,
    client: String,
    session: Option<SessionId>,
    key: String,
}
#[derive(Debug, Clone)]
struct Entry {
    command: ControlCommand,
    result: Option<ControlResult>,
    reservation: u64,
    bytes: usize,
}
#[derive(Debug, Clone)]
pub struct Reservation {
    key: Key,
    sequence: u64,
}
#[derive(Debug, Clone)]
pub enum ReservationOutcome {
    New(Reservation),
    Completed(ControlResult),
    Pending,
}
pub struct IdempotencyLedger {
    runtime: RuntimeInstanceId,
    entries: HashMap<Key, Entry>,
    max_entries: usize,
    max_bytes: usize,
    bytes: usize,
    next: Option<u64>,
}
impl IdempotencyLedger {
    pub fn new(
        runtime: RuntimeInstanceId,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Self, ErrorCode> {
        if runtime.0.trim().is_empty() || max_entries == 0 || max_bytes == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self {
            runtime,
            entries: HashMap::new(),
            max_entries,
            max_bytes,
            bytes: 0,
            next: Some(0),
        })
    }
    /// Commands must first pass the bounded codec. Authorization is rechecked on
    /// every retry. Full ledgers reject admission; entries are not silently evicted.
    pub fn reserve(
        &mut self,
        peer: &AuthenticatedPeer,
        request: &ControlRequest,
    ) -> Result<ReservationOutcome, ErrorCode> {
        request.command.authorize(peer)?;
        if peer.session.runtime != self.runtime {
            return Err(ErrorCode::StaleGeneration);
        }
        if request.protocol_version != CONTROL_PROTOCOL_VERSION {
            return Err(ErrorCode::UnsupportedVersion);
        }
        if request.request_id.client_instance.trim().is_empty()
            || request.request_id.client_instance.len() > 128
        {
            return Err(ErrorCode::InvalidInput);
        }
        request.command.validate(self.max_bytes)?;
        let key = Key {
            runtime: self.runtime.clone(),
            uid: peer.uid,
            client: request.request_id.client_instance.clone(),
            session: request
                .command
                .session_scoped()
                .then(|| peer.session.clone()),
            key: request
                .command
                .idempotency_key()
                .ok_or(ErrorCode::InvalidInput)?
                .to_owned(),
        };
        if let Some(entry) = self.entries.get(&key) {
            if entry.command != request.command {
                return Err(ErrorCode::IdempotencyConflict);
            }
            return Ok(match &entry.result {
                Some(result) => ReservationOutcome::Completed(result.clone()),
                None => ReservationOutcome::Pending,
            });
        }
        let bytes = json_size_bounded(&request.command, self.max_bytes)?
            .checked_add(key.client.len())
            .and_then(|n| n.checked_add(key.key.len()))
            .ok_or(ErrorCode::Limit)?;
        if self.entries.len() >= self.max_entries
            || bytes > self.max_bytes.saturating_sub(self.bytes)
        {
            return Err(ErrorCode::Capacity);
        }
        let sequence = self.next.ok_or(ErrorCode::Capacity)?;
        self.next = sequence.checked_add(1);
        self.bytes += bytes;
        self.entries.insert(
            key.clone(),
            Entry {
                command: request.command.clone(),
                result: None,
                reservation: sequence,
                bytes,
            },
        );
        Ok(ReservationOutcome::New(Reservation { key, sequence }))
    }
    pub fn complete(
        &mut self,
        reservation: Reservation,
        result: ControlResult,
    ) -> Result<(), ErrorCode> {
        result.validate()?;
        let entry = self
            .entries
            .get_mut(&reservation.key)
            .ok_or(ErrorCode::NotFound)?;
        if entry.reservation != reservation.sequence || entry.result.is_some() {
            return Err(ErrorCode::InvalidTransition);
        }
        let bytes = json_size_bounded(&result, self.max_bytes.saturating_sub(self.bytes))?;
        if bytes > self.max_bytes.saturating_sub(self.bytes) {
            return Err(ErrorCode::Capacity);
        }
        self.bytes += bytes;
        entry.bytes += bytes;
        entry.result = Some(result);
        Ok(())
    }
    /// Session teardown can discard transient results only after the owner has
    /// fenced/released that session's resources. Persistent entries remain.
    pub fn release_fenced_session(&mut self, session: &SessionId) {
        self.entries.retain(|key, entry| {
            if key.session.as_ref() == Some(session) {
                self.bytes -= entry.bytes;
                false
            } else {
                true
            }
        });
    }
}

fn json_size_bounded(value: &impl serde::Serialize, limit: usize) -> Result<usize, ErrorCode> {
    struct Counter {
        n: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.n) {
                return Err(std::io::ErrorKind::FileTooLarge.into());
            }
            self.n += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { n: 0, limit };
    serde_json::to_writer(&mut counter, value).map_err(|_| ErrorCode::Capacity)?;
    Ok(counter.n)
}
