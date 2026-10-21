//! Bounded replay, retention protection and ordered cutover contracts.
//! Storage, publication IO and production routing are owned by later packages.
use super::{
    cursor::{BookGeneration, StreamCursor},
    delivery::DeliveryCapacity,
    demand::RetentionSessionId,
    envelope::LiveEvent,
    memory::RetainedSize,
};
use crate::{formats::depth::DepthEventBoundary, live::ports::routing::EventBatch};
use std::{collections::VecDeque, sync::Arc, time::Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReplayError {
    #[error("incompatible cursor namespace")]
    Namespace,
    #[error("invalid range or frontier")]
    Range,
    #[error("cursor exhausted")]
    Exhausted,
    #[error("requested coverage is unavailable")]
    Unavailable,
    #[error("retention protection owner or generation mismatch")]
    Ownership,
    #[error("retention protection expired")]
    Expired,
    #[error("retention protection is inactive")]
    Inactive,
    #[error("bounded capacity exceeded")]
    Capacity,
    #[error("cursor gap")]
    Gap,
    #[error("invalid canonical event")]
    InvalidEvent,
    #[error("book generation changed")]
    Generation,
    #[error("operation cancelled")]
    Cancelled,
    #[error("retention spill failed")]
    SpillFailed,
    #[error("retention segment is corrupted")]
    SegmentCorrupted,
    #[error("retention storage is unavailable")]
    StorageUnavailable,
    #[error("requested retention durability is unavailable")]
    DurabilityUnavailable,
    #[error("handoff is incomplete")]
    Incomplete,
}
pub fn same_namespace(a: &StreamCursor, b: &StreamCursor) -> bool {
    a.stream_id == b.stream_id && a.log_epoch == b.log_epoch
}
fn compare(a: &StreamCursor, b: &StreamCursor) -> Result<std::cmp::Ordering, ReplayError> {
    if !same_namespace(a, b) {
        return Err(ReplayError::Namespace);
    }
    Ok(a.offset.cmp(&b.offset))
}

/// Both ends inclusive. Open end follows the current live frontier within finite
/// admitted capacity. ResumeAfter is a separate checked constructor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayRange {
    first: StreamCursor,
    last: Option<StreamCursor>,
}
impl ReplayRange {
    pub fn inclusive(first: StreamCursor, last: Option<StreamCursor>) -> Result<Self, ReplayError> {
        if first.stream_id.0.trim().is_empty() {
            return Err(ReplayError::Range);
        }
        if let Some(last) = &last {
            if compare(&first, last)?.is_gt() {
                return Err(ReplayError::Range);
            }
        }
        Ok(Self { first, last })
    }
    pub fn resume_after(
        mut cursor: StreamCursor,
        last: Option<StreamCursor>,
    ) -> Result<Self, ReplayError> {
        cursor.offset = cursor.offset.checked_add(1).ok_or(ReplayError::Exhausted)?;
        Self::inclusive(cursor, last)
    }
    pub fn first(&self) -> &StreamCursor {
        &self.first
    }
    pub fn last(&self) -> Option<&StreamCursor> {
        self.last.as_ref()
    }
    pub fn contains(&self, cursor: &StreamCursor) -> bool {
        same_namespace(&self.first, cursor)
            && cursor.offset >= self.first.offset
            && self
                .last
                .as_ref()
                .is_none_or(|end| cursor.offset <= end.offset)
    }
    /// Validate a contiguous page; increasing cursors alone are not coverage.
    pub fn check_page(&self, batch: &EventBatch) -> Result<(), ReplayError> {
        if batch.first_cursor() != &self.first {
            return Err(ReplayError::Gap);
        }
        for event in batch.events() {
            if !self.contains(&event.cursor) {
                return Err(ReplayError::Range);
            }
        }
        if batch
            .events()
            .windows(2)
            .any(|p| p[0].cursor.offset.checked_add(1) != Some(p[1].cursor.offset))
        {
            return Err(ReplayError::Gap);
        }
        Ok(())
    }
}

/// Verified contiguous positions, never maximum observed IDs or timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPositions {
    accepted: StreamCursor,
    committed: Option<StreamCursor>,
    durable: Option<StreamCursor>,
}
impl RetentionPositions {
    pub fn new(
        accepted: StreamCursor,
        committed: Option<StreamCursor>,
        durable: Option<StreamCursor>,
    ) -> Result<Self, ReplayError> {
        if accepted.stream_id.0.trim().is_empty() {
            return Err(ReplayError::Range);
        }
        if let Some(c) = &committed {
            if compare(c, &accepted)?.is_gt() {
                return Err(ReplayError::Range);
            }
        }
        if let Some(d) = &durable {
            if compare(d, committed.as_ref().ok_or(ReplayError::Range)?)?.is_gt() {
                return Err(ReplayError::Range);
            }
        }
        Ok(Self {
            accepted,
            committed,
            durable,
        })
    }
    pub fn accepted(&self) -> &StreamCursor {
        &self.accepted
    }
    pub fn committed(&self) -> Option<&StreamCursor> {
        self.committed.as_ref()
    }
    pub fn durable(&self) -> Option<&StreamCursor> {
        self.durable.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtectionToken {
    pub id: u64,
    pub generation: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtectionState {
    Active,
    Released,
    Cancelled,
    Expired,
    Failed,
}

/// Retention-owner state, not a clonable promise. Admission requires verified
/// coverage through the frontier and finite resource limits. The owner must pin
/// that coverage atomically with admission. The consumer cannot construct a
/// storage proof by calling this state machine alone. Deadlines are monotonic;
/// leases are not restorable across restart without a separate durable record.
#[derive(Debug)]
pub struct ProtectionLease {
    token: ProtectionToken,
    owner: RetentionSessionId,
    range: ReplayRange,
    frontier: StreamCursor,
    acknowledged: Option<StreamCursor>,
    expires: Instant,
    maximum_expiry: Instant,
    capacity: DeliveryCapacity,
    state: ProtectionState,
}
impl ProtectionLease {
    #[allow(clippy::too_many_arguments)]
    pub fn admit(
        token: ProtectionToken,
        owner: RetentionSessionId,
        range: ReplayRange,
        covered: ReplayRange,
        frontier: StreamCursor,
        capacity: DeliveryCapacity,
        retained_events: usize,
        retained_bytes: u64,
        now: Instant,
        expires: Instant,
        maximum_expiry: Instant,
    ) -> Result<Self, ReplayError> {
        if owner.0.trim().is_empty() {
            return Err(ReplayError::Ownership);
        }
        if expires <= now || expires > maximum_expiry {
            return Err(ReplayError::Expired);
        }
        if capacity.max_pending_events == 0
            || capacity.max_pending_bytes == 0
            || retained_events > capacity.max_pending_events
            || retained_bytes > capacity.max_pending_bytes
        {
            return Err(ReplayError::Capacity);
        }
        if covered.last().is_none()
            || !covered.contains(range.first())
            || !covered.contains(&frontier)
            || !range.contains(&frontier)
            || range
                .last()
                .is_some_and(|end| !covered.contains(end) || end.offset > frontier.offset)
        {
            return Err(ReplayError::Unavailable);
        }
        let count = frontier
            .offset
            .checked_sub(range.first.offset)
            .and_then(|n| n.checked_add(1))
            .ok_or(ReplayError::Capacity)?;
        if u64::try_from(retained_events).map_err(|_| ReplayError::Capacity)? < count
            || retained_bytes == 0
        {
            return Err(ReplayError::Capacity);
        }
        Ok(Self {
            token,
            owner,
            range,
            frontier,
            acknowledged: None,
            expires,
            maximum_expiry,
            capacity,
            state: ProtectionState::Active,
        })
    }
    fn authorize(
        &self,
        owner: &RetentionSessionId,
        token: ProtectionToken,
    ) -> Result<(), ReplayError> {
        if owner != &self.owner || token != self.token {
            return Err(ReplayError::Ownership);
        }
        Ok(())
    }
    fn active(&mut self, now: Instant) -> Result<(), ReplayError> {
        if self.state == ProtectionState::Active && now >= self.expires {
            self.state = ProtectionState::Expired;
        }
        match self.state {
            ProtectionState::Active => Ok(()),
            ProtectionState::Expired => Err(ReplayError::Expired),
            ProtectionState::Cancelled => Err(ReplayError::Cancelled),
            _ => Err(ReplayError::Inactive),
        }
    }
    pub fn state(&mut self, now: Instant) -> ProtectionState {
        let _ = self.active(now);
        self.state
    }
    pub fn acknowledged(&self) -> Option<&StreamCursor> {
        self.acknowledged.as_ref()
    }
    pub fn token(&self) -> ProtectionToken {
        self.token
    }
    pub fn range(&self) -> &ReplayRange {
        &self.range
    }
    /// Owner reports actual retained usage as new events arrive. Exhaustion is
    /// terminal and observable, never a silent downgrade to best effort.
    pub fn update_frontier(
        &mut self,
        frontier: StreamCursor,
        events: usize,
        bytes: u64,
        now: Instant,
    ) -> Result<(), ReplayError> {
        self.active(now)?;
        if compare(&frontier, &self.frontier)?.is_lt() || !self.range.contains(&frontier) {
            return Err(ReplayError::Range);
        }
        if events > self.capacity.max_pending_events || bytes > self.capacity.max_pending_bytes {
            self.state = ProtectionState::Failed;
            return Err(ReplayError::Capacity);
        }
        let required = match &self.acknowledged {
            Some(ack) => frontier.offset.checked_sub(ack.offset),
            None => frontier
                .offset
                .checked_sub(self.range.first.offset)
                .and_then(|n| n.checked_add(1)),
        }
        .ok_or(ReplayError::Range)?;
        if u64::try_from(events).map_err(|_| ReplayError::Capacity)? < required
            || (events != 0 && bytes == 0)
        {
            return Err(ReplayError::Range);
        }
        self.frontier = frontier;
        Ok(())
    }
    /// A safely-consumed acknowledgement, not socket delivery or receipt in RAM.
    pub fn advance(
        &mut self,
        owner: &RetentionSessionId,
        token: ProtectionToken,
        through: StreamCursor,
        now: Instant,
    ) -> Result<(), ReplayError> {
        self.authorize(owner, token)?;
        self.active(now)?;
        if !self.range.contains(&through)
            || compare(&through, &self.frontier)?.is_gt()
            || self
                .acknowledged
                .as_ref()
                .is_some_and(|old| through.offset < old.offset)
        {
            return Err(ReplayError::Range);
        }
        self.acknowledged = Some(through);
        Ok(())
    }
    /// Check a protected read without interpreting delivery as safe consumption.
    pub fn validate_read(
        &mut self,
        owner: &RetentionSessionId,
        token: ProtectionToken,
        range: &ReplayRange,
        now: Instant,
    ) -> Result<(), ReplayError> {
        self.authorize(owner, token)?;
        self.active(now)?;
        if (range.last().is_none() && self.range.last().is_some())
            || !self.protects(range.first(), now)
            || range.last().is_some_and(|end| !self.range.contains(end))
        {
            return Err(ReplayError::Unavailable);
        }
        Ok(())
    }
    pub fn renew(
        &mut self,
        owner: &RetentionSessionId,
        token: ProtectionToken,
        expires: Instant,
        now: Instant,
    ) -> Result<ProtectionToken, ReplayError> {
        self.authorize(owner, token)?;
        self.active(now)?;
        if expires <= self.expires || expires > self.maximum_expiry {
            return Err(ReplayError::Range);
        }
        let generation = self
            .token
            .generation
            .checked_add(1)
            .ok_or(ReplayError::Exhausted)?;
        self.token.generation = generation;
        self.expires = expires;
        Ok(self.token)
    }
    pub fn release(
        &mut self,
        owner: &RetentionSessionId,
        token: ProtectionToken,
        now: Instant,
    ) -> Result<(), ReplayError> {
        self.authorize(owner, token)?;
        if self.state == ProtectionState::Released {
            return Ok(());
        }
        self.active(now)?;
        self.state = ProtectionState::Released;
        Ok(())
    }
    /// Cancellation is terminal and never evidence of successful consumption.
    pub fn cancel(
        &mut self,
        owner: &RetentionSessionId,
        token: ProtectionToken,
        now: Instant,
    ) -> Result<(), ReplayError> {
        self.authorize(owner, token)?;
        if self.state == ProtectionState::Cancelled {
            return Ok(());
        }
        self.active(now)?;
        self.state = ProtectionState::Cancelled;
        Ok(())
    }
    /// Owner invalidates a promise after a storage/coverage failure. It cannot
    /// continue advertising protection while recovery is unresolved.
    pub fn invalidate(&mut self) {
        if self.state == ProtectionState::Active {
            self.state = ProtectionState::Failed;
        }
    }
    pub fn protects(&mut self, cursor: &StreamCursor, now: Instant) -> bool {
        self.active(now).is_ok()
            && self.range.contains(cursor)
            && self
                .acknowledged
                .as_ref()
                .is_none_or(|a| cursor.offset > a.offset)
    }
}

/// A book version is never a publication cursor offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookVersion(pub u64);

/// A subscriber initialization VIEW at an accepted publication boundary. It is
/// not a new accepted source event and does not allocate or replace its cursor.
/// Capture must be installed before acquiring this complete consistent image.
#[derive(Debug, Clone)]
pub struct BookViewBoundary {
    cursor: StreamCursor,
    generation: BookGeneration,
    image: super::envelope::LiveL2Group,
    book_version: BookVersion,
    configuration_revision: u64,
    metadata_revision: u64,
}
impl BookViewBoundary {
    pub fn new(
        cursor: StreamCursor,
        generation: BookGeneration,
        image: super::envelope::LiveL2Group,
        book_version: BookVersion,
        configuration_revision: u64,
        metadata_revision: u64,
    ) -> Result<Self, ReplayError> {
        if cursor.stream_id.0.trim().is_empty()
            || image.grouped_boundary != DepthEventBoundary::Initialization
        {
            return Err(ReplayError::InvalidEvent);
        }
        image.check().map_err(|_| ReplayError::InvalidEvent)?;
        Ok(Self {
            cursor,
            generation,
            image,
            book_version,
            configuration_revision,
            metadata_revision,
        })
    }
    pub fn cursor(&self) -> &StreamCursor {
        &self.cursor
    }
    pub fn generation(&self) -> BookGeneration {
        self.generation
    }
    pub fn image(&self) -> &super::envelope::LiveL2Group {
        &self.image
    }
    pub fn book_version(&self) -> BookVersion {
        self.book_version
    }
    pub fn configuration_revision(&self) -> u64 {
        self.configuration_revision
    }
    pub fn metadata_revision(&self) -> u64 {
        self.metadata_revision
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutoverState {
    Replaying,
    Following,
    Failed,
}
/// One bounded ordered capture at a stream-owner-installed barrier. This pure
/// consumer state machine never implements a router or subscribes after reading
/// a view. Capture receives every event after `cut` while replay runs through it.
/// The owner must install capture and obtain `cut` in one serialized operation.
#[derive(Debug)]
pub struct ReplayCutover {
    first: StreamCursor,
    cut: StreamCursor,
    book_view: bool,
    replay_next: Option<u64>,
    capture_next: Option<u64>,
    book_generation: Option<BookGeneration>,
    capacity: DeliveryCapacity,
    buffered_bytes: u64,
    buffer: VecDeque<(Arc<LiveEvent>, u64)>,
    state: CutoverState,
}
impl ReplayCutover {
    pub fn new(
        first: StreamCursor,
        cut: StreamCursor,
        book_generation: Option<BookGeneration>,
        capacity: DeliveryCapacity,
    ) -> Result<Self, ReplayError> {
        ReplayRange::inclusive(first.clone(), Some(cut.clone()))?;
        if capacity.max_pending_events == 0 || capacity.max_pending_bytes == 0 {
            return Err(ReplayError::Capacity);
        }
        Ok(Self {
            first: first.clone(),
            book_view: false,
            replay_next: Some(first.offset),
            capture_next: cut.offset.checked_add(1),
            cut,
            book_generation,
            capacity,
            buffered_bytes: 0,
            buffer: VecDeque::new(),
            state: CutoverState::Replaying,
        })
    }
    /// Full book-view cutover uses the SAME bounded capture and cursor order,
    /// without pretending the view is an exchange-originated accepted event.
    pub fn for_book(
        view: &BookViewBoundary,
        capacity: DeliveryCapacity,
    ) -> Result<Self, ReplayError> {
        let mut capture = Self::new(
            view.cursor.clone(),
            view.cursor.clone(),
            Some(view.generation),
            capacity,
        )?;
        capture.book_view = true;
        Ok(capture)
    }
    pub fn consume_view(&mut self, view: &BookViewBoundary) -> Result<(), ReplayError> {
        if self.state != CutoverState::Replaying
            || self.replay_next != Some(self.cut.offset)
            || view.cursor != self.cut
            || Some(view.generation) != self.book_generation
        {
            return Err(ReplayError::Generation);
        }
        self.replay_next = None;
        Ok(())
    }
    /// Protection expiry/failure invalidates replay handoff even if every event
    /// reached the consumer. Safely-consumed advancement is separately owned.
    pub fn follow_protected(
        &mut self,
        lease: &mut ProtectionLease,
        owner: &RetentionSessionId,
        token: ProtectionToken,
        now: Instant,
    ) -> Result<(), ReplayError> {
        lease.authorize(owner, token)?;
        if let Err(error) = lease.active(now) {
            self.state = CutoverState::Failed;
            return Err(error);
        }
        if !lease.range.contains(&self.first) || !lease.range.contains(&self.cut) {
            self.state = CutoverState::Failed;
            return Err(ReplayError::Unavailable);
        }
        self.activate_follow()
    }
    pub fn state(&self) -> CutoverState {
        self.state
    }
    fn event_valid(&mut self, event: &LiveEvent) -> Result<(), ReplayError> {
        if self.state == CutoverState::Failed {
            return Err(ReplayError::Inactive);
        }
        let result = if event.check().is_err() {
            Err(ReplayError::InvalidEvent)
        } else if !same_namespace(&self.cut, &event.cursor) {
            Err(ReplayError::Namespace)
        } else if self.book_generation.is_some()
            && event.provenance.book_generation != self.book_generation
        {
            Err(ReplayError::Generation)
        } else {
            Ok(())
        };
        if result.is_err() {
            self.state = CutoverState::Failed;
        }
        result
    }
    pub fn replay(&mut self, event: &LiveEvent) -> Result<(), ReplayError> {
        self.event_valid(event)?;
        if self.state != CutoverState::Replaying {
            return Err(ReplayError::Inactive);
        }
        if self.replay_next != Some(event.cursor.offset) || event.cursor.offset > self.cut.offset {
            self.state = CutoverState::Failed;
            return Err(ReplayError::Gap);
        }
        self.replay_next = if event.cursor == self.cut {
            None
        } else {
            event.cursor.offset.checked_add(1)
        };
        Ok(())
    }
    pub fn capture(&mut self, event: &Arc<LiveEvent>) -> Result<(), ReplayError> {
        self.event_valid(&event)?;
        // Repeats at/before the cut are replay-owned and must not be emitted twice.
        if event.cursor.offset <= self.cut.offset {
            return Ok(());
        }
        if self.capture_next != Some(event.cursor.offset) {
            self.state = CutoverState::Failed;
            return Err(ReplayError::Gap);
        }
        let bytes = match event.retained_size_bytes() {
            Ok(bytes) => bytes,
            Err(_) => {
                self.state = CutoverState::Failed;
                return Err(ReplayError::Capacity);
            }
        };
        let next_bytes = match self.buffered_bytes.checked_add(bytes) {
            Some(bytes) => bytes,
            None => {
                self.state = CutoverState::Failed;
                return Err(ReplayError::Capacity);
            }
        };
        if self.buffer.len() >= self.capacity.max_pending_events
            || next_bytes > self.capacity.max_pending_bytes
        {
            self.state = CutoverState::Failed;
            return Err(ReplayError::Capacity);
        }
        self.buffer.push_back((event.clone(), bytes));
        self.buffered_bytes = next_bytes;
        self.capture_next = event.cursor.offset.checked_add(1);
        Ok(())
    }
    /// Only after every replay event through the captured cut has been consumed.
    pub fn first_cursor(&self) -> &StreamCursor {
        &self.first
    }
    pub fn cut(&self) -> &StreamCursor {
        &self.cut
    }
    pub fn follow_view(&mut self) -> Result<(), ReplayError> {
        if !self.book_view {
            return Err(ReplayError::Ownership);
        }
        self.activate_follow()
    }
    fn activate_follow(&mut self) -> Result<(), ReplayError> {
        if self.state != CutoverState::Replaying {
            return Err(ReplayError::Inactive);
        }
        if self.replay_next.is_some() {
            return Err(ReplayError::Incomplete);
        }
        self.state = CutoverState::Following;
        Ok(())
    }
    pub fn pop_follow(&mut self) -> Result<Option<Arc<LiveEvent>>, ReplayError> {
        if self.state != CutoverState::Following {
            return Err(ReplayError::Inactive);
        }
        Ok(self.buffer.pop_front().map(|(event, bytes)| {
            self.buffered_bytes -= bytes;
            event
        }))
    }
    /// Failure leaves captured responsibility inspectable, never marks completion.
    pub fn cancel(&mut self) {
        self.state = CutoverState::Failed;
    }
    /// Explicit transfer of buffered responsibility after cancellation/failure.
    pub fn into_unresolved(self) -> Vec<Arc<LiveEvent>> {
        self.buffer.into_iter().map(|(event, _)| event).collect()
    }
    pub fn buffered_events(&self) -> usize {
        self.buffer.len()
    }
}
