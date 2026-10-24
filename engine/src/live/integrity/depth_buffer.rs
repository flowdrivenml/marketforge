use std::collections::VecDeque;

// ============================================================
// SYNCHRONIZATION IDENTITY
// ============================================================

/// Identifies one REST snapshot synchronization attempt.
///
/// `stream_generation` is assigned by the authoritative
/// stream runtime.
///
/// `attempt` distinguishes successive synchronization
/// attempts within that generation.
///
/// Both values are required to fence asynchronous
/// REST responses and incoming WebSocket updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DepthSyncToken {
    pub stream_generation: u64,
    pub attempt: u64,
}

// ============================================================
// BUFFER LIMITS
// ============================================================

/// Resource limits for one pre-snapshot update buffer.
///
/// `max_bytes` limits accounted message payload bytes.
///
/// The caller must supply an authoritative size for
/// each retained message, including an upper bound on
/// its retained memory footprint.
///
/// This limit does not automatically measure Rust
/// allocation overhead or allocator capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthBufferLimits {
    pub max_messages: usize,
    pub max_bytes: usize,
}

impl DepthBufferLimits {
    pub fn validate(self) -> Result<Self, DepthBufferError> {
        if self.max_messages == 0 {
            return Err(DepthBufferError::InvalidLimits);
        }

        if self.max_bytes == 0 {
            return Err(DepthBufferError::InvalidLimits);
        }

        Ok(self)
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthBufferError {
    InvalidLimits,

    /// An operation belongs to a different synchronization
    /// generation or attempt.
    ObsoleteAttempt,

    /// A previous capacity overflow made the current
    /// synchronization attempt unusable.
    Invalidated,

    /// Adding another message would exceed a resource limit.
    ///
    /// The current attempt is invalidated when this occurs.
    Overflow,

    /// The accounted size was invalid or arithmetic overflowed.
    InvalidMessageSize,
}

// ============================================================
// RETAINED MESSAGE
// ============================================================

#[derive(Debug)]
struct BufferedMessage<T> {
    value: T,
}

// ============================================================
// BUFFER
// ============================================================

/// A bounded FIFO buffer for one snapshot synchronization
/// attempt.
///
/// T is normally NormalizedDepth, but the implementation
/// is generic to permit isolated testing and reuse.
///
/// This component owns temporary updates only.
///
/// It does not own:
/// - The authoritative order book.
/// - The accepted sequence baseline.
/// - REST snapshot acquisition.
/// - Recovery scheduling.
/// - Publication or recording.
/// - Gap incident history.
#[derive(Debug)]
pub struct PreSnapshotDepthBuffer<T> {
    token: DepthSyncToken,
    limits: DepthBufferLimits,

    queue: VecDeque<BufferedMessage<T>>,
    accounted_bytes: usize,

    invalidated: bool,
}

impl<T> PreSnapshotDepthBuffer<T> {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(token: DepthSyncToken, limits: DepthBufferLimits) -> Result<Self, DepthBufferError> {
        let limits = limits.validate()?;

        Ok(Self {
            token,
            limits,

            queue: VecDeque::new(),
            accounted_bytes: 0,

            invalidated: false,
        })
    }

    // ========================================================
    // INSPECTION
    // ========================================================

    pub fn token(&self) -> DepthSyncToken {
        self.token
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn accounted_bytes(&self) -> usize {
        self.accounted_bytes
    }

    pub fn limits(&self) -> DepthBufferLimits {
        self.limits
    }

    pub fn is_invalidated(&self) -> bool {
        self.invalidated
    }

    // ========================================================
    // ADMISSION
    // ========================================================

    /// Append one complete normalized update.
    ///
    /// Messages are retained in arrival order.
    ///
    /// A token mismatch does not affect the current buffer.
    ///
    /// An overflow invalidates the entire current attempt.
    ///
    /// The caller must not publish buffered updates as
    /// synchronized market data before bridge validation.
    pub fn push(
        &mut self,
        token: DepthSyncToken,
        value: T,
        accounted_bytes: usize,
    ) -> Result<(), DepthBufferError> {
        self.require_token(token)?;

        if self.invalidated {
            return Err(DepthBufferError::Invalidated);
        }

        // Zero is not a valid retained-message footprint.
        //
        // Require the source runtime to provide a meaningful
        // bound rather than bypassing memory accounting.
        if accounted_bytes == 0 {
            return Err(DepthBufferError::InvalidMessageSize);
        }

        let next_len = self
            .queue
            .len()
            .checked_add(1)
            .ok_or(DepthBufferError::Overflow)?;

        let next_bytes = self
            .accounted_bytes
            .checked_add(accounted_bytes)
            .ok_or(DepthBufferError::Overflow)?;

        if next_len > self.limits.max_messages || next_bytes > self.limits.max_bytes {
            self.invalidate();

            return Err(DepthBufferError::Overflow);
        }

        self.queue.push_back(BufferedMessage { value });

        self.accounted_bytes = next_bytes;

        Ok(())
    }

    // ========================================================
    // SNAPSHOT HANDOFF
    // ========================================================

    /// Consume the current buffer for a matching snapshot
    /// synchronization attempt.
    ///
    /// The returned values preserve arrival order.
    ///
    /// This does NOT validate the snapshot sequence,
    /// establish synchronization, or mutate BookStore.
    ///
    /// The synchronization controller must validate
    /// the complete bridge before accepting the result.
    pub fn drain(&mut self, token: DepthSyncToken) -> Result<Vec<T>, DepthBufferError> {
        self.require_token(token)?;

        if self.invalidated {
            return Err(DepthBufferError::Invalidated);
        }

        let messages = self.queue.drain(..).map(|entry| entry.value).collect();

        self.accounted_bytes = 0;

        Ok(messages)
    }

    // ========================================================
    // ATTEMPT REPLACEMENT
    // ========================================================

    /// Begin a newer synchronization attempt.
    ///
    /// Previously buffered messages are discarded.
    ///
    /// The caller is responsible for allocating
    /// monotonically increasing attempt identifiers.
    ///
    /// A new stream generation may restart its local
    /// attempt numbering.
    pub fn begin_attempt(&mut self, next: DepthSyncToken) -> Result<(), DepthBufferError> {
        let newer = next.stream_generation > self.token.stream_generation
            || (next.stream_generation == self.token.stream_generation
                && next.attempt > self.token.attempt);

        if !newer {
            return Err(DepthBufferError::ObsoleteAttempt);
        }

        self.queue.clear();
        self.accounted_bytes = 0;
        self.invalidated = false;
        self.token = next;

        Ok(())
    }

    // ========================================================
    // INVALIDATION
    // ========================================================

    /// Permanently invalidate the current attempt.
    ///
    /// Only begin_attempt() can make the buffer usable again.
    pub fn invalidate(&mut self) {
        self.queue.clear();
        self.accounted_bytes = 0;
        self.invalidated = true;
    }

    // ========================================================
    // INTERNAL VALIDATION
    // ========================================================

    fn require_token(&self, token: DepthSyncToken) -> Result<(), DepthBufferError> {
        if token != self.token {
            return Err(DepthBufferError::ObsoleteAttempt);
        }

        Ok(())
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn token(generation: u64, attempt: u64) -> DepthSyncToken {
        DepthSyncToken {
            stream_generation: generation,
            attempt,
        }
    }

    fn buffer() -> PreSnapshotDepthBuffer<u64> {
        PreSnapshotDepthBuffer::new(
            token(10, 1),
            DepthBufferLimits {
                max_messages: 3,
                max_bytes: 100,
            },
        )
        .unwrap()
    }

    #[test]
    fn preserves_arrival_order() {
        let mut buffer = buffer();

        buffer.push(token(10, 1), 101, 10).unwrap();
        buffer.push(token(10, 1), 102, 10).unwrap();
        buffer.push(token(10, 1), 103, 10).unwrap();

        assert_eq!(buffer.len(), 3);

        let messages = buffer.drain(token(10, 1)).unwrap();

        assert_eq!(messages, vec![101, 102, 103]);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.accounted_bytes(), 0);
    }

    #[test]
    fn message_capacity_overflow_invalidates_attempt() {
        let mut buffer = buffer();

        for id in 1..=3 {
            buffer.push(token(10, 1), id, 10).unwrap();
        }

        assert_eq!(
            buffer.push(token(10, 1), 4, 10),
            Err(DepthBufferError::Overflow)
        );

        assert!(buffer.is_invalidated());
        assert!(buffer.is_empty());

        assert_eq!(
            buffer.push(token(10, 1), 5, 10),
            Err(DepthBufferError::Invalidated)
        );
    }

    #[test]
    fn byte_capacity_overflow_invalidates_attempt() {
        let mut buffer = buffer();

        buffer.push(token(10, 1), 1, 60).unwrap();

        assert_eq!(
            buffer.push(token(10, 1), 2, 50),
            Err(DepthBufferError::Overflow)
        );

        assert_eq!(buffer.accounted_bytes(), 0);
        assert!(buffer.is_invalidated());
    }

    #[test]
    fn exact_capacity_is_allowed() {
        let mut buffer = buffer();

        buffer.push(token(10, 1), 1, 60).unwrap();
        buffer.push(token(10, 1), 2, 40).unwrap();

        assert_eq!(buffer.accounted_bytes(), 100);
        assert!(!buffer.is_invalidated());
    }

    #[test]
    fn obsolete_message_cannot_modify_buffer() {
        let mut buffer = buffer();

        buffer.push(token(10, 1), 1, 10).unwrap();

        assert_eq!(
            buffer.push(token(9, 1), 99, 10),
            Err(DepthBufferError::ObsoleteAttempt)
        );

        assert_eq!(buffer.len(), 1);
        assert!(!buffer.is_invalidated());
    }

    #[test]
    fn stale_snapshot_cannot_drain_new_attempt() {
        let mut buffer = buffer();

        buffer.push(token(10, 1), 1, 10).unwrap();

        buffer.begin_attempt(token(10, 2)).unwrap();

        buffer.push(token(10, 2), 2, 10).unwrap();

        assert_eq!(
            buffer.drain(token(10, 1)),
            Err(DepthBufferError::ObsoleteAttempt)
        );

        assert_eq!(buffer.drain(token(10, 2)).unwrap(), vec![2]);
    }

    #[test]
    fn new_stream_generation_discards_old_messages() {
        let mut buffer = buffer();

        buffer.push(token(10, 1), 1, 10).unwrap();

        buffer.begin_attempt(token(11, 1)).unwrap();

        assert!(buffer.is_empty());

        assert_eq!(
            buffer.push(token(10, 1), 2, 10),
            Err(DepthBufferError::ObsoleteAttempt)
        );

        buffer.push(token(11, 1), 3, 10).unwrap();

        assert_eq!(buffer.drain(token(11, 1)).unwrap(), vec![3]);
    }

    #[test]
    fn rejected_attempt_cannot_be_reactivated() {
        let mut buffer = buffer();

        buffer.invalidate();

        assert_eq!(
            buffer.begin_attempt(token(10, 1)),
            Err(DepthBufferError::ObsoleteAttempt)
        );

        assert!(buffer.is_invalidated());

        buffer.begin_attempt(token(10, 2)).unwrap();

        assert!(!buffer.is_invalidated());
    }

    #[test]
    fn rejects_invalid_limits() {
        assert_eq!(
            PreSnapshotDepthBuffer::<u64>::new(
                token(1, 1),
                DepthBufferLimits {
                    max_messages: 0,
                    max_bytes: 100,
                },
            )
            .unwrap_err(),
            DepthBufferError::InvalidLimits
        );
    }

    #[test]
    fn rejects_zero_message_size() {
        let mut buffer = buffer();

        assert_eq!(
            buffer.push(token(10, 1), 1, 0),
            Err(DepthBufferError::InvalidMessageSize)
        );

        assert!(!buffer.is_invalidated());
        assert!(buffer.is_empty());
    }
}
