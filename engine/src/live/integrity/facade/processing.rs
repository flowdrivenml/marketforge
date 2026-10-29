use super::admission::IntegrityAdmissionError;
use crate::live::{
    infrastructure::normalization::{NormalizedDepth, NormalizedTrades},
    integrity::{
        BoundTradeIngressOutcome, DepthControllerOutcome, DepthSourceEpoch, TradeSourceEpoch,
    },
};

use super::handle::{IntegrityState, StreamIntegrityHandle};

// ============================================================
// NORMALIZED INPUT
// ============================================================

/// One normalized market-data message together with its
/// trusted source-acquisition context.
///
/// Source epochs must be assigned by the runtime.
/// They must not be extracted from exchange payloads.
#[derive(Debug)]
pub enum IntegrityInput<'a> {
    Depth {
        epoch: DepthSourceEpoch,
        message: NormalizedDepth,
        accounted_bytes: usize,
    },

    Trades {
        epoch: TradeSourceEpoch,
        message: &'a NormalizedTrades,
    },
}

// ============================================================
// PROCESSING OUTCOME
// ============================================================

/// Preserves the existing channel-specific Integrity
/// outcomes without changing their semantics.
///
/// Accepted depth outputs are available through
/// DepthControllerOutcome::accepted_output().
///
/// Accepted trades are available through
/// TradeControllerOutcome::Accepted(...).
#[derive(Debug)]
pub enum IntegrityOutcome {
    Depth(DepthControllerOutcome),

    Trades(BoundTradeIngressOutcome),

    /// An input channel does not match this handle.
    ///
    /// No Integrity state was modified.
    WrongChannel,
    /// Facade-level lifecycle admission rejected the input.
    ///
    /// The underlying ingress was not called.
    AdmissionBlocked(IntegrityAdmissionError),
}

// ============================================================
// BATCH PROCESSING RESULT
// ============================================================

/// Ordered results of processing multiple inputs.
///
/// Every outcome corresponds to the input at the
/// same position.
///
/// This API does not reorder, collapse, or deduplicate
/// complete messages across message boundaries.
///
/// Each underlying Integrity controller retains its
/// own exchange-specific admission semantics.
#[derive(Debug)]
pub struct IntegrityBatchResult {
    pub outcomes: Vec<IntegrityOutcome>,
}

impl IntegrityBatchResult {
    pub fn len(&self) -> usize {
        self.outcomes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.outcomes.is_empty()
    }
}

// ============================================================
// STREAM INTEGRITY PROCESSING
// ============================================================

impl StreamIntegrityHandle {
    /// Process one normalized WebSocket message.
    ///
    /// This operation is synchronous.
    ///
    /// No network I/O, recovery scheduling, or downstream
    /// publication is performed.
    pub fn process(&mut self, input: IntegrityInput<'_>) -> IntegrityOutcome {
        // Lifecycle admission is checked before entering
        // the channel-specific Integrity implementation.
        //
        // No baselines, identities, book levels, or
        // synchronization buffers are modified on rejection.
        if let Some(reason) = self.admission.rejection() {
            return IntegrityOutcome::AdmissionBlocked(reason);
        }

        match (&mut self.state, input) {
            (
                IntegrityState::Depth(ingress),
                IntegrityInput::Depth {
                    epoch,
                    message,
                    accounted_bytes,
                },
            ) => IntegrityOutcome::Depth(ingress.on_websocket(epoch, message, accounted_bytes)),

            (IntegrityState::Trades(ingress), IntegrityInput::Trades { epoch, message }) => {
                IntegrityOutcome::Trades(ingress.on_websocket(epoch, message))
            }

            _ => IntegrityOutcome::WrongChannel,
        }
    }

    /// Publish an ordinary trade batch before committing its identities and baseline.
    /// Publication failure leaves the accepted baseline and identity history unchanged.
    pub fn process_trades_with(
        &mut self,
        epoch: TradeSourceEpoch,
        message: &NormalizedTrades,
        mut publish: impl FnMut(&IntegrityOutcome) -> Result<(), String>,
    ) -> Result<IntegrityOutcome, String> {
        if let Some(reason) = self.admission.rejection() {
            return Ok(IntegrityOutcome::AdmissionBlocked(reason));
        }
        let IntegrityState::Trades(ingress) = &mut self.state else {
            return Ok(IntegrityOutcome::WrongChannel);
        };
        let mut published = false;
        let result = ingress.on_websocket_with(epoch, message, |accepted| {
            let outcome = IntegrityOutcome::Trades(BoundTradeIngressOutcome::Processed(accepted));
            publish(&outcome)?;
            published = true;
            let IntegrityOutcome::Trades(BoundTradeIngressOutcome::Processed(accepted)) = outcome
            else {
                unreachable!("publication cannot replace its borrowed outcome")
            };
            Ok(accepted)
        })?;
        let outcome = IntegrityOutcome::Trades(result);
        if !published {
            publish(&outcome)?;
        }
        Ok(outcome)
    }

    /// Process a sequence of normalized messages in order.
    ///
    /// Every input is processed using exactly the same
    /// path as process().
    ///
    /// An Integrity failure does not erase subsequent
    /// inputs from this batch: the underlying ingress
    /// determines whether they are ignored, suspended,
    /// or otherwise rejected.
    ///
    /// The caller must retain all returned outcomes and
    /// preserve their order when handling publication
    /// and recovery effects.
    /// Evaluate multiple normalized messages in their original order.
    ///
    /// WARNING: DEFERRED PUBLICATION
    ///
    /// This method processes every message before returning any outcomes.
    ///
    /// For depth streams, the authoritative BookStore may have advanced
    /// beyond the baseline associated with earlier returned outcomes.
    /// Consequently, those outcomes must not be submitted later to
    /// DepthPublicationFence as independent live publications.
    ///
    /// This method is intended for:
    /// - Offline validation.
    /// - Tests and diagnostics.
    /// - Consumers that do not require incremental depth publication.
    ///
    /// For production stream processing with publication, use
    /// process_batch_with() instead.
    ///
    /// Each input still passes through the same Integrity validation
    /// logic as process().
    pub fn evaluate_batch<'a>(
        &mut self,
        inputs: impl IntoIterator<Item = IntegrityInput<'a>>,
    ) -> IntegrityBatchResult {
        let outcomes = inputs
            .into_iter()
            .map(|input| self.process(input))
            .collect();

        IntegrityBatchResult { outcomes }
    }
    /// Process inputs sequentially, invoking a callback
    /// immediately after each Integrity transition.
    ///
    /// The callback receives read-only access to the current
    /// Integrity state and owns the processing outcome.
    ///
    /// Unlike process_batch(), this allows downstream publication
    /// preparation before the next input mutates the book.
    ///
    /// The runtime must ensure that the callback completes its
    /// required publication-fence protocol before returning.
    ///
    /// The first callback error stops batch processing.
    ///
    /// This does not itself perform publication or recovery.
    pub fn process_batch_with<'a, E>(
        &mut self,
        inputs: impl IntoIterator<Item = IntegrityInput<'a>>,
        mut handle: impl FnMut(&StreamIntegrityHandle, IntegrityOutcome) -> Result<(), E>,
    ) -> Result<(), E> {
        for input in inputs {
            let outcome = self.process(input);

            handle(self, outcome)?;
        }

        Ok(())
    }
}
