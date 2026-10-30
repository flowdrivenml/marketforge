use thiserror::Error;

use crate::live::stream::registry::{RuntimeGeneration, StreamRuntimeError, StreamRuntimeRegistry};

use super::{
    instance::{InstanceError, ProcessorInstance, StopOutcome},
    scheduler::ProcessorToken,
};

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error(transparent)]
    Runtime(#[from] StreamRuntimeError),

    #[error(transparent)]
    Instance(#[from] InstanceError),

    #[error("stream generation is no longer current")]
    StaleGeneration,

    #[error("processor controller has already stopped")]
    AlreadyStopped,

    #[error("invalid controller operation: {0}")]
    InvalidPhase(&'static str),
}

/// Coordinates one processing instance with its authoritative
/// MarketForge stream lifecycle.
///
/// Does not own the stream registry or publication authority.
pub struct DerivedProcessorController<E> {
    stream_id: String,
    stream_generation: RuntimeGeneration,

    instance: ProcessorInstance<E>,

    // A stop operation gets a new registry generation.
    // Preserve it so an interrupted stop can be retried
    // without calling begin_stop() twice.
    stop_generation: Option<RuntimeGeneration>,

    // Preserve unprocessed events until lifecycle shutdown
    // has been recorded successfully.
    pending_outcome: Option<StopOutcome<E>>,

    stopped: bool,
}

impl<E: Send + 'static> DerivedProcessorController<E> {
    /// Attach an already registered processor to its owning
    /// stream generation.
    ///
    /// The caller must serialize lifecycle operations and
    /// ensure the processor belongs to this stream.
    pub fn new(
        stream_id: String,
        stream_generation: RuntimeGeneration,
        instance: ProcessorInstance<E>,
    ) -> Self {
        Self {
            stream_id,
            stream_generation,
            instance,
            stop_generation: None,
            pending_outcome: None,
            stopped: false,
        }
    }

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn processor(&self) -> ProcessorToken {
        self.instance.token()
    }

    pub fn stream_generation(&self) -> RuntimeGeneration {
        self.stream_generation
    }

    /// Stop this processor generation.
    ///
    /// Must be invoked by the serialized stream lifecycle owner.
    ///
    /// Blocking: the current CPU-processing turn may need
    /// to finish before this operation returns.
    ///
    /// Do not call directly from a Tokio executor worker.

    /// Begin shutdown in the authoritative stream registry.
    ///
    /// This operation must be serialized by the stream lifecycle owner.
    ///
    /// It does not wait for CPU processing.
    pub fn begin_stop(
        &mut self,
        registry: &mut StreamRuntimeRegistry,
    ) -> Result<RuntimeGeneration, ControllerError> {
        if self.stopped {
            return Err(ControllerError::AlreadyStopped);
        }

        // Retrying an already initiated stop must not advance
        // the stream generation a second time.
        if let Some(generation) = self.stop_generation {
            return Ok(generation);
        }

        let status = registry
            .status(&self.stream_id)
            .ok_or_else(|| StreamRuntimeError::UnknownStream(self.stream_id.clone()))?;

        if status.generation != self.stream_generation {
            return Err(ControllerError::StaleGeneration);
        }

        // This advances the registry generation and revokes
        // the stream's publication authority.
        let generation = registry.begin_stop(&self.stream_id)?;

        self.stop_generation = Some(generation);

        Ok(generation)
    }
    /// Retire processor execution and collect unprocessed events.
    ///
    /// This operation can block while a CPU turn finishes.
    ///
    /// It must run outside the stream registry lock and must
    /// not execute on a Tokio worker thread.
    ///
    /// The stop outcome is retained until complete_stop()
    /// successfully records the authoritative transition.
    pub fn retire_processor(&mut self) -> Result<(), ControllerError> {
        if self.stopped {
            return Err(ControllerError::AlreadyStopped);
        }

        if self.stop_generation.is_none() {
            return Err(ControllerError::InvalidPhase(
                "begin_stop must complete before retirement",
            ));
        }

        // Idempotent after successful retirement.
        if self.pending_outcome.is_some() {
            return Ok(());
        }

        // ProcessorInstance::stop() closes its mailbox,
        // waits for the current CPU turn, and returns
        // accepted but unprocessed events.
        let outcome = self.instance.stop()?;

        self.pending_outcome = Some(outcome);

        Ok(())
    }
    /// Complete the authoritative stream shutdown.
    ///
    /// Only succeeds after the processor has retired.
    ///
    /// A stale generation cannot complete shutdown for a
    /// newer stream generation.
    ///
    /// The unprocessed events are returned exactly once.
    pub fn complete_stop(
        &mut self,
        registry: &mut StreamRuntimeRegistry,
    ) -> Result<StopOutcome<E>, ControllerError> {
        if self.stopped {
            return Err(ControllerError::AlreadyStopped);
        }

        let stop_generation = self.stop_generation.ok_or(ControllerError::InvalidPhase(
            "begin_stop must complete before finalization",
        ))?;

        if self.pending_outcome.is_none() {
            return Err(ControllerError::InvalidPhase(
                "retire_processor must complete before finalization",
            ));
        }

        // Do not allow an obsolete stop operation to alter
        // the current stream generation.
        let applied = registry.mark_stopped(&self.stream_id, stop_generation)?;

        if !applied {
            return Err(ControllerError::StaleGeneration);
        }

        self.stopped = true;

        // Transfer ownership of unprocessed events only after
        // the registry confirms shutdown.
        Ok(self
            .pending_outcome
            .take()
            .expect("retirement outcome was validated"))
    }
    /// Convenience method for standalone tests and synchronous callers.
    ///
    /// Production orchestration should use the three explicit phases
    /// so it can release its registry lock during CPU retirement.
    pub fn stop(
        &mut self,
        registry: &mut StreamRuntimeRegistry,
    ) -> Result<StopOutcome<E>, ControllerError> {
        self.begin_stop(registry)?;
        self.retire_processor()?;
        self.complete_stop(registry)
    }
}
