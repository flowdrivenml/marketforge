use crate::live::infrastructure::processing::{
    instance::{InstanceError, ProcessorInstance, StopOutcome},
    scheduler::ProcessorToken,
};
use crate::live::infrastructure::processing::{
    mailbox::{MailboxLimits, ProcessorMailbox},
    pool::{ProcessingPool, Processor},
};
use crate::live::stream::registry::{RuntimeGeneration, StreamRuntimeError, StreamRuntimeRegistry};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
use thiserror::Error;

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

    #[error("processor startup failed: {0}")]
    Startup(String),

    #[error("stream generation changed during processor startup")]
    StartupSuperseded,
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
        let applied = if let Some(failure) = self
            .pending_outcome
            .as_ref()
            .and_then(|outcome| outcome.failure)
        {
            registry.mark_failed(
                &self.stream_id,
                stop_generation,
                format!("processor failed during retirement: {failure:?}"),
            )?
        } else {
            registry.mark_stopped(&self.stream_id, stop_generation)?
        };

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

    /// Start a processing instance under a new authoritative
    /// stream runtime generation.
    ///
    /// No ingress handle is exposed until startup succeeds.
    ///
    /// The readiness callback must verify every prerequisite
    /// that this particular processor requires before publication.
    ///
    /// This is a synchronous startup operation. The owning
    /// orchestrator must serialize stream lifecycle mutations.
    /// Compatibility helper for standalone synchronous callers. The owning
    /// ProcessingService is required for shared registries, cancellation and deadlines.
    /// Do not invoke this convenience method while holding a shared registry mutex.
    pub fn start<P, F, R>(
        registry: &mut StreamRuntimeRegistry,
        pool: Arc<ProcessingPool>,
        stream_id: impl Into<String>,
        processor_token: ProcessorToken,
        limits: MailboxLimits,
        factory: F,
        readiness: R,
    ) -> Result<Self, ControllerError>
    where
        P: Processor,
        F: FnOnce(Arc<ProcessorMailbox<E>>) -> P,
        R: FnOnce(&ProcessorInstance<E>) -> Result<(), String>,
    {
        let stream_id = stream_id.into();

        // Phase 1: establish the authoritative stream generation.
        //
        // If this fails, no processing resources have been created.
        let generation = registry.begin_start(&stream_id)?;

        // Phase 2: construct and register the processor.
        let mut instance = match catch_unwind(AssertUnwindSafe(|| {
            ProcessorInstance::start(Arc::clone(&pool), processor_token, limits, factory)
        })) {
            Ok(Ok(instance)) => instance,
            Err(_) => {
                registry.mark_failed(&stream_id, generation, "processor factory panicked")?;
                return Err(ControllerError::Startup(
                    "processor factory panicked".into(),
                ));
            }

            Ok(Err(error)) => {
                // Registration failed after stream startup began.
                // Record failure and revoke publication authority.
                registry.mark_failed(
                    &stream_id,
                    generation,
                    format!("processor registration failed: {error}"),
                )?;

                return Err(error.into());
            }
        };

        // Phase 3: verify processor-specific readiness.
        //
        // No external ingress handle has been published yet,
        // so the mailbox cannot contain externally admitted work.
        if let Err(reason) = catch_unwind(AssertUnwindSafe(|| readiness(&instance)))
            .unwrap_or_else(|_| Err("processor readiness panicked".into()))
        {
            registry.mark_failed(
                &stream_id,
                generation,
                format!("processor readiness failed: {reason}"),
            )?;
            instance.mailbox().close();
            // Retire resources before returning.
            //
            // Preserve the original startup error while ensuring
            // that cleanup has succeeded.
            if let Err(cleanup_error) = instance.stop() {
                registry.mark_failed(
                    &stream_id,
                    generation,
                    format!(
                        "processor readiness failed: {reason}; \
                     cleanup failed: {cleanup_error}"
                    ),
                )?;

                return Err(cleanup_error.into());
            }

            registry.mark_failed(
                &stream_id,
                generation,
                format!("processor readiness failed: {reason}"),
            )?;

            return Err(ControllerError::Startup(reason));
        }

        // Phase 4: confirm authoritative readiness.
        let running = match registry.mark_running(&stream_id, generation) {
            Ok(running) => running,

            Err(error) => {
                // The processor is registered, but its stream
                // could not transition to Running.
                let cleanup = instance.stop();

                if let Err(cleanup_error) = cleanup {
                    registry.mark_failed(
                        &stream_id,
                        generation,
                        format!(
                            "mark_running failed: {error}; \
                         cleanup failed: {cleanup_error}"
                        ),
                    )?;

                    return Err(cleanup_error.into());
                }

                registry.mark_failed(
                    &stream_id,
                    generation,
                    format!("mark_running failed: {error}"),
                )?;

                return Err(error.into());
            }
        };

        if !running {
            // A different operation superseded this startup.
            // Never return a controller for an obsolete generation.
            instance.stop()?;

            return Err(ControllerError::StartupSuperseded);
        }

        // All startup phases succeeded.
        //
        // Only now may the caller publish an ingress handle.
        Ok(Self::new(stream_id, generation, instance))
    }
}
