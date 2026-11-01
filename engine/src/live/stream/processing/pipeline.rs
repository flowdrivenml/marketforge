use crate::live::{
    infrastructure::processing::{
        mailbox::{BufferBudget, WorkBatch},
        *,
    },
    stream::{registry::RuntimeGeneration, router::*},
};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Notify;

/// A complete Router delivery with its upstream identity. Router sequence and
/// registry generation remain distinct from the processor's registration identity.
#[derive(Debug, Clone)]
pub struct ProcessingInput {
    pub stream_id: String,
    pub input_position: usize,
    pub role: crate::live::stream::request::InputRole,
    pub admission_ordinal: u64,
    pub delivery: RouterDelivery,
}
impl ProcessingInput {
    pub fn retained_bytes(&self) -> Option<usize> {
        let body = match &self.delivery {
            RouterDelivery::Publication {
                publication,
                admission,
                ..
            } => publication
                .retained_bytes()?
                .checked_add(admission.retained_bytes()?)?,
            RouterDelivery::ContinuityLost(_) => 0,
        };
        body.checked_add(std::mem::size_of::<Self>())?
            .checked_add(self.stream_id.capacity().checked_mul(4)?)?
            .checked_add(512)
    }
    pub fn generation(&self) -> Option<RuntimeGeneration> {
        match self.delivery {
            RouterDelivery::Publication {
                runtime_generation, ..
            } => Some(runtime_generation),
            _ => None,
        }
    }
}
pub(crate) struct DeliveryRejection {
    pub input: ProcessingInput,
    pub reason: &'static str,
}
/// One Router custody unit becomes exactly one managed admission. On rejection,
/// ownership is returned for the coordinator's explicit terminal-discard policy.
pub(crate) fn forward_router_input(
    spec: &crate::live::stream::request::ProcessingInputSpec,
    admission_counter: &Mutex<u64>,
    delivery: RouterDelivery,
    ingress: &ProcessorIngress<ProcessingInput>,
    acquiring_baselines: bool,
) -> Result<(), DeliveryRejection> {
    let input = ProcessingInput {
        stream_id: spec.stream_id.clone(),
        input_position: spec.position,
        role: spec.role,
        admission_ordinal: 0,
        delivery,
    };
    if !matches!(&input.delivery,RouterDelivery::Publication{event_valid:true,synchronized,..} if *synchronized || acquiring_baselines)
    {
        return Err(DeliveryRejection {
            input,
            reason: "upstream continuity or synchronization lost",
        });
    }
    let Some(bytes) = input.retained_bytes() else {
        return Err(DeliveryRejection {
            input,
            reason: "input accounting overflow",
        });
    };
    admit_ordered(ingress, input, bytes, admission_counter).map_err(|error| match error {
        AdmissionError::Mailbox(error) => DeliveryRejection {
            input: error.event,
            reason: "bounded processor ingress rejected Router delivery",
        },
        AdmissionError::Notification { event, .. } => DeliveryRejection {
            input: event,
            reason: "processor ingress registration closed",
        },
    })
}
fn admit_ordered(
    ingress: &ProcessorIngress<ProcessingInput>,
    mut input: ProcessingInput,
    bytes: usize,
    counter: &Mutex<u64>,
) -> Result<(), AdmissionError<ProcessingInput, PoolError>> {
    let mut ordinal = counter.lock().unwrap_or_else(|e| e.into_inner());
    let Some(next) = ordinal.checked_add(1) else {
        return Err(AdmissionError::Mailbox(MailboxPushError {
            reason: MailboxError::SizeOverflow,
            event: input,
        }));
    };
    let original = input.admission_ordinal;
    input.admission_ordinal = next;
    match ingress.admit(input, bytes) {
        Ok(()) => {
            *ordinal = next;
            Ok(())
        }
        Err(AdmissionError::Mailbox(mut error)) => {
            error.event.admission_ordinal = original;
            Err(AdmissionError::Mailbox(error))
        }
        Err(AdmissionError::Notification { reason, mut event }) => {
            event.admission_ordinal = original;
            Err(AdmissionError::Notification { reason, event })
        }
    }
}

/// Rejection always returns the original complete output. Admission here is
/// local retention only; Router custody is recorded separately by the service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputRejectionKind {
    Closed,
    Capacity,
    AccountingOverflow,
}
pub struct OutputRejection {
    pub kind: OutputRejectionKind,
    pub publication: RouterPublication,
    pub reason: &'static str,
}
/// Lock contract: pool state -> generation fence -> mailbox state -> budget.
/// Registry operations may take the fence only after releasing authority guards;
/// no fence holder acquires pool state or the registry. Publication holds the
/// fence, then Router state -> authority through custody insertion. Registry
/// revocation takes authority only, and releases it before processing fencing.
/// No processor callback, await, or processor destruction runs under these locks.
#[derive(Clone)]
pub struct ProcessingOutput {
    pub(crate) mailbox: Arc<ProcessorMailbox<RouterPublication>>,
    pub(crate) fence: Arc<Mutex<bool>>,
    pub(crate) wake: Arc<Notify>,
    terminal_error: Arc<Mutex<Option<&'static str>>>,
}
impl ProcessingOutput {
    pub(crate) fn new(
        limits: MailboxLimits,
        budget: Arc<BufferBudget>,
        input: &ProcessorMailbox<ProcessingInput>,
        wake: Arc<Notify>,
    ) -> Result<Self, MailboxError> {
        let fence = input.publication_fence();
        let mailbox = Arc::new(ProcessorMailbox::with_fence(limits, fence.clone())?);
        mailbox.manage(budget);
        Ok(Self {
            mailbox,
            fence,
            wake,
            terminal_error: Arc::new(Mutex::new(None)),
        })
    }
    pub fn try_emit(&self, publication: RouterPublication) -> Result<(), OutputRejection> {
        let bytes = publication.retained_bytes();
        self.try_emit_accounted(publication, bytes)
    }
    fn try_emit_accounted(
        &self,
        publication: RouterPublication,
        bytes: Option<usize>,
    ) -> Result<(), OutputRejection> {
        let mut fence = self.fence.lock().unwrap_or_else(|e| e.into_inner());
        if *fence {
            self.mailbox.record_rejection();
            return Err(OutputRejection {
                publication,
                reason: "processor output is fenced",
                kind: OutputRejectionKind::Closed,
            });
        }
        let Some(bytes) = bytes else {
            self.mailbox.record_rejection();
            self.fail_output(&mut fence, "output accounting overflow");
            return Err(OutputRejection {
                publication,
                reason: "output accounting overflow",
                kind: OutputRejectionKind::AccountingOverflow,
            });
        };
        if let Err(error) = self.mailbox.push_fenced(&fence, publication, bytes) {
            self.fail_output(&mut fence, "bounded output capacity exhausted");
            return Err(OutputRejection {
                publication: error.event,
                reason: "bounded output capacity exhausted",
                kind: OutputRejectionKind::Capacity,
            });
        }
        self.wake.notify_one();
        Ok(())
    }
    fn fail_output(&self, fence: &mut MutexGuard<'_, bool>, reason: &'static str) {
        **fence = true;
        *self
            .terminal_error
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(reason);
        self.mailbox.close_fenced(fence);
        self.wake.notify_one();
    }
    pub(crate) fn failure_reason(&self) -> Option<&'static str> {
        *self
            .terminal_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
    pub fn status(&self) -> MailboxStatus {
        self.mailbox.status()
    }
    pub(crate) fn fence(&self) {
        let mut fence = self.fence.lock().unwrap_or_else(|e| e.into_inner());
        self.mailbox.close_fenced(&mut fence);
        self.wake.notify_one();
    }
    pub(crate) fn publish_one(&self, producer: &ProducerHandle) -> Result<bool, PublishError> {
        let fence = self.fence.lock().unwrap_or_else(|e| e.into_inner());
        if *fence {
            return Err(PublishError::Revoked);
        }
        let batch = self.mailbox.take_work(1);
        if batch.is_empty() {
            batch.complete();
            return Ok(false);
        }
        // Router's existing API consumes a publication even on rejection. The Arc
        // payload clone preserves the original until custody has been accepted.
        match producer.try_publish(batch[0].clone()) {
            Ok(_) => {
                batch.complete();
                Ok(true)
            }
            Err(error) => {
                self.mailbox.return_work(batch);
                Err(error)
            }
        }
    }
    pub(crate) fn retained(&self) -> WorkBatch<RouterPublication> {
        self.mailbox.take_work(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Barrier, mpsc},
        thread,
        time::Duration,
    };

    // A lock regression must fail the test, not hang the test runner. Worker
    // threads are detached on timeout; no blocking join is used on failure.
    fn bounded(test: impl FnOnce() + Send + 'static) {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            test();
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(5))
            .expect("processing locking deadline");
    }
    fn setup(
        max_events: usize,
    ) -> (
        Arc<ProcessorMailbox<ProcessingInput>>,
        ProcessingOutput,
        Arc<BufferBudget>,
    ) {
        let limits = MailboxLimits {
            max_events,
            max_bytes: 1 << 20,
        };
        let budget = Arc::new(BufferBudget::new(limits).unwrap());
        let input = Arc::new(ProcessorMailbox::new(limits).unwrap());
        input.manage(budget.clone());
        let output =
            ProcessingOutput::new(limits, budget.clone(), &input, Arc::new(Notify::new())).unwrap();
        assert!(Arc::ptr_eq(
            &output.fence,
            &output.mailbox.publication_fence()
        ));
        (input, output, budget)
    }
    fn publication(id: u64) -> RouterPublication {
        RouterPublication::Depth(
            vec![RouterDepthEvent::Invalidated {
                stream_generation: id,
                book_generation: 1,
            }]
            .into(),
        )
    }
    fn same_payload(a: &RouterPublication, b: &RouterPublication) {
        match (a, b) {
            (RouterPublication::Depth(a), RouterPublication::Depth(b)) => {
                assert!(Arc::ptr_eq(a, b))
            }
            _ => panic!("payload changed"),
        }
    }
    #[test]
    fn successful_output_admission_preserves_publication_and_charge() {
        bounded(|| {
            let (_input, output, budget) = setup(2);
            let original = publication(1);
            assert!(output.try_emit(original.clone()).is_ok());
            assert_eq!(output.status().queued_events, 1);
            let batch = output.retained();
            same_payload(&original, &batch[0]);
            assert_eq!(budget.status().in_flight_events, 1);
            batch.complete();
            assert_eq!(budget.used(), (0, 0));
            assert_eq!(budget.status().completed_events, 1);
        });
    }
    #[test]
    fn capacity_failure_fences_and_retains_previously_accepted_output() {
        bounded(|| {
            let (input, output, budget) = setup(1);
            let accepted = publication(1);
            let rejected = publication(2);
            assert!(output.try_emit(accepted.clone()).is_ok());
            let error = output.try_emit(rejected.clone()).err().unwrap();
            same_payload(&rejected, &error.publication);
            assert_eq!(error.kind, OutputRejectionKind::Capacity);
            assert_eq!(
                output.failure_reason(),
                Some("bounded output capacity exhausted")
            );
            assert!(input.status().closed);
            assert!(output.status().closed);
            let report = output.retained().canceled();
            same_payload(&accepted, &report[0]);
            assert_eq!(budget.status().retained_terminal_events, 1);
            assert_eq!(budget.status().rejected_events, 1);
            drop(report);
            assert_eq!(budget.used(), (0, 0));
            assert_eq!(budget.status().abandoned_events, 1);
        });
    }
    #[test]
    fn accounting_overflow_fences_and_returns_original_output() {
        bounded(|| {
            let (input, output, budget) = setup(2);
            let original = publication(1);
            // Inject the size calculator's overflow result: allocating a real
            // usize::MAX payload is neither feasible nor necessary to test it.
            let error = output
                .try_emit_accounted(original.clone(), None)
                .err()
                .unwrap();
            assert_eq!(error.kind, OutputRejectionKind::AccountingOverflow);
            same_payload(&original, &error.publication);
            assert_eq!(output.failure_reason(), Some("output accounting overflow"));
            assert!(input.status().closed);
            assert_eq!(
                output.try_emit(original).err().unwrap().kind,
                OutputRejectionKind::Closed
            );
            assert_eq!(budget.used(), (0, 0));
        });
    }
    #[test]
    fn concurrent_emission_and_input_retirement_preserve_every_ownership_unit() {
        bounded(|| {
            for _ in 0..32 {
                let (input, output, budget) = setup(128);
                assert!(output.try_emit(publication(0)).is_ok());
                let start = Arc::new(Barrier::new(2));
                let emitter = output.clone();
                let ready = start.clone();
                let worker = thread::spawn(move || {
                    ready.wait();
                    let mut accepted = 0;
                    for id in 1..=64 {
                        let original = publication(id);
                        match emitter.try_emit(original.clone()) {
                            Ok(()) => accepted += 1,
                            Err(error) => {
                                assert_eq!(error.kind, OutputRejectionKind::Closed);
                                same_payload(&original, &error.publication);
                            }
                        }
                    }
                    accepted
                });
                start.wait();
                input.close(); // actual processor retirement's admission fence
                for id in 65..=68 {
                    assert_eq!(
                        output.try_emit(publication(id)).err().unwrap().kind,
                        OutputRejectionKind::Closed
                    );
                }
                let accepted = worker.join().unwrap() + 1;
                let batch = output.retained().canceled();
                assert_eq!(batch.len(), accepted);
                assert_eq!(budget.status().retained_terminal_events, accepted);
                assert_eq!(output.status().rejected_events as usize, 69 - accepted);
                batch.abandon();
                assert_eq!(budget.used(), (0, 0));
            }
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_publication_and_authoritative_retirement_retain_unpublished_output() {
        use crate::live::stream::{persistence::*, registry::*};
        let data: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/live/support/data/instruments/btc_sources.json"
        ))
        .unwrap();
        let mut source = data["instruments"][0].clone();
        for key in [
            "exchange_id",
            "tick_size",
            "qty_step",
            "min_qty",
            "max_qty",
            "min_notional",
        ] {
            source.as_object_mut().unwrap().remove(key);
        }
        source["channel"] = "depth".into();
        let graph = Arc::new(
            StreamGraph::restore(
                vec![PersistedStreamDefinition {
                    stream_id: "test".into(),
                    recording_stream_key: Some(1),
                    definition: StreamDefinition::Source(serde_json::from_value(source).unwrap()),
                    desired: DesiredStreamState::default(),
                    revision: 1,
                }],
                vec![],
            )
            .unwrap(),
        );
        let mut registry = StreamRuntimeRegistry::new(&graph);
        registry.begin_start("test").unwrap();
        let authority = registry.publication_authority("test").unwrap();
        let router = Arc::new(
            StreamRouter::new(
                RouterConfig::default(),
                RouterContext {
                    graph,
                    demand: Arc::new(Mutex::new(DemandRegistry::new())),
                },
            )
            .unwrap(),
        );
        let producer = router
            .attach_producer(AttachProducerRequest {
                authority: authority.clone(),
            })
            .unwrap();
        let runtime = tokio::runtime::Handle::current();
        let observed_router = router.clone();
        bounded(move || {
            let _runtime = runtime.enter();
            let (_input, output, budget) = setup(3);
            assert!(output.try_emit(publication(1)).is_ok());
            let authority_guard = authority.guard();
            assert_eq!(output.publish_one(&producer), Err(PublishError::Busy));
            assert_eq!(output.status().queued_events, 1);
            assert_eq!(budget.status().completed_events, 0);
            drop(authority_guard);
            // Releasing authority does not reserve the Router state mutex:
            // dispatcher contention may still return Busy without custody.
            let accepted = runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        match output.publish_one(&producer) {
                            Err(PublishError::Busy) => {
                                assert_eq!(output.status().queued_events, 1);
                                assert_eq!(budget.status().completed_events, 0);
                                tokio::task::yield_now().await;
                            }
                            result => break result,
                        }
                    }
                })
                .await
                .expect("Router admission deadline")
            });
            assert_eq!(accepted, Ok(true));
            assert_eq!(observed_router.status().accepted, 1);
            let original = publication(2);
            assert!(output.try_emit(original.clone()).is_ok());
            // Hold the processing fence so publication is already in progress
            // but cannot reach Router admission until registry revocation wins.
            let gate = output.fence.lock().unwrap();
            let publisher = output.clone();
            let (started, rx) = mpsc::channel();
            let publish_runtime = runtime.clone();
            let worker = thread::spawn(move || {
                started.send(()).unwrap();
                publish_runtime.block_on(async {
                    tokio::time::timeout(Duration::from_secs(1), async {
                        loop {
                            match publisher.publish_one(&producer) {
                                Err(PublishError::Busy) => {
                                    assert_eq!(publisher.status().queued_events, 1);
                                    tokio::task::yield_now().await;
                                }
                                result => break result,
                            }
                        }
                    })
                    .await
                    .expect("retired Router admission deadline")
                })
            });
            rx.recv_timeout(Duration::from_secs(1)).unwrap();
            registry.begin_stop("test").unwrap();
            assert!(!*authority.guard());
            drop(gate);
            assert_eq!(worker.join().unwrap(), Err(PublishError::Revoked));
            assert_eq!(observed_router.status().accepted, 1);
            output.fence();
            let retained = output.retained().canceled();
            same_payload(&original, &retained[0]);
            assert_eq!(budget.status().retained_terminal_events, 1);
            retained.abandon();
            assert_eq!(budget.used(), (0, 0));
        });
        tokio::time::timeout(Duration::from_secs(5), router.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}
