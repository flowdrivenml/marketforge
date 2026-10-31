use crate::live::{
    infrastructure::processing::{
        mailbox::{BufferBudget, WorkBatch},
        *,
    },
    stream::{registry::RuntimeGeneration, router::*},
};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// A complete Router delivery with its upstream identity. Router sequence and
/// registry generation remain distinct from the processor's registration identity.
#[derive(Debug, Clone)]
pub struct ProcessingInput {
    pub stream_id: String,
    pub delivery: RouterDelivery,
}
impl ProcessingInput {
    pub fn retained_bytes(&self) -> Option<usize> {
        let body = match &self.delivery {
            RouterDelivery::Publication { publication, .. } => publication.retained_bytes()?,
            RouterDelivery::ContinuityLost(_) => 0,
        };
        body.checked_add(std::mem::size_of::<Self>())?
            .checked_add(self.stream_id.capacity())
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
    stream_id: String,
    delivery: RouterDelivery,
    ingress: &ProcessorIngress<ProcessingInput>,
) -> Result<(), DeliveryRejection> {
    let input = ProcessingInput {
        stream_id,
        delivery,
    };
    if !matches!(
        input.delivery,
        RouterDelivery::Publication {
            synchronized: true,
            ..
        }
    ) {
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
    ingress.admit(input, bytes).map_err(|error| match error {
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
        let mailbox = Arc::new(ProcessorMailbox::new(limits)?);
        mailbox.manage(budget);
        Ok(Self {
            mailbox,
            fence: input.publication_fence(),
            wake,
            terminal_error: Arc::new(Mutex::new(None)),
        })
    }
    pub fn try_emit(&self, publication: RouterPublication) -> Result<(), OutputRejection> {
        let mut fence = self.fence.lock().unwrap_or_else(|e| e.into_inner());
        if *fence {
            self.mailbox.record_rejection();
            return Err(OutputRejection {
                publication,
                reason: "processor output is fenced",
                kind: OutputRejectionKind::Closed,
            });
        }
        let Some(bytes) = publication.retained_bytes() else {
            self.mailbox.record_rejection();
            self.fail_output(&mut fence, "output accounting overflow");
            return Err(OutputRejection {
                publication,
                reason: "output accounting overflow",
                kind: OutputRejectionKind::AccountingOverflow,
            });
        };
        if let Err(error) = self.mailbox.push_managed(publication, bytes) {
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
    fn fail_output(&self, fence: &mut bool, reason: &'static str) {
        *fence = true;
        *self
            .terminal_error
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(reason);
        self.mailbox.close();
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
        *self.fence.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.mailbox.close();
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
