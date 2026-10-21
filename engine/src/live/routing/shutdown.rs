//! Cooperative shutdown of the existing authoritative router owners.
//! Runtime scheduling retains responsibility for completions and retry timers.
use super::{
    admission::DeadlineFuture,
    driver::{DeliveryDrive, DeliveryDriver},
    namespaces::{AdmissionNamespaces, AdmissionNamespacesSnapshot},
    ordered::OrderedAdmissionBudget,
    router::RouterRegistry,
    snapshot::RouterSnapshot,
    supervisor::DeliverySupervisionSnapshot,
};
use crate::live::foundation::resources::ResourceUsage;
use std::task::Context;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterDrainSnapshot {
    pub namespaces: AdmissionNamespacesSnapshot,
    pub admission_usage: ResourceUsage,
    pub destinations: RouterSnapshot,
    pub delivery: DeliverySupervisionSnapshot,
}
impl RouterDrainSnapshot {
    /// Closure and all in-process responsibilities must clear, including
    /// retired namespaces and externally held queue permits. No durability or
    /// end-consumer processing guarantee is established by this observation.
    pub fn is_drained(&self) -> bool {
        self.namespaces.shutting_down
            && self
                .namespaces
                .namespaces
                .iter()
                .all(|entry| entry.order.closed && entry.order.pending_last.is_none())
            && self.admission_usage == ResourceUsage::default()
            && self.destinations.shutting_down
            && self.destinations.destinations.iter().all(|entry| {
                entry.queue.as_ref().is_none_or(|queue| {
                    queue.queue.closed
                        && queue.queue.queued_batches == 0
                        && queue.capacity.shutting_down
                        && queue.capacity.occupied_events == 0
                        && queue.capacity.occupied_bytes == 0
                        && queue.capacity.waiting_operations == 0
                })
            })
            && self.delivery.is_drained()
    }
}

#[derive(Debug)]
#[must_use]
pub enum RouterShutdownStep {
    Drained,
    DeadlineReached,
    /// One bounded delivery turn. Pending/blocked results require an external
    /// wakeup or retry timer; never immediately spin on this value.
    Step(DeliveryDrive),
}

/// Borrows the real owners rather than constructing parallel lifecycle state.
pub struct RouterShutdownOwners<'a> {
    pub namespaces: &'a AdmissionNamespaces,
    pub admission: &'a OrderedAdmissionBudget,
    pub destinations: &'a RouterRegistry,
    pub delivery: &'a mut DeliveryDriver,
}
impl RouterShutdownOwners<'_> {
    pub fn snapshot(&self) -> RouterDrainSnapshot {
        RouterDrainSnapshot {
            namespaces: self.namespaces.snapshot(),
            admission_usage: self.admission.usage(),
            destinations: self.destinations.snapshot(),
            delivery: self.delivery.snapshot(),
        }
    }

    /// Close admissions even on expiry. Expiry does not call adapters or retry
    /// tracking. Retain these owners to resume under a subsequent deadline.
    /// Namespace/worker/capacity wakeups notify the coordinator; reporting and
    /// transferred responsibility still require runtime scheduling.
    pub fn drive(
        &mut self,
        cx: &mut Context<'_>,
        deadline: &mut DeadlineFuture,
    ) -> RouterShutdownStep {
        let _ = self.admission.poll_shutdown(cx);
        let _ = self.namespaces.poll_shutdown(cx);
        let _ = self.destinations.poll_shutdown(cx);
        self.delivery.close();
        if self.snapshot().is_drained() {
            return RouterShutdownStep::Drained;
        }
        if deadline.as_mut().poll(cx).is_ready() {
            return RouterShutdownStep::DeadlineReached;
        }
        RouterShutdownStep::Step(self.delivery.drive(cx))
    }
}
