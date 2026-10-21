//! Cooperative shutdown of the existing authoritative router owners.
//! Runtime scheduling retains responsibility for completions and retry timers.
use super::{
    admission::DeadlineFuture,
    driver::{DeliveryDrive, DeliveryDriver, RegisteredDeliveryDrive},
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
    pub admission_limit: ResourceUsage,
    pub admission_closed: bool,
    pub destinations: RouterSnapshot,
    pub delivery: DeliverySupervisionSnapshot,
}
impl RouterDrainSnapshot {
    /// Read-only bounded observation for control/runtime consumers. Individual
    /// owners are observed consistently; this is not a cross-owner transaction.
    /// Sampling neither closes admission nor polls adapters or delivery work.
    pub fn observe(
        namespaces: &AdmissionNamespaces,
        admission: &OrderedAdmissionBudget,
        destinations: &RouterRegistry,
        delivery: &DeliveryDriver,
    ) -> Self {
        let budget = admission.snapshot();
        Self {
            namespaces: namespaces.snapshot(),
            admission_usage: budget.usage,
            admission_limit: budget.limit,
            admission_closed: budget.closed,
            destinations: destinations.snapshot(),
            delivery: delivery.snapshot(),
        }
    }
    /// Closure and all in-process responsibilities must clear, including
    /// retired namespaces and externally held queue permits. No durability or
    /// end-consumer processing guarantee is established by this observation.
    pub fn is_drained(&self) -> bool {
        self.admission_closed
            && self.namespaces.shutting_down
            && self
                .namespaces
                .namespaces
                .iter()
                .all(|entry| entry.order.closed && entry.order.pending_last.is_none())
            && self.admission_usage == ResourceUsage::default()
            && self.destinations.shutting_down
            && self
                .destinations
                .destinations
                .iter()
                .chain(self.destinations.retired.iter())
                .all(|entry| {
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

#[derive(Debug)]
#[must_use]
pub enum RegisteredRouterShutdownStep {
    Drained,
    DeadlineReached,
    /// Includes attachment/capacity failures; retain the owners and arrange a
    /// notification or bounded retry rather than spinning on a blocked turn.
    Step(RegisteredDeliveryDrive),
}

/// Borrows the real owners rather than constructing parallel lifecycle state.
pub struct RouterShutdownOwners<'a> {
    pub namespaces: &'a AdmissionNamespaces,
    pub admission: &'a OrderedAdmissionBudget,
    pub destinations: &'a RouterRegistry,
    pub delivery: &'a mut DeliveryDriver,
}
impl RouterShutdownOwners<'_> {
    /// Fence admission immediately, then reconcile at most one unclaimed queue
    /// and run one delivery turn. The driver stays open for worker attachment
    /// until the closed registry has no unclaimed queues. This also permits
    /// capacity-limited schedulers to drain and reap workers between attachments.
    ///
    /// Expiry does not attach workers or poll adapters. Keep these owners alive
    /// to resume with another deadline; reporting and correlated completion
    /// remain the runtime's responsibility. Existing externally claimed queues
    /// must still be driven by their owner and remain visible in the snapshot.
    pub fn drive_registered(
        &mut self,
        cx: &mut Context<'_>,
        deadline: &mut DeadlineFuture,
    ) -> RegisteredRouterShutdownStep {
        let _ = self.admission.poll_shutdown(cx);
        let _ = self.namespaces.poll_shutdown(cx);
        let _ = self.destinations.poll_shutdown(cx);
        if self.snapshot().is_drained() {
            return RegisteredRouterShutdownStep::Drained;
        }
        if deadline.as_mut().poll(cx).is_ready() {
            return RegisteredRouterShutdownStep::DeadlineReached;
        }
        let turn = self.delivery.drive_registered(self.destinations, cx);
        if !self.delivery.is_closed() && turn.registry.attachment == Ok(false) {
            self.delivery.close();
            // Observe closure/drain on another bounded turn even for empty IO.
            cx.waker().wake_by_ref();
        }
        RegisteredRouterShutdownStep::Step(turn)
    }

    pub fn snapshot(&self) -> RouterDrainSnapshot {
        RouterDrainSnapshot::observe(
            self.namespaces,
            self.admission,
            self.destinations,
            self.delivery,
        )
    }

    /// Requires all registry queues to have been attached before entry; use
    /// `drive_registered` when shutdown must reconcile unclaimed queues.
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
