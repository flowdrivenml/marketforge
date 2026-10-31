use super::*;
use crate::live::config::ProcessingConfig;
use std::sync::{Arc, Barrier, mpsc};
use std::time::{Duration, Instant};
fn limits() -> MailboxLimits {
    MailboxLimits {
        max_events: 4,
        max_bytes: 32,
    }
}
fn pool() -> ProcessingPool {
    ProcessingPool::new(ProcessingConfig {
        workers: 2,
        ready_queue_capacity: 4,
        buffered_events: 4,
        buffered_bytes: 32,
        ..Default::default()
    })
    .unwrap()
}
struct Idle;
impl Processor for Idle {
    fn run_turn(&mut self, _: usize) -> bool {
        false
    }
}
#[test]
fn tracked_batch_keeps_aggregate_reservations_and_reports_unwind() {
    let mut owner = pool();
    let handle = owner.handle();
    let mut instance = ProcessorInstance::start_with_handle(
        handle.clone(),
        ProcessorToken {
            id: 1,
            generation: 1,
        },
        limits(),
        |_| Idle,
    )
    .unwrap();
    let ingress = instance.ingress();
    ingress.admit(7, 24).unwrap();
    let batch = instance.mailbox().take_work(1);
    assert_eq!(handle.budget().used(), (1, 24));
    assert!(ingress.admit(8, 16).is_err());
    assert_eq!(instance.mailbox().status().in_flight_bytes, 24);
    drop(batch);
    let outcome = instance.stop().unwrap();
    assert_eq!(outcome.accounting.indeterminate_events, 1);
    assert_eq!(outcome.accounting.completed_events, 0);
    assert_eq!(handle.budget().used(), (0, 0));
    assert!(
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .complete
    );
}
#[test]
fn admission_racing_stop_accounts_every_accepted_event() {
    let owner = pool();
    let mut instance = ProcessorInstance::start_with_handle(
        owner.handle(),
        ProcessorToken {
            id: 1,
            generation: 1,
        },
        limits(),
        |_| Idle,
    )
    .unwrap();
    let ingress = instance.ingress();
    let barrier = Arc::new(Barrier::new(2));
    let gate = barrier.clone();
    let producer = std::thread::spawn(move || {
        gate.wait();
        (0..100)
            .map(|id| match ingress.admit(id, 8) {
                Ok(()) => (id, true),
                Err(AdmissionError::Mailbox(e)) => {
                    assert_eq!(e.event, id);
                    (id, false)
                }
                Err(AdmissionError::Notification { event, .. }) => {
                    assert_eq!(event, id);
                    (id, false)
                }
            })
            .collect::<Vec<_>>()
    });
    barrier.wait();
    let outcome = instance.stop().unwrap();
    let admissions = producer.join().unwrap();
    assert_eq!(
        outcome.unprocessed,
        admissions
            .into_iter()
            .filter_map(|(id, yes)| yes.then_some(id))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        outcome.accounting.abandoned_events,
        outcome.unprocessed.len() as u64
    );
    assert_eq!(owner.budget().used(), (0, 0));
}
#[test]
fn exact_logical_token_reuse_cannot_revive_old_ingress_or_cleanup() {
    let owner = pool();
    let token = ProcessorToken {
        id: 1,
        generation: 1,
    };
    let mut old =
        ProcessorInstance::start_with_handle(owner.handle(), token, limits(), |_| Idle).unwrap();
    let ingress = old.ingress();
    old.stop().unwrap();
    let mut replacement =
        ProcessorInstance::start_with_handle(owner.handle(), token, limits(), |_| Idle).unwrap();
    assert_ne!(old.token(), replacement.token());
    assert!(ingress.admit(9, 8).is_err());
    old.request_stop();
    drop(old);
    replacement.ingress().admit(10, 8).unwrap();
    assert_eq!(replacement.stop().unwrap().unprocessed, vec![10]);
}
#[test]
fn acknowledged_failure_remains_available_to_instance_cleanup() {
    let owner = pool();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    struct PanicAfter {
        mailbox: Arc<ProcessorMailbox<u64>>,
        started: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }
    impl Processor for PanicAfter {
        fn run_turn(&mut self, _: usize) -> bool {
            let _batch = self.mailbox.take_work(1);
            self.started.send(()).unwrap();
            self.release.recv_timeout(Duration::from_secs(2)).unwrap();
            panic!("failed after dequeue")
        }
    }
    let mut instance = ProcessorInstance::start_with_handle(
        owner.handle(),
        ProcessorToken {
            id: 2,
            generation: 1,
        },
        limits(),
        |mailbox| PanicAfter {
            mailbox,
            started: started_tx,
            release: release_rx,
        },
    )
    .unwrap();
    instance.ingress().admit(42, 8).unwrap();
    let started = started_rx.recv_timeout(Duration::from_secs(2));
    instance.request_stop();
    release_tx.send(()).unwrap();
    assert!(started.is_ok());
    let token = instance.token();
    owner
        .retire_registration(&instance.registration_for_test())
        .unwrap();
    assert!(owner.acknowledge_failure(token));
    let outcome = instance.stop().unwrap();
    assert_eq!(
        outcome.failure.unwrap().kind,
        ProcessorFailureKind::Panicked
    );
    assert_eq!(outcome.accounting.indeterminate_events, 1);
    assert_eq!(outcome.accounting.completed_events, 0);
    assert!(!owner.acknowledge_failure(token));
    assert!(instance.failure().is_some());
    assert!(instance.ingress().admit(7, 8).is_err());
}
#[test]
fn shutdown_timeout_retains_workers_and_can_be_retried() {
    struct Block(mpsc::Sender<()>, mpsc::Receiver<()>);
    impl Processor for Block {
        fn run_turn(&mut self, _: usize) -> bool {
            self.0.send(()).unwrap();
            self.1.recv_timeout(Duration::from_secs(3)).unwrap();
            false
        }
    }
    let mut owner = pool();
    let (tx, rx) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    owner
        .register(
            ProcessorToken {
                id: 1,
                generation: 1,
            },
            Block(tx, wait),
        )
        .unwrap();
    owner
        .notify(ProcessorToken {
            id: 1,
            generation: 1,
        })
        .unwrap();
    let started = rx.recv_timeout(Duration::from_secs(2));
    let first = owner.shutdown_until(Instant::now());
    release.send(()).unwrap();
    assert!(started.is_ok());
    assert!(!first.complete);
    assert!(first.unfinished_workers > 0);
    assert!(
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .complete
    );
    assert!(owner.shutdown_until(Instant::now()).complete);
}
#[test]
fn processor_destructor_can_reenter_pool() {
    struct Reenter(ProcessingHandle, mpsc::Sender<()>);
    impl Processor for Reenter {
        fn run_turn(&mut self, _: usize) -> bool {
            false
        }
    }
    impl Drop for Reenter {
        fn drop(&mut self) {
            let _ = self.0.stats();
            self.1.send(()).unwrap();
        }
    }
    let owner = pool();
    let (tx, rx) = mpsc::channel();
    let token = ProcessorToken {
        id: 1,
        generation: 1,
    };
    owner.register(token, Reenter(owner.handle(), tx)).unwrap();
    let handle = owner.handle();
    let join = std::thread::spawn(move || handle.retire(token));
    assert!(rx.recv_timeout(Duration::from_secs(2)).is_ok());
    join.join().unwrap().unwrap();
}

#[test]
fn aggregate_budget_is_shared_across_registrations_and_inflight_batches() {
    let owner = ProcessingPool::new(ProcessingConfig {
        buffered_events: 2,
        buffered_bytes: 16,
        ready_queue_capacity: 2,
        ..Default::default()
    })
    .unwrap();
    let token = ProcessorToken {
        id: 1,
        generation: 1,
    };
    let mut first =
        ProcessorInstance::start_with_handle(owner.handle(), token, limits(), |_| Idle).unwrap();
    let mut second = ProcessorInstance::start_with_handle(
        owner.handle(),
        ProcessorToken { id: 2, ..token },
        limits(),
        |_| Idle,
    )
    .unwrap();
    first.ingress().admit(1, 8).unwrap();
    second.ingress().admit(2, 8).unwrap();
    let batch = first.mailbox().take_work(1);
    assert!(second.ingress().admit(3, 8).is_err());
    let status = owner.budget().status();
    assert_eq!(status.queued_events, 1);
    assert_eq!(status.in_flight_events, 1);
    assert_eq!(owner.budget().used(), (2, 16));
    batch.complete();
    second.ingress().admit(3, 8).unwrap();
    assert!(first.stop().unwrap().unprocessed.is_empty());
    assert_eq!(second.stop().unwrap().unprocessed, vec![2, 3]);
    assert_eq!(owner.budget().used(), (0, 0));
}
#[tokio::test]
async fn failures_under_fresh_ids_cannot_outgrow_reservations() {
    struct Panic;
    impl Processor for Panic {
        fn run_turn(&mut self, _: usize) -> bool {
            panic!("retained failure capacity")
        }
    }
    let owner = ProcessingPool::new(ProcessingConfig {
        ready_queue_capacity: 2,
        workers: 2,
        ..Default::default()
    })
    .unwrap();
    let mut updates = owner.subscribe_changes();
    for id in 1..=2 {
        let token = ProcessorToken { id, generation: 1 };
        owner.register(token, Panic).unwrap();
        owner.notify(token).unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while owner.failures().len() != 2 {
            updates.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        owner.register(
            ProcessorToken {
                id: 3,
                generation: 1
            },
            Idle
        ),
        Err(PoolError::Scheduler(SchedulerError::Capacity))
    ));
    assert_eq!(owner.stats().retained_failures, 2);
    assert!(owner.acknowledge_failure(ProcessorToken {
        id: 1,
        generation: 1
    }));
    // Destruction is a separate reserved phase; wait for it rather than assuming
    // a terminal failure means the processor destructor has returned.
    tokio::time::timeout(Duration::from_secs(2), async {
        while owner.stats().retiring != 0 {
            updates.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    owner
        .register(
            ProcessorToken {
                id: 3,
                generation: 1,
            },
            Idle,
        )
        .unwrap();
    assert_eq!(owner.stats().retained_failures, 1);
}
#[test]
fn destructor_panic_is_a_failed_retirement_not_a_clean_stop() {
    struct BadDrop;
    impl Processor for BadDrop {
        fn run_turn(&mut self, _: usize) -> bool {
            false
        }
    }
    impl Drop for BadDrop {
        fn drop(&mut self) {
            panic!("destructor failed")
        }
    }
    let owner = pool();
    let mut instance = ProcessorInstance::<u64>::start_with_handle(
        owner.handle(),
        ProcessorToken {
            id: 1,
            generation: 1,
        },
        limits(),
        |_| BadDrop,
    )
    .unwrap();
    let outcome = instance.stop().unwrap();
    assert!(outcome.failure.is_some());
    assert_eq!(owner.stats().destructor_panics, 1);
    assert!(instance.failure_detail().unwrap().contains("destructor"));
}

#[test]
fn retained_terminal_work_stays_charged_without_being_labeled_executing() {
    let owner = ProcessingPool::new(ProcessingConfig {
        buffered_events: 3,
        buffered_bytes: 16,
        ready_queue_capacity: 2,
        ..Default::default()
    })
    .unwrap();
    let mut first = ProcessorInstance::start_with_handle(
        owner.handle(),
        ProcessorToken {
            id: 1,
            generation: 1,
        },
        limits(),
        |_| Idle,
    )
    .unwrap();
    let mut second = ProcessorInstance::start_with_handle(
        owner.handle(),
        ProcessorToken {
            id: 2,
            generation: 1,
        },
        limits(),
        |_| Idle,
    )
    .unwrap();
    first.ingress().admit(1, 8).unwrap();
    let report = first.mailbox().take_work(1).canceled();
    second.ingress().admit(2, 8).unwrap();
    assert!(second.ingress().admit(3, 8).is_err());
    let status = owner.budget().status();
    assert_eq!(status.retained_terminal_bytes, 8);
    assert_eq!(status.in_flight_bytes, 0);
    assert_eq!(status.queued_bytes, 8);
    report.abandon();
    second.ingress().admit(3, 8).unwrap();
    first.stop().unwrap();
    assert_eq!(second.stop().unwrap().unprocessed, vec![2, 3]);
    assert_eq!(owner.budget().used(), (0, 0));
}
