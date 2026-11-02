use super::*;
use std::time::Duration;

pub(super) fn worker_entry(runtime: Arc<Shared>, control: Arc<WorkerControl>) {
    CURRENT_RUNTIME.with(|current| current.set(Arc::as_ptr(&runtime) as usize));
    {
        let mut state = lock(&runtime);
        while !control.active.load(Ordering::Acquire)
            && !control.retire.load(Ordering::Acquire)
            && !state.shutdown
        {
            state = runtime.wake.wait(state).unwrap_or_else(|e| e.into_inner());
        }
        if control.retire.load(Ordering::Acquire) || state.shutdown {
            state.stats.exited_workers += 1;
            runtime.wake.notify_all();
            return;
        }
        state.stats.running_workers += 1;
        runtime.wake.notify_all();
    }

    let result = catch_unwind(AssertUnwindSafe(|| worker_loop(runtime.clone(), control)));
    let mut state = lock(&runtime);
    state.stats.running_workers -= 1;
    state.stats.exited_workers += 1;
    if result.is_err() {
        state.stats.unexpected_worker_exits += 1;
        state.shutdown = true;
        for entry in state.processors.values() {
            entry.registration.close();
        }
    }
    runtime.wake.notify_all();
    runtime.updates.send_modify(|v| *v = v.wrapping_add(1));
}

pub(super) fn supervise_workers(
    runtime: &Arc<Shared>,
    mut workers: Vec<(JoinHandle<()>, Arc<WorkerControl>)>,
) {
    let mut next_index = workers.len();
    loop {
        let request = {
            let mut state = lock(runtime);
            if state.shutdown {
                None
            } else {
                state.resize_target.take()
            }
        };
        if let Some(target) = request {
            let active = workers
                .iter()
                .filter(|(_, c)| !c.retire.load(Ordering::Acquire))
                .count();
            if target > active {
                let mut staged: Vec<(JoinHandle<()>, Arc<WorkerControl>)> = Vec::new();
                let mut failure = None;
                for _ in active..target {
                    let c = Arc::new(WorkerControl {
                        active: AtomicBool::new(false),
                        retire: AtomicBool::new(false),
                    });
                    let r = runtime.clone();
                    let control = c.clone();
                    #[cfg(test)]
                    let inject = {
                        let mut state = lock(runtime);
                        match state.spawn_failure_after.as_mut() {
                            Some(0) => true,
                            Some(n) => {
                                *n -= 1;
                                false
                            }
                            None => false,
                        }
                    };
                    #[cfg(not(test))]
                    let inject = false;
                    let started = if inject {
                        Err(std::io::Error::other("injected growth failure"))
                    } else {
                        thread::Builder::new()
                            .name(format!("marketforge-cpu-{next_index}"))
                            .spawn(move || worker_entry(r, control))
                    };
                    next_index += 1;
                    match started {
                        Ok(worker) => {
                            lock(runtime).stats.started_workers += 1;
                            staged.push((worker, c));
                        }
                        Err(_) => {
                            failure =
                                Some("worker creation failed; original pool preserved".to_string());
                            break;
                        }
                    }
                }
                // Commit activation under the same lock that begins shutdown.
                let mut state = lock(runtime);
                if state.shutdown || failure.is_some() {
                    for (_, c) in &staged {
                        c.retire.store(true, Ordering::Release);
                    }
                    state.resize_error =
                        failure.or_else(|| Some("shutdown interrupted resize".into()));
                } else {
                    for (_, c) in &staged {
                        c.active.store(true, Ordering::Release);
                    }
                }
                workers.extend(staged);
                runtime.wake.notify_all();
            } else {
                let _state = lock(runtime);
                for (_, c) in workers
                    .iter()
                    .rev()
                    .filter(|(_, c)| !c.retire.load(Ordering::Acquire))
                    .take(active - target)
                {
                    c.retire.store(true, Ordering::Release);
                }
                runtime.wake.notify_all();
            }
        }
        let mut index = 0;
        while index < workers.len() {
            if workers[index].0.is_finished() {
                let (worker, _) = workers.swap_remove(index);
                let _ = worker.join();
            } else {
                index += 1;
            }
        }
        let mut state = lock(runtime);
        state.stats.retiring_workers = workers
            .iter()
            .filter(|(_, c)| c.retire.load(Ordering::Acquire))
            .count();
        state.stats.starting_workers = state
            .stats
            .started_workers
            .saturating_sub(state.stats.exited_workers + state.stats.running_workers);
        if state.stats.resize_pending
            && state.resize_target.is_none()
            && (workers.len() == state.stats.requested_workers || state.resize_error.is_some())
            && state.stats.retiring_workers == 0
            && state.stats.starting_workers == 0
            && (state.resize_error.is_some()
                || state.stats.running_workers == state.stats.requested_workers)
        {
            state.stats.resize_pending = false;
            runtime.updates.send_modify(|v| *v = v.wrapping_add(1));
        }
        if state.shutdown && workers.is_empty() {
            if state.stats.resize_pending {
                state
                    .resize_error
                    .get_or_insert_with(|| "resize interrupted by shutdown".into());
            }
            state.stats.resize_pending = false;
            state.resize_target = None;
            return;
        }
        let _guard = runtime
            // A supervisor must never consume a scheduler notify_one intended
            // for a CPU worker. Lifecycle polling has its own wait channel.
            .supervisor_wake
            .wait_timeout(state, Duration::from_millis(10))
            .unwrap_or_else(|e| e.into_inner());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{atomic::AtomicUsize, mpsc};
    fn pool(workers: usize) -> ProcessingPool {
        ProcessingPool::new(ProcessingConfig {
            workers,
            ..Default::default()
        })
        .unwrap()
    }
    fn settled(p: &ProcessingPool, target: usize) {
        let end = Instant::now() + Duration::from_secs(3);
        while p.stats().resize_pending || p.stats().running_workers != target {
            assert!(Instant::now() < end, "{:?}", p.stats());
            thread::yield_now();
        }
    }
    #[test]
    fn supervisor_does_not_consume_worker_activation_wakeups() {
        struct Acknowledge(mpsc::Sender<()>);
        impl Processor for Acknowledge {
            fn run_turn(&mut self, _: usize) -> bool {
                self.0.send(()).unwrap();
                false
            }
        }
        let mut p = pool(1);
        let (tx, rx) = mpsc::channel();
        let token = p.fresh_token(1);
        p.register(token, Acknowledge(tx)).unwrap();
        for _ in 0..128 {
            p.notify(token).unwrap();
            rx.recv_timeout(Duration::from_secs(2)).expect("accepted activation stranded");
        }
        p.shutdown();
    }
    #[test]
    fn repeated_growth_and_idle_retirement_preserve_registrations() {
        struct Count(Arc<AtomicUsize>);
        impl Processor for Count {
            fn run_turn(&mut self, _: usize) -> bool {
                self.0.fetch_add(1, Ordering::SeqCst);
                false
            }
        }
        let mut p = pool(2);
        let count = Arc::new(AtomicUsize::new(0));
        let token = p.fresh_token(1);
        p.register(token, Count(count.clone())).unwrap();
        for _ in 0..10 {
            let rev = p.stats().resize_revision;
            p.set_workers(4, rev).unwrap();
            settled(&p, 4);
            p.notify(token).unwrap();
            let rev = p.stats().resize_revision;
            p.set_workers(1, rev).unwrap();
            settled(&p, 1);
            assert_eq!(p.stats().registered, 1);
        }
        assert_eq!(p.stats().unexpected_worker_exits, 0);
        assert!(count.load(Ordering::SeqCst) > 0);
        p.shutdown();
        assert!(p.stats().joined);
    }
    #[test]
    fn failed_staged_growth_preserves_original_pool() {
        let mut p = pool(2);
        lock(&p.shared).spawn_failure_after = Some(1);
        p.set_workers(4, 0).unwrap();
        settled(&p, 2);
        assert!(p.resize_error().is_some());
        assert_eq!(p.stats().unexpected_worker_exits, 0);
        p.shutdown();
    }
    #[test]
    fn shrink_waits_for_turns_and_shutdown_retains_ownership() {
        struct Block(mpsc::Sender<()>, mpsc::Receiver<()>);
        impl Processor for Block {
            fn run_turn(&mut self, _: usize) -> bool {
                self.0.send(()).unwrap();
                self.1.recv_timeout(Duration::from_secs(3)).unwrap();
                false
            }
        }
        let mut p = pool(2);
        let (entered, rx) = mpsc::channel();
        let mut releases = vec![];
        for n in 0..2 {
            let (tx, gate) = mpsc::channel();
            releases.push(tx);
            let token = p.fresh_token(n);
            p.register(token, Block(entered.clone(), gate)).unwrap();
            p.notify(token).unwrap();
        }
        for _ in 0..2 {
            rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        p.set_workers(1, 0).unwrap();
        assert!(p.set_workers(3, 0).is_err());
        assert!(p.stats().resize_pending);
        assert!(
            !p.shutdown_until(Instant::now() + Duration::from_millis(10))
                .complete
        );
        assert!(matches!(p.set_workers(3, 1), Err(PoolError::Shutdown)));
        for tx in releases {
            let _ = tx.send(());
        }
        assert!(
            p.shutdown_until(Instant::now() + Duration::from_secs(3))
                .complete
        );
    }
    #[test]
    fn retirement_during_turns_returns_state_and_execution_leases() {
        struct Stateful {
            runs: usize,
            entered: mpsc::Sender<()>,
            release: Option<mpsc::Receiver<()>>,
            done: mpsc::Sender<usize>,
        }
        impl Processor for Stateful {
            fn run_turn(&mut self, _: usize) -> bool {
                if let Some(rx) = self.release.take() {
                    self.entered.send(()).unwrap();
                    rx.recv_timeout(Duration::from_secs(3)).unwrap();
                }
                self.runs += 1;
                self.done.send(self.runs).unwrap();
                false
            }
        }
        let mut p = pool(2);
        let (entered, ready) = mpsc::channel();
        let (done, completed) = mpsc::channel();
        let mut releases = vec![];
        let mut tokens = vec![];
        for n in 0..2 {
            let (tx, rx) = mpsc::channel();
            releases.push(tx);
            let token = p.fresh_token(n);
            tokens.push(token);
            p.register(
                token,
                Stateful {
                    runs: 0,
                    entered: entered.clone(),
                    release: Some(rx),
                    done: done.clone(),
                },
            )
            .unwrap();
            p.notify(token).unwrap();
        }
        for _ in 0..2 {
            ready.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        p.set_workers(1, 0).unwrap();
        let until = Instant::now() + Duration::from_secs(2);
        while p.stats().retiring_workers == 0 {
            assert!(Instant::now() < until);
            thread::yield_now();
        }
        assert_eq!(p.stats().executing_processors, 2);
        assert_eq!(p.stats().registered, 2);
        for tx in releases {
            let _ = tx.send(());
        }
        settled(&p, 1);
        for _ in 0..2 {
            assert_eq!(completed.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
        }
        for token in tokens {
            p.notify(token).unwrap();
        }
        for _ in 0..2 {
            assert_eq!(completed.recv_timeout(Duration::from_secs(2)).unwrap(), 2);
        }
        assert_eq!(p.stats().registered, 2);
        assert_eq!(p.stats().unexpected_worker_exits, 0);
        p.shutdown();
    }
    #[test]
    fn growth_cannot_activate_workers_after_shutdown() {
        let mut p = pool(1);
        p.set_workers(8, 0).unwrap();
        p.begin_shutdown();
        assert!(
            p.shutdown_until(Instant::now() + Duration::from_secs(3))
                .complete
        );
        assert_eq!(p.stats().running_workers, 0);
        assert!(!p.stats().resize_pending);
        assert_eq!(p.stats().unexpected_worker_exits, 0);
    }
}
