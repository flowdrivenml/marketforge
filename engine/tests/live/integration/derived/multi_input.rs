use super::harness::*;
struct Collect {
    context: ProcessorContext,
    tx: tokio::sync::mpsc::Sender<ProcessingInput>,
}
impl Processor for Collect {
    fn run_turn(&mut self, max: usize) -> bool {
        let batch = self.context.input.take_work(max);
        for e in batch.iter() {
            assert!(
                std::thread::current()
                    .name()
                    .unwrap()
                    .starts_with("marketforge-cpu-")
            );
            self.tx.try_send(e.clone()).unwrap();
        }
        batch.complete();
        !self.context.input.is_empty()
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_inputs_fifo_identity_fairness_and_owned_cleanup() {
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b", "c"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        )],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let (tx, mut rx) = tokio::sync::mpsc::channel(128);
    let (constructed, ready) = tokio::sync::oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    struct Release(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }
    let mut guard = Release(Some(release));
    let spec = rig.spec("m", move |context| {
        let _ = constructed.send(());
        gate.recv_timeout(WAIT).unwrap();
        Ok(PreparedProcessor::ready(Collect { context, tx }))
    });
    let starter = handle.clone();
    let task = tokio::spawn(async move { starter.start(spec).await });
    bounded(ready).await.unwrap();
    for n in 1..=20 {
        publish(&rig.producers[0], publication("a", n, n as i64)).await;
    }
    publish(&rig.producers[1], publication("b", 1, 1)).await;
    publish(&rig.producers[2], publication("c", 1, 1)).await;
    bounded(async {
        while rig.router.status().ingress_publications != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    guard.0.take().unwrap().send(()).unwrap();
    let stream = bounded(task).await.unwrap().unwrap();
    let mut counts = [0; 3];
    let mut ordinal = 0;
    for delivered in 0..22 {
        let e = bounded(rx.recv()).await.unwrap();
        assert!(e.admission_ordinal > ordinal);
        ordinal = e.admission_ordinal;
        let i = e.input_position;
        if i != 0 {
            assert!(delivered < 3, "hot input starved rank {i}");
        }
        counts[i] += 1;
        assert_eq!(e.stream_id, ["a", "b", "c"][i]);
        let RouterDelivery::Publication {
            sequence,
            publication,
            runtime_generation,
            ..
        } = e.delivery
        else {
            panic!()
        };
        assert_eq!(sequence, counts[i]);
        assert_eq!(
            runtime_generation,
            rig.registry
                .lock()
                .unwrap()
                .status(&e.stream_id)
                .unwrap()
                .generation
        );
        assert!(
            matches!(publication.as_ref(),RouterPublication::Trades{records,..} if records[0].sequence==Some(sequence))
        );
    }
    assert_eq!(counts, [20, 1, 1]);
    bounded(handle.stop(&stream)).await.unwrap();
    let report = handle.take_report("m", stream.generation).unwrap();
    report.discard();
    assert_eq!(rig.router.status().subscriptions, 0);
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_attachment_failure_rolls_back_earlier_subscriptions() {
    let mut config = RouterConfig::default();
    config.max_subscriptions = 2;
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b", "c"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        )],
        config,
    );
    assert!(
        bounded(
            rig.service
                .handle()
                .start(rig.spec("m", |_| panic!("must not construct")))
        )
        .await
        .is_err()
    );
    assert_eq!(rig.router.status().subscriptions, 0);
    assert!(
        rig.registry
            .lock()
            .unwrap()
            .publication_authority("m")
            .is_none()
    );
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_multi_input_start_releases_all_owned_inputs() {
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b", "c"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        )],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let (tx, ready) = tokio::sync::oneshot::channel();
    let (events, _) = tokio::sync::mpsc::channel(16);
    let spec = rig.spec("m", move |context| {
        let _ = tx.send(());
        Ok(PreparedProcessor {
            processor: Box::new(Collect {
                context,
                tx: events,
            }),
            readiness: Box::pin(std::future::pending()),
        })
    });
    let starter = handle.clone();
    let task = tokio::spawn(async move { starter.start(spec).await });
    bounded(ready).await.unwrap();
    let g = handle.status().associations[0].generation;
    task.abort();
    let _ = task.await;
    bounded(handle.wait_terminal("m", g)).await.unwrap();
    handle.take_report("m", g).unwrap().discard();
    assert_eq!(rig.router.status().subscriptions, 0);
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_input_restart_fences_generation_without_rebinding_state() {
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b", "c"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        )],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let (tx, _) = tokio::sync::mpsc::channel(16);
    let stream = bounded(handle.start(rig.spec("m", move |context| {
        Ok(PreparedProcessor::ready(Collect { context, tx }))
    })))
    .await
    .unwrap();
    {
        let mut r = rig.registry.lock().unwrap();
        let g = r.begin_stop("b").unwrap();
        r.mark_stopped("b", g).unwrap();
        let g = r.begin_start("b").unwrap();
        r.mark_running("b", g).unwrap();
    }
    let terminal = bounded(handle.wait_terminal("m", stream.generation))
        .await
        .unwrap();
    assert!(terminal.reason.is_some());
    assert!(stream.admission_closed());
    handle
        .take_report("m", stream.generation)
        .unwrap()
        .discard();
    assert_eq!(rig.router.status().subscriptions, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_roles_positions_and_undeclared_inputs_never_attach() {
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b", "c"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        )],
        RouterConfig::default(),
    );
    for mode in 0..4 {
        let mut spec = rig.spec("m", |_| panic!("invalid request cannot construct"));
        match mode {
            0 => spec.inputs[0].role = InputRole::Reference,
            1 => spec.inputs[0].position = 2,
            2 => spec.inputs[0].stream_id = "other".into(),
            _ => spec.inputs[1] = spec.inputs[0].clone(),
        };
        assert!(bounded(rig.service.handle().start(spec)).await.is_err());
        assert_eq!(rig.router.status().subscriptions, 0);
    }
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_processor_preserves_independent_dependency_demand() {
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        )],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let mut independent = subscribe(&rig.router, "a").await;
    let stream = bounded(handle.start_derived("m")).await.unwrap();
    bounded(handle.stop(&stream)).await.unwrap();
    handle
        .take_report("m", stream.generation)
        .unwrap()
        .discard();
    assert_eq!(rig.router.status().subscriptions, 1);
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    assert!(matches!(
        bounded(independent.recv()).await,
        Some(RouterDelivery::Publication { .. })
    ));
    drop(independent);
    assert_eq!(rig.router.status().subscriptions, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_owns_processing_publication_sequence_and_admission_ordinals() {
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        )],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let stream = bounded(handle.start(rig.spec("m", move |context| {
        Ok(PreparedProcessor::ready(Collect { context, tx }))
    })))
    .await
    .unwrap();
    // Exchange record identifiers are distinct from Router custody sequences.
    for (index, native_sequence) in [100, 100, 2, 200].into_iter().enumerate() {
        publish(
            &rig.producers[0],
            publication("a", native_sequence, index as i64),
        )
        .await;
        let actual = bounded(rx.recv()).await.unwrap();
        assert_eq!(actual.admission_ordinal, index as u64 + 1);
        let RouterDelivery::Publication {
            sequence,
            publication,
            admission,
            ..
        } = actual.delivery
        else {
            panic!()
        };
        assert_eq!(sequence, index as u64 + 1);
        assert_eq!(admission.sequence(), sequence);
        assert!(
            matches!(publication.as_ref(),RouterPublication::Trades{records,..} if records[0].sequence==Some(native_sequence))
        );
    }
    bounded(handle.stop(&stream)).await.unwrap();
    assert!(stream.admission_closed());
    let report = handle.take_report("m", stream.generation).unwrap();
    assert_eq!(report.input_accounting.completed_events, 4);
    report.discard();
    assert_eq!(handle.status().accounted_bytes, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn required_upstream_failure_fences_the_whole_derived_chain() {
    let rig = Rig::new(
        vec![
            derived(
                "m",
                DerivedOperation::Merge,
                &["a", "b"],
                &[],
                Some(MergeOrdering::ArrivalOrder),
            ),
            derived("v", DerivedOperation::Convert, &["m"], &[], None),
        ],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let mut sub = subscribe(&rig.router, "v").await;
    let m = bounded(handle.start_derived("m")).await.unwrap();
    let v = bounded(handle.start_derived("v")).await.unwrap();
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    assert!(matches!(
        bounded(sub.recv()).await,
        Some(RouterDelivery::Publication { .. })
    ));
    {
        let mut r = rig.registry.lock().unwrap();
        let g = r.status("b").unwrap().generation;
        r.mark_failed("b", g, "synthetic upstream continuity failure")
            .unwrap();
    }
    let m_terminal = bounded(handle.wait_terminal("m", m.generation))
        .await
        .unwrap();
    let v_terminal = bounded(handle.wait_terminal("v", v.generation))
        .await
        .unwrap();
    assert!(m_terminal.reason.unwrap().contains("b"));
    assert!(v_terminal.reason.unwrap().contains("m"));
    assert!(
        rig.registry
            .lock()
            .unwrap()
            .publication_authority("v")
            .is_none()
    );
    handle.take_report("v", v.generation).unwrap().discard();
    handle.take_report("m", m.generation).unwrap().discard();
    drop(sub);
    assert_eq!(rig.router.status().subscriptions, 0);
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
