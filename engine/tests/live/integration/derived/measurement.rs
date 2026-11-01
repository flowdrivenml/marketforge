//! Small opt-in local measurement; no datasets, database, or exchange traffic.
use super::harness::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit local throughput measurement, not a correctness gate"]
async fn real_router_merge_throughput() {
    println!("mode,inputs,total_events,events_per_second,wall_ns_per_event");
    for ordering in [MergeOrdering::ArrivalOrder, MergeOrdering::EventTime] {
        for fan_in in [2, 4, 16] {
            for total in [32, 512, 2048] {
                let ids = (0..fan_in)
                    .map(|i| format!("source_{i:02}"))
                    .collect::<Vec<_>>();
                let mut defs = ids
                    .iter()
                    .enumerate()
                    .map(|(i, id)| {
                        let mut d = source::definition("binance_spot", SourceChannel::Trades, id);
                        d.recording_stream_key = Some(i as i64 + 1);
                        d
                    })
                    .collect::<Vec<_>>();
                defs.push(derived(
                    "m",
                    DerivedOperation::Merge,
                    &ids.iter().map(String::as_str).collect::<Vec<_>>(),
                    &[],
                    Some(ordering),
                ));
                let rig = Rig::replay(defs);
                let handle = rig.service.handle();
                let mut sub = subscribe(&rig.router, "m").await;
                let stream = bounded(handle.start_derived("m")).await.unwrap();
                let start = std::time::Instant::now();
                let feeding = async {
                    for ordinal in 0..total {
                        let rank = ordinal % fan_in;
                        publish(
                            &rig.producers[rank],
                            publication(
                                &ids[rank],
                                (ordinal / fan_in + 1) as u64,
                                ordinal as i64 * 100,
                            ),
                        )
                        .await;
                    }
                    for producer in &rig.producers {
                        publish(producer, RouterPublication::ReplayComplete).await;
                    }
                };
                let receiving = async {
                    let mut received = 0;
                    loop {
                        match bounded(sub.recv()).await {
                            Some(RouterDelivery::Publication { publication, .. }) => {
                                match publication.as_ref() {
                                    RouterPublication::Derived(events) => received += events.len(),
                                    RouterPublication::ReplayComplete => break,
                                    other => panic!("{other:?}"),
                                }
                            }
                            other => panic!("{other:?}; {:?}", handle.status()),
                        }
                    }
                    assert_eq!(received, total);
                };
                bounded(async {
                    tokio::join!(feeding, receiving);
                })
                .await;
                let nanos = start.elapsed().as_nanos();
                println!(
                    "{ordering:?},{fan_in},{total},{:.0},{}",
                    total as f64 * 1e9 / nanos as f64,
                    nanos / total as u128
                );
                bounded(handle.stop(&stream)).await.unwrap();
                handle
                    .take_report("m", stream.generation)
                    .unwrap()
                    .discard();
                assert_eq!(handle.status().accounted_events, 0);
                drop(sub);
                rig.close().await;
            }
        }
    }
}
