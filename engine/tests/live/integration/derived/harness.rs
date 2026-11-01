#![allow(dead_code)]
pub use marketforge_engine::live::{
    config::*,
    infrastructure::processing::*,
    stream::{persistence::*, processing::*, registry::*, request::*, router::*},
};
pub use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[path = "../router/harness.rs"]
pub mod source;
pub const WAIT: Duration = Duration::from_secs(5);
pub async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, f)
        .await
        .expect("derived deadline")
}
pub fn derived(
    id: &str,
    operation: DerivedOperation,
    inputs: &[&str],
    references: &[&str],
    ordering: Option<MergeOrdering>,
) -> PersistedStreamDefinition {
    PersistedStreamDefinition {
        stream_id: id.into(),
        recording_stream_key: None,
        definition: StreamDefinition::Derived(CreateDerivedStreamRequest {
            name: id.into(),
            operation,
            inputs: inputs.iter().map(|s| s.to_string()).collect(),
            reference_inputs: references.iter().map(|s| s.to_string()).collect(),
            conversion: (operation == DerivedOperation::Convert).then_some(ConversionSettings {
                max_lateness_ns: 10,
                ..Default::default()
            }),
            ordering,
            max_lateness_ns: ordering
                .filter(|o| *o == MergeOrdering::EventTime)
                .map(|_| 10),
            target_quote_asset: if operation == DerivedOperation::Convert {
                Some("USDT".into())
            } else {
                None
            },
        }),
        desired: DesiredStreamState::default(),
        revision: 1,
    }
}
pub struct Rig {
    pub router: Arc<StreamRouter>,
    pub registry: Arc<Mutex<StreamRuntimeRegistry>>,
    pub service: ProcessingService,
    pub producers: Vec<ProducerHandle>,
}
impl Rig {
    pub fn new(extra: Vec<PersistedStreamDefinition>, config: RouterConfig) -> Self {
        let mut defs = vec![];
        for (i, id) in ["a", "b", "c"].iter().enumerate() {
            let mut d = source::definition("binance_spot", SourceChannel::Trades, id);
            d.recording_stream_key = Some(i as i64 + 1);
            defs.push(d);
        }
        defs.extend(extra);
        Self::definitions(defs, config)
    }
    pub fn definitions(defs: Vec<PersistedStreamDefinition>, config: RouterConfig) -> Self {
        Self::configured(defs, config, Default::default())
    }
    pub fn configured(
        defs: Vec<PersistedStreamDefinition>,
        config: RouterConfig,
        limits: general::DerivedProcessingConfig,
    ) -> Self {
        Self::backend(defs, config, limits, false)
    }
    pub fn replay(defs: Vec<PersistedStreamDefinition>) -> Self {
        Self::backend(defs, RouterConfig::default(), Default::default(), true)
    }
    fn backend(
        defs: Vec<PersistedStreamDefinition>,
        config: RouterConfig,
        limits: general::DerivedProcessingConfig,
        finite: bool,
    ) -> Self {
        let edges = defs
            .iter()
            .flat_map(|d| match &d.definition {
                StreamDefinition::Derived(r) => r
                    .input_specs()
                    .into_iter()
                    .map(|s| StreamDependency {
                        derived_stream_id: d.stream_id.clone(),
                        input_stream_id: s.stream_id,
                        input_position: s.position as i32,
                    })
                    .collect(),
                _ => vec![],
            })
            .collect();
        let graph = Arc::new(StreamGraph::restore(defs, edges).unwrap());
        let mut registry = StreamRuntimeRegistry::new(&graph);
        let router = Arc::new(
            StreamRouter::new(
                config,
                RouterContext {
                    graph: graph.clone(),
                    demand: Arc::new(Mutex::new(DemandRegistry::new())),
                },
            )
            .unwrap(),
        );
        let producers = graph
            .stream_ids()
            .filter(|id| {
                matches!(
                    graph.get(id).unwrap().definition,
                    StreamDefinition::Source(_)
                )
            })
            .map(|id| {
                let g = registry.begin_start(id).unwrap();
                registry.mark_running(id, g).unwrap();
                if finite {
                    router
                        .attach_replay_producer(AttachProducerRequest {
                            authority: registry.publication_authority(id).unwrap(),
                        })
                        .unwrap()
                } else {
                    router
                        .attach_producer(AttachProducerRequest {
                            authority: registry.publication_authority(id).unwrap(),
                        })
                        .unwrap()
                }
            })
            .collect();
        let registry = Arc::new(Mutex::new(registry));
        let service = ProcessingService::new(
            &RuntimeConfig {
                shutdown_timeout_seconds: 1,
                processing: Some(ProcessingConfig {
                    workers: 2,
                    ready_queue_capacity: 16,
                    derived: limits,
                    ..Default::default()
                }),
            },
            registry.clone(),
            router.clone(),
        )
        .unwrap();
        Self {
            router,
            registry,
            service,
            producers,
        }
    }
    pub fn spec(
        &self,
        id: &str,
        factory: impl FnOnce(ProcessorContext) -> Result<PreparedProcessor, String> + Send + 'static,
    ) -> StartProcessor {
        StartProcessor {
            output_semantics: PublicationSemantics::Derived,
            stream_id: id.into(),
            inputs: self.router.processing_inputs(id).unwrap(),
            input_limits: MailboxLimits {
                max_events: 128,
                max_bytes: 1 << 20,
            },
            output_limits: MailboxLimits {
                max_events: 128,
                max_bytes: 1 << 20,
            },
            factory: Box::new(factory),
        }
    }
    pub async fn close(mut self) {
        assert!(bounded(self.service.shutdown()).await.unwrap().complete);
        self.router.shutdown().await.unwrap();
    }
}
pub fn publication(id: &str, sequence: u64, time: i64) -> RouterPublication {
    let mut t = source::trade(sequence);
    t.envelope.stream_id = id.into();
    t.envelope.event_timestamp_ns = time;
    RouterPublication::trades(vec![t])
}
pub async fn publish(p: &ProducerHandle, value: RouterPublication) {
    bounded(async {
        loop {
            match p.try_publish(value.clone()) {
                Ok(_) => break,
                Err(PublishError::Busy) => tokio::task::yield_now().await,
                Err(e) => panic!("publish {e:?}"),
            }
        }
    })
    .await
}
pub async fn subscribe(r: &StreamRouter, id: &str) -> SubscriptionHandle {
    r.subscribe(SubscribeRequest {
        stream_id: id.into(),
        owner: source::owner(id),
    })
    .await
    .unwrap()
}
