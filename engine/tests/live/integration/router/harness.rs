use marketforge_engine::{
    live::stream::{persistence::*, registry::*, request::*, router::*},
    shared::domain::canonical::*,
};
use std::sync::{Arc, Mutex};
pub const DEADLINE: std::time::Duration = std::time::Duration::from_secs(8);
pub fn request(profile: &str, channel: SourceChannel) -> CreateSourceStreamRequest {
    let data: serde_json::Value = serde_json::from_str(include_str!(
        "../../support/data/instruments/btc_sources.json"
    ))
    .unwrap();
    let mut value = data["instruments"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| {
            format!(
                "{}_{}",
                v["exchange"].as_str().unwrap(),
                v["market_category"].as_str().unwrap()
            ) == profile
        })
        .unwrap()
        .clone();
    for key in [
        "exchange_id",
        "tick_size",
        "qty_step",
        "min_qty",
        "max_qty",
        "min_notional",
    ] {
        value.as_object_mut().unwrap().remove(key);
    }
    value["channel"] = channel.as_str().into();
    serde_json::from_value(value).unwrap()
}
pub fn definition(profile: &str, channel: SourceChannel, id: &str) -> PersistedStreamDefinition {
    PersistedStreamDefinition {
        stream_id: id.into(),
        recording_stream_key: Some(1),
        definition: StreamDefinition::Source(request(profile, channel)),
        desired: DesiredStreamState::default(),
        revision: 1,
    }
}
pub struct Rig {
    pub router: StreamRouter,
    pub runtime: StreamRuntimeRegistry,
    pub context: RouterContext,
    pub producer: ProducerHandle,
}
impl Rig {
    pub fn new(channel: SourceChannel, config: RouterConfig) -> Self {
        let graph = Arc::new(
            StreamGraph::restore(vec![definition("binance_spot", channel, "test")], vec![])
                .unwrap(),
        );
        let mut runtime = StreamRuntimeRegistry::new(&graph);
        runtime.begin_start("test").unwrap();
        let context = RouterContext {
            graph,
            demand: Arc::new(Mutex::new(DemandRegistry::new())),
        };
        let router = StreamRouter::new(config, context.clone()).unwrap();
        let producer = router
            .attach_producer(AttachProducerRequest {
                authority: runtime.publication_authority("test").unwrap(),
            })
            .unwrap();
        Self {
            router,
            runtime,
            context,
            producer,
        }
    }
    pub async fn subscribe(&self, name: &str) -> SubscriptionHandle {
        self.router
            .subscribe(SubscribeRequest {
                stream_id: "test".into(),
                owner: owner(name),
            })
            .await
            .unwrap()
    }
}
pub fn owner(name: &str) -> DemandOwner {
    DemandOwner::Direct {
        consumer_id: "integration".into(),
        subscription_id: name.into(),
    }
}
pub fn trade(id: u64) -> Trade {
    Trade {
        envelope: EventEnvelope {
            event_timestamp_ns: id as i64,
            system_timestamp_ns: None,
            exchange: Exchange::Binance,
            instrument_id: request("binance_spot", SourceChannel::Trades).instrument_id,
            symbol: "BTCUSDT".into(),
            stream_id: "test".into(),
        },
        trade_id: Some(id.to_string()),
        sequence: Some(id),
        side: TradeSide::Buy,
        price: 80000.into(),
        quantity_base: Some(2.into()),
        quantity_quote: None,
        quantity_contracts: None,
        is_rpi: None,
        trade_iv: None,
        mark_iv: None,
        index_price: None,
        mark_price: None,
    }
}
pub fn publication(id: u64) -> RouterPublication {
    RouterPublication::trades(vec![trade(id)])
}
pub async fn receive(sub: &mut SubscriptionHandle) -> RouterDelivery {
    tokio::time::timeout(DEADLINE, sub.recv())
        .await
        .expect("delivery deadline")
        .expect("unexpected EOF")
}
pub fn records(delivery: RouterDelivery) -> (u64, Arc<RouterPublication>) {
    match delivery {
        RouterDelivery::Publication {
            sequence,
            publication,
            ..
        } => (sequence, publication),
        other => panic!("unexpected {other:?}"),
    }
}
pub async fn wait_dispatch(router: &StreamRouter) {
    tokio::time::timeout(DEADLINE, async {
        while router.status().ingress_publications != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
