use crate::{
    client::management::{MarketForgeManagementClient, Notification},
    management_protocol::*,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
#[derive(Debug)]
pub enum Intent {
    Preview(Action),
    Apply(ChangePlan, String),
    Acquire(LeaseRequest),
    Release(String),
    Integrity(String),
    Refresh,
    Metadata(String),
}
#[derive(Debug)]
pub enum Update {
    Event(ManagementEvent),
    Overview(Overview),
    Disconnected,
    Error(String),
    Plan(ChangePlan),
    Operation(Operation),
    Capabilities(Capabilities),
    Settings(Vec<SettingView>),
    Clients(serde_json::Value),
    Integrity(serde_json::Value),
    Lease(DemandLease),
    Released(String),
}
/// Small management-access boundary; only the HTTP-backed implementation is provided.
pub struct HttpBackend {
    commands: mpsc::Sender<Intent>,
    updates: mpsc::Receiver<Update>,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl HttpBackend {
    pub fn start(client: MarketForgeManagementClient) -> Self {
        let (commands, mut rx) = mpsc::channel(16);
        let (tx, updates) = mpsc::channel(32);
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut feed = client.notifications();
            let mut leases = Vec::<String>::new();
            let mut renew = tokio::time::interval(Duration::from_secs(20));
            let mut jobs = tokio::task::JoinSet::new();
            let initial = client.clone();
            let sink = tx.clone();
            jobs.spawn(async move {
                if let Ok(mut c) = initial.capabilities().await {
                    if let Ok(templates) = initial.source_templates("").await {
                        c.source_templates = templates;
                    }
                    let _ = sink.send(Update::Capabilities(c)).await;
                }
                if let Ok(c) = initial.config().await {
                    let _ = sink.send(Update::Settings(c)).await;
                }
                if let Ok(c) = initial.clients().await {
                    let _ = sink.send(Update::Clients(c)).await;
                }
                None::<Update>
            });
            loop {
                tokio::select! {
                    _=&mut stopped=>break,
                    command=rx.recv()=>{
                        let Some(command)=command else{break};if jobs.len()>=8{let _=tx.try_send(Update::Error("UI request capacity reached; retry deliberately".into()));continue;}
                        let client=client.clone();jobs.spawn(async move{let result=match command{
                            Intent::Preview(a)=>client.preview(a).await.map(Update::Plan),Intent::Apply(p,k)=>client.apply(&p.id,&k).await.map(Update::Operation),Intent::Acquire(r)=>client.acquire_lease(r).await.map(Update::Lease),Intent::Release(id)=>client.release_lease(&id).await.map(|_|Update::Released(id)),Intent::Integrity(id)=>client.integrity(&id).await.map(Update::Integrity),Intent::Refresh=>client.overview().await.map(Update::Overview),Intent::Metadata(filter)=>match client.capabilities().await{Ok(mut c)=>client.source_templates(&filter).await.map(|templates|{c.source_templates=templates;Update::Capabilities(c)}),Err(e)=>Err(e)},
                        };Some(result.unwrap_or_else(|e|Update::Error(e.to_string())))});
                    },
                    result=jobs.join_next(),if !jobs.is_empty()=>{if let Some(Ok(Some(update)))=result{match &update{Update::Lease(l)=>{if !leases.contains(&l.id){leases.push(l.id.clone());}},Update::Released(id)=>leases.retain(|l|l!=id),_=>()};if tx.send(update).await.is_err(){break;}}},
                    message=feed.recv()=>match message{
                        Some(Ok(Notification::Snapshot(v)))=>{if tx.send(Update::Overview(v)).await.is_err(){break;}},
                        Some(Ok(Notification::Event(event)))=>{let _=tx.try_send(Update::Event(event));if jobs.len()<8{let client=client.clone();jobs.spawn(async move{Some(match client.overview().await{Ok(v)=>Update::Overview(v),Err(_)=>Update::Disconnected})});}},
                        Some(Ok(Notification::Disconnected))|Some(Err(_))=>{let _=tx.try_send(Update::Disconnected);},None=>{let _=tx.try_send(Update::Disconnected);tokio::time::sleep(Duration::from_millis(100)).await;feed=client.notifications();},
                    },
                    _=renew.tick()=>{for id in &leases{if let Err(e)=client.renew_lease(id,60).await{let _=tx.try_send(Update::Error(format!("Lease renewal failed; expiry may release this UI hold: {e}")));}}}
                }
            }
            feed.close().await;
            jobs.abort_all();
            while jobs.join_next().await.is_some() {}
            let cleanup = async {
                for id in &leases {
                    let _ = client.release_lease(id).await;
                }
            };
            let _ = tokio::time::timeout(Duration::from_secs(2), cleanup).await;
        });
        Self {
            commands,
            updates,
            stop: Some(stop),
            task: Some(task),
        }
    }
    pub fn send(&self, intent: Intent) -> std::result::Result<(), String> {
        self.commands
            .try_send(intent)
            .map_err(|_| "UI command capacity or closed backend".into())
    }
    pub async fn recv(&mut self) -> Option<Update> {
        self.updates.recv().await
    }
    pub async fn close(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for HttpBackend {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
