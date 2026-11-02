#[cfg(unix)]
use marketforge_engine::{client::streaming::*, stream_protocol::StreamMessage};
#[cfg(unix)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let socket = args
        .next()
        .ok_or("usage: stream-consumer SOCKET STREAM_ID")?;
    let stream = args.next().ok_or("missing stream ID")?;
    let client = MarketForgeStreamClient::connect_unix(socket, ClientOptions::default()).await?;
    let mut subscription = client.subscribe(&stream).await?;
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            message=subscription.recv()=>match message? {
                Some(StreamMessage::Publication(p))=>println!("{} generation={} publication={} records={}",p.stream_id,p.runtime_generation,p.router_sequence,p.records.len()),
                Some(StreamMessage::IntegritySnapshot(s))=>for e in s.sources {println!("integrity {:?} coverage={} observations={:?}",e.id,e.coverage,e.observations);},
                Some(StreamMessage::IntegrityUpdate(u))=>println!("integrity {:?}: {:?}",u.evidence.id,u.evidence.observations),
                Some(StreamMessage::End(e))=>{println!("subscription ended: {}",e.reason);break;},
                None=>break,
            }
        }
    }
    subscription.close().await?;
    client.close().await?;
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("The streaming connector requires Unix.");
}
