use crate::live::{config::general::PublicationConfig, stream::router::StreamRouter};
use std::sync::Arc;
pub(super) async fn serve(
    socket: tokio::net::UnixStream,
    router: Arc<StreamRouter>,
    config: PublicationConfig,
    id: u64,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let (r, w) = socket.into_split();
    super::super::session::serve(r, w, router, config, id, shutdown).await;
}
