use tokio::net::TcpListener;

pub(crate) async fn listen() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}
