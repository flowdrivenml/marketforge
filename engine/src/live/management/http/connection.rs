//! Bounded HTTP connection ownership and socket progress deadlines.
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Sleep,
};
pub(super) struct LimitedListener {
    pub listener: TcpListener,
    pub permits: Arc<Semaphore>,
    pub timeout: Duration,
}
pub(super) struct Connection {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
    timeout: Duration,
    read: Option<Pin<Box<Sleep>>>,
    write: Option<Pin<Box<Sleep>>>,
}
impl axum::serve::Listener for LimitedListener {
    type Io = Connection;
    type Addr = std::net::SocketAddr;
    async fn accept(&mut self) -> (Connection, Self::Addr) {
        loop {
            match self.listener.accept().await {
                Ok((stream, address)) => {
                    if let Ok(permit) = self.permits.clone().try_acquire_owned() {
                        let _ = stream.set_nodelay(true);
                        // Administration has bounded responses; cap the kernel
                        // send buffer as well so non-readers reach a deadline.
                        use std::os::fd::AsRawFd;
                        let bytes: libc::c_int = 65536;
                        let configured = unsafe {
                            libc::setsockopt(
                                stream.as_raw_fd(),
                                libc::SOL_SOCKET,
                                libc::SO_SNDBUF,
                                &bytes as *const _ as *const libc::c_void,
                                std::mem::size_of_val(&bytes) as libc::socklen_t,
                            )
                        };
                        if configured != 0 {
                            continue;
                        }

                        return (
                            Connection {
                                stream,
                                _permit: permit,
                                timeout: self.timeout,
                                read: Some(Box::pin(tokio::time::sleep(self.timeout))),
                                write: None,
                            },
                            address,
                        );
                    }
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    }
    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}
fn deadline(timer: &mut Option<Pin<Box<Sleep>>>, timeout: Duration, cx: &mut Context<'_>) -> bool {
    timer
        .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)))
        .as_mut()
        .poll(cx)
        .is_ready()
}
impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let timeout = self.timeout;
        if deadline(&mut self.read, timeout, cx) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP read deadline",
            )));
        }
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}
impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.stream).poll_write(cx, bytes) {
            Poll::Ready(result) => {
                self.write = None;
                self.read = None;
                Poll::Ready(result)
            }
            Poll::Pending => {
                let timeout = self.timeout;
                if deadline(&mut self.write, timeout, cx) {
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "HTTP write progress deadline",
                    )))
                } else {
                    Poll::Pending
                }
            }
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.stream).poll_flush(cx) {
            Poll::Ready(result) => {
                self.write = None;
                Poll::Ready(result)
            }
            Poll::Pending => {
                let timeout = self.timeout;
                if deadline(&mut self.write, timeout, cx) {
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "HTTP flush deadline",
                    )))
                } else {
                    Poll::Pending
                }
            }
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
