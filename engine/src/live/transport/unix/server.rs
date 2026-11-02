use super::connection;
use crate::live::{config::general::PublicationConfig, stream::router::StreamRouter};
use std::{
    io,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    net::UnixListener,
    sync::{Semaphore, oneshot},
    task::{JoinHandle, JoinSet},
};
static CONNECTION: AtomicU64 = AtomicU64::new(1);
struct SocketGuard {
    path: PathBuf,
    dev: u64,
    ino: u64,
}
impl Drop for SocketGuard {
    fn drop(&mut self) {
        if std::fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.dev() == self.dev && m.ino() == self.ino)
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
/// Owns only its socket instance and transport tasks. Never starts logical streams.
pub struct UnixStreamingServer {
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
}
impl UnixStreamingServer {
    pub async fn bind(router: Arc<StreamRouter>, config: PublicationConfig) -> io::Result<Self> {
        config.validate_streaming().map_err(io::Error::other)?;
        let configured = Path::new(&config.socket_path);
        let absolute_path = if configured.is_absolute() {
            configured.to_path_buf()
        } else {
            std::env::current_dir()?.join(configured)
        };
        let path = absolute_path.as_path();
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .ok_or_else(|| io::Error::other("socket requires parent directory"))?;
        let metadata = std::fs::symlink_metadata(parent)?;
        if !metadata.is_dir() || metadata.permissions().mode() & 0o022 != 0 {
            return Err(io::Error::other(
                "socket parent must be a private, non-symlink directory",
            ));
        }
        // Reject every existing endpoint, including stale sockets; never unlink on startup.
        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "socket path exists",
                ));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(e),
        }
        let parent_owner = metadata.uid();
        let listener = UnixListener::bind(path)?;
        let metadata = std::fs::symlink_metadata(path)?;
        let guard = SocketGuard {
            path: path.into(),
            dev: metadata.dev(),
            ino: metadata.ino(),
        };
        if parent_owner != metadata.uid() {
            return Err(io::Error::other(
                "socket parent must belong to the server user",
            ));
        }
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = guard;
            let permits = Arc::new(Semaphore::new(config.max_connections));
            let mut sessions = JoinSet::new();
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            loop {
                tokio::select! {
                    _=&mut stopped=>break,
                    result=listener.accept()=> {
                        let (socket,_)=result?;
                        if sessions.len()>=config.max_connections {continue;}
                        if let Ok(permit)=permits.clone().try_acquire_owned() {
                            let id=CONNECTION.fetch_update(Ordering::Relaxed,Ordering::Relaxed,|n|n.checked_add(1)).map_err(|_|io::Error::other("session identity exhausted"))?;
                            let router=router.clone(); let config=config.clone();let shutdown=shutdown_rx.clone();
                            sessions.spawn(async move {let _permit=permit; connection::serve(socket,router,config,id,shutdown).await;});
                        }
                    },
                    _=sessions.join_next(), if !sessions.is_empty()=>(),
                }
            }
            let _ = shutdown_tx.send(true);
            let timeout = std::time::Duration::from_millis(config.streaming.shutdown_timeout_ms);
            if tokio::time::timeout(timeout, async {
                while sessions.join_next().await.is_some() {}
            })
            .await
            .is_err()
            {
                sessions.abort_all();
                while sessions.join_next().await.is_some() {}
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "transport shutdown deadline",
                ));
            }
            Ok(())
        });
        Ok(Self {
            stop: Some(stop),
            task: Some(task),
        })
    }
    pub async fn shutdown(mut self) -> io::Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.map_err(io::Error::other)??;
        }
        Ok(())
    }
}
impl Drop for UnixStreamingServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
