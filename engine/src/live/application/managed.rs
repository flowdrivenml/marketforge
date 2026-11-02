//! Composition owner for the runtime, management service and HTTP listener.
use super::*;
use crate::live::{
    control::http::ManagementHttpServer,
    management::{Credentials, DefinitionStore, ManagementHandle, ManagementService},
};
use std::{path::PathBuf, sync::Arc};
pub struct ManagedLiveApplication {
    http: ManagementHttpServer,
    management: ManagementService,
    runtime: LiveApplication,
}
impl ManagedLiveApplication {
    pub async fn start(
        options: LiveApplicationOptions,
        store: Arc<dyn DefinitionStore>,
        credentials: Credentials,
        config_path: Option<PathBuf>,
    ) -> Result<Self, ApplicationError> {
        let address = options
            .config
            .control
            .listen_address
            .parse()
            .map_err(|_| ApplicationError("invalid management listener address".into()))?;
        let limits = options.config.control.management.clone();
        let mut runtime = LiveApplication::start(options).await?;
        let service = match config_path {
            Some(path) => {
                ManagementService::start_with_config(runtime.handle(), store, limits, path)
            }
            None => ManagementService::start(runtime.handle(), store, limits),
        };
        let mut management = match service {
            Ok(s) => s,
            Err(e) => {
                let report = runtime.shutdown().await;
                return Err(ApplicationError(format!(
                    "management initialization failed: {e}; cleanup complete={}",
                    report.complete
                )));
            }
        };
        match ManagementHttpServer::bind(address, management.handle(), credentials).await {
            Ok(http) => Ok(Self {
                http,
                management,
                runtime,
            }),
            Err(e) => {
                let _ = runtime.shutdown().await;
                let _ = management.shutdown().await;
                Err(ApplicationError(format!("management listener: {e}")))
            }
        }
    }
    pub fn handle(&self) -> LiveApplicationHandle {
        self.runtime.handle()
    }
    pub fn management(&self) -> ManagementHandle {
        self.management.handle()
    }
    pub fn management_address(&self) -> std::net::SocketAddr {
        self.http.address
    }
    /// A timeout retains every owner in this value. Observations remain available
    /// during unfinished runtime cleanup; retry after blocked turns can return.
    pub async fn shutdown(&mut self) -> ShutdownReport {
        let mut report = self.runtime.shutdown().await;
        if report.cleanup_continues {
            return report;
        }
        if let Err(e) = self.http.shutdown().await {
            report.complete = false;
            report.cleanup_continues = true;
            report.errors.push(format!("management HTTP cleanup: {e}"));
        }
        if !self.management.shutdown().await {
            report.complete = false;
            report.cleanup_continues = true;
            report
                .errors
                .push("accepted management work cleanup remains owned".into());
        }
        report
    }
}
