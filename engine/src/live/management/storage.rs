use crate::live::stream::persistence::StreamDefinitionRepository;
use crate::management_protocol::{definitions::*, *};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
pub type StoreFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;
/// Production uses PostgreSQL. Test/embedding stores must declare durability.
pub trait DefinitionStore: Send + Sync + 'static {
    fn durable(&self) -> bool;
    fn templates(
        &self,
        _filter: String,
        _limit: usize,
    ) -> StoreFuture<Vec<CreateSourceStreamRequest>> {
        Box::pin(async { Ok(vec![]) })
    }
    fn save(
        &self,
        previous: Option<PersistedStreamDefinition>,
        next: Option<PersistedStreamDefinition>,
    ) -> StoreFuture<Option<PersistedStreamDefinition>>;
}
#[derive(Clone)]
pub struct PostgresDefinitionStore(pub StreamDefinitionRepository);
impl DefinitionStore for PostgresDefinitionStore {
    fn templates(
        &self,
        filter: String,
        limit: usize,
    ) -> StoreFuture<Vec<CreateSourceStreamRequest>> {
        let repo = self.0.clone();
        Box::pin(async move {
            repo.management_templates(filter, limit).await.map_err(|_| {
                super::error(
                    "metadata_unavailable",
                    "catalog lookup failed or unsupported metadata",
                )
            })
        })
    }
    fn durable(&self) -> bool {
        true
    }
    fn save(
        &self,
        previous: Option<PersistedStreamDefinition>,
        next: Option<PersistedStreamDefinition>,
    ) -> StoreFuture<Option<PersistedStreamDefinition>> {
        let repo = self.0.clone();
        Box::pin(async move {
            repo.management_save(previous, next).await.map_err(|e| {
                use crate::live::stream::persistence::StreamDefinitionError as E;
                match e {
                    E::RevisionConflict(_) => {
                        super::error("revision_conflict", "durable definition changed")
                    }
                    E::AlreadyExists(_) => {
                        super::error("already_exists", "durable definition already exists")
                    }
                    E::NotFound(_) => super::error("not_found", "durable definition missing"),
                    E::HasDependents(_) => {
                        super::error("has_dependents", "durable dependent definitions exist")
                    }
                    E::RegistrationMismatch(_) => super::error(
                        "source_registration_mismatch",
                        "Source identity does not match its persistent instrument registration",
                    ),
                    E::Invalid(detail) => super::error("invalid_definition", &detail),
                    _ => super::error(
                        "persistence_failed",
                        "definition transaction failed; database details redacted",
                    ),
                }
            })
        })
    }
}
/// Explicit non-durable embedding/failure-injection store, never a daemon fallback.
#[derive(Clone)]
pub struct MemoryDefinitionStore {
    values: Arc<Mutex<Vec<PersistedStreamDefinition>>>,
    pub fail_next: Arc<std::sync::atomic::AtomicBool>,
}
impl MemoryDefinitionStore {
    pub fn new(values: Vec<PersistedStreamDefinition>) -> Self {
        Self {
            values: Arc::new(Mutex::new(values)),
            fail_next: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
    pub fn snapshot(&self) -> Vec<PersistedStreamDefinition> {
        self.values.lock().unwrap().clone()
    }
}
impl DefinitionStore for MemoryDefinitionStore {
    fn templates(
        &self,
        filter: String,
        limit: usize,
    ) -> StoreFuture<Vec<CreateSourceStreamRequest>> {
        let values = self.snapshot();
        Box::pin(async move {
            Ok(values
                .into_iter()
                .filter_map(|d| match d.definition {
                    StreamDefinition::Source(r) if r.symbol.contains(&filter) => Some(r),
                    _ => None,
                })
                .take(limit)
                .collect())
        })
    }
    fn durable(&self) -> bool {
        false
    }
    fn save(
        &self,
        previous: Option<PersistedStreamDefinition>,
        next: Option<PersistedStreamDefinition>,
    ) -> StoreFuture<Option<PersistedStreamDefinition>> {
        let this = self.clone();
        Box::pin(async move {
            if this
                .fail_next
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return Err(super::error(
                    "persistence_failed",
                    "injected storage failure",
                ));
            }
            let id = previous
                .as_ref()
                .or(next.as_ref())
                .ok_or_else(|| super::error("invalid", "empty storage change"))?
                .stream_id
                .clone();
            let mut values = this.values.lock().unwrap();
            let current = values.iter().find(|d| d.stream_id == id);
            if current != previous.as_ref() {
                return Err(super::error(
                    "revision_conflict",
                    "storage definition changed",
                ));
            }
            let next = next.map(|mut d| {
                if d.recording_stream_key == Some(-1) {
                    d.recording_stream_key = Some(
                        values
                            .iter()
                            .filter_map(|d| d.recording_stream_key)
                            .max()
                            .unwrap_or(0)
                            + 1,
                    );
                }
                d
            });
            values.retain(|d| d.stream_id != id);
            if let Some(d) = &next {
                values.push(d.clone());
            }
            Ok(next)
        })
    }
}

pub struct UnavailableDefinitionStore;
impl DefinitionStore for UnavailableDefinitionStore {
    fn durable(&self) -> bool {
        false
    }
    fn save(
        &self,
        _: Option<PersistedStreamDefinition>,
        _: Option<PersistedStreamDefinition>,
    ) -> StoreFuture<Option<PersistedStreamDefinition>> {
        Box::pin(async {
            Err(super::error(
                "persistence_unavailable",
                "diagnostic mode has no definition persistence; no in-memory fallback",
            ))
        })
    }
}
