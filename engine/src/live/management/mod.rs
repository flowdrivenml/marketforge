//! Authoritative administrative service over the existing application owner.
mod auth;
#[cfg(feature = "management-api")]
mod config_file;
mod planning;
mod storage;
mod views;
use crate::{
    live::{
        application::LiveApplicationHandle, config::general::ManagementConfig, stream::registry::*,
    },
    management_protocol::{definitions::*, *},
};
pub use auth::*;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
pub use storage::*;
use tokio::{
    sync::{Notify, mpsc},
    task::{JoinHandle, JoinSet},
};
static CORRELATION: AtomicU64 = AtomicU64::new(1);
pub(crate) fn error(code: &str, detail: &str) -> ManagementError {
    ManagementError {
        code: code.into(),
        detail: detail
            .chars()
            .filter(|c| !c.is_control())
            .take(512)
            .collect(),
        correlation_id: CORRELATION.fetch_add(1, Ordering::Relaxed).to_string(),
    }
}
struct PlanRecord {
    view: ChangePlan,
    owner: String,
    created: Instant,
}
struct LeaseRecord {
    renewals: VecDeque<(String, u64)>,
    view: DemandLease,
    expires: Instant,
    releasing: bool,
}
struct State {
    next: u64,
    plans: BTreeMap<String, PlanRecord>,
    operations: BTreeMap<String, Operation>,
    order: VecDeque<String>,
    keys: BTreeMap<(String, String), (String, Action, String)>,
    leases: BTreeMap<String, LeaseRecord>,
    lease_keys: BTreeMap<(String, String), (LeaseRequest, String)>,
    events: VecDeque<ManagementEvent>,
    cursor: u64,
    config_revision: u64,
    saved_workers: Option<usize>,
    saved_observation_interval: Option<u64>,
}
struct Shared {
    app: LiveApplicationHandle,
    store: Arc<dyn DefinitionStore>,
    limits: ManagementConfig,
    instance: String,
    state: Mutex<State>,
    closed: AtomicBool,
    changed: Notify,
    mutation: tokio::sync::Mutex<()>,
    observation_interval: AtomicU64,
    #[cfg(feature = "management-api")]
    config_file: Option<config_file::ConfigFile>,
}
struct ApplyJob {
    id: String,
    principal: Principal,
    plan: ChangePlan,
}
#[derive(Clone)]
pub struct ManagementHandle {
    shared: Arc<Shared>,
    commands: mpsc::Sender<ApplyJob>,
}
pub struct ManagementService {
    handle: ManagementHandle,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}
impl ManagementService {
    pub fn start(
        app: LiveApplicationHandle,
        store: Arc<dyn DefinitionStore>,
        limits: ManagementConfig,
    ) -> Result<Self> {
        Self::start_inner(app, store, limits, None)
    }
    pub fn start_with_config(
        app: LiveApplicationHandle,
        store: Arc<dyn DefinitionStore>,
        limits: ManagementConfig,
        path: std::path::PathBuf,
    ) -> Result<Self> {
        Self::start_inner(app, store, limits, Some(path))
    }
    fn start_inner(
        app: LiveApplicationHandle,
        store: Arc<dyn DefinitionStore>,
        limits: ManagementConfig,
        path: Option<std::path::PathBuf>,
    ) -> Result<Self> {
        limits.validate().map_err(|e| error("invalid_config", &e))?;
        #[cfg(feature = "management-api")]
        let config_file = path
            .clone()
            .map(config_file::ConfigFile::open)
            .transpose()?;
        #[cfg(feature = "management-api")]
        let saved_workers = config_file.as_ref().map(|f| f.initial_workers);
        #[cfg(not(feature = "management-api"))]
        let saved_workers = None;
        #[cfg(not(feature = "management-api"))]
        if path.is_some() {
            return Err(error(
                "unsupported",
                "configuration saving requires management-api",
            ));
        }
        let shared = Arc::new(Shared {
            app,
            store,
            instance: format!("{:032x}", rand::random::<u128>()),
            state: Mutex::new(State {
                next: 0,
                plans: BTreeMap::new(),
                operations: BTreeMap::new(),
                order: VecDeque::new(),
                keys: BTreeMap::new(),
                leases: BTreeMap::new(),
                lease_keys: BTreeMap::new(),
                events: VecDeque::new(),
                cursor: 0,
                config_revision: 1,
                saved_workers,
                saved_observation_interval: {
                    #[cfg(feature = "management-api")]
                    {
                        config_file.as_ref().map(|f| f.initial_observation_interval)
                    }
                    #[cfg(not(feature = "management-api"))]
                    {
                        None
                    }
                },
            }),
            closed: AtomicBool::new(false),
            changed: Notify::new(),
            mutation: tokio::sync::Mutex::new(()),
            observation_interval: AtomicU64::new(limits.overview_interval_ms),
            #[cfg(feature = "management-api")]
            config_file,
            limits: limits.clone(),
        });
        let (commands, mut rx) = mpsc::channel::<ApplyJob>(limits.command_capacity + 1);
        let (stop, mut stopping) = tokio::sync::oneshot::channel();
        let owned = shared.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let mut timer = tokio::time::interval(Duration::from_millis(100));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last = Instant::now();
            let mut lifecycle = Vec::new();
            let mut integrity_revision = 0;
            loop {
                tokio::select! {
                    _=&mut stopping=>break,
                    job=rx.recv()=>{let Some(job)=job else{break};let s=owned.clone();tasks.spawn(async move{execute_guarded(s,job).await;});},
                    _=tasks.join_next(),if !tasks.is_empty()=>(),
                    _=timer.tick()=>{
                        expire_leases(&owned).await;
                        let current=owned.app.status().streams;
                        if current!=lifecycle{lifecycle=current;notify(&owned,"lifecycle_changed",None,"runtime lifecycle observations changed; query stream reasons");}
                        let revision=owned.app.router.integrity_view("").revision;
                        if revision!=integrity_revision{integrity_revision=revision;notify(&owned,"integrity_changed",None,"source-attributed integrity revision changed");}

                        if last.elapsed()>=Duration::from_millis(owned.observation_interval.load(Ordering::Acquire)){last=Instant::now();notify(&owned,"observation",None,"cached runtime observations available");}
                    }
                }
            }
            owned.closed.store(true, Ordering::Release);
            rx.close();
            while let Some(job) = rx.recv().await {
                let s = owned.clone();
                tasks.spawn(async move {
                    execute_guarded(s, job).await;
                });
            }
            while tasks.join_next().await.is_some() {}
            let leases = owned
                .state
                .lock()
                .unwrap()
                .leases
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for id in leases {
                let _ = release(&owned, &id, None, true).await;
            }
        });
        Ok(Self {
            handle: ManagementHandle { shared, commands },
            stop: Some(stop),
            task: Some(task),
        })
    }
    pub fn handle(&self) -> ManagementHandle {
        self.handle.clone()
    }
    pub async fn shutdown(&mut self) -> bool {
        self.handle.shared.closed.store(true, Ordering::Release);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.as_mut() {
            match tokio::time::timeout(
                Duration::from_millis(self.handle.shared.limits.request_timeout_ms),
                task,
            )
            .await
            {
                Ok(Ok(())) => {
                    self.task.take();
                    true
                }
                Ok(Err(_)) => {
                    self.task.take();
                    false
                }
                Err(_) => false,
            }
        } else {
            true
        }
    }
}
impl Drop for ManagementService {
    fn drop(&mut self) {
        self.handle.shared.closed.store(true, Ordering::Release);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
fn id(state: &mut State, prefix: &str) -> Result<String> {
    state.next = state
        .next
        .checked_add(1)
        .ok_or_else(|| error("capacity", "identity space exhausted"))?;
    Ok(format!("{prefix}-{}", state.next))
}
fn notify(s: &Shared, kind: &str, operation: Option<&str>, detail: &str) {
    let mut state = s.state.lock().unwrap();
    state.cursor = state.cursor.saturating_add(1);
    let cursor = state.cursor.to_string();
    state.events.push_back(ManagementEvent {
        cursor,
        daemon_instance: s.instance.clone(),
        kind: kind.into(),
        operation_id: operation.map(str::to_owned),
        detail: detail
            .chars()
            .filter(|c| !c.is_control())
            .take(512)
            .collect(),
    });
    while state.events.len() > s.limits.event_history {
        state.events.pop_front();
    }
    drop(state);
    s.changed.notify_waiters();
}
impl ManagementHandle {
    pub fn instance(&self) -> &str {
        &self.shared.instance
    }
    pub fn limits(&self) -> &ManagementConfig {
        &self.shared.limits
    }
    fn mutation_allowed(&self, principal: &Principal, role: Role) -> Result<()> {
        principal.require(role)?;
        if self.shared.closed.load(Ordering::Acquire) || self.shared.app.status().shutting_down {
            return Err(error("shutting_down", "mutations closed"));
        }
        Ok(())
    }
    pub fn preview(&self, principal: &Principal, request: PreviewRequest) -> Result<ChangePlan> {
        self.mutation_allowed(principal, planning::required_role(&request.action))?;
        let demand_revision = self.shared.app.demand_registry.lock().unwrap().revision();
        let graph_revision = self.shared.app.router.graph_revision();
        let pool_revision = self.shared.app.processing.status().workers.resize_revision;
        let mutation = planning::validate(&self.shared, &request.action)?;
        if (!mutation.affected.is_empty()
            && demand_revision != self.shared.app.demand_registry.lock().unwrap().revision())
            || graph_revision != self.shared.app.router.graph_revision()
            || pool_revision != self.shared.app.processing.status().workers.resize_revision
        {
            return Err(error(
                "stale_plan",
                "runtime revisions changed during preview",
            ));
        }
        let mut state = self.shared.state.lock().unwrap();
        state.plans.retain(|_, p| {
            p.created.elapsed() < Duration::from_secs(self.shared.limits.plan_seconds)
        });
        if state.plans.len()
            >= self.shared.limits.plans + usize::from(matches!(request.action, Action::Shutdown))
        {
            return Err(error("capacity", "change plan capacity"));
        }
        let plan=ChangePlan{id:format!("{}-{}",self.shared.instance,id(&mut state,"plan")?),daemon_instance:self.shared.instance.clone(),action:request.action,graph_revision:graph_revision.to_string(),pool_revision:pool_revision.to_string(),config_revision:state.config_revision.to_string(),demand_revision:if mutation.affected.is_empty(){None}else{Some(demand_revision.to_string())},affected_streams:mutation.affected.into_iter().collect(),transitions:mutation.transitions,continuity_impact:mutation.impact,persistence:if self.shared.store.durable(){"definition edits transactionally saved in PostgreSQL; runtime activation is separate"}else{"explicit non-durable embedding store"}.into(),warnings:mutation.warnings,expires_in_seconds:self.shared.limits.plan_seconds};
        state.plans.insert(
            plan.id.clone(),
            PlanRecord {
                view: plan.clone(),
                owner: principal.name.clone(),
                created: Instant::now(),
            },
        );
        Ok(plan)
    }
    pub fn plan(&self, principal: &Principal, id: &str) -> Result<ChangePlan> {
        principal.require(Role::Viewer)?;
        let state = self.shared.state.lock().unwrap();
        let p = state
            .plans
            .get(id)
            .ok_or_else(|| error("not_found", "plan not retained"))?;
        if p.owner != principal.name && principal.role != Role::Administrator {
            return Err(error("forbidden", "plan belongs to another principal"));
        }
        if p.created.elapsed() >= Duration::from_secs(self.shared.limits.plan_seconds) {
            return Err(error("plan_expired", "preview again"));
        }
        Ok(p.view.clone())
    }
    pub fn apply(
        &self,
        principal: &Principal,
        plan_id: &str,
        request: ApplyRequest,
    ) -> Result<Operation> {
        self.mutation_allowed(principal, Role::Operator)?;
        valid_key(&request.idempotency_key)?;
        let key = (principal.name.clone(), request.idempotency_key);
        let mut state = self.shared.state.lock().unwrap();
        if let Some((old_plan, action, op)) = state.keys.get(&key) {
            if old_plan == plan_id
                || state
                    .plans
                    .get(plan_id)
                    .is_some_and(|p| &p.view.action == action)
            {
                return state
                    .operations
                    .get(op)
                    .cloned()
                    .ok_or_else(|| error("not_found", "operation no longer retained"));
            }
            return Err(error(
                "idempotency_conflict",
                "key was used with different parameters",
            ));
        }
        let record = state
            .plans
            .get(plan_id)
            .ok_or_else(|| error("not_found", "plan not retained"))?;
        principal.require(planning::required_role(&record.view.action))?;
        if record.owner != principal.name {
            return Err(error("forbidden", "apply requires the preview owner"));
        }
        if record.created.elapsed() >= Duration::from_secs(self.shared.limits.plan_seconds) {
            return Err(error("plan_expired", "preview again"));
        }
        check_revisions(&self.shared, &state, &record.view)?;
        let plan = record.view.clone();
        if state
            .operations
            .values()
            .filter(|o| !o.cleanup_complete)
            .count()
            >= self.shared.limits.active_operations
            && !matches!(plan.action, Action::Shutdown)
        {
            return Err(error("capacity", "active operation capacity"));
        }
        if !matches!(plan.action, Action::Shutdown) && self.commands.capacity() <= 1 {
            return Err(error(
                "capacity",
                "command capacity; shutdown reserve retained",
            ));
        }
        let permit = self
            .commands
            .try_reserve()
            .map_err(|_| error("capacity", "bounded command admission full or closed"))?;
        while state.operations.len()
            >= self.shared.limits.operation_history
                + usize::from(matches!(plan.action, Action::Shutdown))
        {
            let index = state
                .order
                .iter()
                .position(|id| state.operations.get(id).is_some_and(|o| o.cleanup_complete))
                .ok_or_else(|| error("capacity", "unfinished operations retain ownership"))?;
            let old = state.order.remove(index).unwrap();
            state.operations.remove(&old);
            state.keys.retain(|_, (_, _, id)| id != &old);
        }
        let op = Operation {
            id: format!("{}-{}", self.shared.instance, id(&mut state, "op")?),
            owner: principal.name.clone(),
            action: plan.action.clone(),
            state: "accepted".into(),
            cleanup_complete: false,
            saved: false,
            activation_requested: false,
            runtime_ready: None,
            detail: None,
        };
        state.operations.insert(op.id.clone(), op.clone());
        state.order.push_back(op.id.clone());
        state
            .keys
            .insert(key, (plan_id.into(), plan.action.clone(), op.id.clone()));
        permit.send(ApplyJob {
            id: op.id.clone(),
            principal: principal.clone(),
            plan,
        });
        drop(state);
        notify(
            &self.shared,
            "operation_accepted",
            Some(&op.id),
            "accepted work is owned independently of the caller",
        );
        Ok(op)
    }
    pub fn operation(&self, principal: &Principal, id: &str) -> Result<Operation> {
        principal.require(Role::Viewer)?;
        self.shared
            .state
            .lock()
            .unwrap()
            .operations
            .get(id)
            .cloned()
            .ok_or_else(|| error("not_found", "operation not retained"))
    }
    pub fn operations(&self, principal: &Principal, page: PageRequest) -> Result<Page<Operation>> {
        principal.require(Role::Viewer)?;
        views::page(
            self.shared
                .state
                .lock()
                .unwrap()
                .operations
                .values()
                .cloned()
                .collect(),
            page,
            |o| &o.id,
        )
    }
    pub fn events(
        &self,
        principal: &Principal,
        instance: Option<&str>,
        after: u64,
    ) -> Result<EventPage> {
        principal.require(Role::Viewer)?;
        let state = self.shared.state.lock().unwrap();
        let oldest = state
            .events
            .front()
            .and_then(|e| e.cursor.parse::<u64>().ok())
            .unwrap_or(state.cursor + 1);
        let resync = instance.is_some_and(|i| i != self.shared.instance)
            || after > state.cursor
            || after.saturating_add(1) < oldest;
        Ok(EventPage {
            events: if resync {
                vec![]
            } else {
                state
                    .events
                    .iter()
                    .filter(|e| e.cursor.parse::<u64>().unwrap() > after)
                    .take(128)
                    .cloned()
                    .collect()
            },
            cursor: state.cursor.to_string(),
            resync_required: resync,
        })
    }
    pub async fn changed(&self) {
        self.shared.changed.notified().await;
    }
    pub async fn acquire_lease(
        &self,
        principal: &Principal,
        request: LeaseRequest,
    ) -> Result<DemandLease> {
        self.mutation_allowed(principal, Role::Operator)?;
        valid_key(&request.idempotency_key)?;
        valid_ttl(&self.shared, request.seconds)?;
        let _gate = self.shared.mutation.lock().await;
        let key = (principal.name.clone(), request.idempotency_key.clone());
        let view = {
            let mut state = self.shared.state.lock().unwrap();
            if let Some((old, id)) = state.lease_keys.get(&key) {
                if old.stream_id != request.stream_id || old.seconds != request.seconds {
                    return Err(error(
                        "idempotency_conflict",
                        "lease key parameters changed",
                    ));
                }
                return state
                    .leases
                    .get(id)
                    .map(|l| lease_view(l))
                    .ok_or_else(|| error("lease_released", "lease key already completed"));
            }
            while state.lease_keys.len() >= self.shared.limits.operation_history {
                let old = state
                    .lease_keys
                    .iter()
                    .find(|(_, (_, id))| !state.leases.contains_key(id))
                    .map(|(key, _)| key.clone());
                if let Some(old) = old {
                    state.lease_keys.remove(&old);
                } else {
                    break;
                }
            }
            if state.leases.len() >= self.shared.limits.leases
                || state.lease_keys.len() >= self.shared.limits.operation_history
            {
                return Err(error("capacity", "lease/idempotency capacity"));
            }
            let view = DemandLease {
                id: format!("{}-{}", self.shared.instance, id(&mut state, "lease")?),
                owner: principal.name.clone(),
                stream_id: request.stream_id.clone(),
                remaining_seconds: request.seconds,
            };
            state.lease_keys.insert(key, (request, view.id.clone()));
            state.leases.insert(
                view.id.clone(),
                LeaseRecord {
                    renewals: VecDeque::new(),
                    view: view.clone(),
                    expires: Instant::now() + Duration::from_secs(view.remaining_seconds),
                    releasing: false,
                },
            );
            view
        };
        if let Err(e) = self
            .shared
            .app
            .demand(&view.stream_id, lease_owner(&self.shared, &view.id), true)
            .await
        {
            self.shared.state.lock().unwrap().leases.remove(&view.id);
            return Err(error("demand_rejected", &e.to_string()));
        }
        notify(
            &self.shared,
            "demand_acquired",
            None,
            &format!("{} acquired {}", principal.name, view.id),
        );
        Ok(view)
    }
    pub fn renew_lease(
        &self,
        principal: &Principal,
        id: &str,
        request: RenewLease,
    ) -> Result<DemandLease> {
        self.mutation_allowed(principal, Role::Operator)?;
        valid_ttl(&self.shared, request.seconds)?;
        valid_key(&request.idempotency_key)?;
        let mut state = self.shared.state.lock().unwrap();
        let l = state
            .leases
            .get_mut(id)
            .ok_or_else(|| error("not_found", "lease not retained"))?;
        if l.view.owner != principal.name {
            return Err(error("forbidden", "lease owner required"));
        }
        if l.releasing {
            return Err(error("lease_releasing", "release already owns this lease"));
        }
        if l.expires <= Instant::now() {
            return Err(error("lease_expired", "acquire a new lease"));
        }
        if let Some((_, seconds)) = l
            .renewals
            .iter()
            .find(|(key, _)| key == &request.idempotency_key)
        {
            if *seconds != request.seconds {
                return Err(error(
                    "idempotency_conflict",
                    "renewal key reused with different duration",
                ));
            }
            return Ok(lease_view(l));
        }
        l.renewals
            .push_back((request.idempotency_key, request.seconds));
        while l.renewals.len() > 32 {
            l.renewals.pop_front();
        }
        l.expires = Instant::now() + Duration::from_secs(request.seconds);
        Ok(lease_view(l))
    }
    pub async fn release_lease(
        &self,
        principal: &Principal,
        id: &str,
        force: bool,
    ) -> Result<LeaseRelease> {
        self.mutation_allowed(
            principal,
            if force {
                Role::Administrator
            } else {
                Role::Operator
            },
        )?;
        release(&self.shared, id, Some(&principal.name), force).await
    }
}
fn valid_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 128 || key.chars().any(char::is_control) {
        Err(error(
            "invalid_idempotency_key",
            "bounded printable idempotency key required",
        ))
    } else {
        Ok(())
    }
}
fn valid_ttl(s: &Shared, seconds: u64) -> Result<()> {
    if seconds == 0 || seconds > s.limits.lease_max_seconds {
        Err(error(
            "invalid_lease",
            "lease duration outside configured bounds",
        ))
    } else {
        Ok(())
    }
}
fn lease_owner(s: &Shared, id: &str) -> DemandOwner {
    DemandOwner::Manual {
        session_id: format!("management:{}:{id}", s.instance),
    }
}
fn lease_view(l: &LeaseRecord) -> DemandLease {
    let mut v = l.view.clone();
    let remaining = l.expires.saturating_duration_since(Instant::now());
    v.remaining_seconds = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
    v
}
async fn release(s: &Shared, id: &str, owner: Option<&str>, force: bool) -> Result<LeaseRelease> {
    let completed = {
        let state = s.state.lock().unwrap();
        if state.leases.contains_key(id) {
            None
        } else {
            state
                .lease_keys
                .iter()
                .find(|((identity, _), (_, lease))| {
                    lease == id && (force || Some(identity.as_str()) == owner)
                })
                .map(|(_, (request, _))| request.stream_id.clone())
        }
    };
    if let Some(stream) = completed {
        let required = s
            .app
            .demand_registry
            .lock()
            .unwrap()
            .required_streams(&s.app.router.graph())
            .map_err(|e| error("demand_invalid", &e.to_string()))?
            .contains(&stream);
        return Ok(LeaseRelease {
            released: true,
            stream_still_required: required,
            explanation: "lease already released; no other owner's demand was changed".into(),
        });
    }

    let v = {
        let mut state = s.state.lock().unwrap();
        let l = state
            .leases
            .get_mut(id)
            .ok_or_else(|| error("not_found", "lease not retained"))?;
        if !force && owner != Some(l.view.owner.as_str()) {
            return Err(error("forbidden", "lease owner required"));
        }
        if l.releasing {
            return Err(error("lease_releasing", "release already in progress"));
        }
        l.releasing = true;
        l.view.clone()
    };
    if s.app
        .demand_registry
        .lock()
        .unwrap()
        .owner_stream(&lease_owner(s, id))
        .is_none()
    {
        s.state.lock().unwrap().leases.remove(id);
        return Ok(LeaseRelease {
            released: true,
            stream_still_required: false,
            explanation: "demand ownership already released during application cleanup".into(),
        });
    }
    if let Err(e) = s.app.demand(&v.stream_id, lease_owner(s, id), false).await {
        if let Some(l) = s.state.lock().unwrap().leases.get_mut(id) {
            l.releasing = false;
        }
        return Err(error("demand_release_failed", &e.to_string()));
    }
    s.state.lock().unwrap().leases.remove(id);
    let required = s
        .app
        .demand_registry
        .lock()
        .unwrap()
        .required_streams(&s.app.router.graph())
        .map_err(|e| error("demand_invalid", &e.to_string()))?
        .contains(&v.stream_id);
    notify(
        s,
        if force {
            "demand_force_released"
        } else {
            "demand_released"
        },
        None,
        &format!("lease {id}; actor {}", owner.unwrap_or("expiry/shutdown")),
    );
    Ok(LeaseRelease {
        released: true,
        stream_still_required: required,
        explanation: if required {
            "another owner, AlwaysOn policy, or dependent still requires this stream"
        } else {
            "no remaining activation requirement"
        }
        .into(),
    })
}
async fn expire_leases(s: &Shared) {
    let releasing = {
        let state = s.state.lock().unwrap();
        state
            .leases
            .iter()
            .filter(|(_, l)| l.releasing)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>()
    };
    for id in releasing {
        if s.app
            .demand_registry
            .lock()
            .unwrap()
            .owner_stream(&lease_owner(s, &id))
            .is_none()
        {
            s.state.lock().unwrap().leases.remove(&id);
            notify(
                s,
                "demand_released",
                None,
                "cancelled caller's accepted release completed",
            );
        }
    }
    let ids = {
        let state = s.state.lock().unwrap();
        state
            .leases
            .iter()
            .filter(|(_, l)| l.expires <= Instant::now())
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>()
    };
    for id in ids {
        let _ = release(s, &id, None, true).await;
    }
}
fn check_revisions(s: &Shared, state: &State, plan: &ChangePlan) -> Result<()> {
    if plan
        .demand_revision
        .as_ref()
        .is_some_and(|r| r != &s.app.demand_registry.lock().unwrap().revision().to_string())
        || plan.daemon_instance != s.instance
        || plan.graph_revision != s.app.router.graph_revision().to_string()
        || plan.pool_revision
            != s.app
                .processing
                .status()
                .workers
                .resize_revision
                .to_string()
        || plan.config_revision != state.config_revision.to_string()
    {
        Err(error("stale_plan", "revisions changed; preview again"))
    } else {
        Ok(())
    }
}
async fn execute(s: Arc<Shared>, job: ApplyJob) {
    let _gate = if matches!(
        job.plan.action,
        Action::ResizeWorkers { .. } | Action::Shutdown
    ) {
        None
    } else {
        Some(s.mutation.lock().await)
    };
    {
        if let Some(op) = s.state.lock().unwrap().operations.get_mut(&job.id) {
            op.state = "executing".into();
        }
    }
    notify(&s, "operation_progress", Some(&job.id), "execution started");
    let result = planning::execute(&s, &job).await;
    {
        if let Some(op) = s.state.lock().unwrap().operations.get_mut(&job.id) {
            op.cleanup_complete = true;
            match result {
                Ok(()) => op.state = "completed".into(),
                Err(e) => {
                    op.state = if e.code == "incomplete_cleanup" {
                        "incomplete_cleanup"
                    } else if op.saved {
                        "saved_runtime_failed"
                    } else {
                        "failed"
                    }
                    .into();
                    op.detail = Some(e.to_string());
                }
            }
        }
    }
    notify(
        &s,
        "operation_completed",
        Some(&job.id),
        "operation result retained",
    );
}

async fn execute_guarded(s: Arc<Shared>, job: ApplyJob) {
    use futures_util::FutureExt;
    let identity = job.id.clone();
    if std::panic::AssertUnwindSafe(execute(s.clone(), job))
        .catch_unwind()
        .await
        .is_err()
    {
        if let Some(op) = s
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .operations
            .get_mut(&identity)
        {
            op.cleanup_complete = true;
            op.state = "failed".into();
            op.detail=Some("operation task failed; inspect authoritative runtime for any accepted lifecycle progress".into());
        }
        notify(
            &s,
            "operation_failed",
            Some(&identity),
            "operation task panicked; outcome retained",
        );
    }
}
