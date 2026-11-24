use super::*;
pub(super) struct Mutation {
    pub previous: Option<PersistedStreamDefinition>,
    pub next: Option<PersistedStreamDefinition>,
    pub graph: Option<Arc<StreamGraph>>,
    pub affected: BTreeSet<String>,
    pub transitions: Vec<String>,
    pub impact: String,
    pub warnings: Vec<String>,
}
pub(super) fn required_role(a: &Action) -> Role {
    match a {
        Action::Delete { .. } | Action::Shutdown => Role::Administrator,
        _ => Role::Operator,
    }
}
pub(super) fn edges(defs: &[PersistedStreamDefinition]) -> Vec<StreamDependency> {
    defs.iter()
        .flat_map(|d| match &d.definition {
            StreamDefinition::Derived(r) => r
                .input_specs()
                .into_iter()
                .map(|s| StreamDependency {
                    derived_stream_id: d.stream_id.clone(),
                    input_stream_id: s.stream_id,
                    input_position: s.position as i32,
                })
                .collect(),
            _ => vec![],
        })
        .collect()
}
fn affected(graph: &StreamGraph, id: &str, cascade: bool) -> Result<BTreeSet<String>> {
    let mut result = BTreeSet::from([id.into()]);
    let mut pending = vec![id.to_string()];
    while let Some(id) = pending.pop() {
        if let Some(deps) = graph.dependents(&id) {
            for next in deps {
                if !cascade {
                    return Err(error(
                        "cascade_required",
                        "dependent definitions require explicit impact review",
                    ));
                }
                if result.insert(next.clone()) {
                    pending.push(next.clone());
                }
            }
        }
    }
    Ok(result)
}
pub(super) fn validate(s: &Shared, action: &Action) -> Result<Mutation> {
    if serde_json::to_vec(action)
        .map_err(|_| error("invalid", "action encoding failed"))?
        .len()
        > s.limits.max_body_bytes
    {
        return Err(error("too_large", "action exceeds body budget"));
    }
    let graph = s.app.router.graph();
    let mut defs = graph
        .stream_ids()
        .map(|id| graph.get(id).unwrap().clone())
        .collect::<Vec<_>>();
    let mut m = Mutation {
        previous: None,
        next: None,
        graph: None,
        affected: BTreeSet::new(),
        transitions: vec![],
        impact: "no market continuity change".into(),
        warnings: vec![],
    };
    let mut saved_change = false;
    match action {
        Action::CreateSource {
            stream_id,
            request,
            desired,
        } => {
            request
                .validate()
                .map_err(|e| error("invalid_definition", &e))?;
            if graph.get(stream_id).is_some() {
                return Err(error("already_exists", "stream already exists"));
            }
            m.next = Some(PersistedStreamDefinition {
                stream_id: stream_id.clone(),
                recording_stream_key: Some(-1),
                definition: StreamDefinition::Source(request.clone()),
                desired: *desired,
                revision: 1,
            });
            saved_change = true;
        }
        Action::CreateDerived { request, desired } => {
            request
                .validate()
                .map_err(|e| error("invalid_definition", &e))?;
            if graph.get(&request.name).is_some() {
                return Err(error("already_exists", "stream already exists"));
            }
            m.next = Some(PersistedStreamDefinition {
                stream_id: request.name.clone(),
                recording_stream_key: None,
                definition: StreamDefinition::Derived(request.clone()),
                desired: *desired,
                revision: 1,
            });
            saved_change = true;
        }
        Action::EditDerived {
            stream_id,
            expected_revision,
            request,
            cascade,
        } => {
            let old = graph
                .get(stream_id)
                .ok_or_else(|| error("not_found", "stream unknown"))?;
            if !matches!(old.definition, StreamDefinition::Derived(_)) || request.name != *stream_id
            {
                return Err(error(
                    "unsupported",
                    "source identity edits require a new definition",
                ));
            }
            revision(old, expected_revision)?;
            request
                .validate()
                .map_err(|e| error("invalid_definition", &e))?;
            m.previous = Some(old.clone());
            let mut next = old.clone();
            next.definition = StreamDefinition::Derived(request.clone());
            next.revision = old
                .revision
                .checked_add(1)
                .ok_or_else(|| error("capacity", "definition revision exhausted"))?;
            m.next = Some(next);
            m.affected = affected(&graph, stream_id, *cascade)?;
            saved_change = true;
        }
        Action::SetDesired {
            stream_id,
            expected_revision,
            desired,
            cascade,
        } => {
            let old = graph
                .get(stream_id)
                .ok_or_else(|| error("not_found", "stream unknown"))?;
            revision(old, expected_revision)?;
            m.previous = Some(old.clone());
            let mut next = old.clone();
            next.desired = *desired;
            next.revision = old
                .revision
                .checked_add(1)
                .ok_or_else(|| error("capacity", "definition revision exhausted"))?;
            m.next = Some(next);
            if !desired.enabled {
                m.affected = affected(&graph, stream_id, *cascade)?;
            }
            saved_change = true;
        }
        Action::Delete {
            stream_id,
            expected_revision,
        } => {
            let old = graph
                .get(stream_id)
                .ok_or_else(|| error("not_found", "stream unknown"))?;
            revision(old, expected_revision)?;
            if graph.dependents(stream_id).is_some_and(|d| !d.is_empty()) {
                return Err(error(
                    "has_dependents",
                    "remove dependent definitions in separate reviewed changes first",
                ));
            }
            m.previous = Some(old.clone());
            m.affected.insert(stream_id.clone());
            saved_change = true;
        }
        Action::Restart { stream_id, cascade } => {
            if graph.get(stream_id).is_none() {
                return Err(error("not_found", "stream unknown"));
            }
            m.affected = affected(&graph, stream_id, *cascade)?;
            if !s
                .app
                .demand_registry
                .lock()
                .unwrap()
                .required_streams(&graph)
                .map_err(|e| error("demand_invalid", &e.to_string()))?
                .contains(stream_id)
            {
                return Err(error(
                    "no_demand",
                    "restart requires an activation requirement",
                ));
            }
        }
        Action::ResizeWorkers { workers, save } => {
            if *workers == 0 || *workers > s.limits.max_workers {
                return Err(error(
                    "invalid_workers",
                    "target outside configured worker bounds",
                ));
            }
            if *save && !can_save(s) {
                return Err(error("save_unavailable", "no owned configuration file"));
            }
            if std::thread::available_parallelism().is_ok_and(|n| *workers > n.get()) {
                m.warnings
                    .push("worker target exceeds available CPU parallelism".into());
            }
            m.transitions.push(format!("dedicated CPU pool target becomes {workers}; registrations, generations and buffers retained"));
        }
        Action::SetObservationInterval { milliseconds, save } => {
            if !(100..=60000).contains(milliseconds) {
                return Err(error(
                    "invalid_config",
                    "observation interval outside 100..=60000 ms",
                ));
            }
            if *save && !can_save(s) {
                return Err(error("save_unavailable", "no owned configuration file"));
            }
        }
        Action::Shutdown => {
            m.affected = graph.stream_ids().map(str::to_owned).collect();
            m.warnings
                .push("all consumers end; unfinished turns remain owned until they return".into());
        }
    }
    if saved_change {
        if let Some(old) = &m.previous {
            defs.retain(|d| d.stream_id != old.stream_id);
        }
        if let Some(next) = &m.next {
            defs.push(next.clone());
        }
        let new = StreamGraph::restore(defs.clone(), edges(&defs))
            .map_err(|e| error("invalid_graph", &e.to_string()))?;
        if let Some(config) = s.app.processing_config() {
            crate::live::stream::processing::preflight(&new, config.derived)
                .map_err(|e| error("invalid_processing_definition", &e))?;
        }
        new.required_streams(&std::collections::HashSet::new())
            .map_err(|e| error("unsafe_activation_dependencies", &e.to_string()))?;
        s.app
            .validate_candidate(&new)
            .map_err(|e| error("invalid_definition", &e))?;
        m.graph = Some(Arc::new(new));
        m.transitions.push(
            "save definition transaction, publish graph revision, reconcile activation separately"
                .into(),
        );
    }
    if !m.affected.is_empty() {
        m.impact="affected live subscriptions terminate; explicit new generations acquire fresh authority".into();
        m.transitions
            .push("retire affected dependents before upstream execution".into());
    }
    if matches!(
        action,
        Action::Delete { .. }
            | Action::SetDesired {
                desired: DesiredStreamState { enabled: false, .. },
                ..
            }
    ) {
        let demand = s.app.demand_registry.lock().unwrap();
        if m.affected.iter().any(|id| {
            demand.owners().any(|(owner, stream)| {
                stream == id
                    && !matches!(owner, DemandOwner::Processing { .. })
                    && !(matches!(action, Action::SetDesired { cascade: true, .. })
                        && matches!(owner, DemandOwner::Unix { .. } | DemandOwner::Direct { .. }))
            })
        }) {
            return Err(error(
                "active_demand",
                "release independent demand before disabling or deleting; no implicit owner removal",
            ));
        }
    }
    Ok(m)
}
fn revision(old: &PersistedStreamDefinition, expected: &str) -> Result<()> {
    if expected != &old.revision.to_string() {
        Err(error("revision_conflict", "definition changed"))
    } else {
        Ok(())
    }
}
fn can_save(s: &Shared) -> bool {
    #[cfg(feature = "management-api")]
    {
        s.config_file.is_some()
    }
    #[cfg(not(feature = "management-api"))]
    {
        let _ = s;
        false
    }
}
fn saved(s: &Shared, id: &str) {
    if let Some(op) = s.state.lock().unwrap().operations.get_mut(id) {
        op.saved = true;
    }
}
pub(super) async fn execute(s: &Shared, job: &ApplyJob) -> Result<()> {
    job.principal.require(required_role(&job.plan.action))?;
    if s.app.status().shutting_down && !matches!(job.plan.action, Action::Shutdown) {
        return Err(error(
            "shutting_down",
            "no new administrative execution during application shutdown",
        ));
    }
    {
        let state = s.state.lock().unwrap();
        check_revisions(s, &state, &job.plan)?;
    }
    let m = validate(s, &job.plan.action)?;
    match &job.plan.action {
        Action::ResizeWorkers { workers, save } => {
            if *save {
                save_config(s, "workers", *workers as u64).await?;
                saved(s, &job.id);
                {
                    let mut state = s.state.lock().unwrap();
                    state.saved_workers = Some(*workers);
                    state.config_revision += 1;
                }
            }
            let pool = s.app.processing.pool_handle();
            let mut updates = pool.subscribe_changes();
            pool.set_workers(
                *workers,
                job.plan
                    .pool_revision
                    .parse()
                    .map_err(|_| error("invalid_revision", "pool revision invalid"))?,
            )
            .map_err(|e| error("resize_rejected", &e.to_string()))?;
            loop {
                let stats = pool.stats();
                if stats.shutdown {
                    waiting_cleanup(s, job, "shutdown owns remaining resize workers");
                    if stats.joined {
                        return Err(error(
                            "incomplete_cleanup",
                            "resize interrupted by shutdown; CPU cleanup finished",
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                if !stats.resize_pending {
                    if let Some(reason) = pool.resize_error() {
                        return Err(error("resize_failed", &reason));
                    }
                    if stats.running_workers == *workers {
                        return Ok(());
                    }
                }
                tokio::select! {_=updates.changed()=>(),_=tokio::time::sleep(Duration::from_millis(50))=>()}
            }
        }
        Action::SetObservationInterval { milliseconds, save } => {
            if *save {
                save_config(s, "overview_interval_ms", *milliseconds).await?;
                saved(s, &job.id);
                s.state.lock().unwrap().saved_observation_interval = Some(*milliseconds);
            }
            s.observation_interval
                .store(*milliseconds, Ordering::Release);
            s.state.lock().unwrap().config_revision += 1;
            Ok(())
        }
        Action::Shutdown => {
            let started = Instant::now();
            s.closed.store(true, Ordering::Release);
            s.app
                .request_shutdown()
                .await
                .map_err(|e| error("shutdown_failed", &e.to_string()))?;
            loop {
                let status = s.app.status();
                if started.elapsed() > s.app.shutdown_budget() {
                    waiting_cleanup(
                        s,
                        job,
                        "application cleanup exceeded observation deadline; services remain owned",
                    );
                }
                if let Some(report) = status.shutdown_report {
                    return if report.complete {
                        Ok(())
                    } else {
                        Err(error(
                            "incomplete_cleanup",
                            "application cleanup remains owned",
                        ))
                    };
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Action::Restart { stream_id, .. } => {
            let before = s
                .app
                .registry
                .lock()
                .unwrap()
                .status(stream_id)
                .map(|r| r.generation.get())
                .ok_or_else(|| error("not_found", "stream unknown"))?;
            let reservation = s
                .app
                .router
                .reserve_graph_change(
                    job.plan.graph_revision.parse().unwrap(),
                    job.plan
                        .demand_revision
                        .as_ref()
                        .and_then(|r| r.parse().ok()),
                    &m.affected,
                )
                .map_err(|e| error("transition_conflict", &e))?;
            s.app
                .retry_stream(stream_id)
                .await
                .map_err(|e| error("restart_rejected", &e.to_string()))?;
            drop(reservation);
            if let Some(op) = s.state.lock().unwrap().operations.get_mut(&job.id) {
                op.activation_requested = true;
                op.runtime_ready = Some(false);
                op.state = "waiting_for_readiness".into();
            }
            loop {
                if s.app.status().shutting_down {
                    waiting_cleanup(
                        s,
                        job,
                        "shutdown interrupted restart; lifecycle owners retain cleanup",
                    );
                    if s.app.status().shutdown_report.is_some() {
                        return Err(error(
                            "incomplete_cleanup",
                            "restart interrupted; application cleanup has settled",
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                let status = s
                    .app
                    .registry
                    .lock()
                    .unwrap()
                    .status(stream_id)
                    .cloned()
                    .ok_or_else(|| error("not_found", "stream removed during restart"))?;
                if status.generation.get() > before {
                    if status.is_ready() {
                        if let Some(op) = s.state.lock().unwrap().operations.get_mut(&job.id) {
                            op.runtime_ready = Some(true);
                        }
                        return Ok(());
                    }
                    if status.lifecycle == StreamLifecycleState::Failed {
                        return Err(error(
                            "activation_failed",
                            status
                                .last_error
                                .as_deref()
                                .unwrap_or("replacement execution failed"),
                        ));
                    }
                    if status.lifecycle == StreamLifecycleState::Stopped
                        && !s
                            .app
                            .demand_registry
                            .lock()
                            .unwrap()
                            .required_streams(&s.app.router.graph())
                            .map_err(|e| error("demand_invalid", &e.to_string()))?
                            .contains(stream_id)
                    {
                        if let Some(op) = s.state.lock().unwrap().operations.get_mut(&job.id) {
                            op.detail =
                                Some("retired; no remaining demand or AlwaysOn activation".into());
                        }
                        return Ok(());
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        _ => {
            let reservation = s
                .app
                .router
                .reserve_graph_change(
                    job.plan.graph_revision.parse().unwrap(),
                    job.plan
                        .demand_revision
                        .as_ref()
                        .and_then(|r| r.parse().ok()),
                    &m.affected,
                )
                .map_err(|e| error("transition_conflict", &e))?;
            // Validate dynamic owners again at the reserved admission boundary.
            let m = validate(s, &job.plan.action)?;
            let persisted = s.store.save(m.previous.clone(), m.next.clone()).await?;
            saved(s, &job.id);
            let mut defs = m
                .graph
                .as_ref()
                .unwrap()
                .stream_ids()
                .map(|id| m.graph.as_ref().unwrap().get(id).unwrap().clone())
                .collect::<Vec<_>>();
            if let Some(persisted) = persisted {
                defs.retain(|d| d.stream_id != persisted.stream_id);
                defs.push(persisted);
            }
            let next = Arc::new(
                StreamGraph::restore(defs.clone(), edges(&defs))
                    .map_err(|e| error("saved_graph_invalid", &e.to_string()))?,
            );
            if let Some(op) = s.state.lock().unwrap().operations.get_mut(&job.id) {
                op.activation_requested = true;
                op.runtime_ready = Some(false);
            }
            s.app
                .replace_graph(
                    job.plan.graph_revision.parse().unwrap(),
                    next,
                    m.affected.clone(),
                )
                .await
                .map_err(|e| error("saved_runtime_failed", &e.to_string()))?;
            drop(reservation);
            notify(
                s,
                "definition_changed",
                Some(&job.id),
                "definition saved and graph revision published; activation tracked separately",
            );
            let target = m.next.as_ref().map(|d| d.stream_id.as_str());
            if let Some(target) = target {
                if let Some(op) = s.state.lock().unwrap().operations.get_mut(&job.id) {
                    op.state = "waiting_for_readiness".into();
                }
                loop {
                    if s.app.status().shutting_down {
                        waiting_cleanup(
                            s,
                            job,
                            "application shutdown during activation; lifecycle cleanup remains owned",
                        );
                        if s.app.status().shutdown_report.is_some() {
                            return Err(error(
                                "incomplete_cleanup",
                                "activation interrupted by application shutdown",
                            ));
                        }
                    } else {
                        let graph = s.app.router.graph();
                        let required = s
                            .app
                            .demand_registry
                            .lock()
                            .unwrap()
                            .required_streams(&graph)
                            .map_err(|e| error("demand_invalid", &e.to_string()))?;
                        if !required.contains(target) {
                            if let Some(op) = s.state.lock().unwrap().operations.get_mut(&job.id) {
                                op.runtime_ready = None;
                            }
                            return Ok(());
                        }
                        let status = s.app.registry.lock().unwrap().status(target).cloned();
                        if let Some(status) = status {
                            if status.is_ready() && affected_ready(s, &m.affected, &required)? {
                                if let Some(op) =
                                    s.state.lock().unwrap().operations.get_mut(&job.id)
                                {
                                    op.runtime_ready = Some(true);
                                }
                                return Ok(());
                            }
                            if status.lifecycle == StreamLifecycleState::Failed {
                                return Err(error(
                                    "activation_failed",
                                    status
                                        .last_error
                                        .as_deref()
                                        .unwrap_or("required execution failed"),
                                ));
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            Ok(())
        }
    }
}
async fn save_config(s: &Shared, key: &str, value: u64) -> Result<()> {
    #[cfg(feature = "management-api")]
    {
        s.config_file
            .as_ref()
            .ok_or_else(|| error("save_unavailable", "configuration path not owned"))?
            .update(key, value)
            .await
    }
    #[cfg(not(feature = "management-api"))]
    {
        let _ = (s, key, value);
        Err(error(
            "save_unavailable",
            "configuration saving unavailable",
        ))
    }
}

fn waiting_cleanup(s: &Shared, job: &ApplyJob, detail: &str) {
    let mut state = s.state.lock().unwrap();
    let mut changed = false;
    if let Some(op) = state.operations.get_mut(&job.id) {
        if op.state != "incomplete_cleanup" {
            op.state = "incomplete_cleanup".into();
            op.detail = Some(detail.into());
            changed = true;
        }
    }
    drop(state);
    if changed {
        notify(s, "operation_progress", Some(&job.id), detail);
    }
}

fn affected_ready(
    s: &Shared,
    affected: &BTreeSet<String>,
    required: &BTreeSet<String>,
) -> Result<bool> {
    let registry = s.app.registry.lock().unwrap();
    let mut ready = true;
    for id in affected.iter().filter(|id| required.contains(*id)) {
        let status = registry.status(id);
        if let Some(status) = status {
            if status.lifecycle == StreamLifecycleState::Failed {
                return Err(error(
                    "activation_failed",
                    &format!(
                        "{id}: {}",
                        status
                            .last_error
                            .as_deref()
                            .unwrap_or("required execution failed")
                    ),
                ));
            }
            ready &= status.is_ready();
        } else {
            ready = false;
        }
    }
    Ok(ready)
}
