use super::*;
pub(super) fn exact(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => {
            serde_json::Value::String(n.to_string())
        }
        serde_json::Value::Array(v) => serde_json::Value::Array(v.into_iter().map(exact).collect()),
        serde_json::Value::Object(v) => {
            serde_json::Value::Object(v.into_iter().map(|(k, v)| (k, exact(v))).collect())
        }
        v => v,
    }
}
pub(super) fn page<T>(
    mut items: Vec<T>,
    request: PageRequest,
    key: impl Fn(&T) -> &str,
) -> Result<Page<T>> {
    let limit = request.limit.unwrap_or(64);
    if limit == 0
        || limit > 128
        || request.after.as_ref().is_some_and(|s| s.len() > 4096)
        || request.filter.as_ref().is_some_and(|s| s.len() > 256)
    {
        return Err(error(
            "invalid_pagination",
            "page size outside 1..=128 or filter too long",
        ));
    }
    items.sort_by(|a, b| key(a).cmp(key(b)));
    items.retain(|v| {
        request.after.as_ref().is_none_or(|a| key(v) > a.as_str())
            && request.filter.as_ref().is_none_or(|q| key(v).contains(q))
    });
    let more = items.len() > limit;
    items.truncate(limit);
    let next = if more {
        items.last().map(|v| key(v).to_string())
    } else {
        None
    };
    Ok(Page { items, next })
}
impl ManagementHandle {
    pub async fn source_templates(
        &self,
        principal: &Principal,
        filter: String,
        limit: usize,
    ) -> Result<Vec<CreateSourceStreamRequest>> {
        principal.require(Role::Viewer)?;
        if filter.len() > 128 || limit == 0 || limit > 128 {
            return Err(error("invalid_pagination", "instrument lookup bounds"));
        }
        self.shared.store.templates(filter, limit).await
    }

    pub fn runtime(&self, principal: &Principal) -> Result<PoolView> {
        principal.require(Role::Viewer)?;
        let saved = self.shared.state.lock().unwrap().saved_workers;
        let pool = self.shared.app.processing.pool_handle();
        let workers = pool.stats();
        let retained = pool.budget().used();
        Ok(PoolView {
            revision: workers.resize_revision.to_string(),
            requested_target: workers.requested_workers,
            live_workers: workers.running_workers,
            starting_workers: workers.starting_workers,
            retiring_workers: workers.retiring_workers,
            executing_turns: workers.executing_processors,
            resizing: workers.resize_pending,
            resize_error: pool.resize_error(),
            saved_startup_workers: saved,
            registered_processors: workers.registered,
            ready_processors: Some(workers.ready),
            retained_payload_events: Some(retained.0),
            retained_payload_bytes: Some(retained.1),
        })
    }
    pub fn overview(&self, principal: &Principal) -> Result<Overview> {
        principal.require(Role::Viewer)?;
        // Capture the feed cursor before independently sampling service views.
        let (cursor, config_revision, operations) = {
            let state = self.shared.state.lock().unwrap();
            (
                state.cursor,
                state.config_revision,
                state
                    .order
                    .iter()
                    .rev()
                    .take(32)
                    .filter_map(|id| state.operations.get(id).cloned())
                    .collect(),
            )
        };
        let app = self.shared.app.status();
        let graph = self.shared.app.router.graph();
        let streams = graph
            .stream_ids()
            .map(|id| self.stream_view(id, &graph))
            .collect::<Result<Vec<_>>>()?;
        Ok(Overview {
            daemon_instance:self.shared.instance.clone(),
            observation_unix_ms:SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis().to_string(),
            graph_revision:self.shared.app.router.graph_revision().to_string(),
            config_revision:config_revision.to_string(),event_cursor:cursor.to_string(),
            serving:app.serving,shutting_down:app.shutting_down,streams,pool:self.runtime(principal)?,
            metrics:self.shared.app.latest_metrics().and_then(|m|serde_json::to_value(m.as_ref()).ok()).map(exact),operations,
            consistency:"independently sampled services; inspect observation time, generations and revisions".into(),
        })
    }
    fn stream_view(&self, id: &str, graph: &StreamGraph) -> Result<StreamView> {
        let d = graph
            .get(id)
            .cloned()
            .ok_or_else(|| error("not_found", "stream unknown"))?;
        let runtime = self.shared.app.registry.lock().unwrap().status(id).cloned();
        let sources = self.shared.app.status().sources;
        let source = sources.iter().find(|s| {
            s.stream_id == id
                && runtime
                    .as_ref()
                    .is_some_and(|r| r.generation == s.generation)
        });
        let state = runtime
            .as_ref()
            .map(|s| format!("{:?}", s.lifecycle))
            .unwrap_or_else(|| "Unavailable".into());
        let readiness = source
            .map(|s| format!("{:?}", s.readiness))
            .unwrap_or_else(|| {
                if runtime.as_ref().is_some_and(|r| r.is_ready()) {
                    String::from("Ready")
                } else {
                    String::from("Not ready; see dependency runtime and reason")
                }
            });
        let mut owners = self
            .shared
            .app
            .demand_registry
            .lock()
            .unwrap()
            .owners()
            .filter(|(_, s)| s.as_str() == id)
            .map(|(owner, _)| format!("{owner:?}"))
            .take(33)
            .collect::<Vec<_>>();
        if owners.len() > 32 {
            owners.truncate(32);
            owners.push("additional owners omitted".into());
        }
        let mut why = Vec::new();
        if d.desired.enabled && d.desired.activation_policy == ActivationPolicy::AlwaysOn {
            why.push("Always on".into());
        }
        if !owners.is_empty() {
            why.push("direct demand owners".into());
        }
        let dependents = graph
            .dependents(id)
            .map(|ids| ids.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for dep in &dependents {
            if self
                .shared
                .app
                .registry
                .lock()
                .unwrap()
                .status(dep)
                .is_some_and(|s| s.lifecycle.occupies_runtime())
            {
                why.push(format!("required by {dep}"));
            }
        }
        let snapshot = self.shared.app.router.integrity_view(id);
        let integrity = serde_json::json!({"revision":snapshot.revision.to_string(),"scope_truncated":snapshot.scope_truncated||snapshot.sources.len()>16,"history_truncated":snapshot.history_truncated,"sources":snapshot.sources.into_iter().take(16).map(|e|serde_json::json!({"source":e.id.source,"revision":e.id.revision.map(|r|r.to_string()),"coverage":e.coverage,"details_truncated":e.details_truncated,"coverage_history_unverified":e.coverage_history_unverified})).collect::<Vec<_>>()});
        let reason = runtime
            .as_ref()
            .and_then(|s| s.last_error.clone())
            .or_else(|| {
                if runtime.as_ref().is_some_and(|s| s.is_ready()) {
                    return None;
                }
                if !d.desired.enabled {
                    return Some("Definition disabled".into());
                }
                let required = self
                    .shared
                    .app
                    .demand_registry
                    .lock()
                    .unwrap()
                    .required_streams(graph)
                    .ok();
                if required.as_ref().is_some_and(|ids| !ids.contains(id)) {
                    return Some(
                        "No demand or AlwaysOn requirement; execution may remain stopped".into(),
                    );
                }
                if let StreamDefinition::Derived(request) = &d.definition {
                    let registry = self.shared.app.registry.lock().unwrap();
                    for input in request.input_specs() {
                        if let Some(status) = registry.status(&input.stream_id) {
                            if !status.is_ready() {
                                return Some(format!(
                                    "Required {:?} input {} is {:?} in generation {}",
                                    input.role,
                                    input.stream_id,
                                    status.lifecycle,
                                    status.generation.get()
                                ));
                            }
                        } else {
                            return Some(format!(
                                "Required {:?} input {} runtime is unavailable",
                                input.role, input.stream_id
                            ));
                        }
                    }
                }
                Some(
                    source
                        .map(|s| {
                            format!(
                                "Source readiness {:?}; initial data readiness pending={}",
                                s.readiness, s.initial_readiness_pending
                            )
                        })
                        .unwrap_or_else(|| {
                            "Waiting for execution readiness; inspect current operation and inputs"
                                .into()
                        }),
                )
            });
        Ok(StreamView {
            stream_id: id.into(),
            definition: d.clone(),
            runtime: state,
            generation: runtime
                .as_ref()
                .map(|s| s.generation.get().to_string())
                .unwrap_or_else(|| "unavailable".into()),
            readiness,
            reason,
            inputs: graph.inputs(id).unwrap_or_default().to_vec(),
            reference_inputs: match &d.definition {
                StreamDefinition::Derived(r) => r.reference_inputs.clone(),
                _ => vec![],
            },
            dependents,
            direct_owners: owners,
            why_running: why,
            last_data_age_ms: None,
            integrity,
        })
    }
    pub fn streams(&self, principal: &Principal, request: PageRequest) -> Result<Page<StreamView>> {
        principal.require(Role::Viewer)?;
        let graph = self.shared.app.router.graph();
        let ids = page(
            graph.stream_ids().map(str::to_owned).collect(),
            request,
            |id| id,
        )?;
        Ok(Page {
            items: ids
                .items
                .iter()
                .map(|id| self.stream_view(id, &graph))
                .collect::<Result<Vec<_>>>()?,
            next: ids.next,
        })
    }
    pub fn stream(&self, principal: &Principal, id: &str) -> Result<StreamView> {
        principal.require(Role::Viewer)?;
        self.stream_view(id, &self.shared.app.router.graph())
    }
    pub fn integrity(&self, principal: &Principal, id: &str) -> Result<serde_json::Value> {
        principal.require(Role::Viewer)?;
        if self.shared.app.router.graph().get(id).is_none() {
            return Err(error("not_found", "stream unknown"));
        }
        let (history, truncated) = self.shared.app.router.retained_integrity_view(id);
        Ok(exact(
            serde_json::json!({"current":self.shared.app.router.integrity_view(id),"retained_observations":history,"retained_history_truncated":truncated,"scope":"bounded retained Source observations; historical generations do not establish current continuity"}),
        ))
    }
    pub fn clients(&self, principal: &Principal) -> Result<serde_json::Value> {
        principal.require(Role::Viewer)?;
        let consumers = self.shared.app.router.consumers_view();
        let count = consumers.len();
        let leases = self
            .shared
            .state
            .lock()
            .unwrap()
            .leases
            .values()
            .map(lease_view)
            .collect::<Vec<_>>();
        Ok(
            serde_json::json!({"streaming_consumers":consumers.into_iter().take(128).collect::<Vec<_>>(),"consumers_truncated":count>128,"leases":leases,"management_sessions":"authenticated stateless requests; no durable client session"}),
        )
    }
    pub fn config(&self, principal: &Principal) -> Result<Vec<SettingView>> {
        principal.require(Role::Viewer)?;
        let state = self.shared.state.lock().unwrap();
        let workers = self.shared.app.processing.status().workers;
        Ok(vec![
            SettingView {
                name: "processing.workers".into(),
                effective: workers.running_workers.into(),
                desired: workers.requested_workers.into(),
                mutability: "Live".into(),
                saved: state.saved_workers == Some(workers.requested_workers),
            },
            SettingView {
                name: "control.management.overview_interval_ms".into(),
                effective: self
                    .shared
                    .observation_interval
                    .load(Ordering::Acquire)
                    .into(),
                desired: self
                    .shared
                    .observation_interval
                    .load(Ordering::Acquire)
                    .into(),
                mutability: "Live".into(),
                saved: state.saved_observation_interval
                    == Some(self.shared.observation_interval.load(Ordering::Acquire)),
            },
            SettingView {
                name: "tokio.workers".into(),
                effective: serde_json::Value::Null,
                desired: serde_json::Value::Null,
                mutability: "Engine restart required; runtime default".into(),
                saved: false,
            },
            SettingView {
                name: "recording".into(),
                effective: false.into(),
                desired: false.into(),
                mutability: "Read-only / unsupported; no faithful Router recording adapter".into(),
                saved: false,
            },
            SettingView {
                name: "other_general_configuration".into(),
                effective: serde_json::Value::Null,
                desired: serde_json::Value::Null,
                mutability: "Engine restart required; not editable through management".into(),
                saved: false,
            },
        ])
    }
    pub fn capabilities(&self, principal: &Principal) -> Result<Capabilities> {
        principal.require(Role::Viewer)?;
        let graph = self.shared.app.router.graph();
        let sources = graph
            .stream_ids()
            .filter_map(|id| match &graph.get(id)?.definition {
                StreamDefinition::Source(r) => Some(r.clone()),
                _ => None,
            })
            .take(128)
            .collect();
        Ok(Capabilities{daemon_instance:self.shared.instance.clone(),role:principal.role,actions:vec!["create_source","create_derived","edit_derived","set_desired","restart","delete","resize_workers","set_observation_interval","shutdown"].into_iter().map(str::to_owned).collect(),source_templates:sources,limitations:vec!["Source templates reflect authoritative restored definitions; absent instrument metadata is unavailable, never invented".into(),"definition persistence and runtime activation are separate outcomes".into(),"temporary leases and operation/idempotency history are bounded and process-local".into()]})
    }
}
