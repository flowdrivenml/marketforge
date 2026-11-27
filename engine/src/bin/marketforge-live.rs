#[cfg(unix)]
mod unix_application {
    use clap::Parser;
    use marketforge_engine::{
        live::{application::*, config::*, stream::persistence::*},
        shared::{persistence::postgres, workspace},
    };
    use std::{
        collections::{BTreeMap, BTreeSet},
        path::{Path, PathBuf},
        sync::Arc,
    };
    #[derive(Parser)]
    #[command(about = "Headless MarketForge Live; management API with management-api feature")]
    struct Arguments {
        /// Exchange and format directory (defaults to data/.config/live in the workspace).
        #[arg(long)]
        config_dir: Option<PathBuf>,
        /// Validate static configuration without database access, sockets or exchange I/O.
        #[arg(long)]
        validate_only: bool,
        #[arg(long, alias = "config")]
        general: Option<PathBuf>,
        /// Explicit empty resolved graph for service diagnostics; no database definitions restored.
        #[arg(long)]
        empty_definitions: bool,
        /// Restrict profiles prepared for validation or operation.
        #[arg(long, value_delimiter = ',')]
        profiles: Vec<String>,
        #[arg(long)]
        egress_id: Option<String>,
        /// Verified shared-weight quota, repeated: PROFILE=CAPACITY/WINDOW_SECONDS.
        #[arg(long)]
        rest_weight: Vec<String>,
    }
    pub fn main() -> std::process::ExitCode {
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(_) => {
                eprintln!("marketforge-live: runtime construction failed");
                return std::process::ExitCode::FAILURE;
            }
        };
        let code = match runtime.block_on(execute(Arguments::parse())) {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(reason) => {
                eprintln!("marketforge-live: {reason}");
                std::process::ExitCode::FAILURE
            }
        };
        // Never wait indefinitely in Runtime Drop for a noncooperative CPU join.
        // Pending cleanup after a reported deadline is an explicit nonzero exit.
        runtime.shutdown_timeout(std::time::Duration::from_secs(1));
        code
    }
    fn requires_weight(profile: &ExchangeConfig) -> bool {
        profile
            .rest
            .rate_limiting
            .as_ref()
            .is_some_and(|rate| rate.strategy == "weighted_shared_pool")
    }

    fn validate_weight_bindings(
        store: &ExchangeConfigStore,
        weights: &BTreeMap<String, (u64, u64)>,
    ) -> Result<(), String> {
        for name in weights.keys() {
            let profile = store
                .get(name)
                .ok_or_else(|| format!("REST quota references missing exchange profile {name}"))?;
            if !requires_weight(profile) {
                return Err(format!(
                    "{name}: profile does not accept a REST shared-weight quota"
                ));
            }
        }
        Ok(())
    }

    fn validate_profile(
        profile: &ExchangeConfig,
        formats: &FormatConfigStore,
    ) -> Result<(), String> {
        formats.validate_source_formats(profile)?;
        profile.validate_native_rest_mapping()?;
        for (channel, _) in profile.enabled_channels() {
            if !matches!(channel, "trades" | "depth") {
                tracing::debug!(profile = %profile.exchange.profile, channel, "enabled exchange channel has no Source execution capability");
            }
        }
        use marketforge_engine::live::config::exchange::TradeRecoveryCapability;
        match profile.trade_recovery_capability()? {
            TradeRecoveryCapability::Unsupported => {
                tracing::warn!(
                    profile = %profile.exchange.profile,
                    "trade recovery declared; automatic REST recovery is unsupported for this profile"
                );
            }
            TradeRecoveryCapability::Undeclared | TradeRecoveryCapability::Supported => {}
        }
        Ok(())
    }

    fn prepare_profiles(
        store: &ExchangeConfigStore,
        formats: &FormatConfigStore,
        recovery: &general::RecoveryDefaultsConfig,
        deployment: &general::ResolvedDeployment,
        required: &BTreeSet<String>,
        requested: &BTreeSet<String>,
    ) -> Result<ExchangeConfigStore, String> {
        // An explicitly restricted store must still contain every enabled persisted Source profile.
        if !requested.is_empty() {
            if let Some(name) = required.difference(requested).next() {
                return Err(format!(
                    "enabled persisted Source requires exchange profile {name}, excluded by --profiles"
                ));
            }
        }
        store.selected(required)?;
        let mut unavailable = BTreeMap::new();
        for (name, profile) in store.iter() {
            let candidate = BTreeSet::from([name.to_owned()]);
            let check = || {
                validate_profile(profile, formats)?;
                let selected = store.selected(&candidate)?;
                let limiters =
                    compile_limiters(&selected, &deployment.egress_id, &deployment.rest_weight)?;
                compile_websocket(&selected, &limiters, recovery, &BTreeMap::new())?;
                Ok::<_, String>(())
            };
            match check() {
                Ok(()) => {}
                Err(reason) if required.contains(name) || requested.contains(name) => {
                    return Err(format!("exchange profile {name} unavailable: {reason}"));
                }
                Err(reason) => {
                    tracing::warn!(profile = name, %reason, "exchange profile unavailable");
                    unavailable.insert(name.to_owned(), reason);
                }
            }
        }
        let operational = store.clone().without_unavailable(unavailable);
        if operational.is_empty() {
            return Err(
                "no operational exchange profiles; check the profile prerequisite diagnostics"
                    .into(),
            );
        }
        // The caller compiles the available profiles together so the limiter factory
        // can enforce shared quotas and reject cross-profile conflicts.
        Ok(operational)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn profile_preparation_requires_prerequisites_for_enabled_persisted_sources() {
            let base = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../data/.config/live"));
            let installed = ExchangeConfigStore::load_dir(base.join("exchanges")).unwrap();
            let formats = FormatConfigStore::load_dir(base.join("formats")).unwrap();
            let recovery = general::RecoveryDefaultsConfig {
                initial_backoff_ms: 100,
                max_backoff_seconds: 1,
                jitter: false,
            };
            let deployment = general::DeploymentConfig::default()
                .resolve(None, &[])
                .unwrap();
            let required = BTreeSet::from(["binance_spot".into()]);
            let error = prepare_profiles(
                &installed,
                &formats,
                &recovery,
                &deployment,
                &required,
                &BTreeSet::new(),
            )
            .unwrap_err();
            assert!(error.contains("binance_spot") && error.contains("capacity/window required"));

            let available = prepare_profiles(
                &installed,
                &formats,
                &recovery,
                &deployment,
                &BTreeSet::new(),
                &BTreeSet::new(),
            )
            .unwrap();
            let reason = available.require_operational("binance_spot").unwrap_err();
            assert!(reason.contains("capacity/window required"));
            assert!(available.get("bybit_spot").is_some());
            let error = prepare_profiles(
                &installed,
                &formats,
                &recovery,
                &deployment,
                &BTreeSet::from(["absent_spot".into()]),
                &BTreeSet::new(),
            )
            .unwrap_err();
            assert!(error.contains("missing exchange profile absent_spot"));
        }
    }

    async fn execute(args: Arguments) -> Result<(), String> {
        let invocation = std::env::current_dir().map_err(|e| e.to_string())?;
        let root = workspace::resolve(None, &invocation).map_err(|e| e.to_string())?;
        let config_dir = args
            .config_dir
            .unwrap_or_else(|| root.join("data/.config/live"));
        let general_path = args
            .general
            .unwrap_or_else(|| config_dir.join("general.toml"));
        let mut config = general::GeneralConfig::load(&general_path).map_err(|e| e.to_string())?;
        let socket_path = Path::new(&config.publication.socket_path);
        if socket_path.starts_with("data") {
            config.publication.socket_path = root
                .join(socket_path)
                .into_os_string()
                .into_string()
                .map_err(|_| "managed publication socket path is not UTF-8")?;
        }
        let level = match config.logging.level {
            general::LogLevel::Trace => tracing::Level::TRACE,
            general::LogLevel::Debug => tracing::Level::DEBUG,
            general::LogLevel::Info => tracing::Level::INFO,
            general::LogLevel::Warn => tracing::Level::WARN,
            general::LogLevel::Error => tracing::Level::ERROR,
        };
        tracing_subscriber::fmt()
            .json()
            .with_max_level(level)
            .with_writer(std::io::stderr)
            .try_init()
            .map_err(|_| "logging initialization failed")?;
        let exchanges = ExchangeConfigStore::load_dir(config_dir.join("exchanges"))
            .map_err(|e| e.to_string())?;
        let formats = Arc::new(
            FormatConfigStore::load_dir(config_dir.join("formats")).map_err(|e| e.to_string())?,
        );
        formats.validate_all()?;
        if config.recording.enabled {
            return Err("recording.enabled is unsupported: no faithful Router recorder adapter or durable identity allocator; PostgreSQL remains required for definitions".into());
        }
        let deployment = config
            .deployment
            .resolve(args.egress_id.as_deref(), &args.rest_weight)?;
        validate_weight_bindings(&exchanges, &deployment.rest_weight)?;
        let requested: BTreeSet<_> = args.profiles.into_iter().collect();
        let selected = if requested.is_empty() {
            exchanges
        } else {
            exchanges.selected(&requested)?
        };
        if args.validate_only {
            for (_, profile) in selected.iter() {
                validate_profile(profile, &formats)?;
            }
            let limiters =
                compile_limiters(&selected, &deployment.egress_id, &deployment.rest_weight)?;
            let _runtime = compile_websocket(
                &selected,
                &limiters,
                &config.defaults.recovery,
                &BTreeMap::new(),
            )?;
            tracing::info!(
                event = "configuration_validated",
                definitions_loaded = false,
                "static configuration valid; credentials, connections and persisted Source readiness were not checked"
            );
            return Ok(());
        }
        let pool = if args.empty_definitions {
            None
        } else {
            let database = postgres::PostgresConfig::resolve(
                &root,
                postgres::DatabaseEnvironment::Production,
                None,
            )?;
            Some(
                tokio::time::timeout(
                    std::time::Duration::from_secs(config.runtime.shutdown_timeout_seconds),
                    postgres::connect(&database),
                )
                .await
                .map_err(|_| "definition database connection deadline exceeded")?
                .map_err(
                    |_| "definition database connection failed (connection details redacted)",
                )?,
            )
        };
        let (definitions, dependencies) = if let Some(pool) = &pool {
            tokio::time::timeout(
                std::time::Duration::from_secs(config.runtime.shutdown_timeout_seconds),
                StreamDefinitionRepository::new(pool.clone())
                    .snapshot_bounded(config.runtime.application.max_definitions),
            )
            .await
            .map_err(|_| "definition restoration deadline exceeded")?
            .map_err(|_| "coherent definition restoration failed (database details redacted)")?
        } else {
            (vec![], vec![])
        };
        let required: BTreeSet<_> = definitions
            .iter()
            .filter(|d| d.desired.enabled)
            .filter_map(|d| match &d.definition {
                StreamDefinition::Source(r) => Some(format!(
                    "{}_{}",
                    r.exchange.as_str(),
                    r.market_category.as_str()
                )),
                _ => None,
            })
            .collect();
        let exchanges = Arc::new(prepare_profiles(
            &selected,
            &formats,
            &config.defaults.recovery,
            &deployment,
            &required,
            &requested,
        )?);
        let limiters =
            compile_limiters(&exchanges, &deployment.egress_id, &deployment.rest_weight)?;
        let websocket = compile_websocket(
            &exchanges,
            &limiters,
            &config.defaults.recovery,
            &BTreeMap::new(),
        )?;
        #[cfg(feature = "management-api")]
        let credentials =
            marketforge_engine::live::management::service::Credentials::load_workspace(
                &config.control.management,
                &root,
            )
            .map_err(|e| e.to_string())?;
        // Install both handlers before advertising readiness. A controller may
        // send a signal immediately after observing the serving notification.
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|_| "cannot install SIGTERM handler")?;
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .map_err(|_| "cannot install SIGINT handler")?;
        let options = LiveApplicationOptions {
            config,
            definitions,
            dependencies,
            exchanges,
            formats,
            limiters,
            websocket,
        };
        #[cfg(feature = "management-api")]
        let mut application = {
            use marketforge_engine::live::management::service::*;
            let store: Arc<dyn DefinitionStore> = if let Some(pool) = &pool {
                Arc::new(PostgresDefinitionStore(StreamDefinitionRepository::new(
                    pool.clone(),
                )))
            } else {
                Arc::new(UnavailableDefinitionStore)
            };
            let application =
                ManagedLiveApplication::start(options, store, credentials, Some(general_path))
                    .await
                    .map_err(|e| e.to_string())?;
            tracing::info!(event="management_serving",address=%application.management_address(),"authenticated loopback management API");
            application
        };
        #[cfg(not(feature = "management-api"))]
        let mut application = LiveApplication::start(options)
            .await
            .map_err(|e| e.to_string())?;
        tracing::info!(event="application_serving",prometheus=?application.handle().status().prometheus_address,"stream readiness is reported separately");
        #[cfg(unix)]
        {
            tokio::select! {_=interrupt.recv()=>(),_=terminate.recv()=>(),_=async {while !application.handle().status().shutting_down {tokio::time::sleep(std::time::Duration::from_millis(50)).await;}}=>()};
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c()
                .await
                .map_err(|_| "signal handler failed")?;
        }
        let mut report = application.shutdown().await;
        while !report.complete && report.cleanup_continues {
            tracing::warn!(event="shutdown_incomplete",errors=?report.errors,"cleanup remains owned; management observations remain available");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            report = application.shutdown().await;
        }

        if let Some(pool) = pool {
            postgres::shutdown(&pool).await;
        }
        tracing::info!(event="shutdown",complete=report.complete,cleanup_continues=report.cleanup_continues,errors=?report.errors);
        if report.complete {
            Ok(())
        } else {
            Err("incomplete shutdown; process exiting nonzero, executing CPU work cannot be forcibly stopped".into())
        }
    }
}
#[cfg(unix)]
fn main() -> std::process::ExitCode {
    unix_application::main()
}
#[cfg(not(unix))]
fn main() -> std::process::ExitCode {
    eprintln!("marketforge-live requires Unix for its configured streaming service");
    std::process::ExitCode::FAILURE
}
