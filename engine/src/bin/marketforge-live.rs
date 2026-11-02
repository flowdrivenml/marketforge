#[cfg(unix)]
mod unix_application {
    use clap::Parser;
    use marketforge_engine::{
        live::{application::*, config::*, stream::persistence::*},
        shared::persistence::postgres,
    };
    use std::{
        collections::BTreeMap,
        path::PathBuf,
        sync::Arc,
    };
    #[cfg(not(feature="management-api"))]
    use std::collections::BTreeSet;
    #[derive(Parser)]
    #[command(about = "Headless MarketForge Live; management API with management-api feature")]
    struct Arguments {
        #[arg(long, default_value = "data/.config/live")]
        config_dir: PathBuf,
        /// Validate static configuration without database access, sockets or exchange I/O.
        #[arg(long)]
        validate_only: bool,
        #[arg(long,alias="config")]
        general: Option<PathBuf>,
        /// Explicit empty resolved graph for service diagnostics; no database definitions restored.
        #[arg(long)]
        empty_definitions: bool,
        /// Restrict static validation profiles; running profiles come from definitions.
        #[arg(long, value_delimiter = ',')]
        profiles: Vec<String>,
        #[arg(long, default_value = "local-egress")]
        egress_id: String,
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
    async fn execute(args: Arguments) -> Result<(), String> {
        let general_path=args.general.clone().unwrap_or_else(|| args.config_dir.join("general.toml"));
        let config=general::GeneralConfig::load(&general_path).map_err(|e|e.to_string())?;
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
        let exchanges = ExchangeConfigStore::load_dir(args.config_dir.join("exchanges"))
            .map_err(|e| e.to_string())?;
        let formats = Arc::new(
            FormatConfigStore::load_dir(args.config_dir.join("formats"))
                .map_err(|e| e.to_string())?,
        );
        formats.validate_all()?;
        if config.recording.enabled {
            return Err("recording.enabled is unsupported: no faithful Router recorder adapter or durable identity allocator; PostgreSQL remains required for definitions".into());
        }
        if config.metrics.prometheus.enabled && !config.metrics.enabled {
            return Err("Prometheus requires metrics.enabled".into());
        }
        let mut weights = BTreeMap::new();
        for arg in args.rest_weight {
            let (name, value) = arg
                .split_once('=')
                .ok_or("expected PROFILE=CAPACITY/WINDOW_SECONDS")?;
            let (capacity, window) = value.split_once('/').ok_or("expected capacity/window")?;
            weights.insert(
                name.to_string(),
                (
                    capacity.parse().map_err(|_| "invalid quota capacity")?,
                    window.parse().map_err(|_| "invalid quota window")?,
                ),
            );
        }
        if args.validate_only {
            let selected = if args.profiles.is_empty() {
                exchanges
            } else {
                exchanges.selected(&args.profiles.into_iter().collect())?
            };
            let limiters = compile_limiters(&selected, &args.egress_id, &weights)?;
            let _runtime = compile_websocket(
                &selected,
                &limiters,
                &config.defaults.recovery,
                &BTreeMap::new(),
            )?;
            tracing::info!(
                event = "configuration_validated",
                definitions_loaded = false,
                "static configuration valid; persisted graph not inspected"
            );
            return Ok(());
        }
        let pool = if args.empty_definitions {
            None
        } else {
            let database=postgres::PostgresConfig::from_env().map_err(|_|"MARKETFORGE_DATABASE_URL is required for definition restoration, independently of recording")?;
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
        #[cfg(not(feature="management-api"))]
        let profiles: BTreeSet<_> = definitions
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
        #[cfg(feature="management-api")]
        let exchanges=Arc::new(if args.profiles.is_empty(){exchanges}else{exchanges.selected(&args.profiles.into_iter().collect())?});
        #[cfg(not(feature="management-api"))]
        let exchanges = Arc::new(if profiles.is_empty() {
            if args.profiles.is_empty() {
                exchanges
            } else {
                exchanges.selected(&args.profiles.into_iter().collect())?
            }
        } else {
            exchanges.selected(&profiles)?
        });
        let limiters = compile_limiters(&exchanges, &args.egress_id, &weights)?;
        let websocket = compile_websocket(
            &exchanges,
            &limiters,
            &config.defaults.recovery,
            &BTreeMap::new(),
        )?;
        #[cfg(feature="management-api")]
        let credentials=marketforge_engine::live::management::Credentials::load(&config.control.management).map_err(|e|e.to_string())?;
        // Install both handlers before advertising readiness. A controller may
        // send a signal immediately after observing the serving notification.
        let mut terminate=tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).map_err(|_|"cannot install SIGTERM handler")?;
        let mut interrupt=tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).map_err(|_|"cannot install SIGINT handler")?;
        let options=LiveApplicationOptions {config,definitions,dependencies,exchanges,formats,limiters,websocket};
        #[cfg(feature="management-api")]
        let mut application={
            use marketforge_engine::live::management::*;
            let store:Arc<dyn DefinitionStore>=if let Some(pool)=&pool{Arc::new(PostgresDefinitionStore(StreamDefinitionRepository::new(pool.clone())))}else{Arc::new(UnavailableDefinitionStore)};
            let application=ManagedLiveApplication::start(options,store,credentials,Some(general_path)).await.map_err(|e|e.to_string())?;
            tracing::info!(event="management_serving",address=%application.management_address(),"authenticated loopback management API");application
        };
        #[cfg(not(feature="management-api"))]
        let mut application=LiveApplication::start(options).await.map_err(|e|e.to_string())?;
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
            report=application.shutdown().await;
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
