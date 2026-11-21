use std::{
    fs,
    path::{Path, PathBuf},
};

use clap::{ArgGroup, Parser};

use {
    marketforge_engine::historical::config::{
        FormatCatalog, load_normalization_job, read_prepared_batch,
    },
    marketforge_engine::historical::job::ProcessingJob,
    marketforge_engine::historical::normalize::ProcessingConfig,
    marketforge_engine::historical::normalize::config::{project_root, resolve_path},
    marketforge_engine::historical::normalize::execute_processing_job,
    marketforge_engine::historical::normalize::load_processing_config,
    marketforge_engine::historical::normalize::session::execute_processing_session_reporting,
    marketforge_engine::historical::normalize::{
        ProcessingResult, ProcessingStatus,
        response::{NormalizationError, NormalizationResponse},
    },
};

#[cfg(feature = "monitor-tui")]
use marketforge_engine::tui::processing::{
    execute_prepared_session_with_tui, execute_processing_session_with_tui,
    execute_processing_with_tui,
};

#[derive(Debug, Parser)]
#[command(name = "marketforge-normalize")]
#[command(group(
    ArgGroup::new("input")
        .required(true)
        .args(["batch", "job", "jobs_dir", "validate_profiles", "validate_batch", "validate_dataset", "resolve_batch", "protocol"])
))]
struct Args {
    /// Resolve profiles and bounded archive family identities without normalizing.
    #[arg(long)]
    resolve_batch: Option<PathBuf>,
    /// Validate committed manifest and resolved-job semantics without processing.
    #[arg(long)]
    validate_dataset: Option<PathBuf>,
    /// Report the nonexecuting orchestration protocol.
    #[arg(long)]
    protocol: bool,
    /// Compile a historical configuration root's profile catalogue without processing.
    #[arg(long)]
    validate_profiles: Option<PathBuf>,
    /// Validate a prepared batch with the production parser without opening archives.
    #[arg(long)]
    validate_batch: Option<PathBuf>,
    /// Execute a schema-version-4 prepared normalization batch.
    #[arg(long)]
    batch: Option<PathBuf>,
    /// Execute one processing job.
    #[arg(long, conflicts_with = "jobs_dir")]
    job: Option<PathBuf>,

    /// Execute all processing jobs in a directory.
    #[arg(long, conflicts_with = "job")]
    jobs_dir: Option<PathBuf>,

    /// Historical configuration root containing the effective profiles catalogue.
    #[arg(long)]
    profiles_root: Option<PathBuf>,

    /// Global processing configuration.
    #[arg(long, default_value = "data/.config/historical/general.toml")]
    config: PathBuf,

    /// Project root used to resolve processing configuration.
    #[arg(long, default_value = ".")]
    project_root: PathBuf,

    /// Write the authoritative execution response to a fresh file.
    #[arg(long, requires = "batch")]
    result_file: Option<PathBuf>,

    /// Redirect dataset and staging outputs to a separate directory.
    #[arg(long)]
    output_root: Option<PathBuf>,

    /// Display live processing statistics.
    #[cfg(feature = "monitor-tui")]
    #[arg(long)]
    tui: bool,
}

// -----------------------------------------------------------------------------
// Path resolution
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Load processing jobs
// -----------------------------------------------------------------------------

fn load_jobs(
    directory: &Path,
    root: &Path,
    catalog: &FormatCatalog,
    config: &ProcessingConfig,
) -> Result<Vec<ProcessingJob>, Box<dyn std::error::Error>> {
    let mut paths = fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;

    paths.retain(|path| {
        path.is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
    });

    paths.sort();

    let mut jobs = Vec::with_capacity(paths.len());

    for path in paths {
        jobs.push(load_normalization_job(&path, catalog, config, root)?);
    }

    if jobs.is_empty() {
        return Err(format!("no processing jobs found in {}", directory.display(),).into());
    }

    Ok(jobs)
}

// -----------------------------------------------------------------------------
// Redirect output paths for safe testing
// -----------------------------------------------------------------------------

fn redirect_outputs(
    jobs: &mut [ProcessingJob],
    output_root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Refuse to reuse an existing output root.
    if output_root.exists() {
        return Err(format!("output root already exists: {}", output_root.display(),).into());
    }

    for job in jobs {
        let dataset = format!("dataset-{}", job.dataset_id.0);

        job.output.dataset_path = output_root.join("processed").join(&dataset);

        job.output.staging_path = output_root.join("staging").join(&dataset);
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// CLI
// -----------------------------------------------------------------------------

fn bind_submission(jobs: &mut [ProcessingJob], fingerprint: &str) {
    for task in jobs.iter_mut().flat_map(|job| &mut job.tasks) {
        task.raw_schema["marketforge_submission_sha256"] = serde_json::json!(fingerprint);
    }
}

fn execute(
    args: &Args,
    run_id: &mut Option<String>,
    total: &mut usize,
    results: &mut Vec<ProcessingResult>,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = project_root(&args.project_root)?;
    // Identify batch/run before configuration failure, without opening sources.
    let submission_sha256 = args
        .batch
        .as_deref()
        .map(|path| {
            marketforge_engine::historical::normalize::manifest::file_sha256(&resolve_path(
                &root, path,
            ))
        })
        .transpose()?;
    let batch = args
        .batch
        .as_ref()
        .map(|p| read_prepared_batch(p, &root))
        .transpose()?;
    if let (Some(path), Some(fingerprint)) = (&args.batch, &submission_sha256)
        && marketforge_engine::historical::normalize::manifest::file_sha256(&resolve_path(
            &root, path,
        ))? != *fingerprint
    {
        return Err("prepared submission changed during parsing".into());
    }
    if let Some(batch) = &batch {
        *run_id = Some(batch.run_id.clone());
        *total = batch
            .jobs
            .iter()
            .filter(|job| !job.tasks.is_empty())
            .count();
    }
    let config = load_processing_config(resolve_path(&root, &args.config), &root)?;
    let catalog = FormatCatalog::load(
        args.profiles_root
            .clone()
            .unwrap_or_else(|| root.join("data/.config/historical")),
    )?;
    #[cfg(feature = "monitor-tui")]
    if args.tui
        && let Some(batch) = batch.as_ref()
    {
        let prepared = batch.clone();
        let preparation_config = config.clone();
        let fingerprint = submission_sha256
            .clone()
            .expect("prepared batch fingerprint");
        let output_root = args
            .output_root
            .as_ref()
            .map(|path| resolve_path(&root, path));
        let result = execute_prepared_session_with_tui(batch, config, move |reporter| {
            let mut jobs = prepared.resolve_jobs_with_progress(
                &catalog,
                &preparation_config,
                &root,
                reporter,
            )?;
            bind_submission(&mut jobs, &fingerprint);
            if let Some(path) = output_root {
                redirect_outputs(&mut jobs, &path).map_err(|error| {
                    marketforge_engine::shared::error::MarketForgeError::InvalidConfiguration(
                        error.to_string(),
                    )
                })?;
            }
            Ok(jobs)
        })?;
        *total = result.jobs.len();
        results.extend(result.jobs);
        return Ok(());
    }
    let mut jobs = if let Some(batch) = batch {
        batch.resolve_jobs(&catalog, &config, &root)?
    } else if let Some(path) = &args.job {
        vec![load_normalization_job(
            resolve_path(&root, path),
            &catalog,
            &config,
            &root,
        )?]
    } else {
        load_jobs(
            &resolve_path(&root, args.jobs_dir.as_ref().expect("validated input mode")),
            &root,
            &catalog,
            &config,
        )?
    };
    if let Some(fingerprint) = &submission_sha256 {
        bind_submission(&mut jobs, fingerprint);
    }
    *total = jobs.len();
    if let Some(path) = &args.output_root {
        redirect_outputs(&mut jobs, &resolve_path(&root, path))?;
    }
    #[cfg(feature = "monitor-tui")]
    if args.tui {
        // Interactive rendering owns stdout; the final JSON document is emitted
        // after the terminal is restored. Machine mode never enters the TUI.
        if jobs.len() == 1 {
            results.push(execute_processing_with_tui(jobs.remove(0), config)?);
        } else {
            results.extend(execute_processing_session_with_tui(jobs, config)?.jobs);
        }
        return Ok(());
    }
    if jobs.len() == 1 {
        results.push(execute_processing_job(&jobs[0], &config)?);
    } else {
        execute_processing_session_reporting(&jobs, &config, &mut |r| results.push(r.clone()))?;
    }
    Ok(())
}
fn main() -> std::process::ExitCode {
    let args = Args::parse();
    if args.protocol
        || args.validate_profiles.is_some()
        || args.validate_batch.is_some()
        || args.validate_dataset.is_some()
        || args.resolve_batch.is_some()
    {
        let result = (|| -> Result<serde_json::Value, Box<dyn std::error::Error>> {
            if args.protocol {
                return Ok(
                    serde_json::json!({"schema_version":1,"kind":"forge_engine_protocol","batch_schema_versions":[4],"result_file":true,"profile_schema_versions":[2]}),
                );
            }
            if let Some(path) = &args.validate_dataset {
                use marketforge_engine::historical::normalize::manifest::DatasetManifest;
                let manifest: DatasetManifest =
                    serde_json::from_slice(&fs::read(path.join("manifest.json"))?)?;
                manifest.validate_streams()?;
                let files =
                    marketforge_engine::historical::normalize::manifest::inspect_parquet_files(
                        path,
                    )?;
                let auxiliary =
                    marketforge_engine::historical::normalize::manifest::inspect_auxiliary_files(
                        path,
                    )?;
                if files != manifest.files
                    || auxiliary != manifest.auxiliary_files
                    || manifest.integrity_status != manifest.integrity_evaluation.status
                {
                    return Err(
                        "committed file inventory or integrity evidence differs from manifest"
                            .into(),
                    );
                }
                let job = marketforge_engine::historical::job::load_committed_processing_job(path)?;
                if manifest.job_id != job.job_id.0
                    || manifest.dataset_id != job.dataset_id.0
                    || job.output.dataset_path != *path
                {
                    return Err("dataset publication identity/path mismatch".into());
                }
                return Ok(
                    serde_json::json!({"schema_version":1,"kind":"dataset_validation","status":"valid"}),
                );
            }
            if let Some(path) = &args.validate_profiles {
                let catalog = FormatCatalog::load(path)?;
                return Ok(
                    serde_json::json!({"schema_version":1,"kind":"profile_validation","profiles":catalog.profiles().map(|p| serde_json::json!({"id":p.definition.id,"fingerprint":p.fingerprint,"unsupported_reason":p.definition.unsupported_reason,"format_code":p.definition.format_code})).collect::<Vec<_>>()}),
                );
            }
            let root = project_root(&args.project_root)?;
            let batch = read_prepared_batch(
                args.validate_batch
                    .as_ref()
                    .or(args.resolve_batch.as_ref())
                    .unwrap(),
                &root,
            )?;
            let config = load_processing_config(resolve_path(&root, &args.config), &root)?;
            let catalog = FormatCatalog::load(
                args.profiles_root
                    .clone()
                    .unwrap_or_else(|| root.join("data/.config/historical")),
            )?;
            if args.resolve_batch.is_some() {
                let jobs = batch.resolve_jobs(&catalog, &config, &root)?;
                let mut instruments = std::collections::BTreeMap::new();
                let mut members = Vec::new();
                for task in jobs.iter().flat_map(|job| &job.tasks) {
                    instruments.insert(
                        task.instrument_id,
                        task.raw_schema["marketforge_source"]["instrument"].clone(),
                    );
                    members.push(serde_json::json!({"symbol":task.symbol,"instrument_id":task.instrument_id,"archive_member":task.archive_member}));
                }
                return Ok(
                    serde_json::json!({"schema_version":1,"kind":"normalization_resolution","instrument_namespace":batch.instrument_namespace,"instruments":instruments.into_values().collect::<Vec<_>>(),"members":members,"normalization_executed":false}),
                );
            }
            // Concrete resolution uses the same compiler as execution and never
            // opens archives. Family discovery is deferred to actual execution.
            for job in &batch.jobs {
                let mut concrete = job.clone();
                concrete.tasks.retain(|task| task.scope.is_none());
                if !concrete.tasks.is_empty() {
                    batch.resolve_job(&concrete, &catalog, &config, &root)?;
                }
            }
            Ok(
                serde_json::json!({"schema_version":1,"kind":"batch_validation","status":"valid","run_id":batch.run_id,"level":"production parser and metadata; archives not opened"}),
            )
        })();
        return match result {
            Ok(value) => {
                println!("{value}");
                std::process::ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("Validation failed: {error}");
                std::process::ExitCode::FAILURE
            }
        };
    }
    let mut run_id = None;
    let mut total = 0;
    let mut jobs = vec![];
    let execution = execute(&args, &mut run_id, &mut total, &mut jobs);
    let interrupted = execution
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<std::io::Error>())
        .is_some_and(|error| error.kind() == std::io::ErrorKind::Interrupted);
    let error = execution.err().map(|e| {
        eprintln!("Normalization failed: {e}");
        NormalizationError::from_error(e.as_ref())
    });
    let response = NormalizationResponse::new(run_id, total, jobs, error);
    let success = response.status == ProcessingStatus::Complete;
    match serde_json::to_string(&response) {
        Ok(json) => {
            use std::io::Write;
            if let Some(path) = &args.result_file {
                let result = (|| -> std::io::Result<()> {
                    let mut file = fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(path)?;
                    writeln!(file, "{json}")?;
                    file.sync_all()?;
                    fs::File::open(
                        path.parent()
                            .filter(|p| !p.as_os_str().is_empty())
                            .unwrap_or(Path::new(".")),
                    )?
                    .sync_all()
                })();
                if let Err(error) = result {
                    eprintln!("cannot persist normalization response: {error}");
                    return std::process::ExitCode::FAILURE;
                }
            }
            if writeln!(std::io::stdout().lock(), "{json}").is_err() {
                return std::process::ExitCode::FAILURE;
            }
        }
        Err(e) => {
            eprintln!("cannot serialize normalization response: {e}");
            return std::process::ExitCode::FAILURE;
        }
    }
    if interrupted {
        std::process::ExitCode::from(130)
    } else if success {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::FAILURE
    }
}

#[cfg(test)]
mod runtime_cli_tests {
    use super::*;
    use tempfile::TempDir;

    const EXAMPLE: &str = include_str!("../../../docs/historical/normalize-job-v3.example.json");
    const RUNTIME: &str = include_str!("../shared/historical/config/fixtures/runtime.toml");

    #[test]
    fn family_resolution_is_an_exclusive_nonprocessing_input() {
        let args = Args::try_parse_from(["marketforge-normalize", "--resolve-batch", "batch.json"])
            .unwrap();
        assert_eq!(args.resolve_batch, Some(PathBuf::from("batch.json")));
        assert!(args.batch.is_none() && args.job.is_none());
        assert!(
            Args::try_parse_from([
                "marketforge-normalize",
                "--resolve-batch",
                "batch.json",
                "--job",
                "job.json",
            ])
            .is_err()
        );
    }

    #[test]
    fn cli_defaults_and_job_directory_loading_are_configuration_only() {
        let args = Args::try_parse_from(["marketforge-normalize", "--job", "job.json"]).unwrap();
        assert_eq!(
            args.config,
            PathBuf::from("data/.config/historical/general.toml")
        );
        assert!(
            Args::try_parse_from([
                "marketforge-normalize",
                "--job",
                "one",
                "--jobs-dir",
                "many"
            ])
            .is_err()
        );
        let root = TempDir::new().unwrap();
        let directory = root.path().join("jobs");
        fs::create_dir(&directory).unwrap();
        fs::write(root.path().join("runtime.toml"), RUNTIME).unwrap();
        let config = load_processing_config("runtime.toml", root.path()).unwrap();
        let catalog = FormatCatalog::load(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/historical"),
        )
        .unwrap();
        assert!(load_jobs(&directory, root.path(), &catalog, &config).is_err());
        fs::write(directory.join("b.json"), EXAMPLE).unwrap();
        let mut other: serde_json::Value = serde_json::from_str(EXAMPLE).unwrap();
        other["job_id"] = 1000.into();
        fs::write(
            directory.join("a.json"),
            serde_json::to_vec(&other).unwrap(),
        )
        .unwrap();
        fs::write(directory.join("ignored.txt"), "not JSON").unwrap();
        let jobs = load_jobs(&directory, root.path(), &catalog, &config).unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_id.0, 1000);
        for job in jobs {
            assert_eq!(job.resources, config.resources);
            assert_eq!(job.integrity_policy, config.integrity_policy);
            assert_eq!(
                job.tasks[0].input_path,
                root.path().join("data/raw/BTCUSDT.csv.gz")
            );
            assert!(!job.output.staging_path.exists());
        }
    }
}
