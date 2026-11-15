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
    execute_processing_session_with_tui, execute_processing_with_tui,
};

#[derive(Debug, Parser)]
#[command(name = "marketforge-normalize")]
#[command(group(
    ArgGroup::new("input")
        .required(true)
        .args(["batch", "job", "jobs_dir"])
))]
struct Args {
    /// Execute a schema-version-4 prepared normalization batch.
    #[arg(long)]
    batch: Option<PathBuf>,
    /// Execute one processing job.
    #[arg(long, conflicts_with = "jobs_dir")]
    job: Option<PathBuf>,

    /// Execute all processing jobs in a directory.
    #[arg(long, conflicts_with = "job")]
    jobs_dir: Option<PathBuf>,

    /// Global processing configuration.
    #[arg(long, default_value = "data/.config/historical/general.toml")]
    config: PathBuf,

    /// Project root used to resolve processing configuration.
    #[arg(long, default_value = ".")]
    project_root: PathBuf,

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

fn execute(
    args: &Args,
    run_id: &mut Option<String>,
    total: &mut usize,
    results: &mut Vec<ProcessingResult>,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = project_root(&args.project_root)?;
    // Identify batch/run before configuration failure, without opening sources.
    let batch = args
        .batch
        .as_ref()
        .map(|p| read_prepared_batch(p, &root))
        .transpose()?;
    if let Some(batch) = &batch {
        *run_id = Some(batch.run_id.clone());
        *total = batch.jobs.len();
    }
    let config = load_processing_config(resolve_path(&root, &args.config), &root)?;
    let catalog = FormatCatalog::load(root.join("data/.config/historical"))?;
    let mut jobs = if let Some(batch) = batch {
        batch
            .jobs
            .iter()
            .map(|job| batch.resolve_job(job, &catalog, &config, &root))
            .collect::<marketforge_engine::shared::error::Result<Vec<_>>>()?
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
    let mut run_id = None;
    let mut total = 0;
    let mut jobs = vec![];
    let error = execute(&args, &mut run_id, &mut total, &mut jobs)
        .err()
        .map(|e| {
            eprintln!("Normalization failed: {e}");
            NormalizationError {
                code: "execution_error".into(),
                message: e.to_string(),
            }
        });
    let response = NormalizationResponse::new(run_id, total, jobs, error);
    let success = response.status == ProcessingStatus::Complete;
    match serde_json::to_string(&response) {
        Ok(json) => {
            use std::io::Write;
            if writeln!(std::io::stdout().lock(), "{json}").is_err() {
                return std::process::ExitCode::FAILURE;
            }
        }
        Err(e) => {
            eprintln!("cannot serialize normalization response: {e}");
            return std::process::ExitCode::FAILURE;
        }
    }
    if success {
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
    const RUNTIME: &str = include_str!("../../../data/.config/historical/general.toml");

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
