use std::{
    fs,
    path::{Path, PathBuf},
};

use clap::{ArgGroup, Parser};

use {
    marketforge_engine::historical::config::{FormatCatalog, load_normalization_job},
    marketforge_engine::historical::job::ProcessingJob,
    marketforge_engine::historical::normalize::ProcessingConfig,
    marketforge_engine::historical::normalize::config::{project_root, resolve_path},
    marketforge_engine::historical::normalize::execute_processing_job,
    marketforge_engine::historical::normalize::load_processing_config,
    marketforge_engine::historical::normalize::session::execute_processing_session,
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
        .args(["job", "jobs_dir"])
))]
struct Args {
    /// Execute one processing job.
    #[arg(long, conflicts_with = "jobs_dir")]
    job: Option<PathBuf>,

    /// Execute all processing jobs in a directory.
    #[arg(long, conflicts_with = "job")]
    jobs_dir: Option<PathBuf>,

    /// Global processing configuration.
    #[arg(long, default_value = "data/.config/historical/normalize.toml")]
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let project_root = project_root(&args.project_root)?;

    let config_path = resolve_path(&project_root, &args.config);

    let config = load_processing_config(&config_path, &project_root)?;
    let catalog = FormatCatalog::load(project_root.join("data/.config/historical"))?;

    // -------------------------------------------------------------
    // Single-job execution
    // -------------------------------------------------------------

    if let Some(path) = &args.job {
        let job_path = resolve_path(&project_root, path);

        let mut job = load_normalization_job(&job_path, &catalog, &config, &project_root)?;

        if let Some(output_root) = &args.output_root {
            let output_root = resolve_path(&project_root, output_root);

            redirect_outputs(std::slice::from_mut(&mut job), &output_root)?;
        }

        #[cfg(feature = "monitor-tui")]
        let result = if args.tui {
            execute_processing_with_tui(job, config)?
        } else {
            execute_processing_job(&job, &config)?
        };

        #[cfg(not(feature = "monitor-tui"))]
        let result = execute_processing_job(&job, &config)?;

        println!("{}", serde_json::to_string(&result)?);

        return Ok(());
    }

    // -------------------------------------------------------------
    // Multi-job session
    // -------------------------------------------------------------

    let directory = args.jobs_dir.as_ref().expect("jobs-dir required");

    let directory = resolve_path(&project_root, directory);

    let mut jobs = load_jobs(&directory, &project_root, &catalog, &config)?;

    if let Some(output_root) = &args.output_root {
        let output_root = resolve_path(&project_root, output_root);

        redirect_outputs(&mut jobs, &output_root)?;
    }

    #[cfg(feature = "monitor-tui")]
    let result = if args.tui {
        execute_processing_session_with_tui(jobs, config)?
    } else {
        execute_processing_session(&jobs, &config)?
    };

    #[cfg(not(feature = "monitor-tui"))]
    let result = execute_processing_session(&jobs, &config)?;

    // Preserve machine-readable JSON output.
    println!("{}", serde_json::to_string(&result.jobs)?);

    Ok(())
}

#[cfg(test)]
mod runtime_cli_tests {
    use super::*;
    use tempfile::TempDir;

    const EXAMPLE: &str = include_str!("../../../docs/historical/normalize-job-v3.example.json");
    const RUNTIME: &str = include_str!("../../../data/.config/historical/normalize.toml");

    #[test]
    fn cli_defaults_and_job_directory_loading_are_configuration_only() {
        let args = Args::try_parse_from(["marketforge-normalize", "--job", "job.json"]).unwrap();
        assert_eq!(
            args.config,
            PathBuf::from("data/.config/historical/normalize.toml")
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
