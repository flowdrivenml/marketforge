use std::{
    fs,
    path::{Path, PathBuf},
};

use clap::{ArgGroup, Parser};

use {
    marketforge_engine::historical::job::ProcessingJob,
    marketforge_engine::historical::job::load_processing_job,
    marketforge_engine::historical::normalize::execute_processing_job,
    marketforge_engine::historical::normalize::load_processing_config,
    marketforge_engine::historical::normalize::session::execute_processing_session,
};

#[cfg(feature = "monitor-tui")]
use marketforge_engine::tui::processing::{
    execute_processing_session_with_tui, execute_processing_with_tui,
};

#[derive(Debug, Parser)]
#[command(name = "marketforge-process")]
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
    #[arg(long, default_value = "data/.jobs/processing.json")]
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
// Load processing jobs
// -----------------------------------------------------------------------------

fn load_jobs(directory: &Path) -> Result<Vec<ProcessingJob>, Box<dyn std::error::Error>> {
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
        jobs.push(load_processing_job(&path)?);
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

    let config = load_processing_config(&args.config, &args.project_root)?;

    // -------------------------------------------------------------
    // Single-job execution
    // -------------------------------------------------------------

    if let Some(path) = &args.job {
        let mut job = load_processing_job(path)?;

        if let Some(output_root) = &args.output_root {
            redirect_outputs(std::slice::from_mut(&mut job), output_root)?;
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

    let mut jobs = load_jobs(directory)?;

    if let Some(output_root) = &args.output_root {
        redirect_outputs(&mut jobs, output_root)?;
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
