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
// Path resolution
// -----------------------------------------------------------------------------

fn resolve_path(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

fn resolve_job_paths(job: &mut ProcessingJob, root: &Path) {
    for task in &mut job.tasks {
        task.input_path = resolve_path(root, &task.input_path);
    }

    job.output.dataset_path = resolve_path(root, &job.output.dataset_path);

    job.output.staging_path = resolve_path(root, &job.output.staging_path);

    job.resources.scratch_path = resolve_path(root, &job.resources.scratch_path);
}

// -----------------------------------------------------------------------------
// Load processing jobs
// -----------------------------------------------------------------------------

fn load_jobs(
    directory: &Path,
    root: &Path,
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
        let mut job = load_processing_job(&path)?;
        resolve_job_paths(&mut job, root);
        jobs.push(job);
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

    let project_root = args.project_root.canonicalize().map_err(|error| {
        format!(
            "invalid project root {}: {error}",
            args.project_root.display()
        )
    })?;

    let config_path = resolve_path(&project_root, &args.config);

    let config = load_processing_config(&config_path, &project_root)?;

    // -------------------------------------------------------------
    // Single-job execution
    // -------------------------------------------------------------

    if let Some(path) = &args.job {
        let job_path = resolve_path(&project_root, path);

        let mut job = load_processing_job(&job_path)?;

        resolve_job_paths(&mut job, &project_root);

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

    let mut jobs = load_jobs(&directory, &project_root)?;

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
