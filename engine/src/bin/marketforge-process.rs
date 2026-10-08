use std::path::PathBuf;

use clap::Parser;

use marketforge_engine::{
    job::load_processing_job,
    process::{execute_processing_job, load_processing_config},
};

#[derive(Debug, Parser)]
#[command(name = "marketforge-process")]
struct Args {
    #[arg(long)]
    job: PathBuf,

    #[arg(long, default_value = "data/.jobs/processing.json")]
    config: PathBuf,

    #[arg(long, default_value = ".")]
    project_root: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Load the processing job.
    let job = load_processing_job(&args.job)?;

    // Load authoritative global resources.
    let config = load_processing_config(&args.config, &args.project_root)?;

    // Execute using global resources, not manifest resources.
    let result = execute_processing_job(&job, &config)?;

    // stdout is reserved for machine-readable JSON.
    println!("{}", serde_json::to_string(&result)?);

    Ok(())
}
