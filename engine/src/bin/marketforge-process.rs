use std::path::PathBuf;

use clap::Parser;

use marketforge_engine::{
    job::{load_processing_job, validate_processing_job},
    process::ProcessingResult,
};

#[derive(Debug, Parser)]
#[command(name = "marketforge-process")]
struct Args {
    #[arg(long)]
    job: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let job = load_processing_job(&args.job)?;
    validate_processing_job(&job)?;

    let result = ProcessingResult::complete(job.job_id, job.dataset_id);

    println!("{}", serde_json::to_string(&result)?);

    Ok(())
}
