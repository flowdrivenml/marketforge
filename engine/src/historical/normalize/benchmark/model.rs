use crate::{
    historical::{job::ProcessingJob, normalize::response::NormalizationResponse},
    shared::historical::ProcessingConfig,
};
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum Scheduler {
    PerJob,
    Global,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    Metadata,
    Full,
}
#[derive(Debug, Parser)]
#[command(
    name = "marketforge-normalize-benchmark",
    version,
    subcommand_negates_reqs = true,
    args_conflicts_with_subcommands = true
)]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<AnalysisCommand>,
    #[arg(long, required_unless_present = "execute_trial")]
    pub input: Option<PathBuf>,
    #[arg(long, value_delimiter = ',', default_value = "1")]
    pub workers: Vec<usize>,
    #[arg(long, default_value_t = 3)]
    pub repetitions: usize,
    #[arg(long, default_value_t = 0)]
    pub warmups: usize,
    #[arg(long, value_enum, default_value = "global")]
    pub scheduler: Scheduler,
    #[arg(long, value_enum, default_value = "metadata")]
    pub retention: Retention,
    #[arg(long)]
    pub dry_run: bool,
    #[arg(long, default_value = ".")]
    pub project_root: PathBuf,
    #[arg(long, default_value = "data/.config/engine/normalize.toml")]
    pub config: PathBuf,
    #[arg(long)]
    pub output_root: Option<PathBuf>,
    #[arg(long)]
    pub memory_budget: Option<u64>,
    #[arg(long)]
    pub scratch_budget: Option<u64>,
    #[arg(long)]
    pub row_group_target: Option<u64>,
    #[arg(long)]
    pub file_target: Option<u64>,
    #[arg(long,hide=true,conflicts_with_all=["input","dry_run"])]
    pub execute_trial: Option<PathBuf>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrialPlan {
    pub trial_id: String,
    pub scheduler: Scheduler,
    pub workers: usize,
    pub repetition: usize,
    pub warmup: bool,
    pub order: usize,
    pub directory: PathBuf,
    pub config: ProcessingConfig,
    pub jobs: Vec<ProcessingJob>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub schema_version: u32,
    pub kind: String,
    pub input: PathBuf,
    pub project_root: PathBuf,
    pub directory: PathBuf,
    pub scheduler: Scheduler,
    pub retention: Retention,
    pub workers: Vec<usize>,
    pub repetitions: usize,
    pub warmups: usize,
    pub input_sha256: String,
    pub job_count: usize,
    pub task_count: usize,
    pub trials: Vec<TrialPlan>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Planned,
    Running,
    Complete,
    Failed,
    Interrupted,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessMetrics {
    pub process_cpu_user_seconds: Option<f64>,
    pub process_cpu_system_seconds: Option<f64>,
    pub process_cpu_total_seconds: Option<f64>,
    pub peak_rss_bytes: Option<u64>,
    pub logical_read_bytes: Option<u64>,
    pub logical_written_bytes: Option<u64>,
    pub physical_read_bytes: Option<u64>,
    pub physical_written_bytes: Option<u64>,
    pub peak_accounted_memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildExecution {
    pub schema_version: u32,
    pub measurement_source: String,
    pub response: NormalizationResponse,
    pub execution_wall_seconds: f64,
    pub per_job_wall_seconds: Vec<Option<f64>>,
    pub process: ProcessMetrics,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileObservation {
    pub job_id: u64,
    pub dataset_id: u64,
    pub path: String,
    pub size_bytes: u64,
    pub rows: u64,
    pub row_groups: u64,
    pub compressed_column_bytes: u64,
    pub uncompressed_column_bytes: u64,
    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkVolume {
    pub source_records_read: u64,
    pub canonical_rows: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OutputObservation {
    pub source_records_read: u64,
    pub canonical_rows: u64,
    #[serde(default)]
    pub work_by_content_type: std::collections::BTreeMap<String, WorkVolume>,
    #[serde(default)]
    pub work_by_stream: std::collections::BTreeMap<String, WorkVolume>,
    pub parquet_bytes: u64,
    pub parquet_file_count: u64,
    pub parquet_row_group_count: u64,
    pub files: Vec<FileObservation>,
    pub semantic_stream_sha256: std::collections::BTreeMap<String, String>,
    pub depth_events_sha256: std::collections::BTreeMap<String, String>,
    pub reconstruction_sha256: std::collections::BTreeMap<String, String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrialResult {
    pub schema_version: u32,
    pub kind: String,
    pub benchmark_run_id: String,
    pub trial: TrialPlan,
    pub state: State,
    pub started_unix_ns: String,
    pub ended_unix_ns: Option<String>,
    pub execution: Option<ChildExecution>,
    pub trial_wall_seconds: Option<f64>,
    pub verification_wall_seconds: Option<f64>,
    pub output: Option<OutputObservation>,
    pub source_records_per_second: Option<f64>,
    pub canonical_rows_per_second: Option<f64>,
    pub input_bytes_per_second: Option<f64>,
    pub output_bytes_per_second: Option<f64>,
    pub source_archive_count: u64,
    pub source_file_bytes: Option<u64>,
    pub uncompressed_input_bytes: Option<u64>,
    pub correctness_verified: bool,
    pub dataset_retained: bool,
    pub error: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetadata {
    pub schema_version: u32,
    pub kind: String,
    pub run_id: String,
    pub state: State,
    pub created_unix_ns: String,
    pub finished_unix_ns: Option<String>,
    pub plan: Plan,
    pub trials_complete: usize,
    pub trials_failed: usize,
    pub cache_policy: String,
}
/// A transport abstraction permits predetermined execution responses in tests.
pub trait TrialExecutor {
    fn execute(&mut self, trial: &TrialPlan) -> super::Result<ChildExecution>;
}

impl TrialResult {
    pub fn validate_version(&self) -> super::Result<()> {
        if self.schema_version != 1 || self.kind != "normalization_benchmark_trial" {
            Err("unsupported benchmark trial schema/kind".into())
        } else {
            Ok(())
        }
    }
}
impl RunMetadata {
    pub fn validate_version(&self) -> super::Result<()> {
        if self.schema_version != 1
            || self.kind != "normalization_benchmark_run"
            || self.plan.schema_version != 1
        {
            Err("unsupported benchmark run/plan schema".into())
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum AnalysisCommand {
    Summarize {
        #[arg(long)]
        run: PathBuf,
        #[arg(long, default_value_t = 1)]
        baseline_workers: usize,
    },
    Compare {
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long)]
        candidate: PathBuf,
        #[arg(long, value_delimiter = ',', value_enum)]
        intentional: Vec<super::comparison::Variable>,
        #[arg(long)]
        output_root: Option<PathBuf>,
        #[arg(long, default_value = ".")]
        project_root: PathBuf,
    },
}
