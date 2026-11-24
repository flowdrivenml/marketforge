use clap::Parser;
use marketforge_engine::historical::normalize::benchmark::{model::Args, plan, runner};
fn execute() -> Result<(), String> {
    let args = Args::parse();
    if let Some(command) = &args.command {
        use marketforge_engine::historical::normalize::benchmark::{
            model::AnalysisCommand, reports,
        };
        match command {
            AnalysisCommand::Summarize {
                run,
                baseline_workers,
            } => {
                let summary = reports::summarize_path(run, *baseline_workers)?;
                eprintln!("Summary: {} ({})", summary.run_id, summary.status);
            }
            AnalysisCommand::Compare {
                baseline,
                candidate,
                intentional,
                output_root,
                project_root,
            } => {
                let invocation = std::env::current_dir().map_err(|e| e.to_string())?;
                let root = marketforge_engine::shared::workspace::resolve(
                    project_root.as_deref(),
                    &invocation,
                )
                .map_err(|e| e.to_string())?;
                let output = output_root
                    .as_ref()
                    .map(|p| plan::absolute_from(p, &invocation));
                let directory = reports::compare_paths(
                    baseline,
                    candidate,
                    intentional,
                    &root,
                    output.as_deref(),
                )?;
                let report:marketforge_engine::historical::normalize::benchmark::comparison::Comparison=serde_json::from_slice(&std::fs::read(directory.join("comparison.json")).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
                eprintln!(
                    "Comparison: {} ({:?})",
                    directory.display(),
                    report.comparability
                );
            }
        }
        return Ok(());
    }

    if let Some(request) = args.execute_trial {
        return match runner::child(&request)? {
            true => Ok(()),
            false => Err("normalization trial failed; see execution.json and stderr.log".into()),
        };
    }
    let invocation = std::env::current_dir().map_err(|e| e.to_string())?;
    let plan = plan::plan(&args, &invocation)?;
    if args.dry_run {
        println!(
            "{}",
            serde_json::to_string_pretty(&plan).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    let requested = args
        .output_root
        .as_ref()
        .map(|p| plan::absolute_from(p, &invocation));
    let directory = runner::execute_plan(plan, requested.as_deref())?;
    eprintln!("Benchmark results: {}", directory.display());
    let run: marketforge_engine::historical::normalize::benchmark::model::RunMetadata =
        serde_json::from_slice(
            &std::fs::read(directory.join("run.json")).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
    if run.trials_failed > 0 {
        Err(format!("{} benchmark trials failed", run.trials_failed))
    } else {
        Ok(())
    }
}
fn main() -> std::process::ExitCode {
    match execute() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Benchmark: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
