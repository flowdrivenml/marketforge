//! Usage: cargo run --example normalize-admission --features historical -- [project-root [batch [runtime]]]
//! Never executes normalization or opens source archive payloads.
use marketforge_engine::historical::{
    config::{FormatCatalog, read_prepared_batch},
    normalize::{load_processing_config, resources::inspect_admission},
};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let root = std::fs::canonicalize(args.first().map(String::as_str).unwrap_or("."))?;
    let resolve = |index: usize, default: &str| {
        let p = std::path::PathBuf::from(args.get(index).map(String::as_str).unwrap_or(default));
        if p.is_absolute() { p } else { root.join(p) }
    };
    let runtime =
        load_processing_config(resolve(2, "data/.config/historical/general.toml"), &root)?;
    let batch = read_prepared_batch(
        resolve(1, "engine/tests/historical/fixtures/normalize.json"),
        &root,
    )?;
    let catalog = FormatCatalog::load(root.join("data/.config/historical"))?;
    let jobs = batch
        .jobs
        .iter()
        .map(|j| batch.resolve_job(j, &catalog, &runtime, &root))
        .collect::<marketforge_engine::shared::error::Result<Vec<_>>>()?;
    let diagnostic = inspect_admission(&jobs, &runtime)?;
    serde_json::to_writer_pretty(std::io::stdout().lock(), &diagnostic)?;
    Ok(())
}
