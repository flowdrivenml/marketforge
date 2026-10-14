use std::{fs, path::PathBuf};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{MergeParquetConfig, load_convert_job},
    process::convert::{
        execution::materialize_trade_conversion, plan_conversion, resolve_conversion_catalog,
    },
};

#[path = "support/multihop_fixture.rs"]
mod multihop_fixture;

use multihop_fixture::create_fixture;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn generate_persistent_conversion_fixture() {
    let root = root();

    let output_root = root.join("data/.work/convert-validation");

    let input = output_root.join("input");
    let staging = output_root.join("multihop");

    // Never overwrite an existing fixture.
    if output_root.exists() {
        panic!(
            "persistent conversion fixture already exists: {}",
            output_root.display()
        );
    }

    fs::create_dir_all(&input).unwrap();

    // -------------------------------------------------------------
    // Create physical input fixture
    // -------------------------------------------------------------

    let manifest = create_fixture(&input);

    // -------------------------------------------------------------
    // Load conversion job
    // -------------------------------------------------------------

    let mut job = load_convert_job(root.join("data/.jobs/convert/convert-multihop.json")).unwrap();

    job.input_merge = input.clone();

    // -------------------------------------------------------------
    // Resolve conversion metadata
    // -------------------------------------------------------------

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    // -------------------------------------------------------------
    // Execute physical conversion
    // -------------------------------------------------------------

    let config = MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 4,
        compression_level: 3,
    };

    let metrics = materialize_trade_conversion(&job, &manifest, &plan, &staging, &config).unwrap();

    assert_eq!(metrics.events_read, manifest.events_written,);

    assert_eq!(metrics.events_written, manifest.events_written,);

    println!();
    println!("{}", "=".repeat(80));
    println!("MARKETFORGE — PERSISTENT CONVERSION FIXTURE");
    println!("{}", "=".repeat(80));

    println!("Input       : {}", input.display());
    println!("Output      : {}", staging.display());

    println!("Events      : {}", metrics.events_written);
    println!("Identity    : {}", metrics.identity_events);
    println!("Converted   : {}", metrics.converted_events);
    println!("Unavailable : {}", metrics.unavailable_events);

    println!("Files       : {}", metrics.files_written);
    println!("Row groups  : {}", metrics.row_groups_written);

    println!("STATUS: PASS");
}
