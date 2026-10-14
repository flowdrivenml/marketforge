use std::{fs, path::PathBuf};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::load_convert_job,
    process::{
        convert::{plan_conversion, resolve_conversion_catalog},
        merge::PhysicalMergeManifest,
    },
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn plans_real_depth_conversion() {
    let root = root();

    let job = load_convert_job(root.join("data/.jobs/convert/convert-depth-test.json")).unwrap();

    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(job.input_merge.join("manifest.json")).unwrap()).unwrap();

    let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    assert_eq!(plan.source_streams, 15);
    assert_eq!(plan.reference_streams, 2);

    println!();
    println!("{}", "=".repeat(90));
    println!("MARKETFORGE — REAL DEPTH CONVERSION PLAN");
    println!("{}", "=".repeat(90));

    for stream in &plan.streams {
        println!();
        println!("SOURCE: {}", stream.source.stream_id,);

        println!("QUOTE : {}", stream.source.quote_currency.as_str(),);

        println!("IDENTITY: {}", stream.identity_conversion,);

        println!("PATHS: {}", stream.paths.len());

        for path in &stream.paths {
            let currencies = path
                .currencies
                .iter()
                .map(|currency| currency.as_str())
                .collect::<Vec<_>>();

            println!("  {}", currencies.join(" → "));
        }
    }

    println!();
    println!("Identity streams    : {}", plan.identity_streams);
    println!("Convertible streams : {}", plan.convertible_streams);
    println!("Unresolved streams  : {}", plan.unresolved_streams);

    assert_eq!(plan.identity_streams, 8);
    assert_eq!(plan.convertible_streams, 5);
    assert_eq!(plan.unresolved_streams, 2);

    println!("STATUS              : PASS");
}
