#[path = "support/multihop_fixture.rs"]
mod multihop_fixture;

use multihop_fixture::create_fixture;

use std::{
    fs::{self, File},
    path::PathBuf,
};

use arrow_array::{Array, UInt8Array};

use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{MergeParquetConfig, load_convert_job},
    process::convert::{
        execution::materialize_trade_conversion,
        output::{
            CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY, CONVERSION_STATUS_UNAVAILABLE,
        },
        plan_conversion, resolve_conversion_catalog,
    },
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn materializes_real_multihop_conversion() {
    let temporary = tempfile::tempdir().unwrap();

    let input = temporary.path().join("input");
    let staging = temporary.path().join("staging");

    fs::create_dir_all(&input).unwrap();

    let manifest = create_fixture(&input);

    let mut job =
        load_convert_job(root().join("data/.jobs/convert/convert-multihop.json")).unwrap();

    job.input_merge = input;

    let catalog = InstrumentCatalog::load(&root().join("data/catalog/instruments.json")).unwrap();

    let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root()).unwrap();

    let plan = plan_conversion(&job, &resolved).unwrap();

    let config = MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 4,
        compression_level: 3,
    };

    let metrics = materialize_trade_conversion(&job, &manifest, &plan, &staging, &config).unwrap();

    assert_eq!(metrics.events_written, manifest.events_written);

    println!();
    println!("MULTI-INSTRUMENT CONVERSION");
    println!("Events     : {}", metrics.events_written);
    println!("Identity   : {}", metrics.identity_events);
    println!("Converted  : {}", metrics.converted_events);
    println!("Unavailable: {}", metrics.unavailable_events);

    let mut counts = [0_u64; 3];

    for entry in fs::read_dir(&staging).unwrap() {
        let path = entry.unwrap().path();

        if path.extension().is_none_or(|ext| ext != "parquet") {
            continue;
        }

        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            let statuses = batch
                .column_by_name("conversion_status")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();

            for row in 0..batch.num_rows() {
                let status = statuses.value(row);

                assert!(status <= CONVERSION_STATUS_UNAVAILABLE);

                counts[status as usize] += 1;
            }
        }
    }

    assert_eq!(
        counts[CONVERSION_STATUS_IDENTITY as usize],
        metrics.identity_events
    );
    assert_eq!(
        counts[CONVERSION_STATUS_CONVERTED as usize],
        metrics.converted_events
    );
    assert_eq!(
        counts[CONVERSION_STATUS_UNAVAILABLE as usize],
        metrics.unavailable_events
    );

    assert!(metrics.identity_events > 0);
    assert!(metrics.converted_events > 0);
    assert!(metrics.unavailable_events > 0);
}
