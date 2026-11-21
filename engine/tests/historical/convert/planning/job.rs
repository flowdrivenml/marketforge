use std::path::PathBuf;

use crate::conversion_fixture::canonical_job;
use marketforge_engine::historical::job::{
    ConversionPathPolicy, ConversionPriceSource, ConvertOperation, InstrumentKind, load_convert_job,
};

#[test]
fn production_loader_reads_canonical_combined_job() {
    let job = canonical_job();
    assert_eq!(job.job_id, 265083950276773788);
    assert_eq!(job.operation, ConvertOperation::Convert);
    assert_eq!(
        job.input_merge,
        PathBuf::from("/home/pasha/projects/marketforge/data/datasets/merged/2079541492669036539")
    );
    assert_eq!(
        job.output.dataset_path,
        PathBuf::from(
            "/home/pasha/projects/marketforge/data/datasets/converted/265083950276773788"
        )
    );
    assert_eq!(job.schema_version, 1);
    assert_eq!(job.source.merge_job_id, 2079541492669036539);
    assert_eq!(job.source.streams.len(), 8);
    assert_eq!(job.source.normalized_inputs.len(), 2);
    assert!(job.external_reference.is_none());
    let conversion = job.conversion;
    assert_eq!(conversion.target_quote, "USDT");
    assert_eq!(conversion.price_source, ConversionPriceSource::LastTrade);
    assert_eq!(conversion.max_staleness_ns, 60_000_000_000);
    assert_eq!(conversion.max_hops, 2);
    assert_eq!(conversion.path_policy, ConversionPathPolicy::All);
    assert!(conversion.instrument_kinds.is_none());
    assert!(conversion.reference_merge.is_none());
    assert_eq!(
        conversion
            .references
            .iter()
            .map(|reference| reference.symbol.as_str())
            .collect::<Vec<_>>(),
        ["BTCUSDT", "BTCUSDC"]
    );
    for reference in conversion.references {
        assert_eq!(reference.exchange, "bybit");
        assert_eq!(reference.instrument_kind, InstrumentKind::Spot);
        assert!(reference.contract_kind.is_none());
    }
    assert_eq!(
        (
            job.resources.workers,
            job.resources.memory_limit_mb,
            job.resources.max_open_files
        ),
        (1, 1024, 64)
    );
}

#[test]
fn rejects_relative_input_path() {
    let mut job = canonical_job();

    job.input_merge = PathBuf::from("data/merged/final-trades");

    assert!(job.validate().is_err());
}

#[test]
fn rejects_duplicate_reference_instruments() {
    let mut job = canonical_job();

    job.conversion
        .references
        .push(job.conversion.references[0].clone());

    assert!(job.validate().is_err());
}

#[test]
fn rejects_zero_staleness() {
    let mut job = canonical_job();

    job.conversion.max_staleness_ns = 0;

    assert!(job.validate().is_err());
}

#[test]
fn rejects_invalid_hop_limit() {
    let mut job = canonical_job();

    job.conversion.max_hops = 0;

    assert!(job.validate().is_err());

    job.conversion.max_hops = 5;

    assert!(job.validate().is_err());
}

#[test]
fn rejects_existing_input_as_output() {
    let mut job = canonical_job();

    job.output.dataset_path = job.input_merge.clone();

    assert!(job.validate().is_err());
}

#[test]
fn rejects_unknown_configuration_fields() {
    let job = canonical_job();

    let mut value = serde_json::to_value(job).unwrap();

    value["conversion"]["unexpected"] = serde_json::json!(true);

    let path = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(path.as_file(), &value).unwrap();
    let result = load_convert_job(path.path());

    assert!(result.is_err());
}

#[test]
fn rejects_invalid_reference_identity() {
    let mut job = canonical_job();

    job.conversion.references[0].exchange.clear();

    assert!(job.validate().is_err());
}

#[test]
fn rejects_incomplete_and_unsupported_prepared_documents() {
    let mut value = serde_json::to_value(canonical_job()).unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let verify = |value: &serde_json::Value, message: &str| {
        std::fs::write(file.path(), serde_json::to_vec(value).unwrap()).unwrap();
        let error = load_convert_job(file.path()).unwrap_err().to_string();
        assert!(error.contains(message), "{error}");
    };
    value["schema_version"] = 77.into();
    verify(&value, "unsupported prepared conversion schema version");
    value["schema_version"] = 1.into();
    value.as_object_mut().unwrap().remove("source");
    verify(&value, "re-prepare");
}
