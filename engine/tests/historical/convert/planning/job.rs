use std::path::PathBuf;

use marketforge_engine::historical::job::{ConvertJob, load_convert_job};

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn load_test_job(mode: &str) -> ConvertJob {
    let path = project_root()
        .join("engine/tests/historical/convert/fixtures/jobs")
        .join(format!("convert-{mode}.json"));

    load_convert_job(path).unwrap()
}

#[test]
fn loads_trade_conversion_job() {
    let job = load_test_job("trades");

    assert_eq!(job.job_id, 940001);
    assert_eq!(job.conversion.target_quote, "USDT");
    assert_eq!(job.conversion.references.len(), 1);
    assert_eq!(job.conversion.max_hops, 3);
}

#[test]
fn loads_depth_conversion_job() {
    let job = load_test_job("depth");

    assert_eq!(job.job_id, 940002);
    assert_eq!(job.conversion.references.len(), 1);
    assert_eq!(job.resources.workers, 24);
}

#[test]
fn loads_combined_conversion_job() {
    let job = load_test_job("combined");

    assert_eq!(job.job_id, 940003);
    assert_eq!(job.conversion.references.len(), 1);
}

#[test]
fn rejects_relative_input_path() {
    let mut job = load_test_job("trades");

    job.input_merge = PathBuf::from("data/merged/final-trades");

    assert!(job.validate().is_err());
}

#[test]
fn rejects_duplicate_reference_instruments() {
    let mut job = load_test_job("trades");

    job.conversion
        .references
        .push(job.conversion.references[0].clone());

    assert!(job.validate().is_err());
}

#[test]
fn rejects_zero_staleness() {
    let mut job = load_test_job("trades");

    job.conversion.max_staleness_ns = 0;

    assert!(job.validate().is_err());
}

#[test]
fn rejects_invalid_hop_limit() {
    let mut job = load_test_job("trades");

    job.conversion.max_hops = 0;

    assert!(job.validate().is_err());

    job.conversion.max_hops = 5;

    assert!(job.validate().is_err());
}

#[test]
fn rejects_existing_input_as_output() {
    let mut job = load_test_job("trades");

    job.output.dataset_path = job.input_merge.clone();

    assert!(job.validate().is_err());
}

#[test]
fn rejects_unknown_configuration_fields() {
    let job = load_test_job("trades");

    let mut value = serde_json::to_value(job).unwrap();

    value["conversion"]["unexpected"] = serde_json::json!(true);

    let result = serde_json::from_value::<ConvertJob>(value);

    assert!(result.is_err());
}

#[test]
fn rejects_invalid_reference_identity() {
    let mut job = load_test_job("trades");

    job.conversion.references[0].exchange.clear();

    assert!(job.validate().is_err());
}

#[test]
fn validates_all_production_configs() {
    for mode in ["trades", "depth", "combined"] {
        let job = load_test_job(mode);

        assert!(job.input_merge.is_absolute());
        assert!(job.output.dataset_path.is_absolute());
        assert_eq!(job.resources.workers, 24);

        let reference = &job.conversion.references[0];

        assert_eq!(reference.exchange, "binance");
        assert_eq!(reference.symbol, "BTCUSDT");
    }
}
