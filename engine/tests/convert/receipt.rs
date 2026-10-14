use marketforge_engine::process::convert::{
    execution::PhysicalConversionMetrics,
    output::{ConversionExecutionReceipt, ConversionValidationMetrics},
};

fn metrics() -> PhysicalConversionMetrics {
    PhysicalConversionMetrics {
        events_read: 100,
        events_written: 100,
        identity_events: 60,
        converted_events: 35,
        unavailable_events: 5,
        reference_updates: 100,
        files_written: 1,
        row_groups_written: 2,
    }
}

fn validation() -> ConversionValidationMetrics {
    ConversionValidationMetrics {
        events: 100,
        identity: 60,
        converted: 35,
        unavailable: 5,
        files: 1,
        row_groups: 2,
        references_checked: 70,
        first_timestamp_ns: Some(100),
        last_timestamp_ns: Some(200),
    }
}

#[test]
fn reconciles_valid_receipt() {
    let receipt = ConversionExecutionReceipt::new(940004, &metrics()).unwrap();

    receipt.reconcile(940004, &validation()).unwrap();
}

#[test]
fn rejects_wrong_job_id() {
    let receipt = ConversionExecutionReceipt::new(940004, &metrics()).unwrap();

    assert!(receipt.reconcile(999, &validation()).is_err());
}

#[test]
fn rejects_incorrect_event_count() {
    let receipt = ConversionExecutionReceipt::new(940004, &metrics()).unwrap();

    let mut validation = validation();
    validation.events += 1;

    assert!(receipt.reconcile(940004, &validation).is_err());
}

#[test]
fn rejects_incorrect_status_accounting() {
    let receipt = ConversionExecutionReceipt::new(940004, &metrics()).unwrap();

    let mut validation = validation();
    validation.converted += 1;

    assert!(receipt.reconcile(940004, &validation).is_err());
}

#[test]
fn rejects_incorrect_file_inventory() {
    let receipt = ConversionExecutionReceipt::new(940004, &metrics()).unwrap();

    let mut validation = validation();
    validation.row_groups += 1;

    assert!(receipt.reconcile(940004, &validation).is_err());
}

#[test]
fn serializes_receipt() {
    let temporary = tempfile::tempdir().unwrap();

    let receipt = ConversionExecutionReceipt::new(940004, &metrics()).unwrap();

    receipt.write(temporary.path()).unwrap();

    let loaded = ConversionExecutionReceipt::load(temporary.path()).unwrap();

    loaded.reconcile(940004, &validation()).unwrap();

    assert!(receipt.write(temporary.path()).is_err());
}
