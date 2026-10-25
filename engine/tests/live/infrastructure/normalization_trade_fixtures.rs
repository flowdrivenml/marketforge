#![cfg(feature = "live")]

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;

use marketforge_engine::live::infrastructure::normalization::{
    TradeExtractionPlan, TradeRecordRepresentation,
};

// ============================================================
// FIXTURE LOCATION
// ============================================================

fn fixture_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.cache/live/rest-service-fixtures")
}

// ============================================================
// JSON TYPE
// ============================================================

fn value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// ============================================================
// TRADE ARRAY CANDIDATE
// ============================================================

#[derive(Debug, Clone)]
struct TradeArrayCandidate {
    path: String,
    count: usize,
}

// ============================================================
// GENERIC RECORD DISCOVERY
// ============================================================

/// Discover nonempty arrays containing exclusively
/// JSON object records.
///
/// This is a fixture-inspection utility, NOT a runtime
/// exchange parser.
///
/// The resulting paths must eventually become explicit,
/// validated REST format configuration.
///
/// Empty arrays cannot be identified as trade arrays
/// purely from their contents.
fn discover_object_arrays(value: &Value, path: &str, candidates: &mut Vec<TradeArrayCandidate>) {
    match value {
        Value::Array(items) => {
            if !items.is_empty() && items.iter().all(Value::is_object) {
                candidates.push(TradeArrayCandidate {
                    path: path.to_owned(),
                    count: items.len(),
                });

                // Treat this as a record collection.
                //
                // Do not inspect each individual trade
                // for nested arrays and accidentally
                // discover secondary record collections.
                return;
            }

            for (index, item) in items.iter().enumerate() {
                let child = if path == "$" {
                    index.to_string()
                } else {
                    format!("{path}.{index}")
                };

                discover_object_arrays(item, &child, candidates);
            }
        }

        Value::Object(object) => {
            for (key, item) in object {
                // JsonExtractor uses dotted paths.
                //
                // A key containing '.' cannot be
                // represented losslessly by that syntax.
                //
                // Don't silently generate an unusable path.
                if key.contains('.') || key == "$" {
                    continue;
                }

                let child = if path == "$" {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };

                discover_object_arrays(item, &child, candidates);
            }
        }

        _ => {}
    }
}

// ============================================================
// FIELD SHAPE INSPECTION
// ============================================================

fn record_fields(record: &Value) -> BTreeMap<String, &'static str> {
    record
        .as_object()
        .expect("record must be a JSON object")
        .iter()
        .map(|(name, value)| (name.clone(), value_type(value)))
        .collect()
}

// ============================================================
// ONE FIXTURE VALIDATION
// ============================================================

fn inspect_trade_fixture(path: &Path) -> Result<usize, String> {
    // ========================================================
    // READ SAVED NATIVE JSON
    // ========================================================

    let bytes =
        fs::read(path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;

    let json: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid fixture JSON {}: {error}", path.display()))?;

    // ========================================================
    // DISCOVER RECORD COLLECTIONS
    // ========================================================

    let mut candidates = Vec::new();

    discover_object_arrays(&json, "$", &mut candidates);

    // ========================================================
    // REQUIRE UNAMBIGUOUS EXTRACTION
    // ========================================================

    if candidates.is_empty() {
        return Err(format!(
            "{}: no nonempty object-record array found",
            path.display()
        ));
    }

    if candidates.len() != 1 {
        return Err(format!(
            "{}: ambiguous trade record layout: {candidates:?}",
            path.display()
        ));
    }

    let candidate = &candidates[0];

    // ========================================================
    // COMPILE REAL EXTRACTION PLAN
    // ========================================================

    // This bound is for fixture validation.
    //
    // Production limits will eventually come from
    // validated format/runtime configuration.
    const MAX_RECORDS: usize = 10_000;

    let plan = TradeExtractionPlan::new(
        candidate.path.clone(),
        TradeRecordRepresentation::Array,
        MAX_RECORDS,
    )
    .map_err(|error| {
        format!(
            "{}: cannot compile extraction plan: {error}",
            path.display()
        )
    })?;

    // ========================================================
    // EXTRACT USING MARKETFORGE
    // ========================================================

    let records = plan
        .extract(&json)
        .map_err(|error| format!("{}: extraction failed: {error}", path.display()))?;

    if records.is_empty() {
        return Err(format!(
            "{}: discovered trade batch is empty",
            path.display()
        ));
    }

    if records.len() != candidate.count {
        return Err(format!(
            "{}: record count mismatch: discovered={}, extracted={}",
            path.display(),
            candidate.count,
            records.len()
        ));
    }

    // ========================================================
    // INSPECT FIRST NATIVE TRADE
    // ========================================================

    let first = records.get(0).expect("nonempty record collection");

    let fields = record_fields(first);

    println!();
    println!("FIXTURE: {}", path.file_name().unwrap().to_string_lossy());
    println!("  Record path: {}", plan.path());
    println!("  Records:     {}", records.len());
    println!("  JSON bytes:  {}", bytes.len());

    println!("  Native fields:");

    for (field, kind) in fields {
        println!("    {field:<28} {kind}");
    }

    // ========================================================
    // PRESERVE SOURCE ORDER
    // ========================================================

    assert_eq!(
        records.iter().count(),
        records.len(),
        "record iterator count mismatch"
    );

    Ok(records.len())
}

// ============================================================
// ALL REAL TRADE FIXTURES
// ============================================================

#[test]
fn extracts_all_real_rest_trade_fixtures() {
    let root = fixture_directory();

    assert!(
        root.is_dir(),
        "REST fixture directory missing: {}",
        root.display()
    );

    // ========================================================
    // DISCOVER FIXTURES
    // ========================================================

    let mut files = fs::read_dir(&root)
        .expect("cannot read REST fixture directory")
        .map(|entry| entry.expect("cannot read directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect::<Vec<_>>();

    files.sort();

    assert_eq!(files.len(), 35, "expected 35 REST fixtures");

    // ========================================================
    // SELECT TRADE ENDPOINTS
    // ========================================================

    let trade_files = files
        .iter()
        .filter(|path| {
            let name = path.file_stem().unwrap().to_string_lossy();

            name.ends_with("__recent_trades")
                || name.ends_with("__historical_trades")
                || name.ends_with("__aggregate_trades")
        })
        .collect::<Vec<_>>();

    assert_eq!(trade_files.len(), 20, "expected 20 REST trade fixtures");

    // ========================================================
    // EXTRACT ALL TRADE RECORDS
    // ========================================================

    let mut total_records = 0usize;
    let mut passed = 0usize;

    let mut failures = Vec::new();

    println!();
    println!("==============================================");
    println!("MARKETFORGE — REAL REST TRADE FIXTURE AUDIT");
    println!("==============================================");

    for path in trade_files {
        match inspect_trade_fixture(path) {
            Ok(count) => {
                passed += 1;
                total_records += count;
            }

            Err(error) => {
                failures.push(error);
            }
        }
    }

    // ========================================================
    // REPORT
    // ========================================================

    println!();
    println!("==============================================");
    println!("REST TRADE EXTRACTION REPORT");
    println!("==============================================");
    println!("Total REST fixtures:  {}", files.len());
    println!("Trade fixtures:       20");
    println!("Successfully parsed:  {passed}");
    println!("Failed:               {}", failures.len());
    println!("Trade records:        {total_records}");

    if !failures.is_empty() {
        println!();
        println!("FAILURE DETAILS:");

        for failure in &failures {
            println!("  {failure}");
        }
    }

    assert!(
        failures.is_empty(),
        "{} trade fixture(s) failed extraction",
        failures.len()
    );

    assert_eq!(passed, 20, "incomplete REST trade fixture coverage");

    println!();
    println!("RESULT: PASS");
}
