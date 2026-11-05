//! Parsing/validation only. No jobs execute, no archives are opened, no database is used.
use super::*;
use tempfile::TempDir;

const RAW_ID: &str = "bybit.trades.bybit-t1.v1";
const PROFILE_ID: &str = "bybit.spot.spot.bybit-t1.v1";
const RAW: &str = r#"
schema_version = 1
kind = "physical"
id = "bybit.trades.bybit-t1.v1"
container_format = "csv"
record_format = "rows"
supported_containers = ["gzip"]
[raw_schema]
header = true
fields = [
 { name = "timestamp", type = "integer", unit = "milliseconds" },
 { name = "price", type = "decimal" },
 { name = "quantity", type = "decimal" },
 { name = "side", type = "string" }
]
"#;
const PROFILE: &str = r#"
schema_version = 1
kind = "profile"
id = "bybit.spot.spot.bybit-t1.v1"
physical = "bybit.trades.bybit-t1.v1"
format_code = "BYBIT-T1"
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
source_ordering = "source_ordered"
[[rules]]
target = "trade"
timestamp_encoding = "milliseconds"
quantity_encoding = "base"
rules = { event_timestamp = { source = "timestamp" }, price = { source = "price" }, quantity = { source = "quantity" }, side = { source = "side", transform = "casefold" } }
"#;

fn fixture(raw: &str, profile: &str) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "raw_schemas/trades/raw.toml", raw);
    write(dir.path(), "profiles/trades/profile.toml", profile);
    dir
}
fn write(root: &Path, relative: &str, text: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}
fn error(raw: &str, profile: &str) -> String {
    let dir = fixture(raw, profile);
    FormatCatalog::load(dir.path()).unwrap_err().to_string()
}

#[test]
fn valid_documents_resolve_in_separate_trees_preserving_spot_spot() {
    let dir = fixture(RAW, PROFILE);
    // Drafts, reports, and unrelated runtime configuration are not catalog input.
    write(dir.path(), "format-drafts/broken.toml", "not valid TOML");
    write(dir.path(), "normalize.toml", "not valid TOML");
    let c = FormatCatalog::load(dir.path()).unwrap();
    let p = c.get(PROFILE_ID).unwrap();
    assert_eq!(p.definition.id, PROFILE_ID);
    assert_eq!(p.definition.physical, RAW_ID);
    assert_eq!(p.physical.id, RAW_ID);
    assert_eq!(p.normalizations.len(), 1);
    assert_eq!(
        p.normalizations[0].timestamp_encoding,
        TimestampEncoding::Milliseconds
    );
    assert_eq!(
        p.normalizations[0].quantity_encoding,
        QuantityEncoding::Base
    );
    assert!(c.get("bybit.spot.bybit-t1.v1").is_err());
}

#[test]
fn shared_raw_schema_is_not_duplicated_for_different_markets() {
    let dir = fixture(RAW, PROFILE);
    let linear = PROFILE
        .replace(PROFILE_ID, "bybit.linear.perpetual.bybit-t1.v1")
        .replace(
            "instrument_type = \"spot\"",
            "instrument_type = \"perpetual\"",
        )
        .replace("market_category = \"spot\"", "market_category = \"linear\"");
    write(dir.path(), "profiles/trades/linear.toml", &linear);
    let c = FormatCatalog::load(dir.path()).unwrap();
    let spot = c.get(PROFILE_ID).unwrap();
    let linear = c.get("bybit.linear.perpetual.bybit-t1.v1").unwrap();
    assert!(Arc::ptr_eq(&spot.physical, &linear.physical));
    assert_ne!(spot.fingerprint, linear.fingerprint);
}

#[test]
fn duplicate_raw_and_profile_ids_are_rejected() {
    for (path, text, message) in [
        ("raw_schemas/trades/copy.toml", RAW, "duplicate physical"),
        ("profiles/trades/copy.toml", PROFILE, "duplicate profile"),
    ] {
        let dir = fixture(RAW, PROFILE);
        write(dir.path(), path, text);
        let e = FormatCatalog::load(dir.path()).unwrap_err().to_string();
        assert!(e.contains(message), "{e}");
        assert!(e.contains(".toml"));
    }
}

#[test]
fn unresolved_raw_reference_is_rejected() {
    let e = error(RAW, &PROFILE.replace(RAW_ID, "bybit.trades.missing.v1"));
    assert!(
        e.contains("missing physical format bybit.trades.missing.v1"),
        "{e}"
    );
}

#[test]
fn unsupported_document_versions_fail_with_path() {
    for (raw, profile) in [
        (
            RAW.replace("schema_version = 1", "schema_version = 99"),
            PROFILE.to_owned(),
        ),
        (
            RAW.to_owned(),
            PROFILE.replace("schema_version = 1", "schema_version = 99"),
        ),
    ] {
        let e = error(&raw, &profile);
        assert!(e.contains("version") && e.contains(".toml"), "{e}");
    }
}

#[test]
fn profile_identity_keeps_all_metadata_dimensions_and_format_code() {
    for profile in [
        PROFILE.replace(PROFILE_ID, "bybit.spot.bybit-t1.v1"),
        PROFILE.replace("format_code = \"BYBIT-T1\"", "format_code = \"BYBIT-T2\""),
        PROFILE.replace("market_category = \"spot\"", "market_category = \"linear\""),
        PROFILE.replace(PROFILE_ID, "bybit.spot.spot.v1"),
    ] {
        assert!(error(RAW, &profile).contains("profile identity"));
    }
    let incompatible = PROFILE
        .replace("market_category = \"spot\"", "market_category = \"linear\"")
        .replace(PROFILE_ID, "bybit.linear.spot.bybit-t1.v1");
    assert!(error(RAW, &incompatible).contains("incompatible profile"));
}

#[test]
fn raw_identity_is_descriptive_and_versioned() {
    for id in [
        "physical-12345678",
        "trades-deadbeef",
        "bybit.trades.bybit-t1.v0",
        "bybit.trades.bybit-t1.v01",
    ] {
        assert!(
            error(&RAW.replace(RAW_ID, id), &PROFILE.replace(RAW_ID, id)).contains("raw schema ID")
        );
    }
}

#[test]
fn wrong_document_section_or_data_type_is_rejected() {
    for (path, text, expected) in [
        ("profiles/trades/raw.toml", RAW, "raw schema must be under"),
        (
            "raw_schemas/trades/profile.toml",
            PROFILE,
            "profile must be under",
        ),
        (
            "profiles/unknown/profile.toml",
            PROFILE,
            "expected trades/ or depth/",
        ),
    ] {
        let dir = fixture(RAW, PROFILE);
        write(dir.path(), path, text);
        assert!(
            FormatCatalog::load(dir.path())
                .unwrap_err()
                .to_string()
                .contains(expected)
        );
    }
    let dir = fixture(RAW, PROFILE);
    fs::create_dir_all(dir.path().join("profiles/depth")).unwrap();
    fs::rename(
        dir.path().join("profiles/trades/profile.toml"),
        dir.path().join("profiles/depth/profile.toml"),
    )
    .unwrap();
    assert!(
        FormatCatalog::load(dir.path())
            .unwrap_err()
            .to_string()
            .contains("data types disagree")
    );
}

#[test]
fn missing_tree_is_not_replaced_with_legacy_or_working_directory_fallback() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "formats/trades/old.toml", RAW);
    let e = FormatCatalog::load(dir.path()).unwrap_err().to_string();
    assert!(e.contains("raw_schemas"));
}

#[test]
fn source_field_errors_and_unsupported_rules_fail_closed() {
    for (old, new, expected) in [
        (
            "source = \"quantity\"",
            "source = \"absent\"",
            "absent from raw schema",
        ),
        ("casefold", "not_implemented", "unknown variant"),
        (
            "source = \"price\"",
            "source = \"price\", typo = true",
            "unknown field",
        ),
    ] {
        let e = error(RAW, &PROFILE.replace(old, new));
        assert!(e.contains(expected), "{e}");
        assert!(e.contains(PROFILE_ID));
    }
}

#[test]
fn raw_field_and_container_validation_is_preserved() {
    for raw in [
        RAW.replace("name = \"side\"", "name = \"price\""),
        RAW.replace("type = \"integer\"", "type = \"unknown\""),
        RAW.replace("header = true", "header = \"yes\""),
        RAW.replace("[\"gzip\"]", "[]"),
        RAW.replace("[\"gzip\"]", "[\"gzip\", \"gzip\"]"),
        RAW.replace("name = \"timestamp\"", "position = 2, name = \"timestamp\""),
    ] {
        let e = error(&raw, PROFILE);
        assert!(e.contains("raw.toml"), "{e}");
    }
}

#[test]
fn fingerprint_ignores_locations_and_comments_but_includes_raw_identity() {
    let a = fixture(RAW, PROFILE);
    let b = fixture(&format!("# comment\n{RAW}\n"), &format!("\n{PROFILE}\n"));
    fs::rename(
        b.path().join("profiles/trades/profile.toml"),
        b.path().join("profiles/trades/renamed.toml"),
    )
    .unwrap();
    let a = FormatCatalog::load(a.path()).unwrap();
    let b = FormatCatalog::load(b.path()).unwrap();
    let hash = &a.get(PROFILE_ID).unwrap().fingerprint;
    assert_eq!(hash, &b.get(PROFILE_ID).unwrap().fingerprint);
    assert_eq!(hash.len(), 64);
    let dir = fixture(
        &RAW.replace(RAW_ID, "bybit.trades.bybit-t1.revised.v1"),
        &PROFILE.replace(RAW_ID, "bybit.trades.bybit-t1.revised.v1"),
    );
    let revised = FormatCatalog::load(dir.path()).unwrap();
    assert_ne!(hash, &revised.get(PROFILE_ID).unwrap().fingerprint);
    assert_eq!(
        *hash,
        fingerprint(&(
            &a.get(PROFILE_ID).unwrap().definition,
            a.get(PROFILE_ID).unwrap().physical.as_ref()
        ))
        .unwrap()
    );
}

#[test]
fn unsupported_profiles_remain_inventory_only_with_valid_references() {
    let unsupported = PROFILE
        .replace(
            "source_ordering = \"source_ordered\"",
            "unsupported_reason = \"Deliberately deferred parser capability\"",
        )
        .replace("timestamp_encoding = \"milliseconds\"\n", "")
        .replace("quantity_encoding = \"base\"\n", "")
        .replace("casefold", "future_transform");
    let dir = fixture(RAW, &unsupported);
    let c = FormatCatalog::load(dir.path()).unwrap();
    assert_eq!(c.profiles().next().unwrap().normalizations.len(), 0);
    assert!(
        c.get(PROFILE_ID)
            .unwrap_err()
            .to_string()
            .contains("unsupported profile")
    );
    assert!(
        error(
            RAW,
            &unsupported.replace("source = \"quantity\"", "source = \"missing\"")
        )
        .contains("absent from raw schema")
    );
    assert!(
        error(
            RAW,
            &unsupported.replace("Deliberately deferred parser capability", " ")
        )
        .contains("empty unsupported_reason")
    );
}

#[test]
fn root_resolution_is_explicit_absolute_and_does_not_depend_on_cwd() {
    assert!(FormatCatalog::load("relative").is_err());
    let default = catalog_root(None, None).unwrap();
    assert!(default.is_absolute());
    assert_eq!(
        default,
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/historical")
    );
    assert!(catalog_root(Some(PathBuf::from("relative")), None).is_err());
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        catalog_root(None, Some(dir.path().join("formats"))).unwrap(),
        dir.path()
    );
    assert_eq!(
        catalog_root(
            Some(dir.path().to_path_buf()),
            Some(dir.path().join("formats"))
        )
        .unwrap(),
        dir.path()
    );
    assert!(
        catalog_root(
            Some(dir.path().join("other")),
            Some(dir.path().to_path_buf())
        )
        .is_err()
    );
}

#[test]
fn available_catalog_compiles_without_opening_archives() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/historical");
    let c = FormatCatalog::load(root).unwrap();
    assert!(c.profiles().next().is_some());
    for p in c.profiles() {
        if p.definition.unsupported_reason.is_some() {
            assert!(c.get(&p.definition.id).is_err());
            assert!(p.normalizations.is_empty());
        } else {
            assert!(!p.normalizations.is_empty());
        }
    }
}

#[test]
fn gate_spot_side_rule_matches_retained_source_inspection_not_stale_export() {
    // docs/tofixlater/raw-formats/GATE-B1 — Spot L2.md records source sides
    // 1=ask / 2=bid and representative rows; this test parses rules only.
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/historical");
    let c = FormatCatalog::load(root).unwrap();
    let p = c.get("gateio.spot.spot.gate-b1.v1").unwrap();
    for n in &p.normalizations {
        let spec = DepthSpec::from_normalization(n).unwrap();
        let side = spec.scalar_level.unwrap().side;
        assert_eq!(side.source, "side");
        assert_eq!(side.transform.as_deref(), Some("map"));
        assert_eq!(
            side.values.unwrap(),
            serde_json::json!({"1": "ask", "2": "bid"})
        );
    }
}

#[test]
fn pinned_job_documents_reject_stale_fingerprints_without_opening_inputs() {
    use crate::historical::config::load_document_with_catalog;
    let dir = fixture(RAW, PROFILE);
    let c = FormatCatalog::load(dir.path()).unwrap();
    let hash = c.get(PROFILE_ID).unwrap().fingerprint.clone();
    let input = dir.path().join("nonexistent.csv.gz");
    let fail = serde_json::json!({"action": "fail"});
    let mut document = serde_json::json!({
        "protocol_version": 2, "job_id": 1, "dataset_id": 1, "operation": "process",
        "tasks": [{
            "task_id": 1, "stream_id": "bybit.spot.BTCUSDT.trades", "input_path": input,
            "format_profile": PROFILE_ID, "profile_fingerprint": hash,
            "exchange": "bybit", "instrument_id": 1, "symbol": "BTCUSDT",
            "instrument": {"instrument_kind": "spot", "tick_size": "0.01"}
        }],
        "streams": [{"stream_id": "bybit.spot.BTCUSDT.trades", "stream_rank": 1}],
        "ordering": {"primary": "event_timestamp_ns", "tie_break": "stream_rank"},
        "integrity_policy": {
            "enabled": true, "parse_failure": fail, "invalid_record": fail,
            "sequence_gap": fail, "timestamp_regression": fail, "missing_snapshot": fail,
            "invalid_book": fail, "transformation_failure": fail
        },
        "resources": {"workers": 1, "memory_budget_bytes": 1, "scratch_budget_bytes": 1,
            "scratch_path": dir.path().join("scratch"),
            "parquet": {"row_group_target_bytes": 1, "file_target_bytes": 1}},
        "output": {"content_type": "trades", "staging_path": dir.path().join("stage"),
            "dataset_path": dir.path().join("output")}
    });
    let path = dir.path().join("job.json");
    fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    let resolved = load_document_with_catalog(&path, &c).unwrap();
    assert_eq!(
        resolved.tasks[0].raw_schema["marketforge_profile"]["sha256"],
        hash
    );
    document["tasks"][0]["profile_fingerprint"] = "stale".into();
    fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    assert!(
        load_document_with_catalog(&path, &c)
            .unwrap_err()
            .to_string()
            .contains("fingerprint mismatch")
    );
    // Re-read the serialized, already-resolved v1 representation through the same guard.
    let mut resolved = serde_json::to_value(resolved).unwrap();
    resolved["tasks"][0]["raw_schema"]["marketforge_profile"]["sha256"] = "stale".into();
    fs::write(&path, serde_json::to_vec(&resolved).unwrap()).unwrap();
    assert!(
        load_document_with_catalog(&path, &c)
            .unwrap_err()
            .to_string()
            .contains("provenance fingerprint")
    );
    assert!(!input.exists());
    assert!(!dir.path().join("stage").exists());
    assert!(!dir.path().join("output").exists());
}

#[test]
fn inline_depth_catalog_resolves_snapshot_and_update_rules() {
    let raw = r#"
schema_version = 1
kind = "physical"
id = "bybit.depth.bybit-b1.v1"
container_format = "jsonl"
record_format = "events"
supported_containers = ["zip"]
raw_schema = { fields = [{ name = "ts", type = "integer" }, { name = "bids", type = "array" }, { name = "asks", type = "array" }] }
"#;
    let profile = r#"
schema_version = 1
kind = "profile"
id = "bybit.spot.spot.bybit-b1.v1"
physical = "bybit.depth.bybit-b1.v1"
format_code = "BYBIT-B1"
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
source_ordering = "source_ordered"
[[rules]]
target = "l2_snapshot"
timestamp_encoding = "milliseconds"
quantity_encoding = "base"
rules = { event_timestamp = { source = "ts" }, bids = { source = "bids", price_index = 0, quantity_index = 1 }, asks = { source = "asks", price_index = 0, quantity_index = 1 }, snapshot = { type = "single_event" } }
[[rules]]
target = "l2_update"
timestamp_encoding = "milliseconds"
quantity_encoding = "base"
rules = { event_timestamp = { source = "ts" }, bids = { source = "bids", price_index = 0, quantity_index = 1 }, asks = { source = "asks", price_index = 0, quantity_index = 1 }, update_semantics = { type = "absolute_level", zero_quantity = "delete", nonzero_quantity = "set" } }
"#;
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "raw_schemas/depth/raw.toml", raw);
    write(dir.path(), "profiles/depth/profile.toml", profile);
    let c = FormatCatalog::load(dir.path()).unwrap();
    let p = c.get("bybit.spot.spot.bybit-b1.v1").unwrap();
    assert_eq!(p.normalizations.len(), 2);
    assert!(
        p.normalizations
            .iter()
            .all(|n| n.target_schema == TargetSchema::Depth)
    );
    write(
        dir.path(),
        "profiles/depth/profile.toml",
        &profile.replace("target = \"l2_snapshot\"", "target = \"l2_update\""),
    );
    assert!(
        FormatCatalog::load(dir.path())
            .unwrap_err()
            .to_string()
            .contains("boundary mismatch")
    );
}

#[test]
fn supported_profiles_require_encodings_ordering_and_matching_targets() {
    for (before, after, expected) in [
        (
            "timestamp_encoding = \"milliseconds\"",
            "",
            "missing timestamp_encoding",
        ),
        (
            "quantity_encoding = \"base\"",
            "",
            "missing quantity_encoding",
        ),
        (
            "source_ordering = \"source_ordered\"",
            "",
            "missing source_ordering",
        ),
        (
            "target = \"trade\"",
            "target = \"l2_update\"",
            "target disagrees",
        ),
    ] {
        let e = error(RAW, &PROFILE.replace(before, after));
        assert!(e.contains(expected), "{e}");
    }
    let repeated = PROFILE.to_owned() + PROFILE.split_once("[[rules]]").unwrap().1;
    // The duplicate field form is invalid TOML rather than a second target; both must fail.
    assert!(error(RAW, &repeated).contains("profile.toml"));
    let repeated =
        PROFILE.to_owned() + "\n[[rules]]\n" + PROFILE.split_once("[[rules]]").unwrap().1;
    assert!(error(RAW, &repeated).contains("duplicate normalization target"));
}

#[test]
fn declared_semantic_contradictions_are_rejected() {
    for (raw, profile, expected) in [
        (
            RAW.to_owned(),
            PROFILE.replace(
                "timestamp_encoding = \"milliseconds\"",
                "timestamp_encoding = \"seconds\"",
            ),
            "source field unit",
        ),
        (
            RAW.to_owned(),
            PROFILE.replace(
                "quantity_encoding = \"base\"",
                "quantity_encoding = \"contracts\"",
            ),
            "spot profile",
        ),
        (
            RAW.replace("header = true", "header = true\nsorting_required = true"),
            PROFILE.to_owned(),
            "required archive sorting",
        ),
        (
            RAW.replace(RAW_ID, "binance.trades.bybit-t1.v1"),
            PROFILE.replace(RAW_ID, "binance.trades.bybit-t1.v1"),
            "raw identity",
        ),
        (
            RAW.replace(
                "unit = \"milliseconds\"",
                "unit = \"milliseconds\", representation = \"iso8601\"",
            ),
            PROFILE.to_owned(),
            "source representation",
        ),
    ] {
        let e = error(&raw, &profile);
        assert!(e.contains(expected), "{e}");
    }
}

#[test]
fn fingerprint_ignores_json_object_key_order_but_preserves_values() {
    let a: Value =
        serde_json::from_str(r#"{"fields":[{"name":"time","unit":"seconds"}],"header":true}"#)
            .unwrap();
    let b: Value =
        serde_json::from_str(r#"{"header":true,"fields":[{"unit":"seconds","name":"time"}]}"#)
            .unwrap();
    assert_eq!(fingerprint(&a).unwrap(), fingerprint(&b).unwrap());
    let mut changed = b;
    changed["fields"][0]["unit"] = "milliseconds".into();
    assert_ne!(fingerprint(&a).unwrap(), fingerprint(&changed).unwrap());
}
