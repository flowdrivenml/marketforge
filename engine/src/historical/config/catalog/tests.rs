//! Configuration-only unified profile checks. No source archive access.
use super::*;
use tempfile::TempDir;
const PROFILE: &str = include_str!(
    "../../../../../data/.config/historical/profiles/trades/binance.perpetual.linear.trades.toml"
);
fn fixture(text: &str) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("profiles/trades")).unwrap();
    fs::write(
        dir.path()
            .join("profiles/trades/binance.perpetual.linear.trades.toml"),
        text,
    )
    .unwrap();
    dir
}
fn load(text: &str) -> Result<FormatCatalog> {
    let dir = fixture(text);
    FormatCatalog::load(dir.path())
}
#[test]
fn production_inventory_compiles_without_archives_or_external_schemas() {
    let c = FormatCatalog::load(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/historical"),
    )
    .unwrap();
    let mut ids = BTreeSet::new();
    for p in c.profiles() {
        assert!(ids.insert(&p.definition.id));
        assert_eq!(
            p.path.file_stem().unwrap().to_str().unwrap(),
            p.definition.id
        );
        assert_eq!(p.definition.id, p.physical.id);
        assert!(!p.normalizations.is_empty() || p.definition.unsupported_reason.is_some());
    }
    for id in [
        "binance.perpetual.linear.trades",
        "bitget.spot.spot.depth",
        "bybit.perpetual.inverse.trades",
        "gateio.perpetual.linear.depth",
        "okx.future.inverse.depth",
    ] {
        assert!(c.get(id).is_ok(), "{id}");
    }
}
#[test]
fn strict_unified_documents_reject_unknowns_versions_and_references() {
    for (old, new) in [
        ("schema_version = 2", "schema_version = 99"),
        ("kind = \"profile\"", "kind = \"physical\""),
        ("format_code = \"BINANCE-T2\"", "format_code = \"BYBIT-T2\""),
        ("field = \"qty\"", "field = \"absent\""),
        ("encoding = \"unix_milliseconds\"", "encoding = \"guess\""),
        (
            "quantity_encoding = \"base\"",
            "quantity_encoding = \"guess\"",
        ),
        ("ordering = \"chronological\"", "ordering = \"guess\""),
        ("transform = \"map\"", "transform = \"guess\""),
        ("header = true", "header = true\nunknown = 1"),
        (
            "schema_version = 2",
            "schema_version = 2\nphysical = \"external\"",
        ),
        ("market_category = \"linear\"", "market_category = \"spot\""),
    ] {
        assert!(load(&PROFILE.replace(old, new)).is_err(), "{new}");
    }
}
#[test]
fn filenames_duplicates_categories_and_symlinks_fail_closed() {
    let dir = fixture(PROFILE);
    fs::rename(
        dir.path()
            .join("profiles/trades/binance.perpetual.linear.trades.toml"),
        dir.path().join("profiles/trades/wrong.toml"),
    )
    .unwrap();
    assert!(
        FormatCatalog::load(dir.path())
            .unwrap_err()
            .to_string()
            .contains("filename")
    );
    let dir = fixture(PROFILE);
    fs::create_dir(dir.path().join("profiles/trades/nested")).unwrap();
    fs::write(
        dir.path()
            .join("profiles/trades/nested/binance.perpetual.linear.trades.toml"),
        PROFILE,
    )
    .unwrap();
    assert!(
        FormatCatalog::load(dir.path())
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );
}
#[test]
fn fingerprints_ignore_path_comments_and_key_order_but_include_semantics() {
    let a = load(PROFILE).unwrap();
    let b = load(&format!("# comment\n{PROFILE}")).unwrap();
    let id = "binance.perpetual.linear.trades";
    assert_eq!(
        a.get(id).unwrap().fingerprint,
        b.get(id).unwrap().fingerprint
    );
    let c = load(&PROFILE.replace("true = \"sell\"", "true = \"buy\"")).unwrap();
    assert_ne!(
        a.get(id).unwrap().fingerprint,
        c.get(id).unwrap().fingerprint
    );
    assert_eq!(
        fingerprint(&serde_json::json!({"a":1,"b":2})).unwrap(),
        fingerprint(&serde_json::json!({"b":2,"a":1})).unwrap()
    );
}
#[test]
fn unsupported_profiles_are_inventory_only() {
    let text = PROFILE.replace(
        "format_code =",
        "unsupported_reason = \"not implemented\"\nformat_code =",
    );
    let c = load(&text).unwrap();
    assert_eq!(c.profiles().count(), 1);
    assert!(
        c.get("binance.perpetual.linear.trades")
            .unwrap_err()
            .to_string()
            .contains("unsupported")
    );
}
#[test]
fn xlsx_fixed_layout_settings_are_enforced() {
    let original = include_str!(
        "../../../../../data/.config/historical/profiles/depth/bitget.spot.spot.depth.toml"
    );
    for text in [
        original.replace("workbook_sheets = 1", "workbook_sheets = 2"),
        original.replace("header = true", "header = false"),
        original.replace("sort_by = \"timestamp\"", "sort_by = \"asks\""),
        original.replace("unix_seconds", "unix_milliseconds"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("profiles/depth")).unwrap();
        fs::write(
            dir.path()
                .join("profiles/depth/bitget.spot.spot.depth.toml"),
            text,
        )
        .unwrap();
        assert!(FormatCatalog::load(dir.path()).is_err());
    }
}
#[test]
fn roots_are_absolute_and_missing_trees_do_not_fallback() {
    assert!(FormatCatalog::load("relative").is_err());
    let dir = tempfile::tempdir().unwrap();
    assert!(FormatCatalog::load(dir.path()).is_err());
    assert!(catalog_root(Some("relative".into()), None).is_err());
    assert!(catalog_root(Some("/one".into()), Some("/two".into())).is_err());
}

#[cfg(unix)]
#[test]
fn catalog_symlinks_are_rejected() {
    let dir = fixture(PROFILE);
    std::os::unix::fs::symlink(
        dir.path()
            .join("profiles/trades/binance.perpetual.linear.trades.toml"),
        dir.path().join("profiles/trades/link.toml"),
    )
    .unwrap();
    assert!(
        FormatCatalog::load(dir.path())
            .unwrap_err()
            .to_string()
            .contains("symlinks")
    );
}
