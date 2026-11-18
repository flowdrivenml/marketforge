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

#[test]
fn native_trade_identity_contract_is_strict_and_fingerprinted() {
    let base = load(PROFILE).unwrap();
    let id = "binance.perpetual.linear.trades";
    for scope in ["global", "source_local", "unreliable"] {
        let text = PROFILE.replace(
            "[source]",
            &format!("[source]\ntrade_id_scope = \"{scope}\""),
        );
        let catalog = load(&text).unwrap();
        assert_eq!(
            catalog.get(id).unwrap().physical.raw_schema["marketforge_trade_identity"]["scope"],
            scope
        );
        assert_ne!(
            catalog.get(id).unwrap().fingerprint,
            base.get(id).unwrap().fingerprint
        );
    }
    assert!(
        load(&PROFILE.replace(
            "[source]",
            "[source]\ntrade_id_scope = \"instrument_scoped\""
        ))
        .is_err()
    );
    assert!(load(&PROFILE.replace("[source]", "[source]\ntrade_id_scope = \"guess\"")).is_err());
    assert!(
        load(&PROFILE.replace(
            "[source]",
            "[source]\ntrade_id_scope = \"source_local\"\ntrade_id_reset_domain = \"date\""
        ))
        .is_err()
    );
    let text=PROFILE.replace("[source]","[source]\ntrade_id_scope = \"instrument_scoped\"\ntrade_id_reset_domain = \"instrument_lifetime\"");
    assert!(load(&text).is_ok());
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/historical");
    let catalog = FormatCatalog::load(root).unwrap();
    assert_eq!(
        catalog
            .get("bybit.spot.spot.trades")
            .unwrap()
            .physical
            .raw_schema["marketforge_trade_identity"]["scope"],
        "source_local"
    );
    assert!(
        catalog
            .get("bybit.perpetual.linear.trades")
            .unwrap()
            .physical
            .raw_schema
            .get("marketforge_trade_identity")
            .is_none()
    );
}

#[test]
fn manual_equity_trade_and_depth_profiles_compile_under_their_venue() {
    let directory = tempfile::tempdir().unwrap();
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/content/examples/catalogue");
    for kind in ["trades", "depth"] {
        let id = format!("example_exchange.equity.equity.{kind}");
        let target = directory.path().join("profiles").join(kind);
        fs::create_dir_all(&target).unwrap();
        fs::copy(
            examples.join(format!("{id}.toml")),
            target.join(format!("{id}.toml")),
        )
        .unwrap();
    }
    let catalogue = FormatCatalog::load(directory.path()).unwrap();
    for kind in ["trades", "depth"] {
        let profile = catalogue
            .get(&format!("example_exchange.equity.equity.{kind}"))
            .unwrap();
        assert_eq!(profile.definition.exchange, "example_exchange");
        assert_eq!(profile.definition.instrument_type, InstrumentKind::Equity);
        assert_eq!(profile.definition.market_category, MarketCategory::Equity);
        assert_eq!(profile.fingerprint.len(), 64);
    }
}

#[test]
fn byod_profile_contracts_fail_closed() {
    let examples = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../docs/content/examples/bring-your-own-data/databento");
    let trades_path = examples.join("xnas.equity.equity.trades.toml");
    let trades = fs::read_to_string(&trades_path).unwrap();
    assert!(unified::compile(&trades, &trades_path, "trades").is_ok());
    for field in ["hd.ts_event", "ts_recv"] {
        let header = if field.contains('.') {
            format!("[fields.\"{field}\"]")
        } else {
            format!("[fields.{field}]")
        };
        let mismatched = trades.replace(
            &format!("{header}\ntype = \"string\"\nencoding = \"iso8601_nanoseconds\""),
            &format!("{header}\ntype = \"integer\"\nencoding = \"unix_nanoseconds\""),
        );
        assert_ne!(mismatched, trades);
        assert!(unified::compile(&mismatched, &trades_path, "trades").is_err());
    }

    let depth_path = examples.join("xnas.equity.equity.depth.toml");
    let depth = fs::read_to_string(&depth_path).unwrap();
    assert!(unified::compile(&depth, &depth_path, "depth").is_ok());
    for (old, new) in [
        ("levels_per_side = 10", "levels_per_side = 9"),
        (
            "ordering = \"chronological\"",
            "ordering = \"timestamp_sortable\"",
        ),
    ] {
        assert!(unified::compile(&depth.replace(old, new), &depth_path, "depth").is_err());
    }
}
