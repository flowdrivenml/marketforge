use marketforge_engine::historical::config::{FormatCatalog, load_document_with_catalog};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
pub(super) fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}
pub(super) fn formats() -> FormatCatalog {
    FormatCatalog::load(root().join("data/.config/engine/normalize")).unwrap()
}
pub(super) struct Temp(pub(super) PathBuf);
impl Temp {
    pub(super) fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = crate::support::migration::persistent_path(format!(
            "historical-profile-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    pub(super) fn job(&self, value: &serde_json::Value) -> PathBuf {
        let p = self.0.join("job.json");
        fs::write(&p, serde_json::to_vec(value).unwrap()).unwrap();
        p
    }
}
impl Drop for Temp {
    fn drop(&mut self) {}
}

fn sample() -> serde_json::Value {
    serde_json::to_value(
        crate::support::migration::template("bybit.spot.spot.trades:BTCUSDT").unwrap(),
    )
    .unwrap()
}
pub(super) fn lightweight(
    mut job: serde_json::Value,
    catalog: &FormatCatalog,
) -> serde_json::Value {
    job["protocol_version"] = 2.into();
    for task in job["tasks"].as_array_mut().unwrap() {
        let resolved: marketforge_engine::historical::job::WorkTask =
            serde_json::from_value(task.clone()).unwrap();
        let p = catalog.for_task(&resolved).unwrap().unwrap();
        let obj = task.as_object_mut().unwrap();
        for k in [
            "format_code",
            "raw_schema",
            "normalizations",
            "source_ordering",
            "source_compression",
        ] {
            obj.remove(k);
        }
        obj.insert("format_profile".into(), p.definition.id.clone().into());
        obj.insert("profile_fingerprint".into(), p.fingerprint.clone().into());
    }
    job
}
#[test]
fn lightweight_and_legacy_jobs_resolve_identically() {
    let c = formats();
    let t = Temp::new();
    let legacy = sample();
    let mut a = load_document_with_catalog(t.job(&legacy), &c).unwrap();
    for (priority, task) in a.tasks.iter_mut().enumerate() {
        task.raw_schema["marketforge_source"] = serde_json::json!({"source_priority": priority});
    }
    let b = load_document_with_catalog(t.job(&lightweight(legacy, &c)), &c).unwrap();
    assert_eq!(a.tasks, b.tasks);
    assert_eq!(
        b.protocol_version,
        marketforge_engine::historical::job::PROCESSING_PROTOCOL_VERSION
    );
}
#[test]
fn conflicts_unknown_fields_and_fingerprints_fail_closed() {
    let c = formats();
    let t = Temp::new();
    let mut j = sample();
    j["tasks"][0]["normalizations"][0]["quantity_encoding"] = "quote".into();
    assert!(
        load_document_with_catalog(t.job(&j), &c)
            .unwrap_err()
            .to_string()
            .contains("conflicts")
    );
    let mut j = lightweight(sample(), &c);
    j["tasks"][0]["raw_schema"] = serde_json::json!({});
    assert!(load_document_with_catalog(t.job(&j), &c).is_err());
    let mut j = lightweight(sample(), &c);
    j["tasks"][0]["profile_fingerprint"] = "bad".into();
    assert!(
        load_document_with_catalog(t.job(&j), &c)
            .unwrap_err()
            .to_string()
            .contains("fingerprint")
    );
}
#[test]
fn errors_identify_file_and_invalid_configuration() {
    let t = Temp::new();
    let (_, file) = one_profile_fixture(&t);
    let bad = file.clone();
    let text = fs::read_to_string(&file).unwrap();

    fs::write(
        &bad,
        text.replace("schema_version = 2", "schema_version = 99"),
    )
    .unwrap();
    let e = FormatCatalog::load(&t.0).unwrap_err().to_string();
    assert!(e.contains("bybit.spot.spot.trades.toml"));
    assert!(e.contains("version"));
}

fn one_profile_fixture(temp: &Temp) -> (PathBuf, PathBuf) {
    let catalog = formats();
    let profile = catalog.get("bybit.spot.spot.trades").unwrap();
    let path = temp.0.join("profiles/trades/bybit.spot.spot.trades.toml");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::copy(&profile.path, &path).unwrap();
    (path.clone(), path)
}
#[test]
fn conflicting_profile_identity_is_rejected() {
    let t = Temp::new();
    let (profile, _) = one_profile_fixture(&t);
    // A conflicting definition must use a discoverable filename. Unrelated
    // TOML files are ignored rather than treated as profiles.
    let duplicate = profile.with_file_name("bybit.perpetual.linear.trades.toml");
    fs::copy(profile, duplicate).unwrap();
    let e = FormatCatalog::load(&t.0).unwrap_err().to_string();
    assert!(e.contains("profile filename must match ID"), "{e}");
}
#[test]
fn unsupported_transform_unknown_rule_and_missing_source_fail_at_load() {
    for (before, after) in [
        ("casefold", "unsupported_transform"),
        ("field = \"volume\"", "\"source\" = \"missing_field\""),
        (
            "transform = \"casefold\"",
            "transform = \"casefold\"\ntypo = true",
        ),
    ] {
        let t = Temp::new();
        let (p, _) = one_profile_fixture(&t);
        let old = fs::read_to_string(&p).unwrap();
        assert!(old.contains(before));
        fs::write(&p, old.replace(before, after)).unwrap();
        let e = FormatCatalog::load(&t.0).unwrap_err().to_string();
        assert!(e.contains("bybit.spot.spot.trades"));
        assert!(e.contains("bybit.spot.spot.trades.toml"));
    }
}
#[test]
fn incompatible_instrument_and_container_cannot_override_profile() {
    let c = formats();
    let t = Temp::new();
    for key in ["exchange", "source_compression"] {
        let mut j = lightweight(sample(), &c);
        j["tasks"][0][key] = if key == "exchange" {
            "binance".into()
        } else {
            "tar_gzip".into()
        };
        let e = load_document_with_catalog(t.job(&j), &c)
            .unwrap_err()
            .to_string();
        assert!(e.contains("profile"));
        assert!(e.contains("job.json"));
    }
}
#[test]
fn profile_fingerprint_is_independent_of_file_location_and_whitespace() {
    let t = Temp::new();
    let (p, _) = one_profile_fixture(&t);
    let old = fs::read_to_string(&p).unwrap();
    fs::write(&p, format!("# whitespace and comment\n\n{old}\n")).unwrap();
    let a = formats();
    let b = FormatCatalog::load(&t.0).unwrap();
    let id = "bybit.spot.spot.trades";
    assert_eq!(
        a.get(id).unwrap().fingerprint,
        b.get(id).unwrap().fingerprint
    );
}
#[test]
fn unknown_and_unsupported_profiles_have_explicit_errors() {
    let c = formats();
    assert!(
        c.get("bybit-t1")
            .unwrap_err()
            .to_string()
            .contains("unknown")
    );
    for p in c
        .profiles()
        .filter(|p| p.definition.unsupported_reason.is_some())
    {
        let error = c.get(&p.definition.id).unwrap_err().to_string();
        assert!(error.contains("unsupported"));
        assert!(error.contains(&p.definition.id));
    }
}
#[test]
fn resolved_job_provenance_can_be_reloaded_without_changing_fingerprint() {
    let c = formats();
    let t = Temp::new();
    let a = load_document_with_catalog(t.job(&sample()), &c).unwrap();
    let value = serde_json::to_value(&a).unwrap();
    let b = load_document_with_catalog(t.job(&value), &c).unwrap();
    assert_eq!(a, b);
}

#[test]
fn resolved_snapshot_rejects_changed_profile_identity_or_fingerprint() {
    let c = formats();
    let t = Temp::new();
    let resolved = load_document_with_catalog(t.job(&sample()), &c).unwrap();
    for field in ["id", "sha256"] {
        let mut value = serde_json::to_value(&resolved).unwrap();
        value["tasks"][0]["raw_schema"]["marketforge_profile"][field] = "changed".into();
        let error = load_document_with_catalog(t.job(&value), &c)
            .unwrap_err()
            .to_string();
        assert!(error.contains("provenance"), "{error}");
    }
}
