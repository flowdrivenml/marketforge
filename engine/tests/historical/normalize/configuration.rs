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
    FormatCatalog::load(root().join("data/.config/historical/formats")).unwrap()
}
pub(super) struct Temp(pub(super) PathBuf);
impl Temp {
    pub(super) fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
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
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
#[test]
fn complete_profile_inventory_compiles_and_resolves_uniquely() {
    let catalog = formats();
    let profiles: Vec<_> = catalog.profiles().collect();
    assert_eq!(profiles.len(), 36);
    assert_eq!(
        profiles
            .iter()
            .map(|p| p.definition.rules.len())
            .sum::<usize>(),
        49
    );
    assert_eq!(
        profiles
            .iter()
            .filter(|p| p.definition.unsupported_reason.is_none())
            .count(),
        31
    );
    assert_eq!(
        profiles
            .iter()
            .map(|p| p.normalizations.len())
            .sum::<usize>(),
        42
    );
    for profile in profiles {
        if profile.definition.unsupported_reason.is_some() {
            assert!(catalog.get(&profile.definition.id).is_err());
        } else {
            assert_eq!(
                catalog
                    .get(&profile.definition.id)
                    .unwrap()
                    .fingerprint
                    .len(),
                64
            );
        }
    }
}
#[test]
fn every_existing_job_matches_resolved_toml_configuration() {
    let catalog = formats();
    let mut count = 0;
    for e in fs::read_dir(root().join("data/.jobs/process")).unwrap() {
        let p = e.unwrap().path();
        if p.extension().is_none_or(|e| e != "json") {
            continue;
        }
        let job = load_document_with_catalog(&p, &catalog)
            .unwrap_or_else(|e| panic!("{}: {e}", p.display()));
        for task in job.tasks {
            assert!(
                task.raw_schema["marketforge_profile"]["sha256"]
                    .as_str()
                    .unwrap()
                    .len()
                    == 64
            );
        }
        count += 1;
    }
    assert_eq!(count, 43);
}
fn sample() -> serde_json::Value {
    serde_json::from_slice(
        &fs::read(
            root().join(
                "data/.jobs/process/bybit-spot-spot-BTCUSDT-trade-20260901-20260904-d104.json",
            ),
        )
        .unwrap(),
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
    let a = load_document_with_catalog(t.job(&legacy), &c).unwrap();
    let b = load_document_with_catalog(t.job(&lightweight(legacy, &c)), &c).unwrap();
    assert_eq!(a.tasks, b.tasks);
    let example = load_document_with_catalog(
        root().join("engine/fixtures/jobs/historical-profile-v2.json"),
        &c,
    )
    .unwrap();
    assert_eq!(example.tasks[0].normalizations, a.tasks[0].normalizations);
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
    let source = root().join("data/.config/historical/formats/trades");
    let file = fs::read_dir(source)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("physical-")
        })
        .unwrap();
    let bad = t.0.join("bad.toml");
    fs::write(
        &bad,
        fs::read_to_string(file)
            .unwrap()
            .replace("schema_version = 1", "schema_version = 99"),
    )
    .unwrap();
    let e = FormatCatalog::load(&t.0).unwrap_err().to_string();
    assert!(e.contains("bad.toml"));
    assert!(e.contains("version"));
}

fn one_profile_fixture(temp: &Temp) -> (PathBuf, PathBuf) {
    let catalog = formats();
    let profile = catalog.get("bybit.spot.spot.bybit-t1.v1").unwrap();
    let profile_path = temp.0.join("profile.toml");
    fs::copy(&profile.path, &profile_path).unwrap();
    let physical_path = root()
        .join("data/.config/historical/formats/trades")
        .join(format!(
            "physical-{}.toml",
            profile.definition.physical.trim_start_matches("trades-")
        ));
    let target = temp.0.join("physical.toml");
    fs::copy(physical_path, &target).unwrap();
    (profile_path, target)
}
#[test]
fn duplicate_profile_and_physical_definitions_are_rejected() {
    for which in ["profile.toml", "physical.toml"] {
        let t = Temp::new();
        one_profile_fixture(&t);
        fs::copy(t.0.join(which), t.0.join("duplicate.toml")).unwrap();
        let e = FormatCatalog::load(&t.0).unwrap_err().to_string();
        assert!(e.contains("duplicate") || e.contains("ambiguous"), "{e}");
    }
}
#[test]
fn unsupported_transform_unknown_rule_and_missing_source_fail_at_load() {
    for (before, after) in [
        ("casefold", "unsupported_transform"),
        ("\"source\" = \"volume\"", "\"source\" = \"missing_field\""),
        (
            "\"transform\" = \"casefold\"",
            "\"transform\" = \"casefold\", \"typo\" = true",
        ),
    ] {
        let t = Temp::new();
        let (p, _) = one_profile_fixture(&t);
        let old = fs::read_to_string(&p).unwrap();
        assert!(old.contains(before));
        fs::write(&p, old.replace(before, after)).unwrap();
        let e = FormatCatalog::load(&t.0).unwrap_err().to_string();
        assert!(e.contains("bybit.spot.spot.bybit-t1.v1"));
        assert!(e.contains("profile.toml"));
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
    let id = "bybit.spot.spot.bybit-t1.v1";
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
