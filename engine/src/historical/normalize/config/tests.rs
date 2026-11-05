//! Configuration-only tests. Temporary files contain TOML, never market data.
use super::*;
use tempfile::TempDir;

const TOML: &str = include_str!("../../../../../data/.config/historical/normalize.toml");

fn load(text: &str) -> Result<ProcessingConfig> {
    let temp = TempDir::new().unwrap();
    fs::write(temp.path().join("normalize.toml"), text).unwrap();
    load_processing_config("normalize.toml", temp.path())
}

#[test]
fn runtime_toml_loads_and_resolves_relative_scratch() {
    let temp = TempDir::new().unwrap();
    fs::write(temp.path().join("runtime.toml"), TOML).unwrap();
    let config = load_processing_config("runtime.toml", temp.path()).unwrap();
    assert_eq!(
        config.resources.scratch_path,
        temp.path().join("data/.work/normalize")
    );
    assert_eq!(config.resources.workers, 1);
    assert!(config.integrity_policy.enabled);
    let absolute = temp.path().join("absolute-scratch");
    let text = TOML.replace("data/.work/normalize", absolute.to_str().unwrap());
    assert_eq!(load(&text).unwrap().resources.scratch_path, absolute);
    assert!(!temp.path().join("data").exists());
}

#[test]
fn invalid_toml_reports_file_and_field() {
    for (old, new, expected) in [
        ("schema_version = 1", "schema_version = 2", "schema_version"),
        ("schema_version = 1", "", "schema_version"),
        ("workers = 1", "", "workers"),
        ("workers = 1", "workers = -1", "workers"),
        ("workers = 1", "workers = 0", "resources.workers"),
        (
            "memory_budget_bytes = 1073741824",
            "memory_budget_bytes = 0",
            "memory_budget_bytes",
        ),
        (
            "scratch_budget_bytes = 1073741824",
            "scratch_budget_bytes = 0",
            "scratch_budget_bytes",
        ),
        (
            "scratch_path = \"data/.work/normalize\"",
            "scratch_path = \"\"",
            "scratch_path",
        ),
        (
            "row_group_target_bytes = 67108864",
            "row_group_target_bytes = 0",
            "row_group_target_bytes",
        ),
        (
            "file_target_bytes = 268435456",
            "file_target_bytes = 1",
            "row_group_target_bytes",
        ),
        ("action = \"fail\"", "action = \"degrade\"", "degrade"),
        ("action = \"fail\"", "action = \"ignore\"", "ignore"),
        ("workers = 1", "workers = 1\nunknown = 2", "unknown"),
        (
            "schema_version = 1",
            "schema_version = 1\nunknown = true",
            "unknown",
        ),
        (
            "row_group_target_bytes = 67108864",
            "row_group_target_bytes = 67108864\nunknown = true",
            "unknown",
        ),
        (
            "max_count = 0",
            "max_rate = 1.1",
            "integrity_policy.parse_failure",
        ),
        (
            "max_count = 0",
            "max_rate = nan",
            "integrity_policy.parse_failure",
        ),
    ] {
        // Degrade with max_count is valid; remove thresholds for the invalid case.
        let mut text = TOML.replace(old, new);
        if new == "action = \"degrade\"" {
            text = text.replace("max_count = 0", "");
        }
        let error = load(&text).unwrap_err().to_string();
        assert!(
            error.contains("normalize.toml") && error.contains(expected),
            "{expected}: {error}"
        );
    }
}

#[test]
fn roots_and_absolute_paths_are_explicit() {
    let temp = TempDir::new().unwrap();
    let file = temp.path().join("file");
    fs::write(&file, "").unwrap();
    assert!(project_root(&file).is_err());
    assert!(project_root(temp.path().join("missing")).is_err());
    assert_eq!(resolve_path(temp.path(), &file), file);
    assert_eq!(
        resolve_path(temp.path(), Path::new("relative")),
        temp.path().join("relative")
    );
}

#[test]
fn required_sections_nested_unknown_fields_and_scratch_files_fail() {
    for field in ["resources", "integrity_policy"] {
        let mut document: toml::Value = toml::from_str(TOML).unwrap();
        document.as_table_mut().unwrap().remove(field);
        let error = load(&toml::to_string(&document).unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains(field), "{error}");
    }
    for path in [
        vec!["integrity_policy"],
        vec!["integrity_policy", "parse_failure"],
    ] {
        let mut document: toml::Value = toml::from_str(TOML).unwrap();
        let mut table = &mut document;
        for key in path {
            table = table.get_mut(key).unwrap();
        }
        table
            .as_table_mut()
            .unwrap()
            .insert("unknown".into(), toml::Value::Boolean(true));
        assert!(
            load(&toml::to_string(&document).unwrap())
                .unwrap_err()
                .to_string()
                .contains("unknown")
        );
    }
    let temp = TempDir::new().unwrap();
    let scratch = temp.path().join("file");
    fs::write(&scratch, "configuration-test").unwrap();
    let text = TOML.replace("data/.work/normalize", scratch.to_str().unwrap());
    assert!(
        load(&text)
            .unwrap_err()
            .to_string()
            .contains("resources.scratch_path")
    );
}
