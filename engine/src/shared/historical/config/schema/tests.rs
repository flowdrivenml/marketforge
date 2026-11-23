use super::*;
use std::collections::BTreeSet;

fn leaves(value: &Value, prefix: &str, paths: &mut BTreeSet<String>) {
    if let Some(table) = value.as_object() {
        for (key, value) in table {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            leaves(value, &path, paths);
        }
    } else {
        assert!(paths.insert(prefix.to_owned()));
    }
}

#[test]
fn runtime_authoring_schema_covers_the_wire_and_matches_presence_rules() {
    let mut all = required_example();
    for rule in RULES {
        all["integrity_policy"][rule]["windows"] = json!({
            "hourly": {"max_rate": 0.1, "minimum_samples": 5},
            "daily": {"max_rate": 0.05, "minimum_samples": 10},
        });
    }
    let wire = serde_json::from_value::<RuntimeDocument>(all).unwrap();
    let mut wire = serde_json::to_value(wire).unwrap();
    wire.as_object_mut().unwrap().remove("merge"); // Only the legacy runtime accepts this table.
    let mut wire_paths = BTreeSet::new();
    leaves(&wire, "", &mut wire_paths);

    let fields = fields();
    assert_eq!(
        fields
            .iter()
            .map(|field| field.path.clone())
            .collect::<BTreeSet<_>>(),
        wire_paths
    );
    assert_eq!(fields.len(), wire_paths.len());
    for field in fields {
        let mut omitted = wire.clone();
        let (parent, key) = field.path.rsplit_once('.').unwrap_or(("", &field.path));
        let parent = if parent.is_empty() {
            &mut omitted
        } else {
            omitted
                .pointer_mut(&format!("/{}", parent.replace('.', "/")))
                .unwrap()
        };
        parent.as_object_mut().unwrap().remove(key);
        let loaded = serde_json::from_value::<RuntimeDocument>(omitted);
        assert_eq!(loaded.is_err(), field.required, "{}", field.path);
        if let Ok(loaded) = loaded {
            let loaded = serde_json::to_value(loaded).unwrap();
            assert_eq!(
                loaded
                    .pointer(&format!("/{}", field.path.replace('.', "/")))
                    .unwrap(),
                &field.default,
                "{}",
                field.path
            );
        }
    }
}

#[test]
fn schema_uses_serde_spellings_and_allows_no_unmanaged_table() {
    assert_eq!(
        choices::<ScratchFormat>(),
        ["json", "typed_trades", "typed_events"]
    );
    assert_eq!(
        choices::<TradeTimestamp>(),
        ["strict", "preserve", "reorder", "discard"]
    );
    let schema = runtime_schema();
    assert_eq!(schema["runtime_schema_version"], RUNTIME_SCHEMA_VERSION);
    let listed = schema["fields"].as_array().unwrap();
    assert!(listed.iter().all(|field| {
        !field["path"].as_str().unwrap().starts_with("ordering.")
            && !field["path"].as_str().unwrap().starts_with("merge.")
    }));
    assert_eq!(listed[0]["path"], "schema_version");
    assert_eq!(listed[0]["mutable"], false);
}
