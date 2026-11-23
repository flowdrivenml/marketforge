//! Authoring metadata for the normalization runtime wire contract.
use super::*;
use crate::shared::historical::{IntegrityAction, policies::*};
use serde::{de, de::DeserializeOwned};
use serde_json::{Value, json};

const RULES: [&str; 7] = [
    "parse_failure",
    "invalid_record",
    "sequence_gap",
    "timestamp_regression",
    "missing_snapshot",
    "invalid_book",
    "transformation_failure",
];

#[derive(Serialize)]
struct Dependency {
    path: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    values: Vec<&'static str>,
}

fn when(path: impl Into<String>, values: &[&'static str]) -> Dependency {
    Dependency {
        path: path.into(),
        values: values.to_vec(),
    }
}

#[derive(Serialize)]
struct Field {
    path: String,
    kind: &'static str,
    risk: &'static str,
    purpose: &'static str,
    constraint: String,
    required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    required_when: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    integer_bits: Option<u32>,
    mutable: bool,
    default: Value,
    choices: Vec<&'static str>,
    dependencies: Vec<Dependency>,
}

#[derive(Debug)]
struct Variants(Vec<&'static str>);

impl std::fmt::Display for Variants {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("runtime enum schema probe")
    }
}
impl std::error::Error for Variants {}
impl de::Error for Variants {
    fn custom<T: std::fmt::Display>(_error: T) -> Self {
        Self(vec![])
    }
    fn unknown_variant(_variant: &str, expected: &'static [&'static str]) -> Self {
        Self(expected.to_vec())
    }
}

fn choices<T: DeserializeOwned>() -> Vec<&'static str> {
    // Serde reports the accepted spellings directly, including newly added variants.
    let probe = de::value::StrDeserializer::<Variants>::new("!runtime-schema-probe!");
    let values = T::deserialize(probe)
        .err()
        .expect("probe must be an unknown enum variant")
        .0;
    assert!(!values.is_empty(), "expected a Serde string enum");
    values
}

fn required_example() -> Value {
    let rules = RULES
        .into_iter()
        .map(|rule| (rule.into(), json!({"action":"fail"})))
        .collect::<serde_json::Map<_, _>>();
    json!({
        "schema_version": RUNTIME_SCHEMA_VERSION,
        "resources": {
            "workers": 1, "memory_budget_bytes": 1, "scratch_budget_bytes": 1,
            "scratch_path": ".", "parquet": {"row_group_target_bytes": 1, "file_target_bytes": 1}
        },
        "integrity_policy": rules,
    })
}

fn fields() -> Vec<Field> {
    // Fill defaults through the same tagged wire type used in load_runtime.
    let defaults = serde_json::to_value(
        serde_json::from_value::<RuntimeDocument>(required_example())
            .expect("required runtime schema example"),
    )
    .expect("serializable runtime defaults");
    let mut fields = Vec::new();
    let mut add = |path: &str,
                   kind,
                   risk,
                   purpose,
                   constraint: &str,
                   required,
                   choices,
                   dependencies: Vec<Dependency>| {
        let default = if required {
            Value::Null
        } else {
            defaults
                .pointer(&format!("/{}", path.replace('.', "/")))
                .cloned()
                .unwrap_or_default()
        };
        let required_when = if required {
            dependencies
                .iter()
                .find(|dependency| dependency.values.is_empty())
                .map(|d| d.path.clone())
        } else {
            None
        };
        let integer_bits = match path {
            "resources.workers" | "merge_fan_in" => Some(usize::BITS),
            "schema_version" => Some(u32::BITS),
            _ if matches!(kind, "integer" | "bytes" | "milliseconds") => Some(u64::BITS),
            _ => None,
        };
        fields.push(Field {
            path: path.into(),
            kind,
            risk,
            purpose,
            constraint: constraint.into(),
            required,
            required_when,
            integer_bits,
            mutable: path != "schema_version",
            default,
            choices,
            dependencies,
        });
    };
    const BASIC: &str = "ordinary";
    const RESOURCE: &str = "resource-sensitive";
    const PROCESSING: &str = "processing-semantic";
    const INTEGRITY: &str = "integrity-critical";
    const POSITIVE: &str = "Greater than zero";
    const ADDRESSABLE: &str = "Positive and at most usize::MAX / 128 for this Engine build";
    const RATE: &str = "Finite fraction between 0 and 1 inclusive";
    const LOSS_RATE: &str =
        "Finite fraction between 0 and 1 inclusive, representable as a Rust Decimal";
    const MS: &str = "Must fit in signed nanoseconds after multiplication by 1,000,000";
    let empty = Vec::new;
    add(
        "schema_version",
        "integer",
        BASIC,
        "Normalization runtime schema identity.",
        "Must match the Engine runtime schema version",
        true,
        empty(),
        vec![],
    );
    add(
        "merge_fan_in",
        "integer",
        RESOURCE,
        "Requested merge inputs per pass. Admission can reduce the fan-in to fit memory and file handles.",
        &format!(
            "{} to {} inclusive",
            MERGE_FAN_IN.start(),
            MERGE_FAN_IN.end()
        ),
        false,
        empty(),
        vec![],
    );
    add(
        "scratch_format",
        "enum",
        RESOURCE,
        "Internal scratch serialization. Typed variants use binary trades; typed_events also uses shared-envelope depth where supported by the path. Other records use JSON.",
        "Choices supported by this Engine build",
        false,
        choices::<ScratchFormat>(),
        vec![],
    );
    add(
        "scratch_codec",
        "enum",
        RESOURCE,
        "Compression for temporary scratch records. Final Parquet encoding is independent.",
        "Choices supported by this Engine build",
        false,
        choices::<ScratchCodec>(),
        vec![],
    );
    add(
        "diagnostics.mode",
        "enum",
        BASIC,
        "Controls the detail of Engine progress diagnostics and retained performance evidence.",
        "",
        false,
        choices::<DiagnosticsMode>(),
        vec![],
    );
    add(
        "diagnostics.interval_ms",
        "milliseconds",
        BASIC,
        "Timer-driven diagnostic heartbeat interval.",
        &format!(
            "{} to {} inclusive",
            DIAGNOSTICS_INTERVAL_MS.start(),
            DIAGNOSTICS_INTERVAL_MS.end()
        ),
        false,
        empty(),
        vec![when("diagnostics.mode", &["basic", "detailed"])],
    );
    add(
        "resources.workers",
        "integer",
        RESOURCE,
        "Requested worker concurrency. Increasing it may increase memory, scratch, and handle demand.",
        POSITIVE,
        true,
        empty(),
        vec![],
    );
    add(
        "resources.memory_budget_bytes",
        "bytes",
        RESOURCE,
        "Memory admission budget for Engine work.",
        POSITIVE,
        true,
        empty(),
        vec![],
    );
    add(
        "resources.scratch_budget_bytes",
        "bytes",
        RESOURCE,
        "Scratch-space admission budget for spools and other temporary files.",
        POSITIVE,
        true,
        empty(),
        vec![],
    );
    add(
        "resources.scratch_path",
        "string",
        RESOURCE,
        "Temporary workspace path. Relative paths resolve from the project root.",
        "Nonempty; if it exists it must be a directory",
        true,
        empty(),
        vec![],
    );
    add(
        "resources.parquet.row_group_target_bytes",
        "bytes",
        RESOURCE,
        "Target allocation for one Parquet row group.",
        "Positive and no greater than resources.parquet.file_target_bytes",
        true,
        empty(),
        vec![],
    );
    add(
        "resources.parquet.file_target_bytes",
        "bytes",
        RESOURCE,
        "Target used when splitting output Parquet files.",
        "Positive and at least resources.parquet.row_group_target_bytes",
        true,
        empty(),
        vec![],
    );
    for (name, kind, purpose) in [
        (
            "workbook_bytes",
            "bytes",
            "Selected XLSX source or archive member bytes.",
        ),
        (
            "workbook_xml_bytes",
            "bytes",
            "Expanded XML member and worksheet cell text bound.",
        ),
        (
            "workbook_rows",
            "integer",
            "Materialized/decoded worksheet rows; also bounds shared strings at three times the row limit.",
        ),
        (
            "record_bytes",
            "bytes",
            "Maximum CSV payload, JSONL line, or XLSX cell text bytes.",
        ),
        (
            "depth_levels",
            "integer",
            "Bound on reconstructed book and pending snapshot levels.",
        ),
        (
            "metric_entries",
            "integer",
            "Bounds retained metrics and inventories, including time buckets, files, row groups, and segments.",
        ),
        (
            "open_files",
            "integer",
            "Participating normalization file handles, including session headroom.",
        ),
        (
            "reconciliation_bytes",
            "bytes",
            "Resident routing and reconciliation buffers per execution unit. Disk spools use the scratch budget.",
        ),
    ] {
        add(
            &format!("resources.normalization_limits.{name}"),
            kind,
            RESOURCE,
            purpose,
            ADDRESSABLE,
            false,
            empty(),
            vec![],
        );
    }
    add(
        "integrity_policy.enabled",
        "boolean",
        INTEGRITY,
        "Enables integrity acceptance evaluation. Legacy v1 standard and strict strings both enable it. Use a boolean when editing.",
        "",
        false,
        empty(),
        vec![],
    );
    for rule in RULES {
        let base = format!("integrity_policy.{rule}");
        let enabled = || when("integrity_policy.enabled", &["true"]);
        let degrade = || when(format!("{base}.action"), &["degrade"]);
        add(
            &format!("{base}.action"),
            "enum",
            INTEGRITY,
            "Fail rejects every occurrence. Degrade marks occurrences and rejects when any applicable threshold is exceeded.",
            "Degrade requires a count, rate, hourly, or daily threshold",
            true,
            choices::<IntegrityAction>(),
            vec![enabled()],
        );
        add(
            &format!("{base}.max_count"),
            "integer",
            INTEGRITY,
            "Overall occurrence-count limit. An omitted count supplies no count limit; zero rejects the first occurrence under degrade.",
            "",
            false,
            empty(),
            vec![enabled(), degrade()],
        );
        add(
            &format!("{base}.max_rate"),
            "rate",
            INTEGRITY,
            "Rate limit over eligible samples, evaluated job-wide, per source file, and per stream.",
            RATE,
            false,
            empty(),
            vec![enabled(), degrade()],
        );
        add(
            &format!("{base}.minimum_samples"),
            "integer",
            INTEGRITY,
            "Eligible sample floor for the job, source file, and stream rate checks. Count checks are independent.",
            "",
            false,
            empty(),
            vec![enabled(), degrade(), when(format!("{base}.max_rate"), &[])],
        );
        for window in ["hourly", "daily"] {
            let window_base = format!("{base}.windows.{window}");
            let active = || vec![enabled(), degrade(), when(&window_base, &[])];
            add(
                &format!("{window_base}.max_rate"),
                "rate",
                INTEGRITY,
                "Rate limit per configured UTC window under degrade. Window presence requires trustworthy timestamps even under fail.",
                "Finite fraction between 0 and 1 inclusive; required when this window is present",
                true,
                empty(),
                active(),
            );
            add(
                &format!("{window_base}.minimum_samples"),
                "integer",
                INTEGRITY,
                "Eligible sample floor for the corresponding UTC rate window.",
                "Required when this window is present",
                true,
                empty(),
                active(),
            );
        }
    }
    for data in ["trades", "depth"] {
        let base = format!("reconciliation.{data}");
        let preferred = || when(format!("{base}.overlap_policy"), &["prefer_source"]);
        let allows_loss = || {
            when(
                format!("{base}.prefer_source.on_unique_data_loss"),
                &["mark_degraded"],
            )
        };
        let suffix = format!("{base}.prefer_source");
        let modes = if data == "trades" {
            choices::<TradeOverlap>()
        } else {
            choices::<DepthOverlap>()
        };
        add(
            &format!("{base}.overlap_policy"),
            "enum",
            INTEGRITY,
            "Controls overlap handling. Strict checks agreement; prefer_source selects precedence; depth snapshot_recovery uses verified recovery boundaries.",
            "",
            false,
            modes,
            vec![],
        );
        add(
            &format!("{suffix}.selection"),
            "enum",
            PROCESSING,
            "Selects the earlier or later source for overlap precedence.",
            "",
            false,
            choices::<Selection>(),
            vec![preferred()],
        );
        if data == "depth" {
            add(
                &format!("{suffix}.switch_boundary"),
                "enum",
                INTEGRITY,
                "Chooses snapshot or verified source boundary for depth source switches.",
                "",
                false,
                choices::<SwitchBoundary>(),
                vec![preferred()],
            );
        }
        add(
            &format!("{suffix}.on_unique_data_loss"),
            "enum",
            INTEGRITY,
            "Reject unique loss or mark bounded loss degraded.",
            "mark_degraded requires a positive max_excluded_events even when preferences are inactive",
            false,
            choices::<LossAction>(),
            vec![preferred()],
        );
        add(
            &format!("{suffix}.max_excluded_events"),
            "integer",
            INTEGRITY,
            "Finite cap on excluded unique events. Zero provides no allowance.",
            "Positive when on_unique_data_loss is mark_degraded",
            false,
            empty(),
            vec![preferred(), allows_loss()],
        );
        add(
            &format!("{suffix}.max_excluded_rate"),
            "rate",
            INTEGRITY,
            "Optional additional rate cap on excluded unique events.",
            LOSS_RATE,
            false,
            empty(),
            vec![preferred(), allows_loss()],
        );
        add(
            &format!("{suffix}.minimum_samples"),
            "integer",
            INTEGRITY,
            "Eligible sample floor for the overlap exclusion rate. The event cap still applies.",
            "",
            false,
            empty(),
            vec![
                preferred(),
                allows_loss(),
                when(format!("{suffix}.max_excluded_rate"), &[]),
            ],
        );
        add(
            &format!("{suffix}.max_excluded_interval_ms"),
            "milliseconds",
            INTEGRITY,
            "Optional maximum time span of exclusions.",
            MS,
            false,
            empty(),
            vec![preferred(), allows_loss()],
        );
    }
    let trade = "timestamp_regression.trades.policy";
    let depth = "timestamp_regression.depth.policy";
    add(
        trade,
        "enum",
        INTEGRITY,
        "Strict rejects regressions, preserve retains native order, reorder uses a bounded window, and discard drops bounded events.",
        "Reorder and discard require explicit positive bounds",
        false,
        choices::<TradeTimestamp>(),
        vec![],
    );
    add(
        "timestamp_regression.trades.max_regression_ms",
        "milliseconds",
        INTEGRITY,
        "Largest accepted trade regression under a non-strict policy.",
        "Must fit signed nanoseconds; positive for reorder or discard",
        false,
        empty(),
        vec![when(trade, &["preserve", "reorder", "discard"])],
    );
    add(
        "timestamp_regression.trades.max_reorder_window_ms",
        "milliseconds",
        INTEGRITY,
        "High-water window for trade reordering.",
        "Must fit signed nanoseconds; positive and at least max_regression_ms for reorder",
        false,
        empty(),
        vec![when(trade, &["reorder"])],
    );
    add(
        "timestamp_regression.trades.max_discarded_events",
        "integer",
        INTEGRITY,
        "Maximum discarded trade events.",
        "Positive for discard",
        false,
        empty(),
        vec![when(trade, &["discard"])],
    );
    add(
        "timestamp_regression.trades.max_discarded_rate",
        "rate",
        INTEGRITY,
        "Additional rate cap on discarded trade events.",
        LOSS_RATE,
        false,
        empty(),
        vec![when(trade, &["discard"])],
    );
    add(
        "timestamp_regression.trades.minimum_samples",
        "integer",
        INTEGRITY,
        "Eligible sample floor for the trade discard rate. The event cap still applies.",
        "",
        false,
        empty(),
        vec![
            when(trade, &["discard"]),
            when("timestamp_regression.trades.max_discarded_rate", &[]),
        ],
    );
    add(
        depth,
        "enum",
        INTEGRITY,
        "Strict rejects regressions, preserve retains verified native sequence, and snapshot_recovery resumes at a verified snapshot.",
        "",
        false,
        choices::<DepthTimestamp>(),
        vec![],
    );
    add(
        "timestamp_regression.depth.max_regression_ms",
        "milliseconds",
        INTEGRITY,
        "High-water regression tolerance under depth preserve or timestamp snapshot recovery.",
        MS,
        false,
        empty(),
        vec![when(depth, &["preserve", "snapshot_recovery"])],
    );
    add(
        "timestamp_regression.depth.require_sequence_continuity",
        "boolean",
        INTEGRITY,
        "Keep applicable depth sequence validation enabled. Unsequenced feeds do not require a synthetic sequence.",
        "Must be true",
        false,
        empty(),
        vec![],
    );
    add(
        "timestamp_regression.depth.require_recovery_snapshot",
        "boolean",
        INTEGRITY,
        "Keep verified snapshot initialization enabled for depth recovery.",
        "Must be true",
        false,
        empty(),
        vec![],
    );
    add(
        "timestamp_regression.depth.max_discarded_events",
        "integer",
        INTEGRITY,
        "Finite allowance for observations excluded during depth timestamp snapshot recovery.",
        "",
        false,
        empty(),
        vec![when(depth, &["snapshot_recovery"])],
    );
    fields
}

pub fn runtime_schema() -> Value {
    json!({
        "schema_version": 1, "kind": "normalization_runtime_schema",
        "runtime_schema_version": RUNTIME_SCHEMA_VERSION, "word_bits": usize::BITS,
        "fields": fields(),
    })
}

#[cfg(test)]
mod tests;
