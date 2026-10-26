use crate::live::config::exchange::RestApplicationResponseConfig;
#[test]
fn validates_raw_response() {
    let config: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "raw"
"#,
    )
    .unwrap();

    assert!(config.validate().is_ok());
}

#[test]
fn rejects_envelope_without_code_field() {
    let config: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "envelope"
success_codes = ["0"]
"#,
    )
    .unwrap();

    assert!(config.validate().is_err());
}

#[test]
fn rejects_envelope_without_success_codes() {
    let config: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "envelope"
code_field = "code"
"#,
    )
    .unwrap();

    assert!(config.validate().is_err());
}

#[test]
fn rejects_duplicate_success_codes() {
    let config: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "envelope"
code_field = "code"
success_codes = ["0", "0"]
"#,
    )
    .unwrap();

    assert!(config.validate().is_err());
}

#[test]
fn rejects_envelope_fields_for_raw_response() {
    let config: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "raw"
code_field = "code"
success_codes = ["0"]
"#,
    )
    .unwrap();

    assert!(config.validate().is_err());
}

#[test]
fn rejects_empty_optional_field_names() {
    let config: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "envelope"
code_field = "code"
success_codes = ["0"]
message_field = ""
"#,
    )
    .unwrap();

    assert!(config.validate().is_err());
}

#[test]
fn rejects_duplicate_envelope_field_names() {
    let config: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "envelope"
code_field = "code"
success_codes = ["0"]
message_field = "code"
"#,
    )
    .unwrap();

    assert!(config.validate().is_err());
}

#[test]
fn validates_complete_envelope() {
    let config: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "envelope"
code_field = "retCode"
success_codes = ["0"]
message_field = "retMsg"
data_field = "result"
"#,
    )
    .unwrap();

    assert!(config.validate().is_ok());
}

mod behavior;
