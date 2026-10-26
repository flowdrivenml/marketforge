use std::{net::SocketAddr, path::Path};

use super::model::GeneralConfig;

// ============================================================
// PUBLIC VALIDATION ENTRY POINT
// ============================================================

pub fn validate(config: &GeneralConfig) -> Result<(), String> {
    validate_schema_version(config)?;

    validate_runtime(config)?;
    validate_publication(config)?;
    validate_control(config)?;
    validate_recording(config)?;
    validate_metrics(config)?;
    validate_logging(config)?;
    validate_recovery(config)?;

    validate_cross_section_constraints(config)?;

    Ok(())
}

// ============================================================
// SCHEMA VERSION
// ============================================================

fn validate_schema_version(config: &GeneralConfig) -> Result<(), String> {
    if config.schema_version != 1 {
        return Err(format!(
            "unsupported general configuration schema_version: {} (expected 1)",
            config.schema_version
        ));
    }

    Ok(())
}

// ============================================================
// RUNTIME
// ============================================================

fn validate_runtime(config: &GeneralConfig) -> Result<(), String> {
    if config.runtime.shutdown_timeout_seconds == 0 {
        return Err("runtime.shutdown_timeout_seconds must be greater than zero".into());
    }

    Ok(())
}

// ============================================================
// PUBLICATION
// ============================================================

fn validate_publication(config: &GeneralConfig) -> Result<(), String> {
    validate_unix_socket_path(&config.publication.socket_path, "publication.socket_path")?;

    if config.publication.max_connections == 0 {
        return Err("publication.max_connections must be greater than zero".into());
    }

    if config.publication.subscriber_queue_bytes == 0 {
        return Err("publication.subscriber_queue_bytes must be greater than zero".into());
    }

    Ok(())
}

// ============================================================
// CONTROL REST API
// ============================================================

fn validate_control(config: &GeneralConfig) -> Result<(), String> {
    let address = parse_listen_address(&config.control.listen_address, "control.listen_address")?;

    // The management API has no built-in authentication or TLS
    // configuration in v1.
    //
    // Bind it to loopback and expose it remotely through a
    // separately secured reverse proxy or private-network tunnel.
    if !address.ip().is_loopback() {
        return Err("control.listen_address must use a loopback IP address \
             (127.0.0.1 or ::1)"
            .into());
    }

    Ok(())
}

// ============================================================
// RECORDING
// ============================================================

fn validate_recording(config: &GeneralConfig) -> Result<(), String> {
    // These settings must be valid even when recording is
    // currently disabled, since recording can be enabled later.

    if config.recording.batch_max_records == 0 {
        return Err("recording.batch_max_records must be greater than zero".into());
    }

    if config.recording.batch_max_delay_ms == 0 {
        return Err("recording.batch_max_delay_ms must be greater than zero".into());
    }

    Ok(())
}

// ============================================================
// METRICS
// ============================================================

fn validate_metrics(config: &GeneralConfig) -> Result<(), String> {
    if config.metrics.snapshot_interval_ms == 0 {
        return Err("metrics.snapshot_interval_ms must be greater than zero".into());
    }

    validate_prometheus(config)?;

    Ok(())
}

// ============================================================
// PROMETHEUS
// ============================================================

fn validate_prometheus(config: &GeneralConfig) -> Result<(), String> {
    let prometheus = &config.metrics.prometheus;

    // Validate the configuration even when Prometheus is disabled.
    // This prevents invalid dormant settings from being accepted.

    parse_listen_address(
        &prometheus.listen_address,
        "metrics.prometheus.listen_address",
    )?;

    if prometheus.path.is_empty() {
        return Err("metrics.prometheus.path must not be empty".into());
    }

    if !prometheus.path.starts_with('/') {
        return Err("metrics.prometheus.path must start with '/'".into());
    }

    if prometheus.path.contains('?') || prometheus.path.contains('#') {
        return Err("metrics.prometheus.path must not contain a query or fragment".into());
    }

    if prometheus.path.chars().any(char::is_whitespace) {
        return Err("metrics.prometheus.path must not contain whitespace".into());
    }

    if prometheus.max_concurrent_requests == 0 {
        return Err("metrics.prometheus.max_concurrent_requests must be greater than zero".into());
    }

    if prometheus.max_response_bytes == 0 {
        return Err("metrics.prometheus.max_response_bytes must be greater than zero".into());
    }

    if prometheus.cache_interval_ms == 0 {
        return Err("metrics.prometheus.cache_interval_ms must be greater than zero".into());
    }

    Ok(())
}

// ============================================================
// LOGGING
// ============================================================

fn validate_logging(_config: &GeneralConfig) -> Result<(), String> {
    // LogLevel is a strongly typed enum.
    //
    // Serde rejects unsupported values during deserialization,
    // so no additional semantic checks are needed.

    Ok(())
}

// ============================================================
// RECOVERY DEFAULTS
// ============================================================

fn validate_recovery(config: &GeneralConfig) -> Result<(), String> {
    let recovery = &config.defaults.recovery;

    if recovery.initial_backoff_ms == 0 {
        return Err("defaults.recovery.initial_backoff_ms must be greater than zero".into());
    }

    if recovery.max_backoff_seconds == 0 {
        return Err("defaults.recovery.max_backoff_seconds must be greater than zero".into());
    }

    let max_backoff_ms = recovery
        .max_backoff_seconds
        .checked_mul(1_000)
        .ok_or_else(|| "defaults.recovery.max_backoff_seconds is too large".to_string())?;

    if recovery.initial_backoff_ms > max_backoff_ms {
        return Err("defaults.recovery.initial_backoff_ms must not exceed \
             defaults.recovery.max_backoff_seconds"
            .into());
    }

    Ok(())
}

// ============================================================
// CROSS-SECTION CONSTRAINTS
// ============================================================

fn validate_cross_section_constraints(config: &GeneralConfig) -> Result<(), String> {
    // Prometheus cannot bind to the exact same address as the
    // mandatory control REST API when both are active.

    if config.metrics.prometheus.enabled {
        let control =
            parse_listen_address(&config.control.listen_address, "control.listen_address")?;

        let prometheus = parse_listen_address(
            &config.metrics.prometheus.listen_address,
            "metrics.prometheus.listen_address",
        )?;

        if control == prometheus {
            return Err(
                "control.listen_address and metrics.prometheus.listen_address \
                 must differ when Prometheus is enabled"
                    .into(),
            );
        }
    }

    Ok(())
}

// ============================================================
// ADDRESS VALIDATION
// ============================================================

fn parse_listen_address(value: &str, field: &str) -> Result<SocketAddr, String> {
    let address = value.parse::<SocketAddr>().map_err(|_| {
        format!(
            "{field} must be a valid IP socket address \
             (for example, 127.0.0.1:8080)"
        )
    })?;

    if address.port() == 0 {
        return Err(format!("{field} must specify a nonzero TCP port"));
    }

    Ok(address)
}

// ============================================================
// UNIX SOCKET PATH VALIDATION
// ============================================================

fn validate_unix_socket_path(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{field} must not be empty"));
    }

    if value.trim() != value {
        return Err(format!(
            "{field} must not contain leading or trailing whitespace"
        ));
    }

    if value.as_bytes().contains(&0) {
        return Err(format!("{field} must not contain NUL bytes"));
    }

    let path = Path::new(value);

    if path.file_name().is_none() {
        return Err(format!("{field} must identify a socket filename"));
    }

    if !value.ends_with(".sock") {
        return Err(format!("{field} must end with '.sock'"));
    }

    Ok(())
}

#[cfg(test)]
mod tests;
