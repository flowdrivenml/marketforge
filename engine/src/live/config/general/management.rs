use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ManagementConfig {
    pub command_capacity: usize,
    pub active_operations: usize,
    pub operation_history: usize,
    pub plans: usize,
    pub leases: usize,
    pub event_history: usize,
    pub max_clients: usize,
    pub max_requests: usize,
    pub sse_clients: usize,
    pub max_body_bytes: usize,
    pub max_response_bytes: usize,
    pub max_workers: usize,
    pub plan_seconds: u64,
    pub lease_max_seconds: u64,
    pub request_timeout_ms: u64,
    pub overview_interval_ms: u64,
    pub credentials_file: Option<String>,
    pub credentials_env: String,
}
impl Default for ManagementConfig {
    fn default() -> Self {
        Self {
            command_capacity: 32,
            active_operations: 16,
            operation_history: 256,
            plans: 128,
            leases: 128,
            event_history: 256,
            max_clients: 128,
            max_requests: 64,
            sse_clients: 16,
            max_body_bytes: 65536,
            max_response_bytes: 4 * 1024 * 1024,
            max_workers: 64,
            plan_seconds: 60,
            lease_max_seconds: 300,
            request_timeout_ms: 10000,
            overview_interval_ms: 1000,
            credentials_file: None,
            credentials_env: "MARKETFORGE_MANAGEMENT_CREDENTIALS".into(),
        }
    }
}
impl ManagementConfig {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
    pub fn validate(&self) -> Result<(), String> {
        if [
            self.command_capacity,
            self.active_operations,
            self.operation_history,
            self.plans,
            self.leases,
            self.event_history,
            self.max_clients,
            self.max_requests,
            self.sse_clients,
            self.max_workers,
        ]
        .iter()
        .any(|v| *v == 0 || *v > 4096)
            || self.active_operations > self.operation_history
            || self.max_workers > 1024
            || !(4096..=1024 * 1024).contains(&self.max_body_bytes)
            || !(4096..=16 * 1024 * 1024).contains(&self.max_response_bytes)
            || !(1..=3600).contains(&self.plan_seconds)
            || !(1..=86400).contains(&self.lease_max_seconds)
            || !(100..=60000).contains(&self.request_timeout_ms)
            || !(100..=60000).contains(&self.overview_interval_ms)
            || self.credentials_env.len() > 128
            || self.credentials_env.is_empty()
            || self
                .credentials_file
                .as_ref()
                .is_some_and(|p| p.len() > 4096)
        {
            return Err("invalid management resource limits".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_limits_are_valid_and_invalid_budgets_are_rejected() {
        let defaults = ManagementConfig::default();
        defaults.validate().unwrap();
        for invalid in [
            ManagementConfig {
                command_capacity: 0,
                ..defaults.clone()
            },
            ManagementConfig {
                active_operations: 257,
                ..defaults.clone()
            },
            ManagementConfig {
                max_workers: 1025,
                ..defaults.clone()
            },
            ManagementConfig {
                max_body_bytes: usize::MAX,
                ..defaults.clone()
            },
            ManagementConfig {
                overview_interval_ms: 0,
                ..defaults.clone()
            },
            ManagementConfig {
                credentials_env: String::new(),
                ..defaults.clone()
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }
}
