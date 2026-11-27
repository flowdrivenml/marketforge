use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeploymentConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub egress_id: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub rest_weight: BTreeMap<String, RestWeightQuota>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestWeightQuota {
    pub capacity: u64,
    pub window_seconds: u64,
}

impl DeploymentConfig {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        if let Some(egress) = &self.egress_id {
            validate_egress(egress)?;
        }
        for (profile, quota) in &self.rest_weight {
            validate_quota(profile, quota.capacity, quota.window_seconds)?;
        }
        Ok(())
    }

    /// Resolve operator overrides while retaining the existing compiler input.
    pub fn resolve(
        &self,
        egress_override: Option<&str>,
        weight_overrides: &[String],
    ) -> Result<ResolvedDeployment, String> {
        self.validate()?;
        let egress = egress_override
            .or(self.egress_id.as_deref())
            .unwrap_or("local-egress");
        validate_egress(egress)?;
        let mut overrides = BTreeMap::new();
        for arg in weight_overrides {
            let (profile, value) = arg
                .split_once('=')
                .ok_or("expected PROFILE=CAPACITY/WINDOW_SECONDS")?;
            let (capacity, window) = value.split_once('/').ok_or("expected capacity/window")?;
            let capacity = capacity.parse().map_err(|_| "invalid quota capacity")?;
            let window = window.parse().map_err(|_| "invalid quota window")?;
            validate_quota(profile, capacity, window)?;
            if overrides
                .insert(profile.to_owned(), (capacity, window))
                .is_some()
            {
                return Err(format!("duplicate --rest-weight declaration for {profile}"));
            }
        }
        let mut rest_weight: BTreeMap<_, _> = self
            .rest_weight
            .iter()
            .map(|(profile, quota)| (profile.clone(), (quota.capacity, quota.window_seconds)))
            .collect();
        rest_weight.extend(overrides);
        Ok(ResolvedDeployment {
            egress_id: egress.into(),
            rest_weight,
        })
    }
}

pub struct ResolvedDeployment {
    pub egress_id: String,
    pub rest_weight: BTreeMap<String, (u64, u64)>,
}

fn validate_egress(egress: &str) -> Result<(), String> {
    if egress.is_empty()
        || egress.len() > 256
        || egress.trim() != egress
        || egress.chars().any(char::is_control)
    {
        return Err("deployment.egress_id must contain 1..256 bytes of printable text without surrounding whitespace".into());
    }
    Ok(())
}

fn validate_quota(profile: &str, capacity: u64, window: u64) -> Result<(), String> {
    if profile.is_empty()
        || !profile
            .bytes()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == b'_')
    {
        return Err("deployment.rest_weight requires a valid profile name".into());
    }
    if capacity == 0 || window == 0 {
        return Err(format!(
            "deployment.rest_weight.{profile} capacity and window_seconds must be positive"
        ));
    }
    if std::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(window))
        .is_none()
    {
        return Err(format!(
            "deployment.rest_weight.{profile} window_seconds exceeds the supported timer range"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_quotas_resolve_cli_overrides_and_keep_unmentioned_profiles() {
        let config: DeploymentConfig = toml::from_str(
            r#"
egress_id = "office"
[rest_weight.binance_spot]
capacity = 1200
window_seconds = 60
[rest_weight.binance_linear]
capacity = 100
window_seconds = 10
"#,
        )
        .unwrap();
        assert_eq!(config.resolve(None, &[]).unwrap().egress_id, "office");
        let resolved = config
            .resolve(Some("temporary"), &["binance_spot=2000/30".into()])
            .unwrap();
        assert_eq!(resolved.egress_id, "temporary");
        assert_eq!(resolved.rest_weight["binance_spot"], (2000, 30));
        assert_eq!(resolved.rest_weight["binance_linear"], (100, 10));
        assert!(config.resolve(Some(" "), &[]).is_err());
        assert!(
            config
                .resolve(
                    None,
                    &["binance_spot=1/1".into(), "binance_spot=2/1".into()]
                )
                .is_err()
        );
        assert!(config.resolve(None, &["binance_spot=0/60".into()]).is_err());
        assert!(config.resolve(None, &["binance_spot=1/0".into()]).is_err());
    }
}
