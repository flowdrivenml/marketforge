use super::*;
#[derive(Clone)]
pub struct Principal {
    pub(crate) name: String,
    pub(crate) role: Role,
}
impl Principal {
    /// Trusted in-process embedding boundary; network adapters must authenticate.
    pub fn trusted(name: &str, role: Role) -> Result<Self> {
        if name.is_empty() || name.len() > 128 || name.chars().any(char::is_control) {
            return Err(error(
                "invalid_identity",
                "principal must be bounded printable text",
            ));
        }
        Ok(Self {
            name: name.into(),
            role,
        })
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub(crate) fn require(&self, role: Role) -> Result<()> {
        if self.role < role {
            Err(error("forbidden", "insufficient administrative capability"))
        } else {
            Ok(())
        }
    }
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Credential {
    identity: String,
    role: Role,
    token: String,
}
pub struct Credentials(Vec<Credential>);
impl Credentials {
    pub fn from_json(text: &str) -> Result<Self> {
        if text.len() > 65536 {
            return Err(error(
                "credentials_invalid",
                "credential file exceeds limit",
            ));
        }
        let entries: Vec<Credential> = serde_json::from_str(text)
            .map_err(|_| error("credentials_invalid", "invalid credential file"))?;
        if entries.is_empty()
            || entries.len() > 128
            || entries.iter().any(|e| {
                e.token.len() < 24
                    || e.token.len() > 256
                    || Principal::trusted(&e.identity, e.role).is_err()
            })
        {
            return Err(error("credentials_invalid", "invalid credential bounds"));
        }
        let mut tokens = std::collections::HashSet::new();
        let mut names = std::collections::HashSet::new();
        if entries
            .iter()
            .any(|e| !tokens.insert(e.token.clone()) || !names.insert(e.identity.clone()))
        {
            return Err(error("credentials_invalid", "duplicate credential"));
        }
        Ok(Self(entries))
    }
    pub fn load(config: &crate::live::config::general::ManagementConfig) -> Result<Self> {
        let text = if let Some(file) = &config.credentials_file {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let m = std::fs::symlink_metadata(file)
                    .map_err(|_| error("credentials_unavailable", "credential file unavailable"))?;
                if !m.is_file() || m.len() > 65536 || m.permissions().mode() & 0o077 != 0 {
                    return Err(error(
                        "credentials_invalid",
                        "credential file must be a private regular file",
                    ));
                }
            }
            std::fs::read_to_string(file)
                .map_err(|_| error("credentials_unavailable", "credential file unavailable"))?
        } else {
            std::env::var(&config.credentials_env).map_err(|_| {
                error(
                    "credentials_unavailable",
                    "configured credential environment variable missing",
                )
            })?
        };
        Self::from_json(&text)
    }
    pub fn authenticate(&self, token: &str) -> Result<Principal> {
        for e in &self.0 {
            let mut difference = e.token.len() ^ token.len();
            for i in 0..256 {
                difference |= (e.token.as_bytes().get(i).copied().unwrap_or(0)
                    ^ token.as_bytes().get(i).copied().unwrap_or(0))
                    as usize;
            }
            if difference == 0 {
                return Principal::trusted(&e.identity, e.role);
            }
        }
        Err(error(
            "unauthorized",
            "valid management credentials required",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const TOKEN: &str = "private-test-credential-with-adequate-length";
    fn json() -> String {
        serde_json::json!([{"identity":"viewer","role":"viewer","token":TOKEN}]).to_string()
    }
    #[test]
    fn credentials_authenticate_and_errors_do_not_disclose_tokens() {
        let credentials = Credentials::from_json(&json()).unwrap();
        let viewer = credentials.authenticate(TOKEN).unwrap();
        assert_eq!(viewer.role(), Role::Viewer);
        assert!(viewer.require(Role::Operator).is_err());
        let error = credentials.authenticate("incorrect-token").err().unwrap();
        assert!(!error.to_string().contains(TOKEN));
        let duplicate = format!(
            "[{},{}]",
            &json()[1..json().len() - 1],
            &json()[1..json().len() - 1]
        );
        assert!(Credentials::from_json(&duplicate).is_err());
        assert!(Credentials::from_json(&"x".repeat(65537)).is_err());
    }
    #[test]
    fn credential_files_must_be_private_regular_files() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("credentials.json");
        std::fs::write(&file, json()).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        let config = ManagementConfig {
            credentials_file: Some(file.to_string_lossy().into()),
            ..Default::default()
        };
        assert!(Credentials::load(&config).is_err());
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(Credentials::load(&config).is_ok());
        let link = directory.path().join("link");
        symlink(&file, &link).unwrap();
        let linked = ManagementConfig {
            credentials_file: Some(link.to_string_lossy().into()),
            ..config
        };
        assert!(Credentials::load(&linked).is_err());
    }
}
