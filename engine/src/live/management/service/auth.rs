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
        Self::resolve(
            config,
            |key| std::env::var(key),
            || crate::shared::workspace::resolve(None, &std::env::current_dir()?),
        )
    }

    pub fn load_workspace(
        config: &crate::live::config::general::ManagementConfig,
        root: &std::path::Path,
    ) -> Result<Self> {
        Self::resolve(config, |key| std::env::var(key), || Ok(root.to_owned()))
    }

    fn resolve(
        config: &crate::live::config::general::ManagementConfig,
        environment: impl FnOnce(&str) -> std::result::Result<String, std::env::VarError>,
        workspace: impl FnOnce() -> std::io::Result<std::path::PathBuf>,
    ) -> Result<Self> {
        if let Some(file) = &config.credentials_file {
            return Self::from_file(std::path::Path::new(file));
        }
        match environment(&config.credentials_env) {
            Ok(text) => Self::from_json(&text),
            Err(std::env::VarError::NotPresent)
                if config.credentials_env == "MARKETFORGE_MANAGEMENT_CREDENTIALS" =>
            {
                let root = workspace().map_err(|_| {
                    error(
                        "credentials_unavailable",
                        "cannot resolve management credential workspace",
                    )
                })?;
                Self::from_file(&root.join(".secrets/live-management.json")).map_err(|cause| {
                    error(
                        &cause.code,
                        &format!("workspace .secrets/live-management.json ({})", cause.detail),
                    )
                })
            }
            Err(_) => Err(error(
                "credentials_unavailable",
                "configured management credential environment variable missing or invalid",
            )),
        }
    }

    fn from_file(file: &std::path::Path) -> Result<Self> {
        let text = crate::shared::secret_file::read(file, 65536).map_err(|cause| {
            if cause.kind() == std::io::ErrorKind::InvalidData {
                error("credentials_invalid", "management credentials require a bounded private regular UTF-8 JSON file owned by the running user")
            } else {
                error("credentials_unavailable", &format!("cannot read the selected management credential file ({})", cause.kind()))
            }
        })?;
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
    #[cfg(unix)]
    #[test]
    fn workspace_credentials_preserve_selected_source_precedence() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let config = ManagementConfig::default();
        let missing = |_: &str| Err(std::env::VarError::NotPresent);
        assert!(
            Credentials::resolve(&config, missing, || Ok(root.to_owned()))
                .err()
                .unwrap()
                .detail
                .contains(".secrets/live-management.json")
        );
        std::fs::create_dir(root.join(".secrets")).unwrap();
        let file = root.join(".secrets/live-management.json");
        std::fs::write(&file, json()).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let credentials = Credentials::resolve(&config, missing, || Ok(root.to_owned())).unwrap();
        assert_eq!(
            credentials.authenticate(TOKEN).unwrap().role(),
            Role::Viewer
        );

        let environment = |_: &str| Ok(json().replace("viewer", "operator"));
        let credentials =
            Credentials::resolve(&config, environment, || Ok(root.to_owned())).unwrap();
        assert_eq!(
            credentials.authenticate(TOKEN).unwrap().role(),
            Role::Operator
        );
        assert!(
            Credentials::resolve(&config, |_| Ok(TOKEN.to_owned()), || Ok(root.to_owned()))
                .is_err()
        );

        let explicit = ManagementConfig {
            credentials_file: Some(file.display().to_string()),
            ..config
        };
        let credentials =
            Credentials::resolve(&explicit, environment, || Ok(root.to_owned())).unwrap();
        assert_eq!(
            credentials.authenticate(TOKEN).unwrap().role(),
            Role::Viewer
        );
        std::fs::remove_file(file).unwrap();
        assert!(Credentials::resolve(&explicit, environment, || Ok(root.to_owned())).is_err());
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
