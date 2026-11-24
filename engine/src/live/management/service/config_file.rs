use super::*;
use std::{io::Write, path::PathBuf};
pub(super) struct ConfigFile {
    pub initial_workers: usize,
    pub initial_observation_interval: u64,
    path: PathBuf,
    contents: Arc<tokio::sync::Mutex<String>>,
}
impl ConfigFile {
    pub fn open(path: PathBuf) -> Result<Self> {
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|_| error("config_unavailable", "configuration metadata unavailable"))?;
        if !metadata.is_file() || metadata.len() > 1024 * 1024 {
            return Err(error(
                "config_unavailable",
                "configuration must be a bounded regular file",
            ));
        }
        let text = std::fs::read_to_string(&path)
            .map_err(|_| error("config_unavailable", "configuration file unavailable"))?;
        if text.len() > 1024 * 1024 {
            return Err(error("too_large", "configuration file exceeds 1 MiB"));
        }
        let config = crate::live::config::general::GeneralConfig::load(&path)
            .map_err(|_| error("invalid_config", "configuration file invalid"))?;
        Ok(Self {
            initial_workers: config.runtime.processing.unwrap_or_default().workers,
            initial_observation_interval: config.control.management.overview_interval_ms,
            path,
            contents: Arc::new(tokio::sync::Mutex::new(text)),
        })
    }
    pub async fn update(&self, key: &str, value: u64) -> Result<()> {
        let mut contents = self.contents.lock().await;
        let path = self.path.clone();
        let expected = contents.clone();
        let key = key.to_owned();
        let next = tokio::task::spawn_blocking(move || -> Result<String> {
            if std::fs::read_to_string(&path)
                .map_err(|_| error("config_unavailable", "cannot read configuration"))?
                != expected
            {
                return Err(error(
                    "config_conflict",
                    "external configuration edits detected",
                ));
            }
            let mut document = expected
                .parse::<toml_edit::DocumentMut>()
                .map_err(|_| error("invalid_config", "cannot edit configuration"))?;
            match key.as_str() {
                "workers" => {
                    if !document["runtime"]
                        .as_table()
                        .is_some_and(|t| t.contains_key("processing"))
                    {
                        let defaults = crate::live::config::general::ProcessingConfig::default();
                        document["runtime"]["processing"] = toml_edit::table();
                        document["runtime"]["processing"]["ready_queue_capacity"] =
                            toml_edit::value(defaults.ready_queue_capacity as i64);
                        document["runtime"]["processing"]["max_events_per_turn"] =
                            toml_edit::value(defaults.max_events_per_turn as i64);
                    }
                    document["runtime"]["processing"]["workers"] = toml_edit::value(value as i64);
                }
                "overview_interval_ms" => {
                    document["control"]["management"]["overview_interval_ms"] =
                        toml_edit::value(value as i64)
                }
                _ => return Err(error("unsupported", "unsupported configuration field")),
            }
            let next = document.to_string();
            let config: crate::live::config::general::GeneralConfig = toml::from_str(&next)
                .map_err(|_| error("invalid_config", "complete proposed configuration invalid"))?;
            config.validate().map_err(|e| error("invalid_config", &e))?;
            let parent = path
                .parent()
                .ok_or_else(|| error("invalid_config", "configuration parent missing"))?;
            let temporary = parent.join(format!(
                ".marketforge-config-{:032x}.tmp",
                rand::random::<u128>()
            ));
            let result = (|| -> std::io::Result<()> {
                let mut options = std::fs::OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let mut file = options.open(&temporary)?;
                file.write_all(next.as_bytes())?;
                file.sync_all()?;
                if std::fs::read_to_string(&path)? != expected {
                    return Err(std::io::Error::other("external edit"));
                }
                std::fs::rename(&temporary, &path)?;
                std::fs::File::open(parent)?.sync_all()?;
                Ok(())
            })();
            if result.is_err() {
                let _ = std::fs::remove_file(&temporary);
                return Err(error(
                    "config_save_failed",
                    "atomic configuration save failed or conflicted",
                ));
            }
            Ok(next)
        })
        .await
        .map_err(|_| error("config_save_failed", "configuration writer failed"))??;
        *contents = next;
        Ok(())
    }
}
