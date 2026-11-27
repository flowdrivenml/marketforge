use super::*;
use std::{
    fs::{File, Metadata},
    io::{Read, Write},
    path::{Path, PathBuf},
};

const MAX_CONFIG_BYTES: u64 = 1024 * 1024;

pub(super) struct ConfigFile {
    pub initial_workers: usize,
    pub initial_observation_interval: u64,
    path: PathBuf,
    contents: Arc<tokio::sync::Mutex<String>>,
}

fn read_config(path: &Path) -> Result<(String, Metadata)> {
    let unavailable = || {
        error(
            "config_unavailable",
            "cannot read bounded regular configuration file",
        )
    };
    let metadata = std::fs::symlink_metadata(path).map_err(|_| unavailable())?;
    if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES {
        return Err(unavailable());
    }
    let mut text = String::new();
    File::open(path)
        .map_err(|_| unavailable())?
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|_| unavailable())?;
    if text.len() as u64 > MAX_CONFIG_BYTES {
        return Err(unavailable());
    }
    Ok((text, metadata))
}

fn parse_config(text: &str) -> Result<crate::live::config::general::GeneralConfig> {
    let config: crate::live::config::general::GeneralConfig =
        toml::from_str(text).map_err(|_| error("invalid_config", "configuration file invalid"))?;
    config.validate().map_err(|e| error("invalid_config", &e))?;
    Ok(config)
}

fn check_contents(path: &Path, expected: &str) -> Result<Metadata> {
    let (current, metadata) = read_config(path)?;
    if current != expected {
        return Err(error(
            "config_conflict",
            "external configuration edits detected",
        ));
    }
    Ok(metadata)
}

impl ConfigFile {
    pub fn open(path: PathBuf) -> Result<Self> {
        let (text, _) = read_config(&path)?;
        let config = parse_config(&text)?;
        Ok(Self {
            initial_workers: config.runtime.processing.unwrap_or_default().workers,
            initial_observation_interval: config.control.management.overview_interval_ms,
            path,
            contents: Arc::new(tokio::sync::Mutex::new(text)),
        })
    }

    /// Returns whether the directory entry was synchronized after replacement.
    pub async fn update(&self, key: &str, value: u64) -> Result<bool> {
        let contents = self.contents.clone().lock_owned().await;
        let path = self.path.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut contents = contents;
            let metadata = check_contents(&path, &contents)?;
            let next = edit_config(&contents, &key, value)?;
            if next.len() as u64 > MAX_CONFIG_BYTES {
                return Err(error("too_large", "updated configuration exceeds 1 MiB"));
            }
            parse_config(&next)?;
            let synced = replace_config(&path, &contents, &next, &metadata)?;
            *contents = next;
            Ok(synced)
        })
        .await
        .map_err(|_| error("config_save_failed", "configuration writer failed"))?
    }
}

fn edit_config(expected: &str, key: &str, value: u64) -> Result<String> {
    let mut document = expected
        .parse::<toml_edit::DocumentMut>()
        .map_err(|_| error("invalid_config", "cannot edit configuration"))?;
    let item = match key {
        "workers" => {
            let processing = &mut document["runtime"]["processing"];
            if processing.is_none() {
                let defaults = crate::live::config::general::ProcessingConfig::default();
                processing["ready_queue_capacity"] =
                    toml_edit::value(defaults.ready_queue_capacity as i64);
                processing["max_events_per_turn"] =
                    toml_edit::value(defaults.max_events_per_turn as i64);
            }
            &mut processing["workers"]
        }
        "overview_interval_ms" => &mut document["control"]["management"]["overview_interval_ms"],
        _ => return Err(error("unsupported", "unsupported configuration field")),
    };
    let mut replacement = toml_edit::Value::from(
        i64::try_from(value)
            .map_err(|_| error("invalid_config", "value exceeds TOML integer range"))?,
    );
    if let Some(previous) = item.as_value() {
        *replacement.decor_mut() = previous.decor().clone();
    }
    *item = toml_edit::Item::Value(replacement);
    Ok(document.to_string())
}

fn replace_config(path: &Path, expected: &str, next: &str, metadata: &Metadata) -> Result<bool> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let failed = || error("config_save_failed", "atomic configuration save failed");
    let directory = File::open(parent).map_err(|_| failed())?;
    let temporary = parent.join(format!(
        ".marketforge-config-{:032x}.tmp",
        rand::random::<u128>()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(|_| failed())?;
    let result = (|| {
        file.write_all(next.as_bytes()).map_err(|_| failed())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, fchown};
            let temporary_metadata = file.metadata().map_err(|_| failed())?;
            let uid = (temporary_metadata.uid() != metadata.uid()).then(|| metadata.uid());
            let gid = (temporary_metadata.gid() != metadata.gid()).then(|| metadata.gid());
            if uid.is_some() || gid.is_some() {
                fchown(&file, uid, gid).map_err(|_| failed())?;
            }
        }
        file.set_permissions(metadata.permissions())
            .map_err(|_| failed())?;
        file.sync_all().map_err(|_| failed())?;
        let current = check_contents(path, expected)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if (
                current.dev(),
                current.ino(),
                current.uid(),
                current.gid(),
                current.mode(),
            ) != (
                metadata.dev(),
                metadata.ino(),
                metadata.uid(),
                metadata.gid(),
                metadata.mode(),
            ) {
                return Err(error(
                    "config_conflict",
                    "configuration file identity or ownership changed",
                ));
            }
        }
        std::fs::rename(&temporary, path).map_err(|_| failed())?;
        Ok(directory.sync_all().is_ok())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}
