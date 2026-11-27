use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
    path::PathBuf,
};

pub(crate) struct Capture {
    stage: PathBuf,
    paths: BTreeSet<String>,
    pub(crate) weights: BTreeMap<String, (u64, u64)>,
}

impl Capture {
    pub(crate) fn from_env() -> Self {
        let stage = PathBuf::from(
            std::env::var_os("MF_LIVE_FIXTURE_STAGE").expect("run fixtures.py collect"),
        );
        let paths = std::env::var("MF_LIVE_FIXTURE_PATHS").expect("run fixtures.py collect");
        let weights = std::env::var("MF_LIVE_FIXTURE_WEIGHTS").unwrap_or_default();
        Self {
            stage,
            paths: paths.lines().map(str::to_owned).collect(),
            weights: weights
                .lines()
                .map(|entry| {
                    let (profile, quota) = entry.split_once('=').expect("expected profile=quota");
                    let (capacity, seconds) =
                        quota.split_once('/').expect("expected capacity/window");
                    (
                        profile.into(),
                        (capacity.parse().unwrap(), seconds.parse().unwrap()),
                    )
                })
                .collect(),
        }
    }

    pub(crate) fn wants(&self, path: &str) -> bool {
        self.paths.contains(path)
    }

    pub(crate) fn wants_rest_profile(&self, profile: &str) -> bool {
        self.paths.iter().any(|path| {
            path.split_once('/').is_some_and(|(_, file)| {
                file.split_once("__")
                    .is_some_and(|(name, _)| name == profile)
            })
        })
    }

    /// Publish a finished capture within the staging tree. The orchestration
    /// script validates and promotes each staged file independently.
    pub(crate) fn save(&self, relative: &str, bytes: &[u8]) -> io::Result<()> {
        assert!(self.wants(relative), "capture was not requested");
        let path = self.stage.join(relative);
        let parent = path.parent().unwrap();
        std::fs::create_dir_all(parent)?;
        let mut file = tempfile::NamedTempFile::new_in(parent)?;
        file.write_all(bytes)?;
        file.as_file().sync_all()?;
        file.persist(path).map_err(|error| error.error)?;
        Ok(())
    }
}
