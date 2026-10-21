//! The local epoch-journal adapter required by WP-06 restart-safe identity.
//! No event storage or runtime supervision is implemented here.
use crate::live::model::{
    acceptance::{AcceptanceError, EpochStore},
    cursor::LogEpoch,
    envelope::LiveEventError,
    identity::StreamId,
};
use serde::{Deserialize, Serialize};

const MAX_JOURNAL_BYTES: u64 = 1024 * 1024;

/// Private deterministic failure boundaries; the production path uses a no-op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistencePoint {
    LockAcquired,
    BeforeTemporaryOpen,
    TemporaryOpened,
    TemporaryWritten,
    BeforeTemporarySync,
    TemporarySynced,
    BeforeRename,
    Renamed,
    BeforeDirectorySync,
    DirectorySynced,
}

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EpochJournal {
    version: u16,
    #[serde(deserialize_with = "deserialize_epochs")]
    epochs: BTreeMap<String, u64>,
}

// A duplicate key is ambiguous persisted identity state, not last-writer-wins.
fn deserialize_epochs<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<String, u64>, D::Error> {
    struct Entries;
    impl<'de> serde::de::Visitor<'de> for Entries {
        type Value = BTreeMap<String, u64>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("unique stream epoch entries")
        }
        fn visit_map<M: serde::de::MapAccess<'de>>(
            self,
            mut map: M,
        ) -> Result<Self::Value, M::Error> {
            let mut entries = BTreeMap::new();
            while let Some((stream, epoch)) = map.next_entry::<String, u64>()? {
                if entries.insert(stream, epoch).is_some() {
                    return Err(serde::de::Error::custom("duplicate stream identity"));
                }
            }
            Ok(entries)
        }
    }
    d.deserialize_map(Entries)
}

// Explicit unlock bounds ownership even if a descriptor was duplicated (for
// example during a concurrent process spawn). Closing one descriptor alone
// need not release a lock shared with another descriptor of the same file.
struct EpochLock(File);
impl Drop for EpochLock {
    fn drop(&mut self) {
        // Drop cannot report an error. The File is closed immediately afterward
        // as a fallback; no journal state is changed or reconstructed here.
        let _ = self.0.unlock();
    }
}

/// Local restart-safe namespace journal. A stable sidecar file lock serializes
/// namespace allocation across processes;
/// atomic replacement + file and directory sync precede namespace exposure.
/// Every new allocator starts a fresh epoch. No unverified offset continuation,
/// event durability, or exchange coverage is claimed. Never delete this journal
/// while identities from it can still exist. Backups must not roll it backward.
///
/// Interrupted first initialization can leave only a lock and/or pending file.
/// These are not an authoritative journal; opening that state fails closed.
/// A pending file is never promoted on recovery. With a valid journal it may be
/// overwritten by the next locked persistence attempt.
///
/// The sidecar is a persistent lock *inode*, not a disposable process marker.
/// Deleting/replacing it while an owner exists can defeat mutual exclusion.
/// Valid journal rollback is also undetectable without an external monotonic
/// authority. All writers must use the same stable paths, preserve the sidecar,
/// and never externally replace/delete/roll back journal state. Filesystem sync
/// guarantees and supported local file locking are required; this is not a
/// replicated identity service or proof of actual power-loss durability.
pub struct FileEpochStore {
    path: PathBuf,
    _lock: EpochLock,
    journal: EpochJournal,
    fenced: bool,
}
impl FileEpochStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AcceptanceError> {
        Self::open_with(path.as_ref(), &mut |_| Ok(()))
    }
    fn open_with(
        path: &Path,
        fault: &mut impl FnMut(PersistencePoint) -> std::io::Result<()>,
    ) -> Result<Self, AcceptanceError> {
        let path = path.to_path_buf();
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let lock_path = PathBuf::from(lock_path);
        let (lock, created_lock) = match OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&lock_path)
        {
            Ok(file) => (file, true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (
                OpenOptions::new().read(true).write(true).open(&lock_path)?,
                false,
            ),
            Err(e) => return Err(e.into()),
        };
        lock.try_lock()
            .map_err(|e| AcceptanceError::Io(std::io::Error::other(e.to_string())))?;
        let lock = EpochLock(lock);
        fault(PersistencePoint::LockAcquired)?;
        let mut initialize = false;
        let journal = match File::open(&path) {
            Ok(mut file) => {
                let mut bytes = Vec::new();
                std::io::Read::by_ref(&mut file)
                    .take(MAX_JOURNAL_BYTES + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() as u64 > MAX_JOURNAL_BYTES {
                    return Err(AcceptanceError::InvalidJournal);
                }
                let journal: EpochJournal =
                    serde_json::from_slice(&bytes).map_err(|_| AcceptanceError::InvalidJournal)?;
                if journal.version != 1 || journal.epochs.keys().any(|k| k.trim().is_empty()) {
                    return Err(AcceptanceError::InvalidJournal);
                }
                journal
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && created_lock => {
                // Even without a sidecar, a pending artifact is evidence of an
                // interrupted lifecycle, not permission to reset its identity.
                let mut pending = path.as_os_str().to_os_string();
                pending.push(".pending");
                match std::fs::symlink_metadata(PathBuf::from(pending)) {
                    Ok(_) => return Err(AcceptanceError::InvalidJournal),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
                initialize = true;
                EpochJournal {
                    version: 1,
                    epochs: BTreeMap::new(),
                }
            }
            Err(e) => return Err(e.into()),
        };
        let store = Self {
            path,
            _lock: lock,
            journal,
            fenced: false,
        };
        // Initialize before exposing the owner; a later missing journal is never
        // treated as permission to reuse an already issued namespace.
        if initialize {
            store.persist(fault)?;
        }
        Ok(store)
    }
    fn persist(
        &self,
        fault: &mut impl FnMut(PersistencePoint) -> std::io::Result<()>,
    ) -> Result<(), AcceptanceError> {
        let mut pending = self.path.as_os_str().to_os_string();
        pending.push(".pending");
        let pending = PathBuf::from(pending);
        let bytes =
            serde_json::to_vec(&self.journal).map_err(|_| AcceptanceError::InvalidJournal)?;
        if bytes.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(AcceptanceError::InvalidJournal);
        }
        fault(PersistencePoint::BeforeTemporaryOpen)?;
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&pending)?;
        fault(PersistencePoint::TemporaryOpened)?;
        file.write_all(&bytes)?;
        fault(PersistencePoint::TemporaryWritten)?;
        fault(PersistencePoint::BeforeTemporarySync)?;
        file.sync_all()?;
        fault(PersistencePoint::TemporarySynced)?;
        fault(PersistencePoint::BeforeRename)?;
        std::fs::rename(&pending, &self.path)?;
        fault(PersistencePoint::Renamed)?;
        let parent = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fault(PersistencePoint::BeforeDirectorySync)?;
        File::open(parent)?.sync_all()?;
        fault(PersistencePoint::DirectorySynced)?;
        Ok(())
    }
}
impl EpochStore for FileEpochStore {
    fn fresh_epoch(&mut self, stream: &StreamId) -> Result<LogEpoch, AcceptanceError> {
        self.fresh_epoch_with(stream, &mut |_| Ok(()))
    }
}
impl FileEpochStore {
    fn fresh_epoch_with(
        &mut self,
        stream: &StreamId,
        fault: &mut impl FnMut(PersistencePoint) -> std::io::Result<()>,
    ) -> Result<LogEpoch, AcceptanceError> {
        if self.fenced {
            return Err(AcceptanceError::Fenced);
        }
        if stream.0.trim().is_empty() {
            return Err(AcceptanceError::InvalidEvent(LiveEventError::EmptyIdentity));
        }
        let next = self
            .journal
            .epochs
            .get(&stream.0)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(AcceptanceError::Exhausted)?;
        self.journal.epochs.insert(stream.0.clone(), next);
        if let Err(error) = self.persist(fault) {
            self.fenced = true;
            return Err(error);
        }
        Ok(LogEpoch(next))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};

    const WRITE_POINTS: [PersistencePoint; 9] = [
        PersistencePoint::BeforeTemporaryOpen,
        PersistencePoint::TemporaryOpened,
        PersistencePoint::TemporaryWritten,
        PersistencePoint::BeforeTemporarySync,
        PersistencePoint::TemporarySynced,
        PersistencePoint::BeforeRename,
        PersistencePoint::Renamed,
        PersistencePoint::BeforeDirectorySync,
        PersistencePoint::DirectorySynced,
    ];
    fn stream(name: &str) -> StreamId {
        StreamId(name.into())
    }
    fn sidecar(path: &Path, suffix: &str) -> PathBuf {
        let mut value = path.as_os_str().to_os_string();
        value.push(suffix);
        PathBuf::from(value)
    }
    fn read(path: &Path) -> EpochJournal {
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }
    fn fail_at(
        target: PersistencePoint,
        hit: &mut bool,
    ) -> impl FnMut(PersistencePoint) -> std::io::Result<()> + '_ {
        move |point| {
            if point == target {
                *hit = true;
                Err(std::io::Error::other(format!("injected {point:?}")))
            } else {
                Ok(())
            }
        }
    }
    fn after_rename(point: PersistencePoint) -> bool {
        matches!(
            point,
            PersistencePoint::Renamed
                | PersistencePoint::BeforeDirectorySync
                | PersistencePoint::DirectorySynced
        )
    }

    #[test]
    fn interrupted_lock_only_initialization_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        File::create(sidecar(&path, ".lock")).unwrap();
        assert!(FileEpochStore::open(&path).is_err());
        assert!(!path.exists());
        assert!(!sidecar(&path, ".pending").exists());
    }

    #[test]
    fn every_interrupted_initialization_boundary_has_an_explicit_reopen_outcome() {
        for point in std::iter::once(PersistencePoint::LockAcquired).chain(WRITE_POINTS) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("epochs");
            let mut hit = false;
            assert!(
                FileEpochStore::open_with(&path, &mut fail_at(point, &mut hit)).is_err(),
                "{point:?}"
            );
            assert!(hit, "injection not reached: {point:?}");
            assert!(sidecar(&path, ".lock").exists());
            if after_rename(point) {
                // This observes the current running filesystem, NOT a power cut.
                assert!(read(&path).epochs.is_empty());
                let mut reopened = FileEpochStore::open(&path).unwrap();
                assert_eq!(reopened.fresh_epoch(&stream("s")).unwrap(), LogEpoch(1));
            } else {
                assert!(!path.exists(), "{point:?}");
                assert!(FileEpochStore::open(&path).is_err(), "{point:?}");
                assert!(!path.exists());
            }
        }
    }

    #[test]
    fn persistence_failure_at_every_boundary_fences_and_never_exposes_an_epoch() {
        for point in WRITE_POINTS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("epochs");
            let mut owner = FileEpochStore::open(&path).unwrap();
            assert_eq!(owner.fresh_epoch(&stream("s")).unwrap(), LogEpoch(1));
            assert_eq!(owner.fresh_epoch(&stream("other")).unwrap(), LogEpoch(1));
            let original = std::fs::read(&path).unwrap();
            let mut hit = false;
            let result = owner.fresh_epoch_with(&stream("s"), &mut fail_at(point, &mut hit));
            assert!(hit, "injection not reached: {point:?}");
            assert!(matches!(result, Err(AcceptanceError::Io(_))), "{point:?}");
            assert!(matches!(
                owner.fresh_epoch(&stream("s")),
                Err(AcceptanceError::Fenced)
            ));
            assert!(matches!(
                owner.fresh_epoch(&stream("third")),
                Err(AcceptanceError::Fenced)
            ));
            let disk = std::fs::read(&path).unwrap();
            if after_rename(point) {
                assert_eq!(read(&path).epochs["s"], 2);
            } else {
                assert_eq!(disk, original, "{point:?}");
            }
            assert_eq!(read(&path).epochs["other"], 1);
            drop(owner);
            let mut reopened = FileEpochStore::open(&path).unwrap();
            // An unexposed tentative epoch may be retried before rename; after
            // rename conservatively skip it, regardless of which sync failed.
            let next = if after_rename(point) { 3 } else { 2 };
            assert_eq!(reopened.fresh_epoch(&stream("s")).unwrap(), LogEpoch(next));
            assert_eq!(reopened.fresh_epoch(&stream("other")).unwrap(), LogEpoch(2));
        }
    }

    #[test]
    fn real_rename_error_preserves_authority_and_fences() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        let mut owner = FileEpochStore::open(&path).unwrap();
        owner.fresh_epoch(&stream("s")).unwrap();
        let saved = dir.path().join("saved");
        let error = owner.fresh_epoch_with(&stream("s"), &mut |point| {
            if point == PersistencePoint::BeforeRename {
                std::fs::rename(&path, &saved)?;
                std::fs::create_dir(&path)?; // A directory cannot be replaced by this file rename.
            }
            Ok(())
        });
        assert!(matches!(error, Err(AcceptanceError::Io(_))));
        assert_eq!(read(&saved).epochs["s"], 1);
        assert!(matches!(
            owner.fresh_epoch(&stream("s")),
            Err(AcceptanceError::Fenced)
        ));
        // The malformed authoritative path is not automatically reconstructed.
        drop(owner);
        assert!(FileEpochStore::open(&path).is_err());
    }

    #[test]
    fn partial_temporary_contents_never_replace_authority_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        let mut owner = FileEpochStore::open(&path).unwrap();
        owner.fresh_epoch(&stream("s")).unwrap();
        let original = std::fs::read(&path).unwrap();
        let error = owner.fresh_epoch_with(&stream("s"), &mut |point| {
            if point == PersistencePoint::TemporaryWritten {
                OpenOptions::new()
                    .write(true)
                    .open(sidecar(&path, ".pending"))?
                    .set_len(5)?;
                return Err(std::io::Error::other("simulated partial temporary write"));
            }
            Ok(())
        });
        assert!(error.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
        drop(owner);
        let mut reopened = FileEpochStore::open(&path).unwrap();
        assert_eq!(reopened.fresh_epoch(&stream("s")).unwrap(), LogEpoch(2));
    }

    #[test]
    fn stale_pending_is_ignored_until_a_locked_write_overwrites_it() {
        for contents in [
            b"".as_slice(),
            b"partial",
            br#"{"version":1,"epochs":{"s":999}}"#,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("epochs");
            let mut owner = FileEpochStore::open(&path).unwrap();
            owner.fresh_epoch(&stream("s")).unwrap();
            drop(owner);
            let original = std::fs::read(&path).unwrap();
            std::fs::write(sidecar(&path, ".pending"), contents).unwrap();
            let mut owner = FileEpochStore::open(&path).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), original);
            assert_eq!(std::fs::read(sidecar(&path, ".pending")).unwrap(), contents);
            assert_eq!(owner.fresh_epoch(&stream("s")).unwrap(), LogEpoch(2));
            assert!(!sidecar(&path, ".pending").exists());
        }
    }

    #[test]
    fn pending_without_authority_is_never_promoted_or_used_to_initialize() {
        for lock_exists in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("epochs");
            if lock_exists {
                File::create(sidecar(&path, ".lock")).unwrap();
            }
            let pending = br#"{"version":1,"epochs":{"s":45}}"#;
            std::fs::write(sidecar(&path, ".pending"), pending).unwrap();
            assert!(FileEpochStore::open(&path).is_err());
            assert!(!path.exists());
            assert_eq!(std::fs::read(sidecar(&path, ".pending")).unwrap(), pending);
            assert!(FileEpochStore::open(&path).is_err());
        }
    }

    #[test]
    fn existing_journal_without_sidecar_is_read_only_even_with_unwritable_pending_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        let bytes = br#"{ "version": 1, "epochs": {"s":7} }"#;
        std::fs::write(&path, bytes).unwrap();
        std::fs::create_dir(sidecar(&path, ".pending")).unwrap();
        let mut visited = vec![];
        let mut owner = FileEpochStore::open_with(&path, &mut |p| {
            visited.push(p);
            Ok(())
        })
        .unwrap();
        assert_eq!(visited, [PersistencePoint::LockAcquired]);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert!(owner.fresh_epoch(&stream("s")).is_err());
        assert!(matches!(
            owner.fresh_epoch(&stream("s")),
            Err(AcceptanceError::Fenced)
        ));
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn malformed_unsupported_and_ambiguous_journals_are_preserved_and_rejected() {
        for bytes in [
            "",
            "{",
            r#"{"version":2,"epochs":{}}"#,
            r#"{"version":1,"epochs":{"":4}}"#,
            r#"{"version":1,"epochs":{"s":-1}}"#,
            r#"{"version":1,"epochs":{"s":18446744073709551616}}"#,
            r#"{"version":1,"epochs":{"s":8,"s":2}}"#,
            r#"{"version":1,"epochs":{},"extra":true}"#,
            r#"{"version":1,"version":1,"epochs":{}}"#,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("epochs");
            std::fs::write(&path, bytes).unwrap();
            for _ in 0..2 {
                assert!(
                    matches!(
                        FileEpochStore::open(&path),
                        Err(AcceptanceError::InvalidJournal)
                    ),
                    "{bytes}"
                );
            }
            assert_eq!(std::fs::read(&path).unwrap(), bytes.as_bytes());
            assert!(!sidecar(&path, ".pending").exists());
        }
    }

    #[test]
    fn journal_size_boundary_is_exact_and_read_only() {
        for extra in [0, 1] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("epochs");
            let mut bytes = br#"{"version":1,"epochs":{}}"#.to_vec();
            bytes.resize(MAX_JOURNAL_BYTES as usize + extra, b' ');
            std::fs::write(&path, &bytes).unwrap();
            if extra == 0 {
                assert!(FileEpochStore::open(&path).is_ok());
            } else {
                assert!(matches!(
                    FileEpochStore::open(&path),
                    Err(AcceptanceError::InvalidJournal)
                ));
            }
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
    }

    #[test]
    fn serialization_size_failure_fences_without_touching_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        let mut owner = FileEpochStore::open(&path).unwrap();
        let original = std::fs::read(&path).unwrap();
        assert!(matches!(
            owner.fresh_epoch(&stream(&"s".repeat(MAX_JOURNAL_BYTES as usize))),
            Err(AcceptanceError::InvalidJournal)
        ));
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert!(!sidecar(&path, ".pending").exists());
        assert!(matches!(
            owner.fresh_epoch(&stream("s")),
            Err(AcceptanceError::Fenced)
        ));
    }

    #[test]
    fn exhaustion_and_invalid_input_do_not_fence_other_streams_or_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        std::fs::write(
            &path,
            format!(r#"{{"version":1,"epochs":{{"full":{},"s":2}}}}"#, u64::MAX),
        )
        .unwrap();
        let mut owner = FileEpochStore::open(&path).unwrap();
        let original = std::fs::read(&path).unwrap();
        assert!(matches!(
            owner.fresh_epoch(&stream("full")),
            Err(AcceptanceError::Exhausted)
        ));
        assert!(matches!(
            owner.fresh_epoch(&stream("  ")),
            Err(AcceptanceError::InvalidEvent(LiveEventError::EmptyIdentity))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert_eq!(owner.fresh_epoch(&stream("s")).unwrap(), LogEpoch(3));
        assert_eq!(owner.fresh_epoch(&stream("new")).unwrap(), LogEpoch(1));
        drop(owner);
        let mut reopened = FileEpochStore::open(&path).unwrap();
        assert_eq!(reopened.fresh_epoch(&stream("s")).unwrap(), LogEpoch(4));
        assert_eq!(reopened.fresh_epoch(&stream("new")).unwrap(), LogEpoch(2));
        assert_eq!(read(&path).epochs["full"], u64::MAX);
    }

    #[test]
    fn concurrent_threads_keep_one_owner_across_journal_renames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        drop(FileEpochStore::open(&path).unwrap());
        let start = std::sync::Arc::new(std::sync::Barrier::new(2));
        let finish = start.clone();
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let path = path.clone();
                let start = start.clone();
                let finish = finish.clone();
                std::thread::spawn(move || {
                    start.wait();
                    let mut owner = FileEpochStore::open(&path).ok();
                    let result = owner.as_mut().map(|o| {
                        let a = o.fresh_epoch(&stream("s")).unwrap();
                        let b = o.fresh_epoch(&stream("s")).unwrap();
                        (a, b)
                    });
                    finish.wait();
                    drop(owner);
                    result
                })
            })
            .collect();
        let results: Vec<_> = threads
            .into_iter()
            .filter_map(|t| t.join().unwrap())
            .collect();
        assert_eq!(results, [(LogEpoch(1), LogEpoch(2))]);
        assert_eq!(
            FileEpochStore::open(&path)
                .unwrap()
                .fresh_epoch(&stream("s"))
                .unwrap(),
            LogEpoch(3)
        );
    }

    #[test]
    fn lock_owner_drop_releases_even_with_a_duplicated_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        let owner = FileEpochStore::open(&path).unwrap();
        let duplicate = owner._lock.0.try_clone().unwrap();
        assert!(FileEpochStore::open(&path).is_err());
        drop(owner);
        let replacement = FileEpochStore::open(&path).unwrap();
        // The duplicate still exists; owner Drop, not last descriptor close,
        // released the lock. Dropping it cannot unlock the replacement owner.
        drop(duplicate);
        assert!(FileEpochStore::open(&path).is_err());
        drop(replacement);
        assert!(FileEpochStore::open(&path).is_ok());
    }

    #[test]
    fn lock_guard_releases_on_error_even_with_a_duplicated_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        drop(FileEpochStore::open(&path).unwrap());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(sidecar(&path, ".lock"))
            .unwrap();
        file.try_lock().unwrap();
        let duplicate = file.try_clone().unwrap();
        let error = (|| -> std::io::Result<()> {
            let _guard = EpochLock(file);
            Err(std::io::Error::other("simulated post-lock open error"))
        })();
        assert!(error.is_err());
        assert!(FileEpochStore::open(&path).is_ok());
        drop(duplicate);
    }

    #[test]
    fn lock_excludes_another_process() {
        const ENV: &str = "MARKETFORGE_EPOCH_LOCK_TEST_PATH";
        if let Some(path) = std::env::var_os(ENV) {
            let mut owner = FileEpochStore::open(PathBuf::from(path)).unwrap();
            assert_eq!(owner.fresh_epoch(&stream("s")).unwrap(), LogEpoch(1));
            println!("EPOCH_LOCK_READY");
            std::io::stdout().flush().unwrap();
            let mut byte = [0];
            std::io::stdin().read_exact(&mut byte).unwrap();
            drop(owner);
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        drop(FileEpochStore::open(&path).unwrap());
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "live::foundation::filesystem::tests::lock_excludes_another_process",
                "--nocapture",
            ])
            .env(ENV, &path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut reader = BufReader::new(child.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(
                reader.read_line(&mut line).unwrap(),
                0,
                "child exited before lock acknowledgement"
            );
            if line.contains("EPOCH_LOCK_READY") {
                break;
            }
        }
        assert!(FileEpochStore::open(&path).is_err());
        child.stdin.take().unwrap().write_all(b"x").unwrap();
        assert!(child.wait().unwrap().success());
        assert_eq!(
            FileEpochStore::open(&path)
                .unwrap()
                .fresh_epoch(&stream("s"))
                .unwrap(),
            LogEpoch(2)
        );
    }

    #[cfg(unix)]
    #[test]
    fn unsupported_lock_deletion_can_split_lock_identity() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        let owner = FileEpochStore::open(&path).unwrap();
        let old_inode = owner._lock.0.metadata().unwrap().ino();
        // Deliberate protocol violation: this test documents a limit, not safety.
        std::fs::remove_file(sidecar(&path, ".lock")).unwrap();
        let replacement = FileEpochStore::open(&path).unwrap();
        assert_ne!(old_inode, replacement._lock.0.metadata().unwrap().ino());
        // Both handles exist: an OS inode lock cannot protect a replacement inode.
        drop((owner, replacement));
    }

    #[test]
    fn unsupported_valid_journal_rollback_is_not_detectable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        let mut owner = FileEpochStore::open(&path).unwrap();
        owner.fresh_epoch(&stream("s")).unwrap();
        let stale = std::fs::read(&path).unwrap();
        let issued = owner.fresh_epoch(&stream("s")).unwrap();
        drop(owner);
        // Deliberate unsupported administrative rollback, NOT crash simulation.
        std::fs::write(&path, stale).unwrap();
        let mut reopened = FileEpochStore::open(&path).unwrap();
        assert_eq!(reopened.fresh_epoch(&stream("s")).unwrap(), issued);
        // A local journal cannot distinguish valid stale bytes from current state.
    }
}
