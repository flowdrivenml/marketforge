//! Cold-path local configuration journal. The configuration owner calls these
//! blocking methods on its dedicated worker, never a market-data task. This
//! adapter does not claim semantic compilation or runtime application.
use crate::live::{
    foundation::clock::{MonotonicClock, WallClock},
    model::{configuration::*, error::ErrorCode, operation::ResourceVersion},
    ports::{configuration::CompiledConfiguration, context::CallContext},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
#[derive(Debug, Clone, Copy)]
pub struct JournalLimits {
    pub bytes: usize,
    pub history: usize,
    pub items: usize,
    pub edges: usize,
}
impl JournalLimits {
    fn validate(self) -> Result<Self, ErrorCode> {
        if self.bytes == 0 || self.bytes == usize::MAX || self.history == 0 || self.items == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(self)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u16,
    revisions: Vec<ConfigurationRevision>,
    checksum: String,
}
struct State {
    journal: Journal,
    fenced: bool,
}
struct Lock(File);
impl Drop for Lock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}
pub struct FileRevisionJournal {
    path: PathBuf,
    _lock: Lock,
    state: Mutex<State>,
    limits: JournalLimits,
    clock: Arc<dyn MonotonicClock>,
    wall: Arc<dyn WallClock>,
    owner: ResourceVersion,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WritePoint {
    BeforeCreate,
    AfterCreate,
    AfterWrite,
    BeforeFileSync,
    AfterFileSync,
    BeforeRename,
    AfterRename,
    BeforeDirectorySync,
    AfterDirectorySync,
}
struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedBuffer {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(data.len())
            .is_none_or(|n| n > self.limit)
        {
            return Err(std::io::Error::other("configuration journal byte limit"));
        }
        self.bytes.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>, ErrorCode> {
    let mut writer = LimitedBuffer {
        bytes: vec![],
        limit,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| ErrorCode::Limit)?;
    Ok(writer.bytes)
}
fn checksum(journal: &Journal, limit: usize) -> Result<String, ErrorCode> {
    let bytes = encode(&(journal.version, &journal.revisions), limit)?;
    Ok(Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(suffix);
    s.into()
}
impl FileRevisionJournal {
    /// Explicit first initialization. An existing sidecar or snapshot is not
    /// permission to reset accepted revision identities.
    pub fn create(
        path: impl AsRef<Path>,
        initial: &CompiledConfiguration,
        limits: JournalLimits,
        clock: Arc<dyn MonotonicClock>,
        wall: Arc<dyn WallClock>,
        owner: ResourceVersion,
    ) -> Result<Self, ErrorCode> {
        let limits = limits.validate()?;
        let path = path.as_ref().to_path_buf();
        if path.symlink_metadata().is_ok() {
            return Err(ErrorCode::RevisionConflict);
        }
        Self::validate_contents(&initial.desired, limits)?;
        if initial.resolved != initial.desired.resolve(limits.items)? {
            return Err(ErrorCode::InvalidInput);
        }
        let lock = Self::acquire(&path, true)?;
        if path.symlink_metadata().is_ok() {
            return Err(ErrorCode::RevisionConflict);
        }
        let revision = ConfigurationRevision {
            id: ConfigRevisionId(1),
            parent: None,
            created_at_ns: wall.unix_time_ns()?,
            desired: initial.desired.clone(),
        };
        let mut journal = Journal {
            version: 1,
            revisions: vec![revision],
            checksum: String::new(),
        };
        journal.checksum = checksum(&journal, limits.bytes)?;
        let store = Self {
            path,
            _lock: lock,
            state: Mutex::new(State {
                journal: journal.clone(),
                fenced: false,
            }),
            limits,
            clock,
            wall,
            owner,
        };
        store.persist(&journal, &mut |_| Ok(()))?;
        Ok(store)
    }
    pub fn open(
        path: impl AsRef<Path>,
        limits: JournalLimits,
        clock: Arc<dyn MonotonicClock>,
        wall: Arc<dyn WallClock>,
        owner: ResourceVersion,
    ) -> Result<Self, ErrorCode> {
        let limits = limits.validate()?;
        let path = path.as_ref().to_path_buf();
        let lock = Self::acquire(&path, false)?;
        match std::fs::symlink_metadata(sidecar(&path, ".pending")) {
            Ok(_) => return Err(ErrorCode::Persistence),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(ErrorCode::Persistence),
        }
        if !std::fs::symlink_metadata(&path)
            .map_err(|_| ErrorCode::Persistence)?
            .file_type()
            .is_file()
        {
            return Err(ErrorCode::Persistence);
        }

        let mut bytes = vec![];
        File::open(&path)
            .map_err(|_| ErrorCode::Persistence)?
            .take(limits.bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| ErrorCode::Persistence)?;
        if bytes.len() > limits.bytes {
            return Err(ErrorCode::Limit);
        }
        let journal: Journal =
            serde_json::from_slice(&bytes).map_err(|_| ErrorCode::Persistence)?;
        if journal.version != 1
            || journal.revisions.is_empty()
            || journal.revisions.len() > limits.history
            || checksum(&journal, limits.bytes)? != journal.checksum
        {
            return Err(ErrorCode::Persistence);
        }
        for (i, revision) in journal.revisions.iter().enumerate() {
            Self::validate_contents(&revision.desired, limits)?;
            if revision.id.0 == 0
                || (revision.id.0 == 1 && revision.parent.is_some())
                || (revision.id.0 > 1
                    && revision.parent != Some(ConfigRevisionId(revision.id.0 - 1)))
            {
                return Err(ErrorCode::Persistence);
            }
            if i > 0 && journal.revisions[i - 1].id.0.checked_add(1) != Some(revision.id.0) {
                return Err(ErrorCode::Persistence);
            }
        }
        Ok(Self {
            path,
            _lock: lock,
            state: Mutex::new(State {
                journal,
                fenced: false,
            }),
            limits,
            clock,
            wall,
            owner,
        })
    }
    fn acquire(path: &Path, new: bool) -> Result<Lock, ErrorCode> {
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        if new {
            options.create_new(true);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(sidecar(path, ".lock"))
            .map_err(|_| ErrorCode::Persistence)?;
        file.try_lock().map_err(|_| ErrorCode::Capacity)?;
        Ok(Lock(file))
    }
    fn validate_contents(
        desired: &DesiredConfiguration,
        limits: JournalLimits,
    ) -> Result<(), ErrorCode> {
        desired.validate_structure(limits.items)?;
        super::validation::dependency_order(desired, limits.items, limits.edges)
            .map_err(|e| e.code)?;
        Ok(())
    }
    pub fn load(&self, context: &CallContext) -> Result<ConfigurationRevision, ErrorCode> {
        context.check(self.clock.as_ref(), Some(&self.owner))?;
        let state = self.state.try_lock().map_err(|_| ErrorCode::Capacity)?;
        if state.fenced {
            return Err(ErrorCode::CommitUnknown);
        }
        Ok(state.journal.revisions.last().unwrap().clone())
    }
    pub fn revision(
        &self,
        context: &CallContext,
        id: ConfigRevisionId,
    ) -> Result<ConfigurationRevision, ErrorCode> {
        context.check(self.clock.as_ref(), Some(&self.owner))?;
        let state = self.state.try_lock().map_err(|_| ErrorCode::Capacity)?;
        if state.fenced {
            return Err(ErrorCode::CommitUnknown);
        }
        state
            .journal
            .revisions
            .iter()
            .find(|r| r.id == id)
            .cloned()
            .ok_or(ErrorCode::NotFound)
    }
    pub fn history(
        &self,
        context: &CallContext,
        after: Option<ConfigRevisionId>,
        limit: usize,
    ) -> Result<Vec<ConfigurationRevision>, ErrorCode> {
        context.check(self.clock.as_ref(), Some(&self.owner))?;
        if limit == 0 || limit > self.limits.history {
            return Err(ErrorCode::Limit);
        }
        let state = self.state.try_lock().map_err(|_| ErrorCode::Capacity)?;
        if state.fenced {
            return Err(ErrorCode::CommitUnknown);
        }
        Ok(state
            .journal
            .revisions
            .iter()
            .filter(|r| after.is_none_or(|id| r.id.0 > id.0))
            .take(limit)
            .cloned()
            .collect())
    }
    /// Caller supplies the real compiler's validated plan. Structural checks
    /// defend the storage boundary; they do not replace capability validation.
    pub fn commit(
        &self,
        context: &CallContext,
        plan: &CompiledConfiguration,
    ) -> Result<ConfigurationRevision, ErrorCode> {
        self.commit_with(context, plan, &mut |_| Ok(()))
    }
    fn commit_with(
        &self,
        context: &CallContext,
        plan: &CompiledConfiguration,
        fault: &mut impl FnMut(WritePoint) -> Result<(), ErrorCode>,
    ) -> Result<ConfigurationRevision, ErrorCode> {
        context.check(self.clock.as_ref(), Some(&self.owner))?;
        let mut state = self.state.try_lock().map_err(|_| ErrorCode::Capacity)?;
        if state.fenced {
            return Err(ErrorCode::CommitUnknown);
        }
        let current = state.journal.revisions.last().unwrap();
        if plan.base_revision != current.id {
            return Err(ErrorCode::RevisionConflict);
        }
        if plan.desired == current.desired {
            return Ok(current.clone());
        }
        Self::validate_contents(&plan.desired, self.limits)?;
        if plan.resolved != plan.desired.resolve(self.limits.items)? {
            return Err(ErrorCode::InvalidInput);
        }
        let revision = ConfigurationRevision {
            id: ConfigRevisionId(current.id.0.checked_add(1).ok_or(ErrorCode::Limit)?),
            parent: Some(current.id),
            created_at_ns: self.wall.unix_time_ns()?,
            desired: plan.desired.clone(),
        };
        let mut journal = state.journal.clone();
        journal.revisions.push(revision.clone());
        if journal.revisions.len() > self.limits.history {
            journal.revisions.remove(0);
        }
        journal.checksum = checksum(&journal, self.limits.bytes)?;
        // Cancellation/deadline rechecked immediately before storage effects.
        // Once persistence begins, an uncertain outcome fences this owner.
        context.check(self.clock.as_ref(), Some(&self.owner))?;
        if let Err(error) = self.persist(&journal, fault) {
            state.fenced = true;
            return Err(error);
        }
        state.journal = journal;
        Ok(revision)
    }
    fn persist(
        &self,
        journal: &Journal,
        fault: &mut impl FnMut(WritePoint) -> Result<(), ErrorCode>,
    ) -> Result<(), ErrorCode> {
        let bytes = encode(journal, self.limits.bytes)?;
        // At most one pending artifact. It is never truncated, promoted or
        // silently deleted. Interrupted pre-rename state requires explicit
        // operator recovery; accepted snapshot recovery remains fail-closed.
        let pending = sidecar(&self.path, ".pending");
        let mut renamed = false;
        let result = (|| {
            fault(WritePoint::BeforeCreate)?;
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&pending).map_err(|_| ErrorCode::Persistence)?;
            fault(WritePoint::AfterCreate)?;
            file.write_all(&bytes).map_err(|_| ErrorCode::Persistence)?;
            fault(WritePoint::AfterWrite)?;
            fault(WritePoint::BeforeFileSync)?;
            file.sync_all().map_err(|_| ErrorCode::Persistence)?;
            fault(WritePoint::AfterFileSync)?;
            fault(WritePoint::BeforeRename)?;
            std::fs::rename(&pending, &self.path).map_err(|_| ErrorCode::Persistence)?;
            renamed = true;
            fault(WritePoint::AfterRename)?;
            fault(WritePoint::BeforeDirectorySync)?;
            let parent = self
                .path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            File::open(parent)
                .and_then(|f| f.sync_all())
                .map_err(|_| ErrorCode::Persistence)?;
            fault(WritePoint::AfterDirectorySync)?;
            Ok(())
        })();
        result.map_err(|error| {
            if renamed {
                ErrorCode::CommitUnknown
            } else {
                error
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::{
        foundation::{CancellationToken, clock::ManualClock},
        model::operation::ResourceId,
    };
    use std::time::{Duration, Instant};
    fn owner() -> ResourceVersion {
        ResourceVersion {
            resource: ResourceId::Configuration,
            generation: 1,
        }
    }
    fn clock() -> Arc<ManualClock> {
        Arc::new(ManualClock::new(Instant::now(), 100))
    }
    fn context(clock: &ManualClock) -> CallContext {
        CallContext {
            cancellation: CancellationToken::new(),
            deadline: clock.now() + Duration::from_secs(10),
            expected: Some(owner()),
        }
    }
    fn plan(base: u64, level: LogLevel) -> CompiledConfiguration {
        let mut desired: DesiredConfiguration = serde_json::from_str(include_str!(
            "../../../tests/live/fixtures/default-config.json"
        ))
        .unwrap();
        desired.global.logging = level;
        CompiledConfiguration {
            base_revision: ConfigRevisionId(base),
            resolved: desired.resolve(100).unwrap(),
            desired,
            impacts: vec![],
            boundaries: vec![],
        }
    }
    fn limits() -> JournalLimits {
        JournalLimits {
            bytes: 65536,
            history: 2,
            items: 100,
            edges: 100,
        }
    }
    #[test]
    fn durable_noop_cas_history_restart_and_competing_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("revisions.json");
        let clock = clock();
        let context = context(&clock);
        let store = FileRevisionJournal::create(
            &path,
            &plan(0, LogLevel::Info),
            limits(),
            clock.clone(),
            clock.clone(),
            owner(),
        )
        .unwrap();
        assert!(matches!(
            FileRevisionJournal::open(&path, limits(), clock.clone(), clock.clone(), owner()),
            Err(ErrorCode::Capacity)
        ));
        assert_eq!(
            store.commit(&context, &plan(1, LogLevel::Info)).unwrap().id,
            ConfigRevisionId(1)
        );
        assert_eq!(
            store
                .commit(&context, &plan(1, LogLevel::Debug))
                .unwrap()
                .id,
            ConfigRevisionId(2)
        );
        assert_eq!(
            store.commit(&context, &plan(1, LogLevel::Trace)),
            Err(ErrorCode::RevisionConflict)
        );
        assert_eq!(
            store.commit(&context, &plan(2, LogLevel::Warn)).unwrap().id,
            ConfigRevisionId(3)
        );
        assert_eq!(
            store.revision(&context, ConfigRevisionId(1)),
            Err(ErrorCode::NotFound)
        );
        assert_eq!(
            store
                .history(&context, None, 2)
                .unwrap()
                .iter()
                .map(|r| r.id.0)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        drop(store);
        let reopened =
            FileRevisionJournal::open(&path, limits(), clock.clone(), clock.clone(), owner())
                .unwrap();
        assert_eq!(
            reopened.load(&context).unwrap().desired.global.logging,
            LogLevel::Warn
        );
        assert_eq!(
            reopened
                .commit(&context, &plan(3, LogLevel::Info))
                .unwrap()
                .id,
            ConfigRevisionId(4)
        );
    }
    #[test]
    fn every_persistence_boundary_is_fail_closed_and_never_partially_published() {
        for point in [
            WritePoint::BeforeCreate,
            WritePoint::AfterCreate,
            WritePoint::AfterWrite,
            WritePoint::BeforeFileSync,
            WritePoint::AfterFileSync,
            WritePoint::BeforeRename,
            WritePoint::AfterRename,
            WritePoint::BeforeDirectorySync,
            WritePoint::AfterDirectorySync,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("revisions.json");
            let clock = clock();
            let context = context(&clock);
            let store = FileRevisionJournal::create(
                &path,
                &plan(0, LogLevel::Info),
                limits(),
                clock.clone(),
                clock.clone(),
                owner(),
            )
            .unwrap();
            let mut hit = false;
            let result = store.commit_with(&context, &plan(1, LogLevel::Debug), &mut |actual| {
                if actual == point {
                    hit = true;
                    Err(ErrorCode::Persistence)
                } else {
                    Ok(())
                }
            });
            assert!(hit, "{point:?}");
            assert!(result.is_err());
            assert_eq!(store.load(&context), Err(ErrorCode::CommitUnknown));
            let raw: Journal = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            assert_eq!(raw.checksum, checksum(&raw, limits().bytes).unwrap());
            let renamed = matches!(
                point,
                WritePoint::AfterRename
                    | WritePoint::BeforeDirectorySync
                    | WritePoint::AfterDirectorySync
            );
            assert_eq!(
                raw.revisions.last().unwrap().id.0,
                if renamed { 2 } else { 1 }
            );
            assert_eq!(
                result,
                Err(if renamed {
                    ErrorCode::CommitUnknown
                } else {
                    ErrorCode::Persistence
                })
            );
            drop(store);
            let reopened =
                FileRevisionJournal::open(&path, limits(), clock.clone(), clock.clone(), owner());
            if renamed || point == WritePoint::BeforeCreate {
                assert!(reopened.is_ok(), "{point:?}");
            } else {
                assert!(
                    matches!(reopened, Err(ErrorCode::Persistence)),
                    "pending artifact must fail closed: {point:?}"
                );
            }
            assert!(std::fs::read_dir(dir.path()).unwrap().count() <= 3); // snapshot, lock, one pending
        }
    }
    #[test]
    fn cancellation_deadline_fence_and_invalid_plan_have_no_filesystem_effects() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("revisions.json");
        let clock = clock();
        let mut context = context(&clock);
        let store = FileRevisionJournal::create(
            &path,
            &plan(0, LogLevel::Info),
            limits(),
            clock.clone(),
            clock.clone(),
            owner(),
        )
        .unwrap();
        let before = std::fs::read(&path).unwrap();
        let mut bad = plan(1, LogLevel::Debug);
        bad.resolved.global.logging = LogLevel::Error;
        assert_eq!(store.commit(&context, &bad), Err(ErrorCode::InvalidInput));
        context.expected.as_mut().unwrap().generation = 2;
        assert_eq!(
            store.commit(&context, &plan(1, LogLevel::Debug)),
            Err(ErrorCode::StaleGeneration)
        );
        context.expected = Some(owner());
        context.cancellation.cancel();
        assert_eq!(
            store.commit(&context, &plan(1, LogLevel::Debug)),
            Err(ErrorCode::Cancelled)
        );
        context.cancellation = CancellationToken::new();
        clock.advance(Duration::from_secs(10)).unwrap();
        assert_eq!(
            store.commit(&context, &plan(1, LogLevel::Debug)),
            Err(ErrorCode::Deadline)
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
    #[test]
    fn concurrent_cas_has_one_winner_and_bounded_busy_outcomes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("revisions.json");
        let clock = clock();
        let context = context(&clock);
        let store = Arc::new(
            FileRevisionJournal::create(
                &path,
                &plan(0, LogLevel::Info),
                limits(),
                clock.clone(),
                clock,
                owner(),
            )
            .unwrap(),
        );
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let outcomes = std::thread::scope(|scope| {
            let mut handles = vec![];
            for level in [LogLevel::Debug, LogLevel::Trace] {
                let store = store.clone();
                let barrier = barrier.clone();
                let context = context.clone();
                handles.push(scope.spawn(move || {
                    barrier.wait();
                    store.commit(&context, &plan(1, level))
                }));
            }
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(outcomes.iter().filter(|r| r.is_ok()).count(), 1);
        assert!(
            outcomes
                .iter()
                .filter_map(|r| r.as_ref().err())
                .all(|e| matches!(e, ErrorCode::Capacity | ErrorCode::RevisionConflict))
        );
        assert_eq!(store.load(&context).unwrap().id, ConfigRevisionId(2));
    }
    #[test]
    fn byte_limit_rejection_and_revision_exhaustion_do_not_touch_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("revisions.json");
        let clock = clock();
        let context = context(&clock);
        let store = FileRevisionJournal::create(
            &path,
            &plan(0, LogLevel::Info),
            limits(),
            clock.clone(),
            clock.clone(),
            owner(),
        )
        .unwrap();
        drop(store);
        let before = std::fs::read(&path).unwrap();
        let mut tight = limits();
        tight.bytes = before.len() + 50;
        let store = FileRevisionJournal::open(&path, tight, clock.clone(), clock, owner()).unwrap();
        assert_eq!(
            store.commit(&context, &plan(1, LogLevel::Debug)),
            Err(ErrorCode::Limit)
        );
        assert_eq!(store.load(&context).unwrap().id, ConfigRevisionId(1));
        {
            let mut state = store.state.lock().unwrap();
            state.journal.revisions.last_mut().unwrap().id = ConfigRevisionId(u64::MAX);
        }
        assert_eq!(
            store.commit(&context, &plan(u64::MAX, LogLevel::Debug)),
            Err(ErrorCode::Limit)
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
    #[test]
    fn corruption_and_missing_snapshot_cannot_reinitialize_revision_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("revisions.json");
        let clock = clock();
        let store = FileRevisionJournal::create(
            &path,
            &plan(0, LogLevel::Info),
            limits(),
            clock.clone(),
            clock.clone(),
            owner(),
        )
        .unwrap();
        drop(store);
        let mut journal: Journal = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        journal.revisions[0].desired.global.logging = LogLevel::Trace;
        std::fs::write(&path, serde_json::to_vec(&journal).unwrap()).unwrap();
        assert!(matches!(
            FileRevisionJournal::open(&path, limits(), clock.clone(), clock.clone(), owner()),
            Err(ErrorCode::Persistence)
        ));
        // Explicit destruction is confined to this newly created temp fixture.
        std::fs::remove_file(&path).unwrap();
        assert!(
            FileRevisionJournal::create(
                &path,
                &plan(0, LogLevel::Info),
                limits(),
                clock.clone(),
                clock,
                owner()
            )
            .is_err()
        );
    }
}
