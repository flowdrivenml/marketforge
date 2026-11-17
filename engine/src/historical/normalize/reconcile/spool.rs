//! Owned, versioned zlib/framed JSON spools. JSON preserves canonical decimal
//! strings and optional fields; frames carry lengths, checksums and an EOF footer.
//! Only this module opens scratch files. Disk reservations precede writes and
//! share a process-local pool across concurrent executions using the same root.
use super::{MemoryLease, Quota};
use crate::shared::{
    error::{MarketForgeError, Result},
    historical::limits,
};
use flate2::{Compression, bufread::ZlibDecoder, write::ZlibEncoder};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
const MAGIC: &[u8; 8] = b"MFSP0002";
const END: u64 = u64::MAX;
const OFFSET: u64 = 0xcbf29ce484222325;
fn invalid(s: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(s.into())
}
fn io_error(e: io::Error) -> MarketForgeError {
    // Preserve typed limit errors that passed through compression/serialization.
    if let Some(limit) = e
        .get_ref()
        .and_then(|e| e.downcast_ref::<crate::shared::error::ResourceLimitError>())
    {
        return MarketForgeError::ResourceLimit(limit.clone());
    }
    invalid(format!("reconciliation scratch I/O: {e}"))
}
fn scratch_io(error: MarketForgeError) -> io::Error {
    match error {
        MarketForgeError::ResourceLimit(limit) => io::Error::other(limit),
        other => io::Error::other(other),
    }
}
struct Pool {
    maximum: u64,
    used: Mutex<u64>,
}
// The pool includes both stored bytes and reservations in flight. An abandoned
// reservation releases itself even if an injected writer unwinds.
struct DiskReservation {
    pool: Arc<Pool>,
    bytes: u64,
}
impl DiskReservation {
    fn new(pool: &Arc<Pool>, bytes: u64) -> io::Result<Self> {
        let mut used = pool
            .used
            .lock()
            .map_err(|_| io::Error::other("scratch pool poisoned"))?;
        let requested = used
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("scratch accounting overflow"))?;
        if requested > pool.maximum {
            return Err(io::Error::other(limits::error(
                "scratch bytes",
                pool.maximum,
                requested,
                "reconciliation scratch write",
            )));
        }
        *used = requested;
        Ok(Self {
            pool: pool.clone(),
            bytes,
        })
    }
    fn commit(mut self, actual: u64) -> io::Result<()> {
        if actual > self.bytes {
            return Err(io::Error::other("scratch write exceeded reservation"));
        }
        let mut used = self
            .pool
            .used
            .lock()
            .map_err(|_| io::Error::other("scratch pool poisoned"))?;
        *used -= self.bytes - actual;
        self.bytes = 0;
        Ok(())
    }
}
impl Drop for DiskReservation {
    fn drop(&mut self) {
        if self.bytes != 0
            && let Ok(mut used) = self.pool.used.lock()
        {
            *used -= self.bytes;
        }
    }
}
static POOLS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<Pool>>>> = OnceLock::new();
// One small pool owner per scratch root, not a report queue. If failure.json
// cannot be persisted, existing sealed evidence stays charged for this process
// even after its last report reference has gone away.
static EVIDENCE_POOLS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Pool>>>> = OnceLock::new();
static NEXT: AtomicU64 = AtomicU64::new(0);
#[derive(Clone, Copy, Default, Serialize)]
struct Counters {
    serialization_calls: u64,
    typed_encoding_calls: u64,
    codec_write_calls: u64,
    codec_input_bytes: u64,
    compressed_bytes_written: u64,
    disk_write_calls: u64,
    parallel_sort_runs: u64,
    routing_blocks: u64,
    frames_written: u64,
    frames_read: u64,
    compressor_initializations: u64,
    spool_opens: u64,
    scratch_bytes_written: u64,
    scratch_bytes_read: u64,
    uncompressed_bytes_written: u64,
    uncompressed_bytes_read: u64,
    runs_generated: u64,
    merge_passes: u64,
    selected_fan_in: usize,
    maximum_fan_in: usize,
}
struct Inner {
    root: PathBuf,
    counters: Cell<Counters>,
    diagnostics: RefCell<
        Option<(
            crate::historical::normalize::progress::ProgressReporter,
            String,
        )>,
    >,
    pool: Arc<Pool>,
    next: Cell<u64>,
    handles: Cell<u64>,
    fan_in: Cell<usize>,
    typed_trades: Cell<bool>,
    typed_depth: Cell<bool>,
    raw: Cell<bool>,
    peak_handles: Cell<u64>,
    peak_disk: Cell<u64>,
    bytes: Cell<u64>,
    quota: Quota,
}
impl Inner {
    fn report(&self) {
        if let Some((reporter, key)) = self.diagnostics.borrow().as_ref() {
            reporter.resource_snapshot(key, serde_json::json!({"counters":self.counters.get(),
                "memory_scope":"shared parent quota; do not sum workspace values","accounted_memory_current":self.quota.used(),"accounted_memory_peak":self.quota.peak(),
                "accounted_memory_limit":self.quota.maximum(),"scratch_current":self.bytes.get(),
                "scratch_workspace_peak":self.peak_disk.get(),"shared_scratch_limit":self.pool.maximum,
                "open_handles":self.handles.get(),"open_handles_peak":self.peak_handles.get(),"codec":if self.raw.get() { "uncompressed" } else { "zlib-fast" },"format":if self.typed_depth.get() {"MFSP0002-typed-events-json-fallback"} else if self.typed_trades.get() { "MFSP0002-typed-trades-json-fallback" } else { "MFSP0002-json" }}));
        }
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        match fs::remove_dir_all(&self.root) {
            Ok(()) => {
                if let Ok(mut used) = self.pool.used.lock() {
                    *used -= self.bytes.get();
                    self.bytes.set(0);
                }
            }
            Err(e) => crate::historical::normalize::progress::log::diagnostic(format_args!(
                "owned scratch cleanup failed {}: {e}",
                self.root.display()
            )),
        }
        self.report();
    }
}
#[derive(Clone)]
pub(crate) struct Workspace(Rc<Inner>);
impl Workspace {
    pub fn new(root: &Path, maximum: u64, quota: Quota) -> Result<Self> {
        fs::create_dir_all(root).map_err(io_error)?;
        let root = fs::canonicalize(root).map_err(io_error)?;
        let pool = {
            let mut pools = POOLS
                .get_or_init(Default::default)
                .lock()
                .map_err(|_| invalid("scratch pool poisoned"))?;
            pools.retain(|_, p| p.strong_count() > 0);
            if let Some(p) = pools.get(&root).and_then(Weak::upgrade) {
                if p.maximum != maximum {
                    return Err(invalid(
                        "concurrent scratch configurations disagree on capacity",
                    ));
                }
                p
            } else {
                let p = Arc::new(Pool {
                    maximum,
                    used: Mutex::new(0),
                });
                pools.insert(root.clone(), Arc::downgrade(&p));
                p
            }
        };
        let owned = loop {
            let path = root.join(format!(
                "normalize-spool-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&path) {
                Ok(()) => break path,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(io_error(e)),
            }
        };
        Ok(Self(Rc::new(Inner {
            root: owned,
            counters: Cell::new(Counters::default()),
            diagnostics: RefCell::new(None),
            pool,
            next: Cell::new(0),
            handles: Cell::new(0),
            fan_in: Cell::new(2),
            typed_trades: Cell::new(false),
            typed_depth: Cell::new(false),
            raw: Cell::new(false),
            peak_handles: Cell::new(0),
            peak_disk: Cell::new(0),
            bytes: Cell::new(0),
            quota,
        })))
    }
    pub fn attach_diagnostics(
        &self,
        reporter: &crate::historical::normalize::progress::ProgressReporter,
        key: String,
    ) {
        if reporter.is_enabled() {
            *self.0.diagnostics.borrow_mut() = Some((reporter.clone(), key));
            self.report();
        }
    }
    fn count(&self, update: impl FnOnce(&mut Counters)) {
        let mut counters = self.0.counters.get();
        update(&mut counters);
        self.0.counters.set(counters);
    }
    pub fn report(&self) {
        self.0.report();
    }
    pub fn parallel_sort_run(&self) {
        self.count(|m| m.parallel_sort_runs += 1);
    }
    #[cfg(test)]
    pub fn parallel_sort_runs(&self) -> u64 {
        self.0.counters.get().parallel_sort_runs
    }
    #[cfg(test)]
    pub fn maximum_fan_in(&self) -> usize {
        self.0.counters.get().maximum_fan_in
    }
    pub fn generated_run(&self) {
        self.count(|m| m.runs_generated += 1);
    }
    pub fn merge_pass(&self, fan: usize) {
        self.count(|m| {
            m.merge_passes += 1;
            m.selected_fan_in = fan;
            m.maximum_fan_in = m.maximum_fan_in.max(fan);
        });
        self.report();
    }
    pub fn create(&self) -> Result<Spool> {
        let n = self.0.next.get();
        self.0.next.set(
            n.checked_add(1)
                .ok_or_else(|| invalid("scratch name overflow"))?,
        );
        Ok(Spool(Rc::new(Owned {
            path: self.0.root.join(format!("{n}.spool")),
            workspace: self.clone(),
            bytes: Cell::new(0),
            count: Cell::new(0),
        })))
    }
    pub fn run(&self, group: u64, pass: u64, index: u64) -> Spool {
        Spool(Rc::new(Owned {
            path: self
                .0
                .root
                .join(format!("run-{group}-{pass}-{index}.spool")),
            workspace: self.clone(),
            bytes: Cell::new(0),
            count: Cell::new(0),
        }))
    }
    pub fn group(&self) -> Result<u64> {
        let n = self.0.next.get();
        self.0.next.set(
            n.checked_add(1)
                .ok_or_else(|| invalid("scratch group overflow"))?,
        );
        Ok(n)
    }
    pub fn quota(&self) -> &Quota {
        &self.0.quota
    }
    pub fn configure_raw(&self, enabled: bool) {
        self.0.raw.set(enabled);
    }
    pub fn configure_typed_depth(&self, enabled: bool) {
        self.0.typed_depth.set(enabled);
    }
    pub fn configure_typed_trades(&self, enabled: bool) {
        self.0.typed_trades.set(enabled);
    }
    pub fn configure_merge(&self, requested: usize) {
        self.0.fan_in.set(requested.clamp(2, 64));
    }
    pub fn fan_in(&self, record_memory: u64) -> usize {
        let remaining = self.quota().maximum().saturating_sub(self.quota().used());
        let output = 384 * 1024 + self.quota().maximum() / 64;
        let memory_fan = remaining.saturating_sub(output) / (384 * 1024 + record_memory);
        self.0.fan_in.get().min(memory_fan as usize).max(2)
    }
    pub fn scratch_io(&self) -> (u64, u64) {
        let c = self.0.counters.get();
        (c.scratch_bytes_read, c.scratch_bytes_written)
    }
    pub fn peaks(&self) -> (u64, u64, u64) {
        (
            self.0.quota.peak(),
            self.0.peak_disk.get(),
            self.0.peak_handles.get(),
        )
    }
    fn handle(&self) -> Result<Handle> {
        // Fixed two-way merge: at most two inputs and one output. Source readers
        // and canonical writers are covered separately in stream admission.
        self.count(|m| m.spool_opens += 1);
        let n = self.0.handles.get() + 1;
        limits::check(
            "scratch open files",
            self.0.fan_in.get() as u64 + 1,
            n,
            "reconciliation scratch open",
        )?;
        // Installed miniz_oxide has bounded dictionary/hash/LZ/output/Huffman
        // buffers (~320 KiB); flate2 adds 32 KiB output, and readers use 8 KiB.
        // Fixed allowance includes codec structures; not measured allocator RSS.
        let memory = self.0.quota.reserve(384 * 1024, "scratch codec buffers")?;
        self.0.handles.set(n);
        self.0.peak_handles.set(self.0.peak_handles.get().max(n));
        Ok(Handle {
            workspace: self.clone(),
            _memory: memory,
        })
    }
}
struct Handle {
    workspace: Workspace,
    _memory: MemoryLease,
}
impl Drop for Handle {
    fn drop(&mut self) {
        self.workspace
            .0
            .handles
            .set(self.workspace.0.handles.get() - 1);
    }
}
struct Owned {
    path: PathBuf,
    workspace: Workspace,
    bytes: Cell<u64>,
    count: Cell<u64>,
}
#[derive(Clone)]
pub(crate) struct Spool(Rc<Owned>);
struct Disk {
    file: File,
    spool: Spool,
    _handle: Handle,
}
impl Write for Disk {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let pool = &self.spool.0.workspace.0.pool;
        let reservation = DiskReservation::new(pool, data.len() as u64)?;
        self.spool.0.workspace.count(|m| m.disk_write_calls += 1);
        let result = self.file.write(data);
        let n = result.as_ref().copied().unwrap_or(0);
        reservation.commit(n as u64)?;
        self.spool.0.bytes.set(self.spool.0.bytes.get() + n as u64);
        let w = &self.spool.0.workspace.0;
        w.bytes.set(w.bytes.get() + n as u64);
        // Workspace usage, not a sum of shared-pool peaks.
        w.peak_disk.set(w.peak_disk.get().max(w.bytes.get()));
        self.spool.0.workspace.count(|m| {
            m.scratch_bytes_written += n as u64;
            if !self.spool.0.workspace.0.raw.get() {
                m.compressed_bytes_written += n as u64;
            }
        });
        result?;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
impl Spool {
    pub fn delete(self) -> Result<()> {
        let size = fs::metadata(&self.0.path).map_err(io_error)?.len();
        fs::remove_file(&self.0.path).map_err(io_error)?;
        let mut used = self
            .0
            .workspace
            .0
            .pool
            .used
            .lock()
            .map_err(|_| invalid("scratch pool poisoned"))?;
        let own = self.0.workspace.0.bytes.get();
        if size > own || size > *used {
            return Err(invalid(
                "scratch file size changed outside owned accounting",
            ));
        }
        *used -= size;
        self.0.workspace.0.bytes.set(own - size);
        Ok(())
    }
    pub fn transfer_to(self, target: &Spool) -> Result<()> {
        if !Rc::ptr_eq(&self.0.workspace.0, &target.0.workspace.0) {
            return Err(invalid("scratch transfer across workspaces"));
        }
        if target.0.path.exists() {
            return Err(invalid("scratch transfer target exists"));
        }
        fs::rename(&self.0.path, &target.0.path).map_err(io_error)?;
        target.0.count.set(self.0.count.get());
        target.0.bytes.set(self.0.bytes.get());
        Ok(())
    }
    // Routing closes each append handle, preventing one open writer per symbol.
    // Each append is a zlib member; Members validates every member checksum.
    fn appender(&self) -> Result<SpoolWriter> {
        if !self.0.workspace.0.raw.get() {
            self.0
                .workspace
                .count(|m| m.compressor_initializations += 1);
        }
        let handle = self.0.workspace.handle()?;
        let fresh = !self.0.path.exists();
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.0.path)
            .map_err(io_error)?;
        let mut writer = SpoolWriter {
            codec: Encoder::new(
                Disk {
                    file,
                    spool: self.clone(),
                    _handle: handle,
                },
                Compression::fast(),
            ),
            spool: self.clone(),
            payload: Payload::new(self.0.workspace.quota().clone()),
            failed: false,
            last_hash: 0,
        };
        if fresh {
            writer.codec.write_all(MAGIC).map_err(io_error)?;
        }
        Ok(writer)
    }
    pub fn seal(&self) -> Result<()> {
        if !self.0.path.exists() {
            return self.writer()?.finish();
        }
        let handle = self.0.workspace.handle()?;
        let file = OpenOptions::new()
            .append(true)
            .open(&self.0.path)
            .map_err(io_error)?;
        if !self.0.workspace.0.raw.get() {
            self.0
                .workspace
                .count(|m| m.compressor_initializations += 1);
        }
        let mut codec = Encoder::new(
            Disk {
                file,
                spool: self.clone(),
                _handle: handle,
            },
            Compression::fast(),
        );
        codec.write_all(&END.to_le_bytes()).map_err(io_error)?;
        codec
            .write_all(&self.0.count.get().to_le_bytes())
            .map_err(io_error)?;
        codec.finish().map_err(io_error)?.flush().map_err(io_error)
    }
    pub fn count(&self) -> u64 {
        self.0.count.get()
    }
    pub fn writer(&self) -> Result<SpoolWriter> {
        if !self.0.workspace.0.raw.get() {
            self.0
                .workspace
                .count(|m| m.compressor_initializations += 1);
        }
        let handle = self.0.workspace.handle()?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.0.path)
            .map_err(io_error)?;
        let mut writer = SpoolWriter {
            codec: Encoder::new(
                Disk {
                    file,
                    spool: self.clone(),
                    _handle: handle,
                },
                Compression::fast(),
            ),
            spool: self.clone(),
            payload: Payload::new(self.0.workspace.quota().clone()),
            failed: false,
            last_hash: 0,
        };
        writer.codec.write_all(MAGIC).map_err(io_error)?;
        Ok(writer)
    }
    pub fn consume<T: DeserializeOwned>(self) -> Result<Consume<T>> {
        let reader = self.reader()?;
        Ok(Consume {
            reader: Some(reader),
            spool: Some(self),
        })
    }
    pub fn depth_reader(
        &self,
    ) -> Result<SpoolReader<crate::shared::domain::canonical::DepthProcessingOutcome>> {
        let mut r = self.reader()?;
        r.decode = super::typed_depth::decode;
        r.allocation = super::typed_depth::allocation;
        Ok(r)
    }
    pub fn frame_reader(&self) -> Result<SpoolReader<()>> {
        let mut r = self.reader()?;
        r.decode = |_| Ok(());
        r.allocation = |_| Ok(0);
        Ok(r)
    }
    pub fn trade_reader(&self) -> Result<SpoolReader<super::TradeEntry>> {
        let mut reader = self.reader()?;
        reader.decode = super::typed::decode;
        Ok(reader)
    }
    pub fn reader<T: DeserializeOwned>(&self) -> Result<SpoolReader<T>> {
        let handle = self.0.workspace.handle()?;
        let file = File::open(&self.0.path).map_err(io_error)?;
        let mut codec = Members::new(file, self.0.workspace.clone());
        let mut magic = [0; 8];
        codec.read_exact(&mut magic).map_err(io_error)?;
        if &magic != MAGIC {
            return Err(invalid("corrupt reconciliation scratch format/version"));
        }
        Ok(SpoolReader {
            codec,
            _handle: handle,
            quota: self.0.workspace.quota().clone(),
            workspace: self.0.workspace.clone(),
            payload: Payload::new(self.0.workspace.quota().clone()),
            count: 0,
            done: false,
            allocation: |bytes| {
                (bytes.len() as u64)
                    .checked_mul(64)
                    .and_then(|n| n.checked_add(1024))
                    .ok_or_else(|| invalid("scratch allocation overflow"))
            },
            decode: |bytes| {
                serde_json::from_slice(bytes)
                    .map_err(|e| invalid(format!("corrupt scratch record: {e}")))
            },
            _type: std::marker::PhantomData,
        })
    }
}
// Zlib has a fixed header, unlike gzip optional name/comment fields. A corrupt
// scratch header therefore cannot induce unbounded dependency header allocation.
struct DiskRead {
    file: File,
    workspace: Workspace,
}
impl Read for DiskRead {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read(output)?;
        self.workspace.count(|m| m.scratch_bytes_read += n as u64);
        Ok(n)
    }
}
struct Members {
    decoder: Option<ZlibDecoder<BufReader<DiskRead>>>,
    raw: Option<BufReader<DiskRead>>,
}
impl Members {
    fn new(file: File, workspace: Workspace) -> Self {
        let raw = workspace.0.raw.get();
        let input = BufReader::with_capacity(8192, DiskRead { file, workspace });
        if raw {
            Self {
                decoder: None,
                raw: Some(input),
            }
        } else {
            Self {
                decoder: Some(ZlibDecoder::new(input)),
                raw: None,
            }
        }
    }
}
impl Read for Members {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if let Some(raw) = self.raw.as_mut() {
            return raw.read(output);
        }
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            let Some(decoder) = self.decoder.as_mut() else {
                return Ok(0);
            };
            let count = decoder.read(output)?;
            if count > 0 {
                return Ok(count);
            }
            let mut input = self.decoder.take().unwrap().into_inner();
            if input.fill_buf()?.is_empty() {
                return Ok(0);
            }
            self.decoder = Some(ZlibDecoder::new(input));
        }
    }
}
struct Fingerprint {
    length: u64,
    hash: u64,
    maximum: u64,
}
impl Write for Fingerprint {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let requested = self
            .length
            .checked_add(b.len() as u64)
            .ok_or_else(|| io::Error::other("spool record length overflow"))?;
        if requested > self.maximum {
            return Err(io::Error::other(limits::error(
                "reconciliation record bytes",
                self.maximum,
                requested,
                "scratch frame serialization",
            )));
        }
        self.length = requested;
        for byte in b {
            self.hash = (self.hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
        }
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
// Retained capacity has its own lease, separate from decoded record leases.
// A boxed slice gives an exact allocation size; growth is reserved before allocating.
struct Payload {
    bytes: Vec<u8>,
    quota: Quota,
    memory: Option<MemoryLease>,
    maximum: u64,
}
impl Payload {
    fn new(quota: Quota) -> Self {
        Self {
            bytes: Vec::new(),
            maximum: quota.maximum() / 128,
            quota,
            memory: None,
        }
    }
    fn ensure_capacity(&mut self, length: usize) -> io::Result<()> {
        let maximum = self.maximum;
        if length > self.bytes.capacity() {
            let capacity = length.next_power_of_two().min(maximum as usize);
            // Both old and new allocations remain charged during the copy.
            let memory = self
                .quota
                .reserve(capacity as u64, "scratch serialization buffer")
                .map_err(scratch_io)?;
            let mut replacement = vec![0u8; capacity].into_boxed_slice().into_vec();
            replacement.clear();
            replacement.extend_from_slice(&self.bytes);
            self.bytes = replacement;
            self.memory = Some(memory);
        }
        Ok(())
    }
}
impl Write for Payload {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let length = self
            .bytes
            .len()
            .checked_add(data.len())
            .ok_or_else(|| io::Error::other("scratch frame length overflow"))?;
        let maximum = self.maximum;
        limits::check(
            "reconciliation record bytes",
            maximum,
            length as u64,
            "scratch frame serialization",
        )
        .map_err(scratch_io)?;
        self.ensure_capacity(length)?;
        self.bytes.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
enum Encoder {
    Zlib(ZlibEncoder<Disk>),
    Raw(BufWriter<Disk>),
}
impl Encoder {
    fn new(disk: Disk, compression: Compression) -> Self {
        if disk.spool.0.workspace.0.raw.get() {
            Self::Raw(BufWriter::with_capacity(32 * 1024, disk))
        } else {
            Self::Zlib(ZlibEncoder::new(disk, compression))
        }
    }
    fn finish(self) -> io::Result<Disk> {
        match self {
            Self::Zlib(codec) => codec.finish(),
            Self::Raw(disk) => disk.into_inner().map_err(|e| e.into_error()),
        }
    }
}
impl Write for Encoder {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Zlib(codec) => {
                let n = codec.write(bytes)?;
                codec
                    .get_ref()
                    .spool
                    .0
                    .workspace
                    .count(|m| m.codec_input_bytes += n as u64);
                Ok(n)
            }
            Self::Raw(disk) => {
                let n = disk.write(bytes)?;
                disk.get_ref()
                    .spool
                    .0
                    .workspace
                    .count(|m| m.codec_input_bytes += n as u64);
                Ok(n)
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Zlib(codec) => codec.flush(),
            Self::Raw(disk) => disk.flush(),
        }
    }
}
pub(crate) struct SpoolWriter {
    codec: Encoder,
    spool: Spool,
    payload: Payload,
    failed: bool,
    last_hash: u64,
}
impl SpoolWriter {
    pub fn last_checksum(&self) -> u64 {
        self.last_hash
    }
    pub fn push<T: Serialize>(&mut self, value: &T) -> Result<()> {
        if self.failed {
            return Err(invalid("scratch writer has a failed append"));
        }
        self.payload.bytes.clear();
        // Serialization failure leaves the file untouched and permits a retry.
        self.spool.0.workspace.count(|m| m.serialization_calls += 1);
        serialize(&mut self.payload, value)?;
        self.write_frame()
    }
    pub fn push_depth(
        &mut self,
        value: &crate::shared::domain::canonical::DepthProcessingOutcome,
    ) -> Result<()> {
        if !self.spool.0.workspace.0.typed_depth.get() {
            return self.push(value);
        }
        if self.failed {
            return Err(invalid("scratch writer has a failed append"));
        }
        self.payload.bytes.clear();
        self.spool
            .0
            .workspace
            .count(|m| m.typed_encoding_calls += 1);
        super::typed_depth::encode(&mut self.payload, value).map_err(io_error)?;
        self.write_frame()
    }
    pub fn push_trade(&mut self, value: &super::TradeEntry) -> Result<()> {
        if !self.spool.0.workspace.0.typed_trades.get() {
            return self.push(value);
        }
        if self.failed {
            return Err(invalid("scratch writer has a failed append"));
        }
        self.payload.bytes.clear();
        self.spool
            .0
            .workspace
            .count(|m| m.typed_encoding_calls += 1);
        super::typed::encode(&mut self.payload, value).map_err(io_error)?;
        self.write_frame()
    }
    fn write_frame(&mut self) -> Result<()> {
        let mut fingerprint = Fingerprint {
            length: 0,
            hash: OFFSET,
            maximum: self.payload.quota.maximum() / 128,
        };
        fingerprint
            .write_all(&self.payload.bytes)
            .map_err(io_error)?;
        // Any I/O failure poisons the writer: it must never seal a partial frame.
        self.failed = true;
        let mut header = [0u8; 16];
        header[..8].copy_from_slice(&fingerprint.length.to_le_bytes());
        header[8..].copy_from_slice(&fingerprint.hash.to_le_bytes());
        self.codec.write_all(&header).map_err(io_error)?;
        self.codec
            .write_all(&self.payload.bytes)
            .map_err(io_error)?;
        self.spool.0.count.set(
            self.spool
                .0
                .count
                .get()
                .checked_add(1)
                .ok_or_else(|| invalid("spool count overflow"))?,
        );
        self.failed = false;
        self.last_hash = fingerprint.hash;
        self.spool.0.workspace.count(|m| {
            m.frames_written += 1;
            m.codec_write_calls += 2;
            m.uncompressed_bytes_written += fingerprint.length;
        });
        if self.spool.0.count.get().is_multiple_of(1024) {
            self.spool.0.workspace.report();
        }
        Ok(())
    }
    fn close_member(self) -> Result<()> {
        if self.failed {
            return Err(invalid("cannot close failed scratch writer"));
        }
        self.codec
            .finish()
            .map_err(io_error)?
            .flush()
            .map_err(io_error)
    }
    pub fn finish(mut self) -> Result<()> {
        self.spool.0.workspace.report();
        if self.failed {
            return Err(invalid("cannot seal failed scratch writer"));
        }
        self.codec.write_all(&END.to_le_bytes()).map_err(io_error)?;
        self.codec
            .write_all(&self.spool.0.count.get().to_le_bytes())
            .map_err(io_error)?;
        self.codec
            .finish()
            .map_err(io_error)?
            .flush()
            .map_err(io_error)
    }
}
fn serialize<T: Serialize>(writer: &mut impl Write, value: &T) -> Result<()> {
    serde_json::to_writer(writer, value).map_err(|e| {
        // serde_json keeps the original io::Error, including its typed payload.
        if e.is_io() {
            return io_error(io::Error::from(e));
        }
        invalid(format!("scratch serialization: {e}"))
    })
}
pub(crate) struct Leased<T> {
    pub value: T,
    _memory: MemoryLease,
    checksum: u64,
}
impl<T> Leased<T> {
    pub fn checksum(&self) -> u64 {
        self.checksum
    }
    pub fn accounted_bytes(&self) -> u64 {
        self._memory.bytes()
    }
}
impl Leased<super::TradeEntry> {
    pub fn compact_accounting(&mut self) -> Result<()> {
        // Canonical trades have only three variable strings and fixed-size
        // numeric/optional fields. After JSON payload/decoder temporaries die,
        // replace the generic DOM allowance with their actual capacities plus
        // vector/cursor capacity and allocator allowance. No identity-key copies.
        let t = &self.value.trade;
        let bytes = (std::mem::size_of::<Self>() as u64)
            .checked_mul(4)
            .and_then(|n| n.checked_add((t.envelope.symbol.capacity() as u64).checked_mul(2)?))
            .and_then(|n| n.checked_add((t.envelope.stream_id.capacity() as u64).checked_mul(2)?))
            .and_then(|n| {
                n.checked_add(
                    (t.trade_id.as_ref().map_or(0, String::capacity) as u64).checked_mul(2)?,
                )
            })
            .and_then(|n| n.checked_add(256))
            .ok_or_else(|| invalid("canonical trade footprint overflow"))?;
        // For very short serialized records the initial generic allowance can
        // already be smaller. Never undercharge by replacing it with a larger
        // allocation silently; retain the initial (still conservative) lease.
        self._memory.shrink_to(bytes);
        Ok(())
    }
}

pub(crate) struct SpoolReader<T> {
    codec: Members,
    _handle: Handle,
    quota: Quota,
    workspace: Workspace,
    payload: Payload,
    count: u64,
    done: bool,
    decode: fn(&[u8]) -> Result<T>,
    allocation: fn(&[u8]) -> Result<u64>,
    _type: std::marker::PhantomData<T>,
}
impl<T: DeserializeOwned> SpoolReader<T> {
    pub fn next(&mut self) -> Result<Option<Leased<T>>> {
        let result = self.read_next();
        if result.is_err() {
            self.done = true;
        }
        result
    }
    fn read_next(&mut self) -> Result<Option<Leased<T>>> {
        if self.done {
            return Ok(None);
        }
        let mut word = [0; 8];
        self.codec.read_exact(&mut word).map_err(io_error)?;
        let length = u64::from_le_bytes(word);
        self.codec.read_exact(&mut word).map_err(io_error)?;
        let hash = u64::from_le_bytes(word);
        if length == END {
            if hash != self.count {
                return Err(invalid("scratch footer record count mismatch"));
            }
            let mut tail = [0];
            if self.codec.read(&mut tail).map_err(io_error)? != 0 {
                return Err(invalid("trailing scratch data"));
            }
            self.done = true;
            return Ok(None);
        }
        limits::check(
            "reconciliation record bytes",
            self.quota.maximum() / 128,
            length,
            "scratch frame read",
        )?;
        // Bounds JSON DOM/string allocations as well as the encoded frame. A
        // deliberately conservative capacity allowance, not allocator telemetry.
        let length_usize =
            usize::try_from(length).map_err(|_| invalid("scratch length not addressable"))?;
        self.payload
            .ensure_capacity(length_usize)
            .map_err(io_error)?;
        self.payload.bytes.resize(length_usize, 0);
        self.codec
            .read_exact(&mut self.payload.bytes)
            .map_err(io_error)?;
        let mut fingerprint = Fingerprint {
            length: 0,
            hash: OFFSET,
            maximum: length,
        };
        fingerprint
            .write_all(&self.payload.bytes)
            .map_err(io_error)?;
        if fingerprint.hash != hash {
            return Err(invalid("scratch record checksum mismatch"));
        }
        let memory = self.quota.reserve(
            (self.allocation)(&self.payload.bytes)?,
            "scratch decoded record",
        )?;
        let value = (self.decode)(&self.payload.bytes)?;
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| invalid("scratch read count overflow"))?;
        self.workspace.count(|m| {
            m.frames_read += 1;
            m.uncompressed_bytes_read += length;
        });
        if self.count.is_multiple_of(1024) {
            self.workspace.report();
        }
        Ok(Some(Leased {
            value,
            _memory: memory,
            checksum: hash,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn workspace(root: &Path, memory: u64, disk: u64) -> Workspace {
        Workspace::new(root, disk, Quota::new(memory)).unwrap()
    }
    #[test]
    fn in_flight_reservations_bound_concurrency_and_release_short_writes_and_errors() {
        let pool = Arc::new(Pool {
            maximum: 100,
            used: Mutex::new(0),
        });
        let hold = DiskReservation::new(&pool, 80).unwrap();
        let other = pool.clone();
        std::thread::spawn(move || {
            assert!(DiskReservation::new(&other, 21).is_err());
            DiskReservation::new(&other, 20).unwrap().commit(7).unwrap();
        })
        .join()
        .unwrap();
        assert_eq!(*pool.used.lock().unwrap(), 87);
        hold.commit(3).unwrap();
        assert_eq!(*pool.used.lock().unwrap(), 10);
        drop(DiskReservation::new(&pool, 90).unwrap());
        assert_eq!(*pool.used.lock().unwrap(), 10);
    }
    #[test]
    fn serialization_failure_does_not_append_and_io_failure_cannot_seal() {
        struct Reject;
        impl Serialize for Reject {
            fn serialize<S: serde::Serializer>(
                &self,
                _: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("rejected record"))
            }
        }
        let root = tempfile::tempdir().unwrap();
        let w = workspace(root.path(), 4 << 20, 1 << 20);
        let spool = w.create().unwrap();
        let mut writer = spool.writer().unwrap();
        assert!(
            writer
                .push(&Reject)
                .unwrap_err()
                .to_string()
                .contains("serialization")
        );
        assert_eq!(spool.count(), 0);
        writer.push(&7u64).unwrap();
        writer.finish().unwrap();
        let mut reader = spool.reader::<u64>().unwrap();
        assert_eq!(reader.next().unwrap().unwrap().value, 7);
        assert!(reader.next().unwrap().is_none());
        drop(reader);
        let root = tempfile::tempdir().unwrap();
        let w = workspace(root.path(), 4 << 20, 10);
        w.configure_raw(true);
        let spool = w.create().unwrap();
        let mut writer = spool.writer().unwrap();
        let mut failed = false;
        for _ in 0..10000 {
            if writer.push(&0u64).is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed);
        assert!(writer.push(&1u64).is_err());
        assert!(writer.finish().is_err());
        assert!(spool.reader::<u64>().is_err());
    }
    #[test]
    fn serializes_once_retains_capacity_and_cache_bounds_writer_initializations() {
        struct Once<'a>(&'a Cell<u64>);
        impl Serialize for Once<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                self.0.set(self.0.get() + 1);
                serializer.serialize_str("payload")
            }
        }
        let root = tempfile::tempdir().unwrap();
        let w = workspace(root.path(), 4 << 20, 1 << 20);
        let spool = w.create().unwrap();
        let mut writer = spool.writer().unwrap();
        let calls = Cell::new(0);
        writer.push(&Once(&calls)).unwrap();
        assert_eq!(calls.get(), 1);
        let capacity = writer.payload.bytes.capacity();
        writer.push(&"").unwrap();
        assert_eq!(writer.payload.bytes.capacity(), capacity);
        writer.finish().unwrap();
        let a = w.create().unwrap();
        let b = w.create().unwrap();
        let before = w.0.counters.get().compressor_initializations;
        let mut cache = AppendCache::new();
        for i in 0..1000u64 {
            cache.push(if i % 2 == 0 { &a } else { &b }, &i).unwrap();
        }
        assert!(w.0.handles.get() <= 2);
        cache.finish().unwrap();
        a.seal().unwrap();
        b.seal().unwrap();
        assert_eq!(w.0.counters.get().compressor_initializations - before, 4);
        assert_eq!(a.count(), 500);
        assert_eq!(b.count(), 500);
        let mut reader = a.reader::<u64>().unwrap();
        for i in (0..1000).step_by(2) {
            assert_eq!(reader.next().unwrap().unwrap().value, i);
        }
        assert!(reader.next().unwrap().is_none());
    }
    #[test]
    fn cache_eviction_transfer_and_both_codecs_preserve_accounting() {
        for raw in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let w = workspace(root.path(), 4 << 20, 1 << 20);
            w.configure_raw(raw);
            let spools: Vec<_> = (0..5).map(|_| w.create().unwrap()).collect();
            let mut cache = AppendCache::new();
            for i in 0..100u64 {
                cache.push(&spools[i as usize % 5], &i).unwrap();
                assert!(w.0.handles.get() <= 2);
            }
            cache.finish().unwrap();
            if !raw {
                assert_eq!(w.0.counters.get().compressor_initializations, 5);
            }
            for (index, spool) in spools.into_iter().enumerate() {
                spool.seal().unwrap();
                let size = fs::metadata(&spool.0.path).unwrap().len();
                let target = w.create().unwrap();
                let writes = w.0.counters.get().scratch_bytes_written;
                spool.transfer_to(&target).unwrap();
                assert_eq!(w.0.counters.get().scratch_bytes_written, writes);
                assert_eq!(fs::metadata(&target.0.path).unwrap().len(), size);
                let mut reader = target.reader::<u64>().unwrap();
                for i in (index as u64..100).step_by(5) {
                    assert_eq!(reader.next().unwrap().unwrap().value, i);
                }
                assert!(reader.next().unwrap().is_none());
                drop(reader);
                target.delete().unwrap();
            }
            assert_eq!(*w.0.pool.used.lock().unwrap(), 0);
            assert_eq!(w.quota().used(), 0);
        }
    }
    #[test]
    fn incremental_roundtrip_larger_than_memory_and_reservations_release() {
        let root = tempfile::tempdir().unwrap();
        let w = workspace(root.path(), 2 << 20, 1 << 20);
        let spool = w.create().unwrap();
        let mut writer = spool.writer().unwrap();
        for i in 0..40000u64 {
            writer
                .push(&(i, "0.1234567890123456789012345678", Some(i), None::<u64>))
                .unwrap();
        }
        writer.finish().unwrap();
        let mut reader = spool
            .reader::<(u64, String, Option<u64>, Option<u64>)>()
            .unwrap();
        for i in 0..40000 {
            let row = reader.next().unwrap().unwrap();
            assert_eq!(
                row.value,
                (i, "0.1234567890123456789012345678".into(), Some(i), None)
            );
        }
        assert!(reader.next().unwrap().is_none());
        drop(reader);
        assert_eq!(w.quota().used(), 0);
        assert!(w.peaks().0 < 2 << 20);
        spool.delete().unwrap();
        assert_eq!(*w.0.pool.used.lock().unwrap(), 0);
    }
    #[test]
    fn canonical_trade_roundtrip_preserves_every_optional_field_and_decimal_scale() {
        let root = tempfile::tempdir().unwrap();
        let w = workspace(root.path(), 4 << 20, 1 << 20);
        let mut entry = super::super::tests::event(17, 2, 300, Some("native-id"), 1);
        entry.ordinal = 999;
        let decimal: rust_decimal::Decimal = "0.1234567890123456789012345678".parse().unwrap();
        let t = &mut entry.trade;
        t.envelope.system_timestamp_ns = Some(-1);
        t.sequence = Some(u64::MAX);
        t.side = crate::shared::domain::canonical::TradeSide::Sell;
        t.price = decimal;
        t.quantity_base = Some(decimal);
        t.quantity_quote = Some(decimal);
        t.quantity_contracts = Some(decimal);
        t.is_rpi = Some(false);
        t.trade_iv = Some(decimal);
        t.mark_iv = Some(decimal);
        t.index_price = Some(decimal);
        t.mark_price = Some(decimal);
        let spool = w.create().unwrap();
        let mut writer = spool.writer().unwrap();
        writer.push(&entry).unwrap();
        writer.finish().unwrap();
        let mut reader = spool.reader::<super::super::TradeEntry>().unwrap();
        let actual = reader.next().unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(&entry).unwrap(),
            serde_json::to_value(&actual.value).unwrap()
        );
        assert!(reader.next().unwrap().is_none());
    }
    #[test]
    fn oversized_corrupted_and_truncated_frames_fail() {
        let root = tempfile::tempdir().unwrap();
        let w = workspace(root.path(), 2 << 20, 1 << 20);
        let spool = w.create().unwrap();
        let mut writer = spool.writer().unwrap();
        assert!(matches!(
            writer.push(&"x".repeat(17000)),
            Err(MarketForgeError::ResourceLimit(_))
        ));
        writer.push(&"valid").unwrap();
        writer.finish().unwrap();
        let original = fs::read(&spool.0.path).unwrap();
        for cut in 1..=8 {
            fs::write(&spool.0.path, &original[..original.len() - cut]).unwrap();
            let mut reader = spool.reader::<String>().unwrap();
            let result = (|| -> Result<()> {
                reader.next()?;
                reader.next()?;
                Ok(())
            })();
            assert!(
                result.is_err(),
                "missing {cut} compressed footer bytes must fail"
            );
        }
        let mut data = original;
        data.truncate(data.len() - 5);
        fs::write(&spool.0.path, &data).unwrap();
        let mut reader = spool.reader::<String>().unwrap();
        reader.next().unwrap();
        assert!(reader.next().is_err());
        drop(reader);
        let corrupt = w.create().unwrap();
        let handle = w.handle().unwrap();
        let f = File::create(&corrupt.0.path).unwrap();
        let mut codec = Encoder::new(
            Disk {
                file: f,
                spool: corrupt.clone(),
                _handle: handle,
            },
            Compression::fast(),
        );
        codec.write_all(MAGIC).unwrap();
        codec.write_all(&5u64.to_le_bytes()).unwrap();
        codec.write_all(&0u64.to_le_bytes()).unwrap();
        codec.write_all(b"\"bad\"").unwrap();
        codec.finish().unwrap();
        assert!(
            corrupt
                .reader::<String>()
                .unwrap()
                .next()
                .err()
                .unwrap()
                .to_string()
                .contains("checksum")
        );
    }
    #[test]
    fn scratch_handles_and_reader_length_checks_fail_promptly() {
        let root = tempfile::tempdir().unwrap();
        let w = workspace(root.path(), 4 << 20, 1 << 20);
        let a = w.handle().unwrap();
        let b = w.handle().unwrap();
        let c = w.handle().unwrap();
        assert!(
            matches!(w.handle(),Err(MarketForgeError::ResourceLimit(ref e)) if e.resource=="scratch open files")
        );
        drop((a, b, c));
        let spool = w.create().unwrap();
        let f = File::create(&spool.0.path).unwrap();
        let handle = w.handle().unwrap();
        let mut g = Encoder::new(
            Disk {
                file: f,
                spool: spool.clone(),
                _handle: handle,
            },
            Compression::fast(),
        );
        g.write_all(MAGIC).unwrap();
        g.write_all(&(u64::MAX - 1).to_le_bytes()).unwrap();
        g.write_all(&0u64.to_le_bytes()).unwrap();
        g.finish().unwrap();
        let mut reader = spool.reader::<String>().unwrap();
        assert!(
            matches!(reader.next(),Err(MarketForgeError::ResourceLimit(ref e)) if e.stage=="scratch frame read")
        );
        assert!(reader.next().unwrap().is_none());
    }
    #[test]
    fn consuming_iterator_closes_before_releasing_disk_and_cleans_early_exit() {
        let root = tempfile::tempdir().unwrap();
        let w = workspace(root.path(), 4 << 20, 1 << 20);
        let spool = w.create().unwrap();
        let mut writer = spool.writer().unwrap();
        for i in 0..5u64 {
            writer.push(&i).unwrap();
        }
        writer.finish().unwrap();
        let mut rows = spool.consume::<u64>().unwrap();
        assert_eq!(rows.next().unwrap().unwrap(), 0);
        assert_eq!(w.0.handles.get(), 1);
        assert!(*w.0.pool.used.lock().unwrap() > 0);
        drop(rows);
        assert_eq!(w.0.handles.get(), 0);
        assert_eq!(*w.0.pool.used.lock().unwrap(), 0);
        assert_eq!(fs::read_dir(&w.0.root).unwrap().count(), 0);
    }
    #[test]
    fn concurrent_workspace_disk_capacity_and_owned_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let a = workspace(root.path(), 2 << 20, 30);
        let b = workspace(root.path(), 2 << 20, 30);
        assert_ne!(a.0.root, b.0.root);
        let spool = a.create().unwrap();
        let mut writer = spool.writer().unwrap();
        writer.push(&"data").unwrap();
        assert!(matches!(
            writer.finish(),
            Err(MarketForgeError::ResourceLimit(_))
        ));
        spool.delete().unwrap();
        assert_eq!(*b.0.pool.used.lock().unwrap(), 0);
        fs::write(root.path().join("unowned"), b"keep").unwrap();
        drop(a);
        drop(b);
        assert!(root.path().join("unowned").exists());
    }
}

/// Byte-bounded blocks across all routed symbols. Alternating symbols do not
/// require a compressor per row. Flushing opens only one writer at a time.
struct PendingBlock {
    spool: Spool,
    payload: Payload,
    records: u64,
    _metadata: MemoryLease,
}
pub(crate) struct AppendCache {
    pending: BTreeMap<PathBuf, PendingBlock>,
    staging: Option<Payload>,
    retained: u64,
}
impl AppendCache {
    pub fn new() -> Self {
        Self {
            pending: BTreeMap::new(),
            staging: None,
            retained: 0,
        }
    }
    pub fn push<T: Serialize>(&mut self, spool: &Spool, value: &T) -> Result<()> {
        let quota = spool.0.workspace.quota();
        let budget = (quota.maximum() / 16).min(256 * 1024);
        let staging = self
            .staging
            .get_or_insert_with(|| Payload::new(quota.clone()));
        staging.bytes.clear();
        spool.0.workspace.count(|m| m.serialization_calls += 1);
        serialize(staging, value)?;
        let length = staging.bytes.len() as u64;
        let mut fingerprint = Fingerprint {
            length: 0,
            hash: OFFSET,
            maximum: quota.maximum() / 128,
        };
        fingerprint.write_all(&staging.bytes).map_err(io_error)?;
        let metadata = spool.0.path.as_os_str().len() as u64 * 2 + 512;
        // A large individual event bypasses block accumulation, still bounded by
        // the frame limit and held in the same admitted reusable buffer.
        if length + 16 > budget / 4 {
            self.flush()?;
            let mut writer = spool.appender()?;
            std::mem::swap(&mut writer.payload, self.staging.as_mut().unwrap());
            writer.write_frame()?;
            spool.0.workspace.count(|m| m.routing_blocks += 1);
            std::mem::swap(&mut writer.payload, self.staging.as_mut().unwrap());
            return writer.close_member();
        }
        if self.retained.saturating_add((length + 16) * 2 + metadata) > budget {
            self.flush()?;
        }
        if !self.pending.contains_key(&spool.0.path) {
            let lease = quota.reserve(metadata, "family routing block metadata")?;
            let mut payload = Payload::new(quota.clone());
            payload.maximum = budget;
            self.pending.insert(
                spool.0.path.clone(),
                PendingBlock {
                    spool: spool.clone(),
                    payload,
                    records: 0,
                    _metadata: lease,
                },
            );
            self.retained += metadata;
        }
        let block = self.pending.get_mut(&spool.0.path).unwrap();
        let before = block.payload.bytes.capacity();
        block
            .payload
            .write_all(&length.to_le_bytes())
            .map_err(io_error)?;
        block
            .payload
            .write_all(&fingerprint.hash.to_le_bytes())
            .map_err(io_error)?;
        block
            .payload
            .write_all(&self.staging.as_ref().unwrap().bytes)
            .map_err(io_error)?;
        self.retained += (block.payload.bytes.capacity() - before) as u64;
        block.records = block
            .records
            .checked_add(1)
            .ok_or_else(|| invalid("routing block count overflow"))?;
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        for (_, block) in std::mem::take(&mut self.pending) {
            let mut writer = block.spool.appender()?;
            writer.failed = true;
            writer
                .codec
                .write_all(&block.payload.bytes)
                .map_err(io_error)?;
            let count = block
                .spool
                .0
                .count
                .get()
                .checked_add(block.records)
                .ok_or_else(|| invalid("routing spool count overflow"))?;
            block.spool.0.count.set(count);
            writer.failed = false;
            block.spool.0.workspace.count(|m| {
                m.routing_blocks += 1;
                m.frames_written += block.records;
                m.codec_write_calls += 1;
                m.uncompressed_bytes_written +=
                    block.payload.bytes.len() as u64 - block.records * 16;
            });
            writer.close_member()?;
        }
        self.retained = 0;
        Ok(())
    }
    pub fn finish(mut self) -> Result<()> {
        self.flush()
    }
}

/// Routing input ownership is consumed once. Close the handle before unlinking
/// and releasing disk capacity (Unix unlink alone can retain open-file storage).
pub(crate) struct Consume<T> {
    reader: Option<SpoolReader<T>>,
    spool: Option<Spool>,
}
impl<T: DeserializeOwned> Iterator for Consume<T> {
    type Item = Result<T>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.as_mut()?.next() {
            Ok(Some(row)) => Some(Ok(row.value)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
impl<T> Drop for Consume<T> {
    fn drop(&mut self) {
        drop(self.reader.take());
        if let Some(spool) = self.spool.take()
            && let Err(e) = spool.delete()
        {
            crate::historical::normalize::progress::log::diagnostic(format_args!(
                "scratch input cleanup failed: {e}"
            ));
        }
    }
}

/// A sealed report may outlive its worker-local workspace. Disk ownership and
/// its shared-pool reservation transfer together; no open handle crosses threads.
#[derive(Clone)]
pub(crate) struct RetainedSpool(Arc<RetainedDisk>);
struct RetainedDisk {
    path: PathBuf,
    pool: Arc<Pool>,
    bytes: u64,
    count: u64,
    buffer_bytes: u64,
    preserve: AtomicBool,
}
impl std::fmt::Debug for RetainedSpool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedSpool")
            .field("path", &self.0.path)
            .finish()
    }
}
impl Drop for RetainedDisk {
    fn drop(&mut self) {
        if self.preserve.load(Ordering::Relaxed) {
            return;
        }
        match fs::remove_file(&self.path) {
            Ok(()) => *self.pool.used.lock().unwrap() -= self.bytes,
            Err(e) => crate::historical::normalize::progress::log::diagnostic(format_args!(
                "retained report cleanup failed {}: {e}",
                self.path.display()
            )),
        }
    }
}
impl Spool {
    pub fn retain(self) -> Result<RetainedSpool> {
        let w = &self.0.workspace.0;
        if w.handles.get() != 0 || Rc::strong_count(&self.0) != 1 {
            return Err(invalid(
                "report transfer requires exclusive closed spool ownership",
            ));
        }
        let bytes = self.0.bytes.get();
        let remaining = w
            .bytes
            .get()
            .checked_sub(bytes)
            .ok_or_else(|| invalid("report transfer accounting underflow"))?;
        // Claim a destination before rename: PID reuse or an orphan from a
        // previous process must never cause replacement of an existing report.
        let handle = self.0.workspace.handle()?;
        let path = loop {
            let path = w.root.parent().unwrap().join(format!(
                "normalize-report-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    drop(file);
                    break path;
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(io_error(e)),
            }
        };
        drop(handle);
        if let Err(error) = fs::rename(&self.0.path, &path) {
            if let Err(cleanup) = fs::remove_file(&path) {
                crate::historical::normalize::progress::log::diagnostic(format_args!(
                    "report handoff placeholder cleanup failed {}: {cleanup}",
                    path.display()
                ));
            }
            return Err(io_error(error));
        }
        w.bytes.set(remaining);
        Ok(RetainedSpool(Arc::new(RetainedDisk {
            path,
            pool: w.pool.clone(),
            bytes,
            count: self.0.count.get(),
            buffer_bytes: w.quota.maximum(),
            preserve: AtomicBool::new(false),
        })))
    }
}
impl RetainedSpool {
    pub fn preserve_evidence(&self, job: u64, task: u64) {
        if !self.0.preserve.swap(true, Ordering::Relaxed) {
            EVIDENCE_POOLS
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .entry(self.0.path.parent().unwrap().to_owned())
                .or_insert_with(|| self.0.pool.clone());
            crate::historical::normalize::progress::log::diagnostic(format_args!(
                "Retained framed observation evidence: job={job} task={task} path={}",
                self.0.path.display()
            ));
        }
    }
    pub fn same_storage(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn visit<T: DeserializeOwned>(
        &self,
        mut visit: impl FnMut(&T) -> Result<()>,
    ) -> Result<()> {
        // One bounded decoder and one frame, regardless of report cardinality.
        let w = Workspace::new(
            self.0.path.parent().unwrap(),
            self.0.pool.maximum,
            Quota::new(self.0.buffer_bytes),
        )?;
        let spool = Spool(Rc::new(Owned {
            path: self.0.path.clone(),
            workspace: w,
            bytes: Cell::new(0),
            count: Cell::new(self.0.count),
        }));
        let mut reader = spool.reader::<T>()?;
        let mut count = 0u64;
        while let Some(entry) = reader.next()? {
            visit(&entry.value)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| invalid("retained report count overflow"))?;
        }
        if count != self.0.count {
            return Err(invalid("retained report frame count mismatch"));
        }
        Ok(())
    }
}
