//! Streaming V2 boundary validation. Only one source (two boundary books) is decoded
//! at a time. A successful return always validates the complete JSON envelope.
use super::segments::{DepthSourceBoundary, DepthStreamBoundaries};
use crate::shared::{
    domain::book::boundary::{
        BoundaryBookLevel, BoundaryBookState, BoundaryContinuity, BoundarySnapshot,
        DepthBoundaryManifest, SnapshotCoverage,
    },
    domain::canonical::Exchange,
    error::{MarketForgeError, ResourceLimitError, Result},
    historical::limits::{self, LimitScope, NormalizationLimits},
};
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use std::{
    cell::{Cell, RefCell},
    collections::HashSet,
    fmt,
    fs::File,
    io::{self, BufReader, Read},
    path::Path,
    rc::Rc,
};

/// Explicit decode bounds, separate from a dataset's total boundary-file length.
#[derive(Debug, Clone, Copy)]
pub struct BoundaryReadLimits {
    pub max_sources: usize,
    pub max_source_bytes: u64,
    pub max_string_bytes: u64,
    pub max_book_levels: u64,
}
impl BoundaryReadLimits {
    /// Persisted identities come from prepared metadata, not input record bytes.
    /// Include their worst-case JSON escaping rather than silently imposing the
    /// decoder record limit on a legal symbol or stream ID.
    pub fn with_identity_bytes(mut self, bytes: u64) -> Result<Self> {
        let maximum = bytes
            .checked_mul(6)
            .ok_or_else(|| invalid("boundary identity bound overflow"))?
            .max(self.max_string_bytes);
        self.max_source_bytes = self
            .max_source_bytes
            .checked_add(
                maximum
                    .checked_sub(self.max_string_bytes)
                    .and_then(|n| n.checked_mul(8))
                    .ok_or_else(|| invalid("boundary identity bound overflow"))?,
            )
            .ok_or_else(|| invalid("boundary identity bound overflow"))?;
        self.max_string_bytes = maximum;
        Ok(self)
    }
    /// Conservative per-reader admission allowance: serialized-token/parser
    /// overlap, four bounded Vec capacities, allocator headroom, fixed I/O, and
    /// duplicate-index rehash overlap. This is planned capacity, not measured RSS.
    pub fn workspace_bytes(self) -> Result<u64> {
        self.max_source_bytes
            .checked_mul(8)
            .and_then(|n| {
                n.checked_add(
                    self.max_book_levels
                        .checked_mul(8)?
                        .checked_mul(std::mem::size_of::<BoundaryBookLevel>() as u64)?,
                )
            })
            .and_then(|n| n.checked_add((self.max_sources as u64).checked_mul(128)?))
            .and_then(|n| n.checked_add(1024 * 1024))
            .ok_or_else(|| invalid("boundary workspace bound overflow"))
    }
    pub fn for_sources(max_sources: usize, limits: NormalizationLimits) -> Result<Self> {
        limits.validate()?;
        // Four side arrays (initial/final bids/asks), decimal strings and field
        // syntax. This is a per-source serialized cap, not an all-source reserve.
        let max_source_bytes = limits
            .depth_levels
            .checked_mul(1024)
            .and_then(|n| n.checked_add(limits.record_bytes.checked_mul(8)?))
            .ok_or_else(|| invalid("boundary decode bound overflow"))?;
        Ok(Self {
            max_sources,
            max_source_bytes,
            max_string_bytes: limits.record_bytes,
            max_book_levels: limits.depth_levels,
        })
    }
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BoundaryReadSummary {
    pub sources: usize,
    pub total_rows: u64,
    pub total_events: u64,
    /// Largest source byte count consumed by the deserializer (not process RSS).
    pub peak_source_bytes: u64,
    pub peak_source_levels: usize,
}
fn invalid(s: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(s.into())
}
struct Control {
    remaining: Cell<u64>,
    consumed: Cell<u64>,
    error: RefCell<Option<ResourceLimitError>>,
    bounds: BoundaryReadLimits,
}
impl Control {
    fn reset(&self, count: u64) {
        self.remaining.set(count);
        self.consumed.set(0);
    }
    fn exceeded(&self, resource: &'static str, permitted: u64, requested: u64) -> io::Error {
        let error = limits::error(resource, permitted, requested, "depth boundary decoding");
        *self.error.borrow_mut() = Some(error.clone());
        io::Error::other(error)
    }
}
/// serde_json's reader consumes bytes incrementally. Bounds apply *before*
/// deserialization can grow a string/array; the BufReader below is fixed at 8 KiB.
struct GuardedRead<R> {
    inner: R,
    control: Rc<Control>,
    string: bool,
    escaped: bool,
    string_bytes: u64,
}
impl<R: Read> Read for GuardedRead<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.control.remaining.get() == 0 {
            return Err(self.control.exceeded(
                "boundary source bytes",
                self.control.bounds.max_source_bytes,
                self.control.consumed.get().saturating_add(1),
            ));
        }
        let n = self.inner.read(&mut out[..1])?;
        if n == 0 {
            return Ok(0);
        }
        self.control.remaining.set(self.control.remaining.get() - 1);
        self.control.consumed.set(self.control.consumed.get() + 1);
        let b = out[0];
        if self.string {
            if !self.escaped && b == b'"' {
                self.string = false;
            } else {
                self.string_bytes += 1;
                if self.string_bytes > self.control.bounds.max_string_bytes {
                    return Err(self.control.exceeded(
                        "boundary string bytes",
                        self.control.bounds.max_string_bytes,
                        self.string_bytes,
                    ));
                }
                if self.escaped {
                    self.escaped = false;
                } else if b == b'\\' {
                    self.escaped = true;
                }
            }
        } else if b == b'"' {
            self.string = true;
            self.string_bytes = 0;
        }
        Ok(1)
    }
}

// Wire types reject duplicate fields; V2 source/envelope reject unknown fields.
// The public persisted structs retain their existing serialization contract.
#[derive(Deserialize)]
struct Level {
    price: rust_decimal::Decimal,
    quantity_base: Option<rust_decimal::Decimal>,
    quantity_quote: Option<rust_decimal::Decimal>,
    quantity_contracts: Option<rust_decimal::Decimal>,
    order_count: Option<u64>,
}
#[derive(Deserialize)]
struct Coverage {
    depth_per_side: Option<u32>,
    bid_levels: u32,
    ask_levels: u32,
}
#[derive(Deserialize)]
struct Book {
    #[serde(deserialize_with = "levels")]
    bids: Vec<BoundaryBookLevel>,
    #[serde(deserialize_with = "levels")]
    asks: Vec<BoundaryBookLevel>,
}
fn levels<'de, D: de::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Vec<BoundaryBookLevel>, D::Error> {
    struct Levels;
    impl<'de> Visitor<'de> for Levels {
        type Value = Vec<BoundaryBookLevel>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a bounded book side")
        }
        fn visit_seq<A: SeqAccess<'de>>(
            self,
            mut a: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let maximum = limits::current()
                .expect("boundary reader owns a limit scope")
                .depth_levels as usize;
            let mut result = Vec::new();
            loop {
                // Check an excess element before decoding it or growing the Vec.
                if result.len() == maximum {
                    struct End;
                    impl<'de> DeserializeSeed<'de> for End {
                        type Value = ();
                        fn deserialize<D: de::Deserializer<'de>>(
                            self,
                            _: D,
                        ) -> std::result::Result<(), D::Error> {
                            Err(de::Error::custom("boundary book level limit exceeded"))
                        }
                    }
                    if a.next_element_seed(End)?.is_none() {
                        return Ok(result);
                    }
                    unreachable!();
                }
                let Some(value) = a.next_element::<Level>()? else {
                    return Ok(result);
                };
                if result.len() == result.capacity() {
                    let growth = result.capacity().max(128).min(maximum - result.capacity());
                    result
                        .try_reserve_exact(growth)
                        .map_err(de::Error::custom)?;
                }
                result.push(BoundaryBookLevel {
                    price: value.price,
                    quantity_base: value.quantity_base,
                    quantity_quote: value.quantity_quote,
                    quantity_contracts: value.quantity_contracts,
                    order_count: value.order_count,
                });
            }
        }
    }
    d.deserialize_seq(Levels)
}
#[derive(Deserialize)]
struct Snapshot {
    sequence: Option<u64>,
    timestamp_ns: i64,
    coverage: Coverage,
    state: Book,
    fingerprint: String,
}
impl Snapshot {
    fn convert(self) -> std::result::Result<BoundarySnapshot, String> {
        let count = self
            .state
            .bids
            .len()
            .checked_add(self.state.asks.len())
            .ok_or("boundary level count overflow")?;
        if count as u64 > limits::current().unwrap().depth_levels {
            return Err("boundary book level limit exceeded".into());
        }
        Ok(BoundarySnapshot {
            sequence: self.sequence,
            timestamp_ns: self.timestamp_ns,
            coverage: SnapshotCoverage {
                depth_per_side: self.coverage.depth_per_side,
                bid_levels: self.coverage.bid_levels,
                ask_levels: self.coverage.ask_levels,
            },
            state: BoundaryBookState {
                bids: self.state.bids,
                asks: self.state.asks,
            },
            fingerprint: self.fingerprint,
        })
    }
}
#[derive(Deserialize)]
struct Boundary {
    version: u32,
    exchange: Exchange,
    instrument_id: i64,
    symbol: String,
    stream_id: String,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    first_timestamp_ns: Option<i64>,
    last_timestamp_ns: Option<i64>,
    initial: Option<Snapshot>,
    final_state: Option<Snapshot>,
    continuity: BoundaryContinuity,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    task_id: u64,
    start_row: u64,
    end_row: u64,
    start_event: u64,
    end_event: u64,
    boundary: Boundary,
}
impl Source {
    fn convert(self) -> std::result::Result<DepthSourceBoundary, String> {
        let b = self.boundary;
        if b.version != 1 {
            return Err("unsupported nested boundary version".into());
        }
        Ok(DepthSourceBoundary {
            task_id: self.task_id,
            start_row: self.start_row,
            end_row: self.end_row,
            start_event: self.start_event,
            end_event: self.end_event,
            boundary: DepthBoundaryManifest {
                version: b.version,
                exchange: b.exchange,
                instrument_id: b.instrument_id,
                symbol: b.symbol,
                stream_id: b.stream_id,
                first_sequence: b.first_sequence,
                last_sequence: b.last_sequence,
                first_timestamp_ns: b.first_timestamp_ns,
                last_timestamp_ns: b.last_timestamp_ns,
                initial: b.initial.map(Snapshot::convert).transpose()?,
                final_state: b.final_state.map(Snapshot::convert).transpose()?,
                continuity: b.continuity,
            },
        })
    }
}
struct Sources<'a, F> {
    control: Rc<Control>,
    callback: &'a mut F,
    summary: &'a mut BoundaryReadSummary,
    callback_error: &'a mut Option<MarketForgeError>,
}
impl<'de, F: FnMut(DepthSourceBoundary) -> Result<()>> DeserializeSeed<'de> for Sources<'_, F> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        d.deserialize_seq(self)
    }
}
impl<'de, F: FnMut(DepthSourceBoundary) -> Result<()>> Visitor<'de> for Sources<'_, F> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("source boundaries")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> std::result::Result<(), A::Error> {
        let mut ids = HashSet::new();
        loop {
            self.control.reset(self.control.bounds.max_source_bytes);
            // Avoid decoding an extra source beyond the admitted compact inventory.
            struct Element {
                allowed: bool,
            }
            impl<'de> DeserializeSeed<'de> for Element {
                type Value = Source;
                fn deserialize<D: de::Deserializer<'de>>(
                    self,
                    d: D,
                ) -> std::result::Result<Source, D::Error> {
                    if !self.allowed {
                        return Err(de::Error::custom(
                            "boundary source inventory limit exceeded",
                        ));
                    }
                    Source::deserialize(d)
                }
            }
            let Some(source) = a.next_element_seed(Element {
                allowed: self.summary.sources < self.control.bounds.max_sources,
            })?
            else {
                break;
            };
            let source = source.convert().map_err(de::Error::custom)?;
            if source.start_row != self.summary.total_rows
                || source.start_event != self.summary.total_events
                || source.end_row < source.start_row
                || source.end_event < source.start_event
                || !ids.insert(source.task_id)
            {
                return Err(de::Error::custom(
                    "invalid depth source boundary ranges/identity",
                ));
            }
            self.summary.sources += 1;
            self.summary.total_rows = source.end_row;
            self.summary.total_events = source.end_event;
            self.summary.peak_source_bytes = self
                .summary
                .peak_source_bytes
                .max(self.control.consumed.get());
            let levels = source
                .boundary
                .initial
                .as_ref()
                .map_or(0, |b| b.state.level_count())
                + source
                    .boundary
                    .final_state
                    .as_ref()
                    .map_or(0, |b| b.state.level_count());
            self.summary.peak_source_levels = self.summary.peak_source_levels.max(levels);
            if let Err(e) = (self.callback)(source) {
                *self.callback_error = Some(e);
                return Err(de::Error::custom("boundary visitor failed"));
            }
        }
        self.control.reset(self.control.bounds.max_source_bytes);
        Ok(())
    }
}
struct Document<'a, F> {
    control: Rc<Control>,
    callback: &'a mut F,
    summary: &'a mut BoundaryReadSummary,
    callback_error: &'a mut Option<MarketForgeError>,
}
impl<'de, F: FnMut(DepthSourceBoundary) -> Result<()>> DeserializeSeed<'de> for Document<'_, F> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        d.deserialize_map(self)
    }
}
impl<'de, F: FnMut(DepthSourceBoundary) -> Result<()>> Visitor<'de> for Document<'_, F> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a V2 independent source boundary document")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> std::result::Result<(), A::Error> {
        let (mut version, mut continuity, mut sources) = (false, false, false);
        while let Some(key) = a.next_key::<String>()? {
            match key.as_str() {
                "version" if !version => {
                    version = true;
                    if a.next_value::<u32>()? != 2 {
                        return Err(de::Error::custom("unsupported stream boundary version"));
                    }
                }
                "continuity" if !continuity => {
                    continuity = true;
                    if a.next_value::<String>()? != "independent_sources" {
                        return Err(de::Error::custom("unsupported stream continuity"));
                    }
                }
                "sources" if !sources => {
                    sources = true;
                    a.next_value_seed(Sources {
                        control: self.control.clone(),
                        callback: self.callback,
                        summary: self.summary,
                        callback_error: self.callback_error,
                    })?;
                }
                _ => {
                    return Err(de::Error::custom(
                        "duplicate or unknown stream boundary field",
                    ));
                }
            }
        }
        if !(version && continuity && sources) {
            return Err(de::Error::custom("missing stream boundary field"));
        }
        Ok(())
    }
}
/// Callbacks are validation-only: they must not commit output. An error after a
/// callback (including an invalid document suffix) still rejects the document.
/// `max_sources` bounds the duplicate index; callers account for its compact O(N)
/// entries and one source's bounded decode workspace in their own admission.
pub fn visit_depth_stream_boundaries<F: FnMut(DepthSourceBoundary) -> Result<()>>(
    directory: &Path,
    bounds: BoundaryReadLimits,
    mut callback: F,
) -> Result<BoundaryReadSummary> {
    if bounds.max_source_bytes == 0 || bounds.max_string_bytes == 0 || bounds.max_book_levels == 0 {
        return Err(invalid("boundary decode limits must be positive"));
    }
    // Validate accounting arithmetic before any reader or duplicate-index allocation.
    bounds.workspace_bytes()?;
    let mut active = limits::current().unwrap_or_default();
    active.depth_levels = bounds.max_book_levels;
    active.validate()?;
    let _scope = LimitScope::enter(active, format!("boundary={}", directory.display()));
    let control = Rc::new(Control {
        remaining: Cell::new(bounds.max_source_bytes),
        consumed: Cell::new(0),
        error: RefCell::new(None),
        bounds,
    });
    let file = File::open(directory.join("boundary.json"))
        .map_err(|e| invalid(format!("boundary open: {e}")))?;
    let read = GuardedRead {
        inner: BufReader::with_capacity(8192, file),
        control: control.clone(),
        string: false,
        escaped: false,
        string_bytes: 0,
    };
    let mut de = serde_json::Deserializer::from_reader(read);
    let mut summary = BoundaryReadSummary::default();
    let mut callback_error = None;
    let result = Document {
        control: control.clone(),
        callback: &mut callback,
        summary: &mut summary,
        callback_error: &mut callback_error,
    }
    .deserialize(&mut de)
    .and_then(|()| de.end());
    if let Some(e) = callback_error {
        return Err(e);
    }
    if let Some(e) = control.error.borrow_mut().take() {
        return Err(e.into());
    }
    result.map_err(|e| invalid(format!("invalid stream boundary document: {e}")))?;
    Ok(summary)
}
/// Compatibility collection is explicitly bounded. Production normalization uses
/// the visitor with its exact expected source count and never calls this reader.
pub fn collect_depth_stream_boundaries(
    directory: &Path,
    bounds: BoundaryReadLimits,
) -> Result<DepthStreamBoundaries> {
    let mut sources = Vec::new();
    visit_depth_stream_boundaries(directory, bounds, |s| {
        sources.push(s);
        Ok(())
    })?;
    Ok(DepthStreamBoundaries {
        version: 2,
        continuity: "independent_sources".into(),
        sources,
    })
}
