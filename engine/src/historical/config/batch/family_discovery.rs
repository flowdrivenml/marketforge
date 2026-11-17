//! Bounded offline discovery. Exact catalog entries remain authoritative; missing
//! entries inherit only the explicitly asserted family specifications.
use super::*;
use crate::historical::config::CompiledProfile;
use crate::shared::{
    historical::limits,
    io::{
        decode::CsvDecoder,
        source::{JsonlDecoder, with_validated_source_members_accounted},
    },
};

/// Version 1 allocates in the upper positive quarter of i64. Existing exact
/// mappings always win; allocation collisions are errors, never probing/renumbering.
fn allocated_id(namespace: &str, instrument: &OfflineInstrument) -> Result<i64> {
    let hash = fingerprint(&json!([
        "marketforge-concrete-id-sha256-62-v1",
        namespace,
        identity(instrument)?
    ]))?;
    let prefix = u64::from_str_radix(&hash[..16], 16).map_err(|e| invalid(e.to_string()))?;
    Ok(((prefix & ((1u64 << 62) - 1)) | (1u64 << 62)) as i64)
}
/// Supported source naming conventions, anchored at a complete symbol and a
/// recognized payload suffix. Arbitrary substring matches are never identities.
pub(crate) fn filename_symbol(exchange: Exchange, name: &str) -> Option<String> {
    let name = Path::new(name).file_name()?.to_str()?;
    fn valid_date(value: &str) -> bool {
        value.len() == 10
            && value.as_bytes()[4] == b'-'
            && value.as_bytes()[7] == b'-'
            && value
                .bytes()
                .enumerate()
                .all(|(i, b)| matches!(i, 4 | 7) || b.is_ascii_digit())
            && value
                .get(5..7)
                .and_then(|v| v.parse::<u8>().ok())
                .is_some_and(|v| (1..=12).contains(&v))
            && value
                .get(8..10)
                .and_then(|v| v.parse::<u8>().ok())
                .is_some_and(|v| (1..=31).contains(&v))
    }
    let symbol = match exchange {
        Exchange::Binance => {
            let (symbol, suffix) = name.split_once("-trades-")?;
            let date = suffix
                .strip_suffix(".csv")
                .or_else(|| suffix.strip_suffix(".zip"))?;
            if !valid_date(date) {
                return None;
            }
            symbol
        }
        Exchange::Okx => {
            if let Some((symbol, suffix)) = name.split_once("-L2orderbook-") {
                let (levels, date) = suffix.split_once("lv-")?;
                if levels.is_empty()
                    || !levels.bytes().all(|b| b.is_ascii_digit())
                    || !valid_date(date.strip_suffix(".data")?)
                {
                    return None;
                }
                symbol
            } else {
                let (symbol, suffix) = name.split_once("-trades-")?;
                let date = suffix
                    .strip_suffix(".csv")
                    .or_else(|| suffix.strip_suffix(".zip"))?;
                if !valid_date(date) {
                    return None;
                }
                symbol
            }
        }
        Exchange::Bybit => {
            let stem = name
                .strip_suffix(".csv.gz")
                .or_else(|| name.strip_suffix(".csv"))?;
            let split = stem.len().checked_sub(10)?;
            let date = stem.get(split..)?;
            if date.len() != 10
                || date.as_bytes()[4] != b'-'
                || date.as_bytes()[7] != b'-'
                || !date
                    .bytes()
                    .enumerate()
                    .all(|(i, b)| matches!(i, 4 | 7) || b.is_ascii_digit())
            {
                return None;
            }
            stem.get(..split)?
        }
        _ => return None,
    };
    family_of(exchange, InstrumentKind::Future, symbol)?;
    Some(symbol.into())
}
fn csv_error(path: &Path, error: csv::Error) -> crate::shared::error::MarketForgeError {
    match error.into_kind() {
        csv::ErrorKind::Io(error) => limits::source_error(path.into(), error),
        error => invalid(format!(
            "family discovery CSV {}: {error:?}",
            path.display()
        )),
    }
}
fn bounded_add(
    symbols: &mut BTreeSet<String>,
    symbol: &str,
    bytes: &mut u64,
    quota: &crate::historical::normalize::reconcile::Quota,
) -> Result<()> {
    if symbols.contains(symbol) {
        return Ok(());
    }
    let requested = bytes
        .checked_add(symbol.len() as u64 + 512)
        .ok_or_else(|| invalid("family discovery inventory overflow"))?;
    if let Some(l) = limits::current() {
        limits::check(
            "family concrete instruments",
            l.metric_entries,
            symbols.len() as u64 + 1,
            "family discovery",
        )?;
        limits::check(
            "family discovery bytes",
            l.reconciliation_bytes,
            requested,
            "family discovery",
        )?;
    }
    quota.charge(requested - *bytes, "family discovery symbol inventory")?;
    symbols.insert(symbol.into());
    *bytes = requested;
    Ok(())
}
pub(super) fn bindings(
    batch: &PreparedBatch,
    task: &PreparedTask,
    profile: &CompiledProfile,
    root: &Path,
    job_id: u64,
) -> Result<Vec<(OfflineInstrument, Option<String>)>> {
    let scope = task
        .scope
        .as_ref()
        .ok_or_else(|| invalid("missing family scope"))?;
    let family = batch
        .families
        .iter()
        .find(|f| Some(f.family_id) == scope.family_id)
        .ok_or_else(|| invalid("unknown family reference"))?;
    let selection = task
        .instrument_selection
        .as_ref()
        .ok_or_else(|| invalid("family requires explicit selection"))?;
    if let InstrumentSelection::Include { symbols } = selection {
        for symbol in symbols {
            if family_of(family.exchange, family.instrument_type, symbol)
                != Some(family.identifier.as_str())
            {
                return Err(invalid(format!(
                    "selected symbol {symbol} is outside family scope {}",
                    family.identifier
                )));
            }
        }
    }
    let input = resolve_path(root, &task.input_path);
    let outer = input
        .file_name()
        .and_then(|s| s.to_str())
        .and_then(|s| filename_symbol(family.exchange, s));
    let mut discovered = BTreeMap::<Option<String>, BTreeSet<String>>::new();
    let mut bytes = 0u64;
    let quota = crate::historical::normalize::reconcile::Quota::new(
        limits::current().unwrap_or_default().reconciliation_bytes,
    );
    let mut inventory_lease = None;
    let raw = &profile.physical.raw_schema;
    let is_csv = raw["format"].as_str() == Some("csv");
    let is_json = raw["format"].as_str() == Some("jsonl");
    // Physical profiles use record_format in some legacy catalogs; inspect the
    // declared record format instead of guessing a decoder from filenames.
    let csv = is_csv
        || matches!(
            profile.physical.container_format,
            super::super::PhysicalRecord::Csv
        );
    let json = is_json
        || matches!(
            profile.physical.container_format,
            super::super::PhysicalRecord::Jsonl
        );
    let reporter = crate::historical::normalize::progress::preparation::reporter();
    let mut decoded_bytes = 0u64;
    let mut next_bytes = 0u64;
    with_validated_source_members_accounted(
        &input,
        profile.physical.supported_containers[0],
        |bytes| {
            drop(inventory_lease.take());
            inventory_lease = Some(quota.reserve(bytes, "family discovery archive inventory")?);
            Ok(())
        },
        |member, raw_reader| {
            let mut counted = crate::historical::normalize::progress::preparation::Reader {
                input: raw_reader,
                reporter: reporter.clone(),
                job: job_id,
                stream: &task.stream_id.0,
                bytes: &mut decoded_bytes,
                next: &mut next_bytes,
            };
            let reader: &mut dyn std::io::Read = &mut counted;
            let named = member.and_then(|m| filename_symbol(family.exchange, m));
            if let (Some(a), Some(b)) = (&outer, &named)
                && a != b
            {
                return Err(invalid(format!(
                    "source/member identity conflict: {a} versus {b}"
                )));
            }
            let hint = named.as_ref().or(outer.as_ref());
            let key = member.map(str::to_owned);
            let mut symbols = BTreeSet::new();
            let mut observe = |symbol: &str| -> Result<()> {
                if hint.is_some_and(|hint| hint != symbol) {
                    return Err(invalid(format!(
                        "filename/record identity conflict in {} member {member:?}: expected {hint:?}, observed {symbol}",
                        input.display()
                    )));
                }
                let actual = family_of(family.exchange, family.instrument_type, symbol)
                    .ok_or_else(|| {
                        invalid(format!("unsupported concrete futures symbol {symbol}"))
                    })?;
                if actual == family.identifier && selection.selects(symbol) {
                    bounded_add(&mut symbols, symbol, &mut bytes, &quota)?;
                }
                Ok(())
            };
            if csv {
                let has_headers = raw["header"].as_bool().unwrap_or(false);
                let mut decoder = CsvDecoder::with_headers(&mut *reader, has_headers);
                let actual = if has_headers {
                    Some(decoder.headers().map_err(|e| csv_error(&input, e))?)
                } else {
                    None
                };
                let headers =
                    crate::shared::formats::trades::resolve_trade_headers(raw, actual.as_ref())?;
                let native = crate::shared::formats::trades::resolve_identity_column(&headers).ok();
                if let Some(native) = native {
                    for record in decoder.records() {
                        let record = record.map_err(|e| csv_error(&input, e))?;
                        let symbol = std::str::from_utf8(
                            record
                                .get(native)
                                .ok_or_else(|| invalid("missing native symbol"))?,
                        )
                        .map_err(|e| invalid(e.to_string()))?;
                        observe(symbol)?;
                    }
                } else {
                    observe(hint.ok_or_else(|| {
                        invalid(
                            "family source lacks a supported filename and native record identity",
                        )
                    })?)?;
                }
            } else if json {
                let native = profile
                    .normalizations
                    .iter()
                    .find_map(|n| n.rules["instrument"]["source"].as_str())
                    .ok_or_else(|| {
                        invalid("family JSON profile lacks native instrument identity")
                    })?;
                let mut decoder = JsonlDecoder::new(&mut *reader);
                while let Some(value) = decoder.next_json().map_err(|e| match e {
                    crate::shared::io::source::JsonlError::Source(e) => {
                        limits::source_error(input.clone(), e)
                    }
                    crate::shared::io::source::JsonlError::Record(e) => {
                        invalid(format!("family discovery JSON {}: {e}", input.display()))
                    }
                })? {
                    let symbol = crate::shared::formats::depth::resolve_json_path(&value, native)
                        .and_then(Value::as_str)
                        .ok_or_else(|| invalid("family record lacks native symbol"))?;
                    observe(symbol)?;
                }
            } else {
                return Err(invalid("unsupported family source format"));
            }
            if let InstrumentSelection::Include { symbols: requested } = selection {
                // An exact selection may be absent from one source. Empty routed
                // candidates preserve that source's boundary and observation counts.
                for symbol in requested {
                    bounded_add(&mut symbols, symbol, &mut bytes, &quota)?;
                }
            }
            std::io::copy(reader, &mut std::io::sink())
                .map_err(|source| limits::source_error(input.clone(), source))?;
            if !symbols.is_empty() {
                let key_bytes = key.as_ref().map_or(0, |k| k.len() as u64) + 256;
                quota.charge(key_bytes, "family discovery member bindings")?;
                bytes = bytes
                    .checked_add(key_bytes)
                    .ok_or_else(|| invalid("family member inventory overflow"))?;
                if let Some(l) = limits::current() {
                    limits::check(
                        "family discovery bytes",
                        l.reconciliation_bytes,
                        bytes,
                        "family member discovery",
                    )?;
                }
                discovered.insert(key, symbols);
            }
            Ok(())
        },
    )?;
    drop(inventory_lease);
    let mut resolved = Vec::new();
    let mut identities = BTreeMap::<i64, String>::new();
    for instrument in &batch.instruments {
        let semantic = identity(instrument)?;
        quota.charge(
            semantic.capacity() as u64 + 512,
            "family supplied identity inventory",
        )?;
        identities.insert(instrument.instrument_id, semantic);
    }
    for (member, symbols) in discovered {
        for symbol in symbols {
            let context_bytes = (symbol.len() as u64
                + member.as_ref().map_or(0, |m| m.len() as u64)
                + family.base_asset.len() as u64
                + family.quote_asset.len() as u64
                + family
                    .instrument
                    .contract_value_asset
                    .as_ref()
                    .map_or(0, |s| s.len() as u64))
            .checked_mul(16)
            .and_then(|n| n.checked_add(2048))
            .ok_or_else(|| invalid("family context accounting overflow"))?;
            quota.charge(context_bytes, "family discovered concrete contexts")?;
            let mut instrument = OfflineInstrument {
                instrument_id: 0,
                exchange: family.exchange,
                instrument_type: family.instrument_type,
                market_category: family.market_category,
                symbol,
                base_asset: family.base_asset.clone(),
                quote_asset: family.quote_asset.clone(),
                instrument: family.instrument.clone(),
            };
            let semantic = identity(&instrument)?;
            if let Some(exact) = batch
                .instruments
                .iter()
                .find(|i| identity(i).is_ok_and(|id| id == semantic))
            {
                if exact.instrument != instrument.instrument
                    || exact.base_asset != instrument.base_asset
                    || exact.quote_asset != instrument.quote_asset
                {
                    return Err(invalid(format!(
                        "exact concrete metadata conflicts with family assertion for {}",
                        instrument.symbol
                    )));
                }
                instrument = exact.clone();
            } else {
                instrument.instrument_id = allocated_id(&batch.instrument_namespace, &instrument)?;
            }
            if identities
                .insert(instrument.instrument_id, semantic.clone())
                .is_some_and(|old| old != semantic)
            {
                return Err(invalid(
                    "concrete instrument ID allocation collision with namespace authority",
                ));
            }
            if let Some(l) = limits::current() {
                limits::check(
                    "family member bindings",
                    l.metric_entries,
                    resolved.len() as u64 + 1,
                    "family discovery",
                )?;
            }
            resolved.push((instrument, member.clone()));
        }
    }
    if resolved.is_empty() {
        return Err(invalid(format!(
            "no eligible concrete instruments discovered for family {} in {}",
            family.identifier,
            input.display()
        )));
    }
    Ok(resolved)
}
