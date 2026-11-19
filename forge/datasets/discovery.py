"""Read-only discovery using manifest inventory and exact, evidenced file membership."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, cast

from .dataset import Dataset, Stream
from .metadata import content
from .parquet import Footer, Predicate, footer


@dataclass(frozen=True)
class Diagnostic:
    path: Path
    code: str
    message: str


class DiscoveryError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def is_staging(path: Path) -> bool:
    return any(
        part == "staging"
        or part.startswith(".staging")
        or ".staging." in part
        or part.endswith(".staging")
        for part in path.parts
    )


def read_object(path: Path) -> dict[str, Any]:
    data: Any = json.loads(path.read_bytes())
    if not isinstance(data, dict):
        raise ValueError("Expected a JSON object.")
    return cast(dict[str, Any], data)


def _range(values: Mapping[str, Any]) -> tuple[int | None, int | None]:
    first, last = values.get("start_timestamp_ns"), values.get("end_timestamp_ns")
    if first is None and last is None:
        return None, None
    if type(first) is not int or type(last) is not int or not -(2**63) <= first <= last < 2**63:
        raise ValueError("Invalid observed timestamp envelope.")
    return first, last


def _primary(root: Path, value: str) -> Path:
    path = root / value
    if Path(value).is_absolute() or ".." in Path(value).parts or path.suffix != ".parquet":
        raise ValueError(f"Invalid primary path {value!r}.")
    if not path.resolve().is_relative_to(root) or path.resolve() != path:
        raise ValueError(f"Primary path escapes or aliases the publication: {value!r}.")
    return path


def _constant(item: Footer, name: str, declared: Any = None) -> Any:
    values: list[Any] = []
    for column in item.chunks(name):
        if column is None:
            continue
        stat = column.statistics
        if stat is None or not stat.has_min_max:
            continue
        lo, hi = stat.min, stat.max
        if column.physical_type in {"BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"} and isinstance(
            lo, (str, bytes)
        ):
            # Parquet can store outward-truncated byte-array bounds. A declared
            # string inside those bounds remains consistent, even if lo != hi.
            if declared is not None and not lo <= declared <= hi:
                raise DiscoveryError(
                    "conflicting_metadata", f"Declared {name} is outside footer bounds."
                )
            if lo != hi:
                continue
        if lo != hi or values and lo != values[0] or declared is not None and lo != declared:
            raise DiscoveryError("conflicting_metadata", f"Conflicting footer identity for {name}.")
        values.append(lo)
    return declared if declared is not None else values[0] if values else None


def _ranks(item: Footer) -> tuple[set[int], bool]:
    """A min/max pair proves its endpoints, but no intervening rank."""
    known: set[int] = set()
    possible: list[tuple[int, int]] = []
    incomplete = False
    for column in item.chunks("stream_rank"):
        stat = column.statistics if column is not None else None
        if stat is None or not stat.has_min_max or stat.null_count != 0:
            incomplete = True
            continue
        known.update((stat.min, stat.max))
        possible.append((stat.min, stat.max))
    complete = not incomplete and all(
        sum(lo <= rank <= hi for rank in known) == hi - lo + 1 for lo, hi in possible
    )
    return known, complete or item.metadata.num_rows == 0


def _declared_identity(value: Mapping[str, Any]) -> dict[str, object]:
    return {
        key: value[key]
        for key in (
            "exchange",
            "symbol",
            "instrument_id",
            "stream_id",
            "instrument_type",
            "market_category",
            "base_asset",
            "quote_asset",
            "stream_rank",
            "instrument_namespace",
        )
        if value.get(key) is not None
    }


def _combine_identity(*records: Mapping[str, Any]) -> dict[str, object]:
    merged: dict[str, object] = {}
    for record in records:
        for key, value in _declared_identity(record).items():
            if key in merged and merged[key] != value:
                raise DiscoveryError("conflicting_metadata", f"Conflicting evidence for {key}.")
            merged[key] = value
    return merged


def _footer_extent(item: Footer) -> tuple[int | None, int | None]:
    if "event_timestamp_ns" not in item.schema.names:
        return None, None
    dtype = item.schema.field("event_timestamp_ns").type
    if str(dtype) == "int64" or str(dtype).startswith("timestamp[ns"):
        return item.extent("event_timestamp_ns")
    return None, None


def _registered_summary(row: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if row is None:
        return {}
    return row.get("summary") or row.get("operation_details") or {}


def _normalize_stream(
    entry: Mapping[str, Any],
    item: Footer,
    manifest: Mapping[str, Any],
    job: Mapping[str, Any],
    registered: Mapping[str, Any],
    kind: str,
    directories: Mapping[str, Mapping[str, Any]],
    instruments: Mapping[int, Mapping[str, Any]],
) -> tuple[Stream, ...]:
    relative = Path(entry["path"])
    declared = directories.get(str(relative.parent), {})
    tasks: list[Mapping[str, Any]] = []
    if relative.parts[0] == "tasks" and len(relative.parts) > 2:
        tasks = [t for t in job.get("tasks", []) if str(t.get("task_id")) == relative.parts[1]]
    registered_members = registered.get("memberships", [])
    if len(registered_members) > 1 or len(tasks) > 1:
        raise DiscoveryError(
            "conflicting_metadata", "Normalized physical file has ambiguous stream ownership."
        )
    task: Mapping[str, Any] = tasks[0] if tasks else {}
    member: Mapping[str, Any] = registered_members[0] if registered_members else {}
    identity = _combine_identity(declared, task, member)
    instrument_id = _constant(item, "instrument_id", identity.get("instrument_id"))
    instrument: Mapping[str, Any] = instruments.get(instrument_id, {})
    identity = _combine_identity(
        identity,
        instrument,
        {
            "instrument_id": instrument_id,
            "instrument_namespace": manifest.get("instrument_namespace"),
        },
    )
    constants = {
        key: _constant(item, key, identity.get(key)) for key in ("stream_id", "exchange", "symbol")
    }
    identity = _combine_identity(identity, constants)
    if not identity.get("stream_id") or not identity.get("exchange") or not identity.get("symbol"):
        return ()
    fingerprint = (
        relative.parts[1]
        if len(relative.parts) >= 4 and relative.parts[0] == "streams" and declared
        else None
    )
    return (
        Stream(
            (item.path,),
            fingerprint,
            kind,
            item.path,
            identity,
            (item,),
            (),
            _range(entry),
            "registry file summary" if member else "normalized manifest / footer",
            item.metadata.num_rows,
        ),
    )


def _merged_streams(
    item: Footer,
    definitions: Mapping[int, Mapping[str, Any]],
    registered: Mapping[str, Any],
    kind: str,
    observed: tuple[int | None, int | None],
) -> tuple[tuple[Stream, ...], bool]:
    if "stream_rank" not in item.schema.names:
        return (), False
    ranks, complete = _ranks(item)
    summaries: dict[int, Mapping[str, Any]] = {}
    if registered.get("membership_scope") == "file":
        for member in registered.get("memberships", []):
            matches = [
                rank
                for rank, stream in definitions.items()
                if all(
                    member.get(key) is None or stream.get(key) == member[key]
                    for key in ("stream_rank", "stream_id", "exchange", "symbol", "instrument_id")
                )
                and (member.get("kind") is None or content(member["kind"]) == stream["kind"])
            ]
            if len(matches) == 1:
                summaries[matches[0]] = member
            elif not matches:
                raise DiscoveryError(
                    "conflicting_metadata", "Registered membership disagrees with merge manifest."
                )
        if len(summaries) == len(registered.get("memberships", [])) and summaries:
            if not ranks <= summaries.keys() or (complete and ranks != summaries.keys()):
                raise DiscoveryError(
                    "conflicting_metadata", "Registered memberships disagree with footer ranks."
                )
            ranks, complete = set(summaries), True
    if not ranks <= definitions.keys():
        raise DiscoveryError(
            "conflicting_metadata", "Footer contains a rank absent from the merge manifest."
        )
    streams: list[Stream] = []
    for rank in sorted(ranks):
        definition = definitions[rank]
        member = summaries.get(rank, {})
        identity = _combine_identity(definition, member)
        dtype = definition["kind"]
        selector: list[Predicate] = [("stream_rank", "=", rank)]
        if kind == "combined":
            if "event_kind" not in item.schema.names:
                raise DiscoveryError(
                    "conflicting_metadata", "Combined payload is missing event_kind."
                )
            selector.append(("event_kind", "=", 0 if dtype == "trades" else 1))
        single = complete and len(ranks) == 1
        streams.append(
            Stream(
                (item.path,),
                None,
                dtype,
                item.path,
                identity,
                (item,),
                tuple(selector),
                _range(member) if member else observed if single else (None, None),
                "registry file summary" if member else "merge manifest + footer rank statistics",
                item.metadata.num_rows if single and kind != "combined" else None,
            )
        )
    return tuple(streams), complete


def _merge_definitions(
    root: Path, manifest: Mapping[str, Any], kind: str
) -> dict[int, Mapping[str, Any]]:
    definitions: dict[int, Mapping[str, Any]] = {}
    parents: dict[Any, dict[int, Any]] = {}
    for parent in manifest.get("input_datasets", []):
        path = root / parent["dataset_path"]
        try:
            source = read_object(path / "manifest.json")
            if (
                source.get("dataset_id") == parent["dataset_id"]
                and source.get("protocol_version") == parent["manifest_version"]
            ):
                parents[parent["dataset_id"]] = {
                    i["instrument"]["instrument_id"]: {
                        **i["instrument"],
                        "instrument_namespace": source.get("instrument_namespace"),
                    }
                    for i in source.get("resolved_instruments", [])
                }
        except (OSError, ValueError, TypeError, KeyError):
            continue
    for stream in manifest["streams"]:
        rank = stream["stream_rank"]
        if type(rank) is not int or rank < 0 or rank in definitions:
            raise ValueError("Invalid or repeated merge stream rank.")
        stored = _declared_identity(stream)
        source = _declared_identity(
            parents.get(stream["dataset_id"], {}).get(stream["instrument_id"], {})
        )
        ambiguous = sorted(
            key for key in stored.keys() & source.keys() if stored[key] != source[key]
        )
        identity = {key: value for key, value in (stored | source).items() if key not in ambiguous}
        definitions[rank] = {
            **identity,
            "kind": content(stream.get("kind", kind)),
            "conflict": f"Rank {rank} has conflicting parent and merge identity for {', '.join(ambiguous)}."
            if ambiguous
            else None,
        }
    return definitions


def inventory(
    root: Path,
    *,
    registry: Mapping[Path, Mapping[str, Any]] | None = None,
    only: Path | None = None,
) -> tuple[list[Dataset], list[Diagnostic]]:
    """Open each primary footer once and retain per-file diagnostics."""
    diagnostics: list[Diagnostic] = []
    datasets: list[Dataset] = []
    registry = registry or {}
    path = root / "manifest.json"
    try:
        manifest = read_object(path)
        normalized = "content_type" in manifest and "dataset_id" in manifest
        merge = "mode" in manifest and "job_id" in manifest
        version = manifest["protocol_version"]
        supported = type(version) is int and (
            normalized
            and version in {3, 4, 5}
            or merge
            and (
                version == 1
                and manifest.get("physical_schema_version", 1) == 1
                or version == 2
                and manifest.get("timeline_schema_version") == 1
            )
        )
        if not supported:
            if normalized or merge:
                raise DiscoveryError(
                    "unsupported_version", f"Unsupported manifest or schema version {version}."
                )
            raise ValueError("Manifest has no recognizable normalization or merge contract.")
        kind = content(manifest["content_type" if normalized else "mode"])
        definitions = _merge_definitions(root, manifest, kind) if merge else {}
        directories = {
            stream["directory"]: stream
            for stream in manifest.get("streams", [])
            if "directory" in stream
        }
        instruments = {
            entry["instrument"]["instrument_id"]: entry["instrument"]
            for entry in manifest.get("resolved_instruments", [])
        }
        raw_entries = manifest["files"]
        if not isinstance(raw_entries, list) or any(
            not isinstance(entry, dict) for entry in cast(list[Any], raw_entries)
        ):
            raise ValueError("Expected primary file entries.")
        entries = cast(list[dict[str, Any]], raw_entries)
        names = [entry["path"] for entry in entries]
        if any(not isinstance(name, str) or not name for name in names):
            raise ValueError("Primary file paths must be nonempty strings.")
        if len(set(names)) != len(names):
            raise ValueError("Duplicate primary file paths.")
        requested: tuple[int, int] | None = None
        if "time_range" in manifest:
            time_range = manifest["time_range"]
            start, end = time_range["start_ns"], time_range["end_ns"]
            if (
                type(start) is not int
                or type(end) is not int
                or not -(2**63) <= start < end < 2**63
            ):
                raise ValueError("Invalid requested half-open timestamp interval.")
            requested = start, end

    except (OSError, UnicodeError, ValueError, KeyError, TypeError, AttributeError) as error:
        code = (
            error.code
            if isinstance(error, DiscoveryError)
            else "unreadable_manifest"
            if isinstance(error, OSError)
            else "malformed_manifest"
        )
        return [], [Diagnostic(path, code, str(error))]

    job: dict[str, Any] = {}
    if normalized and any(str(name).startswith("tasks/") for name in names):
        try:
            job = read_object(root / "processing-job.json")
        except (OSError, ValueError) as error:
            diagnostics.append(
                Diagnostic(root / "processing-job.json", "missing_identity", str(error))
            )
    stage = is_staging(root)
    for entry in entries:
        file = root / str(entry["path"])
        if only is not None and file != only:
            continue
        try:
            file = _primary(root, entry["path"])
            item = footer(file)
        except (OSError, ValueError, TypeError) as error:
            diagnostics.append(Diagnostic(file, "unreadable_file", str(error)))
            continue
        row = registry.get(file)
        recorded: Mapping[str, Any] = _registered_summary(row)
        conflicts: list[str] = []
        try:
            declared = _range(entry)
        except ValueError as error:
            declared = (None, None)
            conflicts.append(str(error))
        from_footer = _footer_extent(item)
        if from_footer[0] is not None and declared[0] is not None and from_footer != declared:
            conflicts.append("Manifest and footer event-time envelopes disagree.")
        if (entry.get("rows") is not None and entry["rows"] != item.metadata.num_rows) or entry.get(
            "size_bytes"
        ) != item.size:
            conflicts.append("Manifest and footer row count or file size disagree.")
        if row is not None and (
            row.get("primary_file") != entry["path"]
            or str(row.get("engine_dataset_id"))
            != str(manifest["dataset_id" if normalized else "job_id"])
            or (
                normalized
                and row.get("instrument_namespace") != manifest.get("instrument_namespace")
            )
        ):
            conflicts.append("Registry and manifest publication identity disagree.")
            row = None
            recorded = _registered_summary(None)
        observed = from_footer if from_footer[0] is not None else declared
        try:
            if normalized:
                streams = _normalize_stream(
                    entry, item, manifest, job, recorded, kind, directories, instruments
                )
                complete = bool(streams)
            else:
                streams, complete = _merged_streams(item, definitions, recorded, kind, observed)
        except (ValueError, KeyError, TypeError) as error:
            streams, complete = (), False
            conflicts.append(str(error))
        if conflicts:
            diagnostics.append(Diagnostic(file, "conflicting_metadata", " ".join(conflicts)))
            streams, complete, observed = (), False, from_footer
        elif not complete:
            diagnostics.append(
                Diagnostic(
                    file,
                    "missing_identity",
                    "Complete file membership is unavailable from stored evidence.",
                )
            )
        rank_conflicts = [
            str(issue)
            for stream in streams
            if isinstance((rank := (stream.identity or {}).get("stream_rank")), int)
            and (issue := definitions.get(rank, {}).get("conflict"))
        ]
        if rank_conflicts:
            diagnostics.append(Diagnostic(file, "conflicting_metadata", " ".join(rank_conflicts)))
            conflicts.extend(rank_conflicts)
        datasets.append(
            Dataset(
                file,
                streams,
                "staging" if stage else row.get("status", "committed") if row else "committed",
                " ".join(conflicts)
                or ("Complete file membership is unavailable" if not complete else None),
                (item,),
                root,
                entry["path"],
                row.get("id") if row else None,
                row.get("registry_context") if row else None,
                row.get("publication_id") if row else None,
                manifest["dataset_id" if normalized else "job_id"],
                manifest.get("instrument_namespace")
                if normalized
                else row.get("instrument_namespace")
                if row
                else None,
                "normalize" if normalized else "merge",
                kind,
                recorded.get("integrity", manifest.get("integrity_status")),
                recorded.get(
                    "integrity_scope", "publication" if manifest.get("integrity_status") else None
                ),
                "conflicting"
                if conflicts
                else "registry file summary"
                if row and complete
                else "manifest / footer"
                if complete
                else "partial membership"
                if streams
                else "unknown membership",
                observed,
                "unknown"
                if observed[0] is None
                else "footer"
                if from_footer[0] is not None
                else "manifest",
                requested,
            )
        )
    return datasets, diagnostics


def standalone(path: Path) -> Dataset:
    item = footer(path)
    stage = is_staging(path)
    extent = _footer_extent(item)
    return Dataset(
        path,
        (),
        "staging" if stage else "unverified",
        "No manifest-backed market identity",
        (item,),
        metadata_scope="footer only",
        observed=extent,
        timestamp_evidence="footer" if extent[0] is not None else "unknown",
    )


def group(root: Path, files: list[Dataset]) -> Dataset:
    groups: dict[tuple[Any, ...], Stream] = {}
    all_footers = tuple(item for dataset in files for item in dataset.footers)
    for dataset in files:
        for stream in dataset.streams_by_key:
            key = (
                (stream.identity or {}).get("stream_id"),
                stream.fingerprint,
                stream.kind,
                stream.predicates,
            )
            old = groups.get(key)
            if old is None:
                groups[key] = replace(stream, dataset_path=root)
            else:
                old_first, old_last = old.observed
                new_first, new_last = stream.observed
                first = (
                    min(old_first, new_first)
                    if old_first is not None and new_first is not None
                    else None
                )
                last = (
                    max(old_last, new_last)
                    if old_last is not None and new_last is not None
                    else None
                )
                groups[key] = replace(
                    old,
                    files=old.files + stream.files,
                    footers=old.footers + stream.footers,
                    observed=(first, last),
                    exact_rows=old.exact_rows + stream.exact_rows
                    if old.exact_rows is not None and stream.exact_rows is not None
                    else None,
                )
    seed = files[0]
    observed: tuple[int | None, int | None] = (
        min((item.observed[0] for item in files if item.observed[0] is not None), default=None),
        max((item.observed[1] for item in files if item.observed[1] is not None), default=None),
    )
    return replace(
        seed,
        path=root,
        primary_file=None,
        dataset_id=None,
        streams_by_key=tuple(groups.values()),
        footers=all_footers,
        metadata_scope="publication grouping" if seed.publication_path else "directory grouping",
        observed=observed,
        timestamp_evidence="known file envelopes" if observed[0] is not None else "unknown",
    )


def open_dataset(path: str | Path, *, allow_staging: bool = False) -> Dataset:
    source = Path(path).expanduser().resolve()
    if not source.exists():
        raise FileNotFoundError(source)
    if is_staging(source) and not allow_staging:
        raise ValueError("Staging output is uncommitted. Pass allow_staging=True explicitly.")
    if source.is_file() and source.suffix != ".parquet":
        raise ValueError("Expected a primary Parquet file or publication directory.")
    candidates = (source, *source.parents) if source.is_dir() else source.parents
    for root in candidates:
        if (root / "manifest.json").is_file():
            files, diagnostics = inventory(root, only=source if source.is_file() else None)
            if source == root and files:
                return group(root, files)
            if found := next((item for item in files if item.path == source), None):
                return found
            raise ValueError(
                next(
                    (
                        str(d.message)
                        for d in diagnostics
                        if d.path in {source, root / "manifest.json"}
                    ),
                    "Path is not a primary manifest member.",
                )
            )
    if source.is_file():
        return standalone(source)
    files = [
        standalone(path)
        for path in sorted(source.rglob("*.parquet"))
        if allow_staging or not is_staging(path)
    ]
    if not files:
        raise ValueError(f"No Parquet files found in {source}")
    return group(source, files)
