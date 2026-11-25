"""Incremental, read-only diagnostic evidence within authorized Runs scopes."""

import hashlib
import json
import os
import stat
from collections import deque
from collections.abc import Generator, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import BinaryIO, cast

from forge.historical.archives.storage.files import disk
from forge.orchestration.inspection import MANAGED_RUN, run_root
from forge.orchestration.runs import RunInspection, inspect_run_evidence, safe_reference
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.shared.presentation.run_diagnostics import (
    Diagnostic,
    EvidenceSource,
    Explanation,
    Stage,
    conversion_route_failure,
    diagnostics,
    mapping,
    stage_evidence,
)

REPORT_LIMIT = 4 * 1024 * 1024
EVENT_LIMIT = 1024 * 1024
LOG_CHUNK = 16 * 1024
REFERENCE_DEPTH = 8
MANAGED_REPORTS = (
    "execution-result.json",
    "registration-result.json",
    "prepare-report.json",
    "execution-intent.json",
    "engine-outcome.json",
    "engine-result.json",
)
ENGINE_LOGS = (
    "engine-stderr.log",
    "validation-stderr.log",
    "engine-tui.log",
    "validation-stdout.log",
    "engine-stdout.log",
)
REPORT_LINKS = frozenset(
    {
        "report_path",
        "report",
        "child_report",
        "child_report_path",
        "execution_report",
        "execution_intent",
        "terminal_outcome",
        "recovery_receipts",
        "failure_report_path",
        "registration_receipt",
        "prepare_report",
        "preparation_report",
        "registration_report",
        "result_path",
        "engine_result_path",
        "engine_diagnostics_path",
        "event_log",
    }
)


@contextmanager
def open_evidence(path: Path, root: Path) -> Generator[BinaryIO]:
    """Pin every directory and reject symlinks, devices and out-of-scope paths."""
    if not path.is_absolute() or not path.is_relative_to(root) or ".." in path.parts:
        raise MarketForgeError("Evidence is outside the Runs artifact scope.")
    with ExitStack() as handles:
        flags = os.O_DIRECTORY | os.O_NOFOLLOW
        directory = os.open(path.anchor, flags)
        handles.callback(os.close, directory)
        for part in path.parts[1:-1]:
            directory = os.open(part, flags, dir_fd=directory)
            handles.callback(os.close, directory)
        descriptor = os.open(path.name, os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        handles.callback(os.close, descriptor)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise MarketForgeError("Evidence is not a regular file.")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            yield stream


@dataclass(frozen=True, slots=True)
class DiagnosticPage:
    entries: tuple[Diagnostic, ...]
    stages: tuple[Stage, ...]
    notices: tuple[str, ...]
    more: bool


def objects(value: object) -> Iterator[Mapping[str, object]]:
    if isinstance(value, list):
        for item in cast(list[object], value):
            if row := mapping(item):
                yield row


class DiagnosticReader:
    """One inspector's cursor over a run and its registry-authorized children."""

    def __init__(
        self, paths: ProjectPaths, inspection: RunInspection, *, purpose: str = ""
    ) -> None:
        self.paths = paths
        self.root = paths.runs_dir
        self.name = inspection.name
        self.status = str(
            (inspection.record or inspection.evidence).get("status")
            or inspection.evidence.get("state")
            or "unknown"
        )
        self.environment = purpose or str(
            (inspection.record or inspection.evidence).get("database_purpose")
            or inspection.evidence.get("environment")
            or ""
        )
        self.notices: dict[str, None] = {}
        self.coverage_source: EvidenceSource | None = None
        self.stages: dict[str, Stage] = {}
        self.files: dict[Path, EvidenceSource] = {}
        self.shared_attempt_files: set[Path] = set()
        self.pending: deque[EvidenceSource] = deque()
        self.children: deque[tuple[str, str, str]] = deque()
        self.seen_runs: set[str] = set()
        self.run_depth = {inspection.name: 0}
        self.child_ids: set[str] = set()
        self.scopes: dict[str, set[Path]] = {}
        self.reports: set[str] = set()
        self.registries: dict[str, tuple[str, str]] = {}
        self.request_context: dict[tuple[str, str, object], dict[str, object]] = {}
        self.lock = Lock()
        self.scanned_bytes = 0
        self.preparations: dict[tuple[Path, str | None, str, str], Explanation] = {}
        self.active: Iterator[Diagnostic | None] | None = None
        self._add_run(inspection)
        if self.status in {"running", "pending", "unknown"}:
            self.notice(
                "The indexed outcome is unfinished or unknown. Retained evidence may change."
            )

    def notice(self, message: str) -> None:
        self.notices[message] = None

    def _add_run(self, inspection: RunInspection, stage: str = "") -> None:
        name = inspection.name
        if name in self.seen_runs:
            return
        self.seen_runs.add(name)
        if inspection.removal and inspection.removal.get("mode") == "full":
            self.notice(f"{name} has an explicit history removal record.")
            return
        record: Mapping[str, object] = inspection.record or {}
        try:
            directory = (
                run_root(self.root, name) if not MANAGED_RUN.fullmatch(name) else self.root / name
            )
            if record and Path(str(record.get("artifact_root"))).resolve() != directory.resolve():
                raise MarketForgeError("Run evidence belongs to a different workspace.")
        except (OSError, MarketForgeError) as error:
            self.notice(str(error))
            return
        self.scopes[name] = {directory}
        operation = str(
            record.get("operation") or inspection.evidence.get("operation") or name.split("-")[0]
        )
        source = EvidenceSource(
            directory, name, operation, stage, status=str(record.get("status") or self.status)
        )
        self.registries[name] = (
            str(record.get("database_id") or ""),
            str(record.get("database_purpose") or self.environment),
        )
        if not record:
            self.notice(f"{name} has no indexed attempt or child inventory.")
        for key in ("attempts", "resources", "children"):
            if record.get(f"{key}_truncated"):
                self.notice(f"{name} has a bounded {key} inventory. Further references may exist.")
        for row in objects(record.get("children")):
            if isinstance(child := row.get("run_id"), str):
                child_stage = str(row.get("operation") or "")
                if operation == "pipeline" and child_stage == "acquisition":
                    child_stage = "acquire"
                self.children.append((child, name, stage or child_stage))
                self.child_ids.add(child)

        attempts = tuple(objects(record.get("attempts")))
        indexed = [
            (record.get("report_path"), source),
            *(
                (row.get(key), replace(source, attempt=str(row.get("attempt_id") or "")))
                for row in attempts
                for key in ("report_path", "outcome_path")
            ),
            *(
                (
                    row.get("reference"),
                    replace(
                        source,
                        stage=str(row.get("stage") or stage),
                        attempt=next(
                            (
                                str(a.get("attempt_id") or "")
                                for a in attempts
                                if a.get("number") == row.get("number")
                            ),
                            "",
                        ),
                    ),
                )
                for row in objects(record.get("resources"))
                if row.get("kind") == "report"
            ),
            *((path, source) for path in inspection.report_files.values()),
            *((path, source) for path in inspection.log_files.values()),
        ]
        for ref, context in indexed:
            self._add_indexed(ref, context)
        self._queue(directory / "report.json", source, optional=True)
        self._queue(directory / "events.jsonl", source, optional=True)
        if MANAGED_RUN.fullmatch(name):
            self._siblings(directory, source)

    def _add_indexed(self, ref: object, source: EvidenceSource) -> None:
        if not isinstance(ref, (str, Path)) or not ref:
            return
        path = Path(ref)
        if not path.is_absolute() or not path.is_relative_to(self.root) or ".." in path.parts:
            self.notice(f"{source.run_id} has an unavailable artifact reference.")
            return
        parent = path.parent
        own = self.root / source.run_id
        parts = path.relative_to(self.root).parts
        if len(parts) < 2:
            self.notice(f"{source.run_id} has a report outside its authorized run directories.")
            return
        managed = bool(MANAGED_RUN.fullmatch(parts[0]))
        if not parent.is_relative_to(own):
            if not managed:
                self.notice(f"{source.run_id} has a report outside its authorized run directories.")
                return
            self.scopes[source.run_id].add(self.root / parts[0])
        self._queue(path, source)
        if path.name == "report.json" and source.operation == "acquisition":
            self._queue(parent / "events.jsonl", source)
        if managed:
            self._siblings(parent, source)

    def _siblings(self, directory: Path, source: EvidenceSource) -> None:
        for name in (*MANAGED_REPORTS, *ENGINE_LOGS):
            self._queue(directory / name, source, optional=True)

    def _queue(self, path: Path, source: EvidenceSource, *, optional: bool = False) -> None:
        if path.suffix not in {".json", ".jsonl", ".log"}:
            return
        if existing := self.files.get(path):
            # Reused report paths cannot identify an attempt unless the document does.
            if source.attempt and existing.attempt and source.attempt != existing.attempt:
                self.shared_attempt_files.add(path)
            self.files[path] = replace(
                existing,
                attempt=""
                if path in self.shared_attempt_files
                else existing.attempt or source.attempt,
                stage=existing.stage or source.stage,
                sha256=existing.sha256 or source.sha256,
            )
            return
        if source.depth > REFERENCE_DEPTH:
            self.notice(f"Reference depth limit reached for {source.run_id} at {path.name}.")
            return
        if optional and not path.is_file() and not path.is_symlink():
            return
        item = replace(source, path=path)
        self.files[path] = item
        self.pending.append(item)

    def _links(self, value: object, source: EvidenceSource, *, attempts: bool = False) -> None:
        if isinstance(value, list):
            for item in cast(list[object], value):
                self._links(item, source, attempts=attempts)
            return
        node = mapping(value)
        source = replace(
            source,
            stage=source.stage or str(node.get("stage") or ""),
            attempt=str(
                node.get("attempt_id") or (node.get("id") if attempts else None) or source.attempt
            ),
        )
        for key, item in node.items():
            if key == "stages":
                for stage, report in mapping(item).items():
                    self._links(report, replace(source, stage=stage))
            elif key == "recovery_receipts" and isinstance(item, list):
                for ref in cast(list[object], item):
                    self._links({"registration_receipt": ref}, source)
            elif key in REPORT_LINKS and isinstance(item, str):
                path = Path(item)
                if any(
                    path.is_relative_to(scope) and ".." not in path.parts
                    for scope in self.scopes[source.run_id]
                ):
                    digest = node.get(f"{key}_sha256")
                    linked = replace(
                        source,
                        sha256=digest if isinstance(digest, str) else None,
                        depth=source.depth + 1,
                    )
                    self._queue(path, linked)
                    if MANAGED_RUN.fullmatch(path.parent.name) or key in {
                        "execution_intent",
                        "execution_report",
                    }:
                        self._siblings(path.parent, replace(linked, sha256=None))
                else:
                    # Indexed children are loaded through their own registry record.
                    if not any(
                        path.is_relative_to(self.root / child) for child, _, _ in self.children
                    ):
                        self.notice(
                            f"{source.run_id} has an unverified referenced artifact in {source.path.name}."
                        )
            elif key == "validation_logs" and isinstance(item, str):
                directory = Path(item)
                if directory.is_relative_to(source.path.parent) and ".." not in directory.parts:
                    for name in ("validation-stderr.log", "validation-stdout.log"):
                        self._queue(directory / name, source, optional=True)
            elif isinstance(item, (list, dict)) and key not in {
                "recipe",
                "submitted",
                "effective_configuration",
                "failed_preparation",
            }:
                self._links(cast(object, item), source, attempts=key == "attempts")

    def _pipeline_failure(
        self, document: Mapping[str, object], source: EvidenceSource
    ) -> Explanation | None:
        active = document.get("active_stage")
        if (
            source.operation != "pipeline"
            or document.get("kind") != "historical_pipeline"
            or document.get("run_id") != source.run_id
            or not isinstance(active, str)
        ):
            return None
        stage = mapping(mapping(document.get("stages")).get(active))
        if stage.get("state") not in {"blocked", "failed"}:
            return None
        error = document.get("error")
        if not isinstance(error, str):
            return None
        directory = self.root / source.run_id
        recorded = mapping(stage.get("failed_preparation"))
        recorded_path = str(recorded.get("report") or "")
        if active == "convert" and (
            stage.get("phase") == "validate" or (recorded and stage.get("phase") == "prepare")
        ):
            prefix = "Conversion preparation did not validate; inspect "
            legacy_path = (
                error[len(prefix) : -1] if error.startswith(prefix) and error.endswith(".") else ""
            )
            if not (reference := recorded_path or legacy_path):
                return None
            path = Path(reference)
            digest = recorded.get("report_sha256")
            fingerprint = digest if isinstance(digest, str) else None
            key = (path, fingerprint, source.run_id, recorded_path)
            if key not in self.preparations:
                self.preparations[key] = self._read_conversion_failure(
                    source,
                    path,
                    directory / "historical" / "conversion-recipe.toml",
                    recorded,
                    fingerprint,
                )
        else:
            reference = stage.get("child_report")
            if not isinstance(reference, str):
                return None
            path = Path(reference)
            digest = stage.get("child_report_sha256")
            fingerprint = digest if isinstance(digest, str) else None
            key = (path, fingerprint, source.run_id, active)
            if key not in self.preparations:
                self.preparations[key] = self._read_stage_failure(source, path, active, fingerprint)
        finished = document.get("finished_at")
        canonical = str(directory / "report.json")
        mirror_identity = (
            (canonical, finished)
            if isinstance(finished, str) and document.get("report_path") == canonical
            else None
        )
        return replace(self.preparations[key], mirror_identity=mirror_identity)

    def _bounded_evidence(self, path: Path) -> bytes:
        with open_evidence(path, self.root) as stream:
            data = stream.read(REPORT_LIMIT + 1)
        self.scanned_bytes += len(data)
        if len(data) > REPORT_LIMIT:
            raise ValueError("artifact exceeds the 4 MiB structured evidence limit")
        return data

    def _read_stage_failure(
        self, source: EvidenceSource, path: Path, stage: str, fingerprint: str | None
    ) -> Explanation:
        summary = Explanation(f"The Pipeline recorded the {stage} stage as unsuccessful.")
        registry, environment = self.registries[source.run_id]
        if (
            not any(path.is_relative_to(scope) for scope in self.scopes[source.run_id])
            or path.suffix != ".json"
            or path == source.path
        ):
            return replace(summary, limitation=f"Unverified stage report reference: {path}")
        try:
            data = self._bounded_evidence(path)
            digest = hashlib.sha256(data).hexdigest()
            report = mapping(json.loads(data, parse_float=Decimal))
            if (
                not report
                or fingerprint
                and fingerprint != digest
                or registry
                and report.get("registry_id") not in (None, registry)
                or environment
                and report.get("environment") not in (None, environment)
            ):
                raise ValueError("report is empty or differs from its retained identity")
            evidence = replace(source, path=path, stage=stage, sha256=digest)
            causes = tuple(
                entry.message
                for entry in diagnostics(report, evidence)
                if entry.severity == "error"
            )
            return replace(summary, cause=causes, evidence=evidence)
        except (OSError, ValueError, MarketForgeError, RecursionError) as error:
            return replace(summary, limitation=f"Unable to verify stage report {path}: {error}")

    def _read_conversion_failure(
        self,
        source: EvidenceSource,
        path: Path,
        recipe: Path,
        recorded: Mapping[str, object],
        fingerprint: str | None,
    ) -> Explanation:
        summary = Explanation(
            "Conversion preparation did not complete successfully.",
            impact="The Pipeline stopped before Conversion execution.",
        )
        registry, environment = self.registries[source.run_id]
        if (
            not registry
            or not environment
            or path.parent.parent != self.root
            or not MANAGED_RUN.fullmatch(path.parent.name)
            or not path.parent.name.startswith("convert-prepare-")
            or path.name != "prepare-report.json"
            or (recorded and (recorded.get("report") != str(path) or not fingerprint))
        ):
            return replace(summary, limitation=f"Unverified preparation report reference: {path}")

        try:
            data = self._bounded_evidence(path)
            digest = hashlib.sha256(data).hexdigest()
            if fingerprint and digest != fingerprint:
                raise ValueError("report differs from its retained fingerprint")
            report = mapping(json.loads(data, parse_float=Decimal))
            binding = mapping(report.get("recipe"))
            allowed_registries = (None, registry) if fingerprint else (registry,)
            recipe_matches = (
                (
                    binding.get("path") == str(recipe)
                    and binding.get("sha256")
                    == hashlib.sha256(self._bounded_evidence(recipe)).hexdigest()
                )
                if binding
                else bool(fingerprint and report.get("recipe_path") == str(recipe))
            )
            if (
                report.get("registry_id") not in allowed_registries
                or mapping(report.get("upstream")).get("registry_id") not in (None, registry)
                or report.get("environment") != environment
                or report.get("report_path") != str(path)
                or report.get("compatibility") != "not_validated"
                or not recipe_matches
            ):
                raise ValueError(
                    "report does not match the Pipeline recipe, outcome, or selected registry"
                )
        except FileNotFoundError:
            return replace(summary, limitation=f"Preparation report or recipe is missing: {path}")
        except (OSError, ValueError, MarketForgeError, RecursionError) as error:
            return replace(
                summary, limitation=f"Unable to verify preparation report {path}: {error}"
            )

        evidence = replace(source, path=path, operation="convert", stage="convert", sha256=digest)
        self._queue(path, replace(evidence, depth=source.depth + 1))
        blockers = tuple(
            entry.message
            for entry in diagnostics(report, evidence)
            if entry.severity == "error"
            and entry.location
            and entry.location[0] in {"blockers", "compatibility_blockers"}
        )
        if len(blockers) == 1 and (known := conversion_route_failure(report, blockers[0])):
            return replace(known, impact=summary.impact, evidence=evidence)
        return replace(
            summary,
            cause=blockers,
            evidence=evidence,
            affected=tuple(
                (key, item)
                for section in ("upstream", "settings")
                for key, item in mapping(report.get(section)).items()
                if key
                in {
                    "registry_publication_id",
                    "engine_publication_id",
                    "target_quote",
                    "price_source",
                    "max_staleness_ns",
                    "max_hops",
                    "path_policy",
                }
            ),
            resolution="Review the recorded preparation blockers before validating again."
            if blockers
            else "",
        )

    def _scan(self, source: EvidenceSource) -> Iterator[Diagnostic | None]:
        path = source.path
        try:
            with open_evidence(path, self.root) as stream:
                if path.suffix == ".jsonl":
                    yield from self._events(stream, source)
                elif path.suffix == ".log":
                    yield from self._log(stream, source)
                else:
                    data = stream.read(REPORT_LIMIT + 1)
                    self.scanned_bytes += len(data)
                    if len(data) > REPORT_LIMIT:
                        self.coverage_source = source
                        self.notice(
                            f"{path.name} for {source.run_id} exceeds the 4 MiB structured report limit."
                        )
                        return
                    if source.sha256 and hashlib.sha256(data).hexdigest() != source.sha256:
                        self.notice(
                            f"{path.name} for {source.run_id} differs from its retained fingerprint."
                        )
                        return
                    value: object = json.loads(data, parse_float=Decimal)
                    if not mapping(value):
                        self.notice(
                            f"{path.name} for {source.run_id} has no readable report object."
                        )
                        return
                    document = mapping(value)
                    registry, environment = self.registries[source.run_id]
                    if (
                        registry
                        and document.get("registry_id") not in (None, registry)
                        or environment
                        and document.get("environment") not in (None, environment)
                    ):
                        self.notice(
                            f"{path.name} for {source.run_id} belongs to a different registry."
                        )
                        return
                    for index, row in enumerate(objects(document.get("requests"))):
                        request = mapping(row.get("request")) or row
                        identity = row.get("request_index", index)
                        if type(identity) is int:
                            self.request_context[(source.run_id, source.attempt, identity)] = dict(
                                request
                            )
                    if path.name not in {"execution-intent.json", "outcome.json"}:
                        self.reports.add(source.run_id)
                    if source.run_id == self.name:
                        for stage in stage_evidence(mapping(value)):
                            self.stages.setdefault(stage.name, stage)
                    self._links(value, source)
                    explanation = self._pipeline_failure(document, source)
                    if explanation and explanation.limitation:
                        self.notice(explanation.limitation)
                    for entry in diagnostics(value, source):
                        if explanation and entry.location == ("error",):
                            entry = replace(
                                entry,
                                source=replace(entry.source, stage=str(document["active_stage"])),
                                explanation=explanation,
                            )
                        elif (
                            source.operation == "convert"
                            and path.name == "prepare-report.json"
                            and entry.location
                            and entry.location[0] in {"blockers", "compatibility_blockers"}
                            and (known := conversion_route_failure(document, entry.message))
                        ):
                            entry = replace(entry, explanation=known)
                        yield entry
        except (OSError, ValueError, MarketForgeError, RecursionError):
            self.notice(f"{path.name} for {source.run_id} is missing, invalid, or unsafe.")

    def _events(self, stream: BinaryIO, source: EvidenceSource) -> Iterator[Diagnostic | None]:
        line = 0
        end = os.fstat(stream.fileno()).st_size
        while stream.tell() < end and (
            data := stream.readline(min(end - stream.tell(), EVENT_LIMIT + 1))
        ):
            self.scanned_bytes += len(data)
            line += 1
            if len(data) > EVENT_LIMIT:
                self.coverage_source = source
                self.notice(
                    f"{source.path.name} has a record over 1 MiB. Open the original log for that record."
                )
                while stream.tell() < end and data and not data.endswith(b"\n"):
                    data = stream.readline(min(end - stream.tell(), EVENT_LIMIT + 1))
                    self.scanned_bytes += len(data)
                    yield None
            else:
                try:
                    event: object = json.loads(data, parse_float=Decimal)
                    if not mapping(event):
                        raise ValueError("Expected an event object.")
                    index = mapping(event).get("request_index")
                    if type(index) is int and (
                        request := self.request_context.get((source.run_id, source.attempt, index))
                    ):
                        event = {"request": request, **mapping(event)}
                    yield from diagnostics(event, source, line=line)
                except (ValueError, RecursionError):
                    self.coverage_source = source
                    self.notice(
                        f"{source.path.name} for {source.run_id} includes incomplete or invalid event records."
                    )
            yield None

    def _log(self, stream: BinaryIO, source: EvidenceSource) -> Iterator[Diagnostic | None]:
        line = 1
        if os.fstat(stream.fileno()).st_size >= 16 * 1024 * 1024:
            self.coverage_source = source
            self.notice(
                f"{source.path.name} reached the Engine capture limit. The producer may have omitted later output."
            )
        # Raw streams have no per-record severity. Keep contiguous output together.
        import codecs

        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        end = os.fstat(stream.fileno()).st_size
        while stream.tell() < end and (data := stream.read(min(LOG_CHUNK, end - stream.tell()))):
            self.scanned_bytes += len(data)
            text = decoder.decode(data)
            if text:
                yield Diagnostic("info", text, source, line=line)
                line += text.count("\n")
        if text := decoder.decode(b"", final=True):
            yield Diagnostic("info", text, source, line=line)

    def _page(self) -> tuple[Diagnostic, ...]:
        with self.lock:
            return self._read_page()

    def _read_page(self) -> tuple[Diagnostic, ...]:
        self.scanned_bytes = 0
        result: list[Diagnostic] = []
        scanned = 0
        while len(result) < 30 and scanned < 256 and self.scanned_bytes < REPORT_LIMIT:
            if self.active is None:
                if not self.pending:
                    break
                item = self.pending.popleft()
                self.active = self._scan(self.files[item.path])
            scanned += 1
            try:
                entry = next(self.active)
            except StopIteration:
                self.active = None
            else:
                if entry is not None:
                    result.append(entry)
        return tuple(result)

    async def load(self) -> DiagnosticPage:
        # Load a child only when its parent's queued evidence has been consumed.
        for _ in range(4):
            if self.pending or self.active is not None or not self.children:
                break
            child, parent, stage = self.children.popleft()
            if child in self.seen_runs:
                continue
            depth = self.run_depth[parent] + 1
            if depth > REFERENCE_DEPTH:
                self.notice(f"Child reference depth limit reached at {child}.")
                continue
            try:
                safe_reference(child)
                evidence = await inspect_run_evidence(self.paths, child, action="show", limit=1)
                registry, environment = self.registries[parent]
                if (
                    not evidence.record
                    or evidence.record.get("parent_run_id") != parent
                    or registry
                    and str(evidence.record.get("database_id")) != registry
                    or environment
                    and evidence.record.get("database_purpose") != environment
                ):
                    raise MarketForgeError("Child relationship is unavailable.")
                self.run_depth[child] = depth
                await disk(self._add_run, evidence, stage)
            except (OSError, MarketForgeError, ValueError):
                self.notice(f"Child run {child} could not be inspected in the selected registry.")
        entries = await disk(self._page)
        more = bool(self.pending or self.active is not None or self.children)
        if not more and self.name not in self.reports:
            self.notice("The execution report is missing or unavailable.")
        return DiagnosticPage(entries, tuple(self.stages.values()), tuple(self.notices), more)

    def close(self) -> None:
        with self.lock:
            if self.active is not None:
                close = getattr(self.active, "close", None)
                if close is not None:
                    close()
                self.active = None
