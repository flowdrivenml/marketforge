"""Rich-free views over merge recipes and retained merge evidence."""

import shlex
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from forge.historical.merge.config import DerivedRecipe
from forge.historical.merge.service import Recipe
from forge.shared.presentation.normalization import binary_size, registry_ids

Fields = tuple[tuple[str, str], ...]
LIMIT = 20


def utc_ns(value: object) -> str:
    if type(value) is not int:
        return "Unavailable"
    seconds, nanos = divmod(value, 10**9)
    return f"{datetime.fromtimestamp(seconds, UTC):%Y-%m-%d %H:%M:%S}.{nanos:09d}Z"


def interval(start: object, end: object) -> str:
    return f"[{utc_ns(start)}, {utc_ns(end)})"


def fields_of(data: dict[str, Any], pairs: Fields) -> Fields:
    return tuple(
        (
            label,
            f"{data[key]:,}"
            if type(data[key]) is int and not label.endswith("ID")
            else str(data[key]),
        )
        for label, key in pairs
        if data.get(key) is not None
    )


@dataclass(frozen=True)
class RecipeView:
    name: str
    path: Path
    recipe: Recipe | DerivedRecipe | None = None
    error: str = ""

    @property
    def form(self) -> str:
        if isinstance(self.recipe, DerivedRecipe):
            return "Normalization selection"
        return "Explicit file IDs" if self.recipe else "Unavailable"

    @property
    def period(self) -> str:
        return interval(self.recipe.start_ns, self.recipe.end_ns) if self.recipe else "Unavailable"

    @property
    def details(self) -> Fields:
        recipe = self.recipe
        if recipe is None:
            return ()
        values: Fields = (
            ("Form", self.form),
            ("Requested interval", self.period),
            ("Timeline ordering", recipe.timeline_ordering or "Resolved during preparation"),
        )
        if isinstance(recipe, Recipe):
            values += (("Registry file IDs (precedence)", ", ".join(map(str, recipe.dataset_ids))),)
        else:
            selection = recipe.selection
            values += (
                ("Normalization recipe", selection.source),
                (
                    "Selection mode",
                    selection.mode
                    + (" (default)" if "mode" not in selection.model_fields_set else ""),
                ),
            )
            labels = (
                ("Data types", "data_types"),
                ("Exchanges", "exchanges"),
                ("Instruments", "instruments"),
                ("Instrument types", "instrument_types"),
                ("Market categories", "market_categories"),
                ("Registry publication IDs", "publication_ids"),
            )
            document = selection.model_dump()
            values += tuple(
                (label, ", ".join(map(str, document[key])))
                for label, key in labels
                if document[key] is not None
            )
        if recipe.stream_ranks:
            values += (
                (
                    "Stream rank overrides",
                    ", ".join(f"{key} → {rank}" for key, rank in recipe.stream_ranks.items()),
                ),
            )
        return values

    def machine(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": str(self.path),
            "valid": self.recipe is not None,
            "configuration": self.recipe.model_dump(mode="json") if self.recipe else None,
            **({"error": self.error} if self.error else {}),
        }


@dataclass(frozen=True)
class MergeView:
    title: str
    status: str
    environment: str
    details: Fields
    selection: Fields
    streams: tuple[tuple[str, ...], ...]
    output: Fields
    registration: Fields
    files: tuple[tuple[str, str], ...]
    artifacts: Fields
    issues: tuple[str, ...]
    next_action: str

    @classmethod
    def from_result(cls, result: dict[str, Any], *, action: str, reference: str) -> "MergeView":
        recovering = action == "register"
        executed = "execution_id" in result
        preparing = not (recovering or executed)
        status = str(result.get("status") or result.get("registration") or "unavailable")
        environment = result.get("environment", "")
        flags = " --testing" if environment == "testing" else ""
        title = (
            "Registration"
            if recovering
            else "Execution"
            if executed
            else "Recipe Created · Source Discovery"
            if action == "discovery"
            else "Preparation"
        )
        details = (
            (
                "Recipe" if result.get("recipe") else "Reference",
                str(result.get("recipe") or reference),
            ),
        )
        details += fields_of(
            result,
            (
                ("Result", "reuse_message"),
                ("Forge run ID", "logical_run_id"),
                ("Engine job ID", "job_id"),
                ("Retained execution ID", "execution_id"),
            ),
        )
        if not result.get("logical_run_id"):
            details += fields_of(result, (("Forge run ID", "run_id"),))
        details += fields_of(
            result,
            (
                ("Physical verification", "verification"),
                ("Registration", "registration"),
            ),
        )
        if executed:
            details += fields_of(result, (("Execution", "execution"),))
        elif recovering:
            retained: dict[str, Any] | None = result.get("execution")
            if retained:
                details += fields_of(retained, (("Retained execution ID", "execution_id"),))
        for label, key in (
            ("Elapsed", "managed_elapsed_ms"),
            ("Engine elapsed", "engine_elapsed_ms"),
        ):
            if key in result:
                seconds = result[key] // 1000
                details += ((label, f"{seconds // 60:02d}:{seconds % 60:02d}"),)
                break
        if recovering and result.get("evidence"):
            details += (("Physical verification", "verified"),)
        if action == "discovery":
            details += (("Structure", "valid"),)
        if preparing:
            compatibility = result.get("compatibility")
            if compatibility:
                validation = (
                    "not attempted"
                    if compatibility == "blocked" and result.get("blockers")
                    else str(compatibility).replace("_", " ")
                )
                details += (("Rust validation", validation),)
            if reuse_plan := result.get("reuse_plan"):
                details += (("Equivalent work", reuse_plan["status"].replace("_", " ")),)
            if "job_path" in result:
                state = "Planned" if result.get("dry_run") else "Generated"
                if compatibility == "validated":
                    state += " · ready for submission"
                details += (("Job", state),)
        coverage = result.get("coverage", {})
        requested = coverage.get("requested")
        selection: Fields = (
            (("Requested interval", interval(requested["start_ns"], requested["end_ns"])),)
            if requested
            else ()
        )
        if source := result.get("source", {}).get("reference"):
            selection += (("Normalization recipe", source),)
        selection += fields_of(
            result,
            (
                ("Requested selections", "requested_count"),
                ("Matched registry files", "matched_count"),
                (
                    "Selected registry files",
                    "selected_file_count" if "selected_file_count" in result else "selected_count",
                ),
                ("Source publications", "publication_count"),
                ("Logical streams", "stream_count"),
                ("Timeline ordering", "timeline_ordering"),
            ),
        )
        if file_ids := result.get("requested_file_ids"):
            preview = ", ".join(map(str, file_ids[:LIMIT]))
            if len(file_ids) > LIMIT:
                preview += f" · {len(file_ids) - LIMIT:,} more"
            selection += (("Requested registry file IDs", preview),)
        inputs: list[dict[str, Any]] | None = result.get("inputs")
        if executed and inputs is not None:
            selection += (
                ("Input registry files", f"{len(inputs):,}"),
                (
                    "Input publications",
                    f"{len({(item.get('namespace'), item.get('engine_id'), item.get('path')) for item in inputs}):,}",
                ),
            )
            members = {
                member["stream_id"]
                for item in inputs
                for member in item.get("memberships", [])
                if "stream_id" in member
            }
            if members:
                selection += (("Input logical streams", f"{len(members):,}"),)
        if coverage.get("observed_extent_gaps"):
            selection += (
                ("Observed extent gaps", f"{len(coverage['observed_extent_gaps']):,} streams"),
            )
        streams = tuple(
            (
                str(row["stream_rank"]),
                row["stream_id"],
                row["content_type"],
                f"{row['file_count']:,}",
            )
            for row in result.get("streams", [])
            if all(key in row for key in ("stream_rank", "stream_id", "content_type", "file_count"))
        )
        evidence_rows: list[dict[str, Any]] = result.get("evidence", [])
        evidence: dict[str, Any] = evidence_rows[0] if evidence_rows else {}
        manifest = result.get("dataset") or evidence.get("manifest", {})
        output: Fields = ()
        if manifest:
            publication = manifest.get("engine_dataset_id", evidence.get("engine_dataset_id"))
            if publication is not None:
                output += (("Engine publication ID", str(publication)),)
            output += fields_of(
                manifest,
                (
                    ("Composition", "mode"),
                    ("Timeline ordering", "timeline_ordering"),
                    ("Market events", "events_written"),
                    ("Trade events", "trade_events"),
                    ("Depth events", "depth_events"),
                    ("Reconstruction events", "reconstruction_events"),
                    ("Primary files", "files_written"),
                    ("Integrity", "integrity_status"),
                ),
            )
            files = manifest.get("files")
            if files and all(type(file.get("rows")) is int for file in files):
                output += (("Physical rows", f"{sum(file['rows'] for file in files):,}"),)
            if files and all(file.get("partition_id") is not None for file in files):
                output += (
                    ("Partitions with files", f"{len({file['partition_id'] for file in files}):,}"),
                )
            size = manifest.get("bytes_written", evidence.get("bytes_written"))
            if size is not None:
                output += (("Recorded size", binary_size(size)),)
            for label, key in (
                ("Observed first event", "start_timestamp_ns"),
                ("Observed last event", "end_timestamp_ns"),
            ):
                if manifest.get(key) is not None:
                    output += ((label, utc_ns(manifest[key])),)
            membership = tuple(
                dict.fromkeys(
                    f"{stream['exchange']} / {stream['symbol']} ({stream['instrument_id']})"
                    for stream in manifest.get("streams", [])
                    if all(key in stream for key in ("exchange", "symbol", "instrument_id"))
                )
            )
            if membership:
                output += (
                    (
                        "Exchange / instrument",
                        ", ".join(membership[:6])
                        + (f" · {len(membership) - 6} more" if len(membership) > 6 else ""),
                    ),
                )
        mapping = result.get("catalogue_dataset_ids", {})
        states = result.get("file_states", {})
        if not states:
            states = {
                path: state
                for publication in result.get("publications", {}).values()
                for path, state in publication.get("file_states", {}).items()
            }
        registered = tuple(
            (str(identifier), str(states.get(path, "registered")))
            for path, identifier in mapping.items()
        )
        registration = fields_of(
            result.get("counts", {}),
            (
                ("Verified publications", "verified_publications"),
                ("Registered primary files", "primary_files"),
                ("New files", "new"),
                ("Reused files", "reused"),
            ),
        )
        if len(mapping) > LIMIT and (identifiers := registry_ids(mapping)):
            registration += (("Registry file IDs", identifiers),)
        publication_ids = tuple(
            str(item["publication_id"])
            for item in result.get("publications", {}).values()
            if "publication_id" in item
        )
        if publication_ids:
            registration += (("Registry publication IDs", ", ".join(publication_ids[:LIMIT])),)
        issues = [
            str(result[key])
            for key in (
                "error",
                "validation_error",
                "registration_error",
                "execution_registry_warning",
            )
            if result.get(key)
        ]
        issues.extend(
            f"{entry['reason']}" + (f" · {entry['suggestion']}" if entry.get("suggestion") else "")
            for entry in result.get("blockers", [])
        )
        candidate_ids = sorted(
            {
                publication
                for entry in result.get("blockers", [])
                for publication in entry.get("publication_ids", [])
            }
        )
        if candidate_ids:
            display = ", ".join(map(str, candidate_ids[:LIMIT]))
            if len(candidate_ids) > LIMIT:
                display += f" · {len(candidate_ids) - LIMIT:,} more"
            selection += (("Candidate registry publications", display),)
        if (missing := result.get("missing")) and not result.get("blockers"):
            issues.append(f"{len(missing):,} requested selections have no approved files.")
        if coverage.get("observed_extent_gaps") and coverage.get("note"):
            issues.append(coverage["note"])
        artifact_labels = (
            ("Runtime source", "configuration_origin"),
            ("Planned job" if result.get("dry_run") else "Prepared job", "job_path"),
            ("Planned report" if result.get("dry_run") else "Report", "report_path"),
            ("Planned output", "planned_output_path"),
            ("Diagnostics", "engine_diagnostics_path"),
            ("Validation logs", "validation_logs"),
        )
        artifacts = fields_of(result, artifact_labels)
        if result.get("disposition") == "reused":
            artifacts += fields_of(result, (("Reused output", "path"),))
        if action == "discovery":
            artifacts = (*fields_of(result, (("Configuration", "path"),)), *artifacts)
        if recovering and result.get("registration_receipt"):
            artifacts += (("Report", result["registration_receipt"]),)
        manifest_path = result.get("engine_result", {}).get("manifest_path") or evidence.get(
            "manifest_path"
        )
        if manifest_path:
            artifacts += (("Manifest", str(manifest_path)),)
        next_action = str(result.get("recovery") or "")
        if next_action:
            next_action += " 2>&1 | cpout"
        if recovering and not mapping and result.get("registration") != "registered":
            next_action = f"mf merge register {shlex.quote(reference)}{flags} 2>&1 | cpout"
        if mapping:
            next_action = f"mf datasets show {next(iter(mapping.values()))}{flags} | cpout"
        return cls(
            title,
            status,
            environment,
            details,
            selection,
            streams,
            output,
            registration,
            registered,
            artifacts,
            tuple(dict.fromkeys(issues)),
            next_action,
        )
