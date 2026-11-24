"""Settings explorer projections and per-file drafts."""

import hashlib
from dataclasses import dataclass
from os import environ
from pathlib import Path
from typing import Any, cast

from forge.historical.engine.managed import inventory
from forge.historical.engine.pipeline_runtime import PipelineChange, PipelineConfiguration
from forge.historical.engine.runtime_config import (
    Change,
    RuntimeConfiguration,
    Snapshot,
    configured_at,
    create_entry,
    native,
    parse_document,
    read_snapshot,
    replace_entry,
    typed_value,
)
from forge.orchestration.inspection import read_report, run_root
from forge.shared.config.loader import configuration_errors
from forge.shared.config.models import AppSettings
from forge.shared.documentation import SITE_URL
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui.controller import document
from forge.tui.settings import application_fields, change_toml

REFERENCE = "reference/configuration/"
PAGES = {
    "application": "application",
    "normalize": "normalization-runtime",
    "merge": "merge-runtime",
    "convert": "conversion-runtime",
    "pipeline": "pipeline-runtime",
}
EDITOR_KINDS = frozenset(
    {"enum", "boolean", "integer", "rate", "bytes", "mib", "milliseconds", "string", "path"}
)
BASIC = frozenset(
    {
        "workers",
        "memory_budget_bytes",
        "memory_limit_mib",
        "memory_limit_mb",
        "row_group_target_mb",
        "file_target_mb",
        "enabled",
        "scratch_directory",
        "scratch_budget_bytes",
        "max_open_files",
        "row_group_target_bytes",
        "file_target_bytes",
        "compression_level",
        "data_root",
        "mode",
        "color",
        "editor",
    }
)


def documentation(component: str = "", path: str = "", *, base: str = SITE_URL) -> str:
    page = PAGES.get(component)
    url = base.rstrip("/") + "/" + REFERENCE + (page + "/" if page else "")
    return url + ("#setting-" + path if path else "")


def editable(field: dict[str, Any]) -> bool:
    return bool(field["mutable"]) and field["kind"] in EDITOR_KINDS


def validate_application(data: bytes, path: Path) -> AppSettings:
    import tomllib

    with configuration_errors(path):
        return AppSettings.model_validate(tomllib.loads(data.decode("utf-8")))


def retained_pipeline_settings(
    context: ApplicationContext,
    run_id: str,
    registry: object,
) -> dict[str, object]:
    """Inspect pinned configuration verified against the retained pipeline report."""
    root = context.paths.runs_dir
    directory = run_root(root, run_id)
    report = read_report(root, run_id)
    if report.get("kind") != "historical_pipeline":
        raise MarketForgeError("Select a retained historical pipeline run.")
    environment = "testing" if context.testing else "production"
    if report.get("environment") != environment:
        raise MarketForgeError(f"The run is outside the selected {environment} environment.")
    if report.get("registry_id") and report["registry_id"] != registry:
        raise MarketForgeError("The run is bound to a different or unverified registry.")
    files = document(report.get("settings_files"))
    effective = document(document(report.get("runtime")).get("effective"))
    result: dict[str, object] = {}
    for stage in ("normalize", "merge", "convert"):
        path = directory / "historical" / f"{stage}.toml"
        if (digest := files.get(str(path))) is None:
            continue
        pinned = read_snapshot(path, limit=1024 * 1024)
        if digest != hashlib.sha256(pinned.data).hexdigest():
            raise MarketForgeError(
                f"Retained {stage} runtime fingerprint does not match the report."
            )
        result[stage] = {
            "Path": str(path),
            "SHA-256": digest,
            "Pinned TOML values": parse_document(pinned.data),
            "Recorded effective values": effective.get(stage),
        }
    if not result:
        raise MarketForgeError("No fingerprinted runtime snapshots are available for this run.")
    return {"Run": run_id, "Environment": environment, "Runtime snapshots": result}


@dataclass(frozen=True)
class ApplicationChange:
    candidate: bytes
    fields: list[dict[str, Any]]
    settings: AppSettings


class ConfigurationDraft:
    def __init__(self, context: ApplicationContext, component: str) -> None:
        self.context = context
        self.component = component
        self.runtime: RuntimeConfiguration | None = None
        self.pipeline: PipelineConfiguration | None = None
        self.original: Snapshot | None
        self.source_error: str | None = None
        if component == "pipeline":
            self.pipeline = PipelineConfiguration(context.paths)
            self.path = self.pipeline.path
            try:
                self.original = self.pipeline.snapshot()
            except MarketForgeError as error:
                self.source_error = str(error)
                self.original = None
        elif component == "application":
            self.path = context.settings_file or context.paths.settings_file
            self.original = (
                read_snapshot(self.path) if self.path.exists() or self.path.is_symlink() else None
            )
        else:
            operations = {
                operation: RuntimeConfiguration(context.paths, operation)
                for operation in ("normalize", "merge", "convert")
            }
            self.runtime = operations[component]
            self.path = self.runtime.path
            try:
                self.original = self.runtime.snapshot()
            except MarketForgeError as error:
                self.source_error = str(error)
                self.original = None
        self.initial = self.original.data if self.original else b""
        self.candidate = self.initial
        self.checked: bytes | None = None
        self.fields: list[dict[str, Any]] = []
        self.status = ""
        self.schema_version: object = None
        self.executable: object = None
        self.refresh()
        self.baseline = {field["path"]: field for field in self.fields}

    def missing_documentation(self) -> list[str] | None:
        source = (
            self.context.paths.root / "docs" / "content" / REFERENCE / f"{PAGES[self.component]}.md"
        )
        try:
            reference = source.read_text(encoding="utf-8")
        except OSError:
            return None
        return [
            field["path"]
            for field in self.fields
            if f'id="setting-{field["path"]}"' not in reference
        ]

    @property
    def dirty(self) -> bool:
        return self.candidate != self.initial

    def refresh(self) -> None:
        self.inspect()
        self.documentation_missing = self.missing_documentation()

    def inspect(self) -> None:
        self.checked = None
        if self.runtime:
            schema = self.runtime.schema()
            self.schema_version = schema["runtime_schema_version"]
            self.executable = next(
                item["path"]
                for item in inventory(self.context.paths)["targets"]
                if item["target"] == self.component
            )
            effective = None
            configured: dict[str, Any] = {}
            try:
                configured = parse_document(self.candidate)
                if self.source_error:
                    raise MarketForgeError(self.source_error)
                effective = self.runtime.validate(self.candidate)
            except MarketForgeError as error:
                self.status = str(error)
            self.fields = [
                self.runtime.describe(field, configured, effective) for field in schema["fields"]
            ]
            if effective is None:
                return
        elif self.pipeline:
            schemas: dict[str, dict[str, Any]] = {}
            versions: dict[str, object] = {}
            for stage, service in self.pipeline.stages.items():
                try:
                    schema = service.schema()
                except MarketForgeError as error:
                    versions[stage] = str(error)
                else:
                    schemas[stage] = schema
                    versions[stage] = schema["runtime_schema_version"]
            self.schema_version = versions
            try:
                if self.source_error:
                    raise MarketForgeError(self.source_error)
                effective, _ = self.pipeline.effective(self.candidate)
            except MarketForgeError as error:
                self.status = str(error)
                effective = {}
            try:
                configured = parse_document(self.candidate)
            except MarketForgeError:
                configured = {}
            self.fields = []
            for stage, schema in schemas.items():
                service = self.pipeline.stages[stage]
                base = None
                try:
                    base = service.validate(service.snapshot().data)
                except MarketForgeError:
                    pass
                for field in schema["fields"]:
                    if not field["mutable"]:
                        continue
                    name = field["path"]
                    self.fields.append(
                        {
                            **service.describe(
                                field, configured.get(stage, {}), effective.get(stage)
                            ),
                            "path": f"{stage}.{name}",
                            "base": service.describe(field, {}, base)["effective"],
                        }
                    )
            identity = configured.get("schema_version")
            self.fields.insert(
                0,
                {
                    "path": "schema_version",
                    "kind": "integer",
                    "default": None,
                    "purpose": "Identifies the format of the pipeline override document.",
                    "required": True,
                    "mutable": False,
                    "configured": identity,
                    "configured_path": "schema_version" if identity is not None else None,
                    "effective": identity if effective else None,
                    "effective_known": bool(effective),
                    "constraint": "Integer 1",
                    "choices": [1],
                },
            )
            if not effective:
                return
        else:
            configured = {}
            try:
                configured = parse_document(self.candidate)
                settings = validate_application(self.candidate, self.path)
            except MarketForgeError as error:
                self.status = str(error)
                settings = self.context.settings
            else:
                self.checked = self.candidate
            self.schema_version = settings.version
            self.fields = []
            for entry in application_fields(settings):
                path = str(entry["path"])
                spec = cast(dict[str, Any], entry["constraints"])
                schema = (
                    next(
                        (
                            item
                            for item in cast(list[dict[str, Any]], spec["anyOf"])
                            if item.get("type") != "null"
                        ),
                        dict[str, Any](),
                    )
                    if "anyOf" in spec
                    else spec
                )
                value, source = configured_at(configured, path)
                effective_source = "Application TOML" if source else "Built-in default"
                if path == "presentation.editor" and not entry["current"]:
                    effective_source = next(
                        (
                            f"{name} (resolved at editor launch)"
                            for name in ("VISUAL", "EDITOR")
                            if environ.get(name)
                        ),
                        "nvim (TUI fallback)",
                    )
                if path == "presentation.color" and entry["current"] == "auto":
                    effective_source += " · terminal detection and NO_COLOR"
                resolved_path = None
                if path == "storage.data_root":
                    selected = Path(settings.storage.data_root).expanduser()
                    resolved_path = str((self.context.paths.root / selected).resolve())
                self.fields.append(
                    {
                        **entry,
                        "configured": native(value),
                        "configured_path": source,
                        "effective": entry["current"],
                        "effective_source": effective_source,
                        "resolved_path": resolved_path,
                        "effective_known": self.checked is not None,
                        "purpose": spec.get("description", ""),
                        "choices": schema.get("enum", []),
                        "required": False,
                        "mutable": path != "version",
                        "kind": "enum"
                        if "enum" in schema
                        else {
                            "integer": "integer",
                            "number": "rate",
                            "boolean": "boolean",
                            "string": "path" if path == "storage.data_root" else "string",
                        }.get(str(schema.get("type")), "unknown"),
                        "constraint": {
                            k: v for k, v in schema.items() if k not in {"type", "enum"}
                        },
                    }
                )
            if self.checked is None:
                return
        self.checked = self.candidate
        self.status = "Validated"

    def patch(self, field: dict[str, Any], raw: str | None) -> None:
        path = field["path"]
        if self.source_error:
            raise MarketForgeError(self.source_error)
        if not editable(field):
            raise MarketForgeError(f"{path} is read-only.")
        if self.runtime:
            self.candidate = self.runtime.patch(self.candidate, path, raw)
        elif self.pipeline:
            self.candidate = self.pipeline.patch(self.candidate, path, raw)
        else:
            value: object = None
            if raw is not None:
                kind = field["kind"]
                if kind in {"enum", "rate", "boolean"}:
                    value = typed_value(field, raw)
                elif kind == "integer":
                    value = int(raw)
                else:
                    value = raw
            self.candidate = change_toml(self.candidate, path, value)

    def preview(self) -> Change | PipelineChange | ApplicationChange:
        if self.source_error:
            raise MarketForgeError(self.source_error)
        if self.runtime and self.original:
            return self.runtime.propose(self.original, self.candidate)
        if self.pipeline and self.original:
            return self.pipeline.propose(self.original, self.candidate)
        settings = validate_application(self.candidate, self.path)
        before = validate_application(self.initial, self.path)
        if settings.storage.data_root != before.storage.data_root:
            ProjectPaths.resolve(
                root=self.context.paths.root, data_root=Path(settings.storage.data_root)
            )
        old = parse_document(self.initial)
        new = parse_document(self.candidate)
        for version in (old.get("version", 1), new.get("version", 1)):
            if version != before.version:
                raise MarketForgeError("Application schema identity is immutable.")
        return ApplicationChange(
            self.candidate,
            [
                {
                    "path": entry["path"],
                    "before": prior["current"],
                    "after": entry["current"],
                    "explicit before": configured_at(old, str(entry["path"]))[1] is not None,
                    "explicit after": configured_at(new, str(entry["path"]))[1] is not None,
                }
                for prior, entry in zip(
                    application_fields(before), application_fields(settings), strict=True
                )
                if prior["current"] != entry["current"]
                or configured_at(old, str(entry["path"])) != configured_at(new, str(entry["path"]))
            ],
            settings,
        )

    def apply(
        self, change: Change | PipelineChange | ApplicationChange, *, acknowledged: bool
    ) -> None:
        if change.candidate != self.candidate:
            raise MarketForgeError("Draft changed after validation. Review it again.")
        if isinstance(change, Change) and self.runtime:
            self.runtime.apply(change, acknowledge_risk=acknowledged)
        elif isinstance(change, PipelineChange) and self.pipeline:
            self.pipeline.apply(change, acknowledge_risk=acknowledged)
        elif isinstance(change, ApplicationChange):
            from forge.historical.authoring import recipe_writer

            if self.original:
                with recipe_writer(self.path):
                    replace_entry(self.path, self.original, change.candidate)
            else:
                create_entry(self.path, change.candidate)
        else:
            raise MarketForgeError("Wrong configuration preview.")
