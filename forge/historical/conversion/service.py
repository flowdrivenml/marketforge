"""Prepare inspectable conversion collections and retain immutable submission evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import platform
import subprocess
from pathlib import Path
from typing import Any, Literal, Self, cast
from uuid import uuid4

from pydantic import model_validator

from forge.datasets.discovery import read_object
from forge.historical.conversion.contract import (
    Currency,
    Identity,
    Parquet,
    PublicationBinding,
    Resources,
    build_job,
)
from forge.historical.conversion.discovery import discover
from forge.historical.conversion.evidence import check_source_environment
from forge.historical.conversion.planning import output_available
from forge.historical.conversion.recipes import LoadedSource, PublicationSource, load_recipe
from forge.historical.conversion.resolution import resolve_references
from forge.historical.engine.managed import checksum, resolve
from forge.historical.engine.process import Lifecycle, check, joined_io
from forge.historical.engine.runtime import read_document
from forge.historical.normalization.service import write_job
from forge.shared.config.loader import configuration_errors
from forge.shared.config.models import SettingsModel
from forge.shared.database.connection import database_purpose, pinned_registry, selected_registry_id
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

COLLECTION = "forge-conversion-collection"


class CatalogEntry(Identity, frozen=True):
    base_currency: Currency
    quote_currency: Currency


class Catalog(SettingsModel, frozen=True):
    version: int
    instruments: list[CatalogEntry]


def catalog_snapshot(sources: list[dict[str, Any]]) -> Catalog:
    items: dict[Identity, CatalogEntry] = {}
    for source in sources:
        for stream in source["streams"]:
            identity = Identity.model_validate(stream["identity"])
            entry = CatalogEntry(
                **identity.model_dump(),
                base_currency=stream["base_currency"],
                quote_currency=stream["quote_currency"],
            )
            if items.setdefault(identity, entry) != entry:
                raise MarketForgeError(
                    f"Conflicting historical currencies for {identity.exchange} {identity.symbol}."
                )
    return Catalog(
        version=1,
        instruments=sorted(
            items.values(),
            key=lambda item: (
                item.exchange,
                item.symbol,
                item.instrument_kind,
                item.contract_kind or "",
            ),
        ),
    )


def catalog_pin(paths: ProjectPaths, snapshot: Catalog) -> dict[str, str] | None:
    path = paths.root / "data/catalog/instruments.json"
    if not path.exists():
        return None
    if path.resolve() != path or not path.is_file():
        raise MarketForgeError(f"Invalid preparation catalog path {path}.")
    data = path.read_bytes()
    installed = Catalog.model_validate_json(data)
    identities = {
        Identity.model_validate(item.model_dump(exclude={"base_currency", "quote_currency"}))
        for item in installed.instruments
    }
    if (
        installed.version != 1
        or len(identities) != len(installed.instruments)
        or any(item.base_currency == item.quote_currency for item in installed.instruments)
        or not set(snapshot.instruments) <= set(installed.instruments)
    ):
        raise MarketForgeError(
            f"Fixed engine catalog {path} disagrees with the pinned historical instrument metadata."
        )
    return {"path": str(path), "sha256": hashlib.sha256(data).hexdigest()}


def file_pin(path: Path, data: bytes) -> dict[str, str]:
    return {"path": str(path), "sha256": hashlib.sha256(data).hexdigest()}


def json_bytes(document: dict[str, Any]) -> bytes:
    return (json.dumps(document, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode()


def finish_report(report: dict[str, Any], directory: Path, dry_run: bool) -> dict[str, Any]:
    if not dry_run:
        report["report_path"] = str(directory / "prepare-report.json")
        write_job(directory / "prepare-report.json", report)
    return report


class ConversionRuntime(SettingsModel, frozen=True):
    """The runtime supplies ceilings in the prepared conversion job's units."""

    schema_version: Literal[1]
    resources: Resources
    parquet: Parquet

    @model_validator(mode="after")
    def limits(self) -> Self:
        omitted = {"workers", "memory_limit_mb"} - self.resources.model_fields_set
        if omitted:
            raise ValueError("Missing resources fields: " + ", ".join(sorted(omitted)))
        if self.parquet.row_group_target_mb > self.parquet.file_target_mb:
            raise ValueError("parquet.row_group_target_mb cannot exceed file_target_mb")
        for field, count in (
            ("resources.memory_limit_mb", self.resources.memory_limit_mb),
            ("parquet.file_target_mb", self.parquet.file_target_mb),
        ):
            if count >= 2**44:
                raise ValueError(f"{field} must fit in unsigned 64-bit bytes")
        return self


def load_runtime(source: Path, data: bytes) -> ConversionRuntime:
    with configuration_errors(source):
        document = read_document(source, data)
        if type(document.get("schema_version")) is not int:
            raise MarketForgeError(f"{source}: schema_version must be the integer 1")
        parquet = document.get("parquet")
        if isinstance(parquet, dict) and "compression_level" not in parquet:
            document["parquet"] = {"compression_level": 3, **cast(dict[str, Any], parquet)}
        return ConversionRuntime.model_validate(document)


def prepared_binding(publication: dict[str, Any]) -> PublicationBinding:
    root = Path(publication["root"])
    index = (root / "publication-checksums.json").read_bytes()
    if json.loads(index) != publication["fingerprints"]:
        raise MarketForgeError(f"Publication checksum inventory changed at {root}.")
    fields = (
        "stream_id",
        "stream_rank",
        "kind",
        "instrument_id",
        "instrument_namespace",
        "identity",
        "base_currency",
        "quote_currency",
    )
    return PublicationBinding.model_validate(
        {
            "merge_job_id": int(publication["engine_publication_id"]),
            "manifest_sha256": publication["fingerprints"]["manifest.json"],
            "publication_checksums_sha256": hashlib.sha256(index).hexdigest(),
            "normalized_inputs": [
                {
                    "dataset_id": int(item["engine_publication_id"]),
                    "manifest_sha256": item["manifest_sha256"],
                    "processing_job_sha256": item["processing_job_sha256"],
                }
                for item in sorted(
                    publication["normalized_inputs"],
                    key=lambda item: int(item["engine_publication_id"]),
                )
            ],
            "streams": [
                {"dataset_id": int(item["dataset_id"]), **{field: item[field] for field in fields}}
                for item in sorted(publication["streams"], key=lambda item: item["stream_rank"])
            ],
        }
    )


def validate_prepared_job(binary: Path, job: Path, plan: Path | None = None) -> str:
    argv = [str(binary), "--job", str(job), "--validate"]

    def run(command: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                command, cwd=job.parent, capture_output=True, text=True, check=False, timeout=60
            )
        except subprocess.TimeoutExpired as error:
            raise MarketForgeError(f"Engine validation timed out for {job}.") from error

    result = run([*argv, "--validation-plan", str(plan)] if plan else argv)
    if plan and result.returncode and "unknown argument: --validation-plan" in result.stderr:
        result = run(argv)
    if result.returncode:
        raise MarketForgeError(f"Engine validation failed for {job}: {result.stderr.strip()}")
    return result.stdout


async def prepare(
    paths: ProjectPaths,
    source: Path,
    *,
    dry_run: bool = False,
    embedded: bool = False,
    executable: Path | None = None,
    runtime_path: Path | None = None,
    lifecycle: Lifecycle = lambda _: None,
) -> dict[str, Any]:
    directory = paths.runs_dir / f"convert-prepare-{uuid4().hex}"
    report: dict[str, Any] = {
        "status": "blocked",
        "compatibility": "not_validated",
        "dry_run": dry_run,
        "environment": database_purpose(),
        "recipe_path": str(source),
        "blockers": [],
    }
    documents: list[tuple[Path, bytes]] = []
    try:
        recipe, upstream, recipe_data = await joined_io(
            lambda: load_recipe(paths, source, retain=not dry_run)
        )
        report.update(
            recipe=file_pin(source, recipe_data),
            upstream=recipe.source.model_dump(mode="json"),
            requested_interval=recipe.interval.model_dump(),
            selection=recipe.selection.model_dump(),
            settings=recipe.conversion.model_dump(),
            resources=recipe.resources.model_dump(),
            upstream_interval=upstream.interval.model_dump(),
            reference_selection=recipe.selection_policy(recipe.version).model_dump(
                mode="json", exclude_none=True
            ),
            note="Observed endpoints do not establish continuous coverage. Routes do not establish reference observations or freshness.",
            execution_scope="whole_merge_publication",
        )
        if isinstance(upstream, PublicationSource):
            check_source_environment(upstream)
        registry = await selected_registry_id(paths.root)
        report["registry_id"] = registry
        with pinned_registry(registry):
            discovery = await discover(paths, recipe, upstream)
        report.update(discovery)
        if report["blockers"]:
            return finish_report(report, directory, dry_run)
        sources: list[dict[str, Any]] = discovery["sources"]
        external: dict[str, Any] | None = discovery["external_reference"]
        selection = recipe.selection_policy(recipe.version)
        resolutions: list[dict[str, Any]] = []
        report["reference_resolutions"] = resolutions
        selections: list[list[dict[str, Any]]] = []
        for publication in sources:
            resolution: dict[str, Any] = {"validation": "not_run"}
            resolutions.append(resolution)
            try:
                with pinned_registry(registry):
                    selected = await resolve_references(
                        paths,
                        publication,
                        external,
                        recipe.conversion,
                        selection,
                        recipe.reference_stream_ids,
                        resolution,
                    )
            except MarketForgeError as error:
                resolution["failure"] = str(error)
                raise
            selections.append(selected)
        snapshot = catalog_snapshot([*sources, *([external] if external else [])])
        artifact_pins: dict[str, Any] = {}
        if isinstance(upstream, PublicationSource):
            sources[0]["publication_source"] = upstream.model_dump(mode="json")
        compatibility: list[str] = []
        snapshot_data = json_bytes(snapshot.model_dump())
        installed_catalog = catalog_pin(paths, snapshot)
        if installed_catalog:
            artifact_pins["preparation_catalog"] = installed_catalog
        catalog_digest = (
            installed_catalog["sha256"]
            if installed_catalog
            else hashlib.sha256(snapshot_data).hexdigest()
        )
        binary = None
        try:
            binary = executable or resolve(paths, "convert")
            artifact_pins["executable"] = {"path": str(binary), "sha256": checksum(binary)}
        except (MarketForgeError, OSError) as error:
            compatibility.append(str(error))
        if platform.system() != "Linux":
            compatibility.append(
                "The current Rust converter supports atomic publication only on Linux."
            )
        runtime = runtime_path or paths.runtime_config("convert")
        runtime_data = runtime.read_bytes()
        settings = load_runtime(runtime, runtime_data)
        requested = recipe.resources.model_dump()
        limits = {
            name: min(requested[name], limit)
            for name, limit in settings.resources.model_dump().items()
        }
        report["effective_resource_limits"] = limits
        artifact_pins["runtime"] = file_pin(directory / "convert.toml", runtime_data)
        report["runtime_origin"] = str(runtime)
        documents.append((directory / "convert.toml", runtime_data))
        parquet = settings.parquet
        external_binding = prepared_binding(external) if external else None

        from forge.historical.conversion.execution import verify_reused_conversion
        from forge.historical.engine.work import (
            READY_MESSAGE,
            WorkIndex,
            convert_key,
            output_present,
        )

        entries: list[dict[str, Any]] = []
        ready: list[dict[str, Any]] = []
        work_index = WorkIndex(paths.root, registry, "convert", directory.name)
        for publication, selected, resolution in zip(sources, selections, resolutions, strict=True):
            identity = uuid4().int & (2**63 - 1)
            destination = (paths.datasets_dir / "converted" / str(identity)).absolute()
            output_available(destination)
            job = build_job(
                identity,
                Path(publication["root"]),
                destination,
                recipe.conversion,
                Resources.model_validate(limits),
                [Identity.model_validate(stream["identity"]) for stream in selected],
                Path(external["root"]) if external else None,
                source_binding=prepared_binding(publication),
                external_binding=external_binding,
                catalog_sha256=catalog_digest,
                parquet=parquet,
            )
            job_path = directory / f"job-{job.job_id}.json"
            data = json_bytes(job.model_dump())
            documents.append((job_path, data))
            entries.append(
                {
                    **file_pin(job_path, data),
                    "job_id": job.job_id,
                    "output": str(destination),
                    "source_registry_publication_id": publication["registry_publication_id"],
                    "source_engine_publication_id": publication["engine_publication_id"],
                    "references": selected,
                    "reference_resolution": resolution,
                    "engine_validation": "not_run",
                }
            )
            if binary is not None:
                key = convert_key(
                    registry, artifact_pins["executable"]["sha256"], job.model_dump(mode="json")
                )
                try:
                    candidates = await work_index.peek([key])
                    states = report.setdefault("reuse_plan", {})
                    states[str(job.job_id)] = {"work_key": key, "status": "would_execute"}
                    if key in candidates:
                        owner, claim, previous = candidates[key]
                        if previous is None:
                            present = await joined_io(output_present, Path(claim["path"]))
                            states[str(job.job_id)].update(
                                status="needs_publication_recovery" if present else "in_progress",
                                attempt=owner,
                            )
                        else:
                            await verify_reused_conversion(binary, publication, previous)
                            states[str(job.job_id)].update(
                                status="would_reuse", path=previous["publication"]["manifest"]
                            )
                            ready.append(
                                {
                                    **previous,
                                    "disposition": "reused",
                                    "reference_resolution": resolution,
                                    "reuse_message": READY_MESSAGE,
                                }
                            )
                except (MarketForgeError, OSError, ValueError, KeyError) as error:
                    report.setdefault("reuse_plan", {})[str(job.job_id)] = {
                        "work_key": key,
                        "status": "blocked",
                        "reason": str(error),
                    }
        if ready and len(ready) == len(entries) and not dry_run and not compatibility:
            for entry in ready:
                entry["reference_resolution"]["validation"] = "verified_reused_output"
            report.update(
                status="complete", disposition="reused", execution="not_started", jobs=ready
            )
            report["report_path"] = str(directory / "reuse-result.json")
            write_job(Path(report["report_path"]), report)
            return report
        snapshots = [
            ("catalog_snapshot", "instruments.json", snapshot_data),
            ("recipe_snapshot", "recipe.toml", recipe_data),
        ]
        if isinstance(upstream, LoadedSource):
            upstream_data = await asyncio.to_thread(Path(upstream.declaration.path).read_bytes)
            if hashlib.sha256(upstream_data).hexdigest() != upstream.declaration.sha256:
                raise MarketForgeError("Upstream recipe changed during conversion preparation.")
            snapshots.append(("upstream_snapshot", "upstream.toml", upstream_data))
        for name, filename, data in snapshots:
            path = directory / filename
            artifact_pins[name] = file_pin(path, data)
            documents.append((path, data))
        collection_path = directory / "convert.json"
        if dry_run:
            report.update(
                status="generated",
                job_path=None,
                compatibility_blockers=compatibility,
                validation="Dry run computed job specifications without writing job artifacts or running Rust job validation.",
                jobs=[
                    {
                        **{
                            key: value
                            for key, value in entry.items()
                            if key not in {"path", "sha256"}
                        },
                        "intended_job_path": entry["path"],
                    }
                    for entry in entries
                ],
            )
            return report

        directory.mkdir(parents=True, exist_ok=False)
        for path, data in documents:
            with path.open("xb") as stream:
                stream.write(data)

        if binary is not None:
            lifecycle("validate")
            for entry in entries:
                try:
                    prepared = Path(entry["path"])
                    plan_path = prepared.with_suffix(".validation-plan.json")
                    if embedded:
                        argv = [str(binary), "--job", str(prepared), "--validate"]
                        result = await check(
                            [*argv, "--validation-plan", str(plan_path)],
                            prepared.parent,
                            directory,
                            f"validation-{entry['job_id']}",
                        )
                        if (
                            result.returncode
                            and "unknown argument: --validation-plan" in result.stdout
                        ):
                            result = await check(
                                argv,
                                prepared.parent,
                                directory,
                                f"compatibility-validation-{entry['job_id']}",
                            )
                        if result.returncode:
                            raise MarketForgeError(
                                f"Engine validation failed for {prepared}: {result.stdout}"
                            )
                        entry["validation_output"] = result.stdout
                    else:
                        entry["validation_output"] = await joined_io(
                            validate_prepared_job, binary, prepared, plan_path
                        )
                    if plan_path.is_file():
                        plan = read_object(plan_path)
                        if plan.get("schema_version") != 1:
                            raise MarketForgeError(f"Unsupported validation plan: {plan_path}")
                        entry["planned_routes"] = plan["streams"]
                    entry["engine_validation"] = "passed"
                    entry["reference_resolution"]["validation"] = "passed"
                except (MarketForgeError, OSError, subprocess.TimeoutExpired) as error:
                    entry["engine_validation"] = "failed"
                    entry["reference_resolution"]["validation"] = "failed"
                    compatibility.append(str(error))

        pinned = {
            "version": 1,
            "type": COLLECTION,
            "environment": database_purpose(),
            "registry_id": registry,
            "jobs": entries,
            "pins": artifact_pins,
            "sources": sources,
            "external_reference": external,
            "compatibility_blockers": list(dict.fromkeys(compatibility)),
        }
        collection_bytes = json_bytes(pinned)
        with collection_path.open("xb") as stream:
            stream.write(collection_bytes)
        validated = all(entry["engine_validation"] == "passed" for entry in entries)
        report.update(
            status="generated",
            environment=database_purpose(),
            registry_id=registry,
            job_path=str(collection_path),
            compatibility="validated" if validated and not compatibility else "not_validated",
            compatibility_blockers=pinned["compatibility_blockers"],
            validation=(
                "Production Engine validated every generated job and its physical inputs."
                if validated
                else "Generated job schema; see compatibility blockers for Engine validation."
            ),
            jobs=entries,
            artifacts={**artifact_pins, "collection": file_pin(collection_path, collection_bytes)},
        )
    except asyncio.CancelledError:
        if not embedded:
            raise
        report.update(status="interrupted")
    except (MarketForgeError, OSError, ValueError, KeyError, TypeError) as error:
        report["blockers"].append(str(error))
        return finish_report(report, directory, dry_run)
    return finish_report(report, directory, dry_run)


def retained_collection(source: Path) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    collection = source if source.name == "convert.json" else source.with_name("convert.json")
    report_path = collection.with_name("prepare-report.json")
    if any(path.resolve() != path for path in (collection, report_path)):
        raise MarketForgeError(
            "Retained conversion collection and report must use their original paths."
        )
    report = read_object(report_path)
    data = collection.read_bytes()
    if report.get("artifacts", {}).get("collection") != file_pin(collection, data):
        raise MarketForgeError("Retained conversion collection or report fingerprint changed.")
    document = read_object(collection)
    if (
        set(document) - {"registry_id"}
        != {
            "type",
            "version",
            "environment",
            "jobs",
            "pins",
            "sources",
            "external_reference",
            "compatibility_blockers",
        }
        or document.get("type") != COLLECTION
        or document.get("version") != 1
        or document.get("environment") not in ("production", "testing")
        or not isinstance(document.get("jobs"), list)
        or not document["jobs"]
    ):
        raise MarketForgeError(
            "Expected a retained Forge conversion collection or its exact per-job document."
        )
    return collection, document, report


def collection_publication_source(document: dict[str, Any]) -> PublicationSource | None:
    sources = document["sources"]
    bound = [source["publication_source"] for source in sources if "publication_source" in source]
    if not bound:
        return None
    if len(sources) != 1:
        raise MarketForgeError("A publication-bound collection must have exactly one source.")
    return PublicationSource.model_validate(bound[0])


def validate_reference(paths: ProjectPaths, source: Path) -> dict[str, Any]:
    """Validate either an exact Rust job or a Forge collection and its pinned jobs."""
    from forge.historical.conversion.contract import Job
    from forge.historical.conversion.execution import read_pin

    document = read_object(source)
    if document.get("type") == COLLECTION or (
        source.name.startswith("job-") and source.with_name("convert.json").is_file()
    ):
        collection, retained, _ = retained_collection(source)
        entries = [
            entry
            for entry in retained["jobs"]
            if source == collection or entry["path"] == str(source)
        ]
        if not entries:
            raise MarketForgeError("Per-job document is absent from the retained collection.")
        jobs = [
            read_pin(entry, collection.parent, expected=f"job-{entry['job_id']}.json")[0]
            for entry in entries
        ]
        provenance = "verified collection and job fingerprints"
    else:
        jobs = [source]
        provenance = "standalone job"
    binary = resolve(paths, "convert")
    diagnostics: list[str] = []
    for job in jobs:
        Job.model_validate(read_object(job))
        diagnostics.append(validate_prepared_job(binary, job))
    return {
        "status": "valid",
        "path": str(source),
        "validation": "\n".join(diagnostics),
        "validation_layers": {
            "structure": "valid",
            "provenance": provenance,
            "engine": "passed",
        },
        "validated_job_count": len(jobs),
    }
