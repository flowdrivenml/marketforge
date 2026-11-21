"""Prepare inspectable conversion collections and retain immutable submission evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import platform
import subprocess
import tomllib
from pathlib import Path
from typing import Any
from uuid import uuid4

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
from forge.historical.conversion.planning import output_available, references
from forge.historical.conversion.recipes import load_recipe
from forge.historical.engine.managed import checksum, resolve
from forge.historical.merge.service import runtime_path
from forge.historical.normalization.service import write_job
from forge.shared.config.models import SettingsModel
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


def runtime_limits(data: bytes, requested: dict[str, int]) -> dict[str, int]:
    settings = tomllib.loads(data.decode())["resources"]
    memory = settings["memory_budget_bytes"]
    if type(memory) is not int or memory % (1024 * 1024):
        raise ValueError("Conversion runtime memory budget must be an integer number of MiB.")
    limits = {
        "workers": settings["workers"],
        "memory_limit_mb": memory // (1024 * 1024),
        "max_open_files": settings.get("normalization_limits", {}).get("open_files", 64),
    }
    if any(type(value) is not int or value <= 0 for value in limits.values()):
        raise ValueError("Converter runtime requires positive integer resource limits.")
    return {name: min(requested[name], limit) for name, limit in limits.items()}


def parquet_settings(data: bytes) -> Parquet:
    settings = tomllib.loads(data.decode())["resources"].get("parquet", {})
    mib = 1024 * 1024
    values = {}
    for name, default in (("row_group_target", 64), ("file_target", 256)):
        size = settings.get(f"{name}_bytes", default * mib)
        if type(size) is not int or size <= 0 or size % mib:
            raise ValueError(f"Conversion {name}_bytes must be a positive whole number of MiB.")
        values[f"{name}_mb"] = size // mib
    if values["file_target_mb"] < values["row_group_target_mb"]:
        raise ValueError("Conversion Parquet file target must be at least the row group target.")
    return Parquet.model_validate({**values, "compression_level": 3})


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


def validate_prepared_job(binary: Path, job: Path) -> str:
    result = subprocess.run(
        [str(binary), "--job", str(job), "--validate"],
        cwd=job.parent,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if result.returncode:
        raise MarketForgeError(f"Engine validation failed for {job}: {result.stderr.strip()}")
    return result.stdout


async def prepare(paths: ProjectPaths, source: Path, *, dry_run: bool = False) -> dict[str, Any]:
    directory = paths.runs_dir / f"convert-prepare-{uuid4().hex}"
    report: dict[str, Any] = {
        "status": "blocked",
        "compatibility": "not_validated",
        "dry_run": dry_run,
        "recipe_path": str(source),
        "blockers": [],
    }
    documents: list[tuple[Path, bytes]] = []
    try:
        recipe, upstream, recipe_data = load_recipe(paths, source)
        report.update(
            recipe=file_pin(source, recipe_data),
            upstream=recipe.source.model_dump(),
            requested_interval=recipe.interval.model_dump(),
            selection=recipe.selection.model_dump(),
            settings=recipe.conversion.model_dump(),
            resources=recipe.resources.model_dump(),
            upstream_interval=upstream.interval.model_dump(),
            note="Observed endpoints do not establish continuous coverage. Routes do not establish reference observations or freshness.",
            execution_scope="whole_merge_publication",
        )
        discovery = await discover(paths, recipe, upstream)
        report.update(discovery)
        if report["blockers"]:
            return finish_report(report, directory, dry_run)
        sources: list[dict[str, Any]] = discovery["sources"]
        external: dict[str, Any] | None = discovery["external_reference"]
        snapshot = catalog_snapshot([*sources, *([external] if external else [])])
        artifact_pins: dict[str, Any] = {}
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
            binary = resolve(paths, "convert")
            artifact_pins["executable"] = {"path": str(binary), "sha256": checksum(binary)}
        except (MarketForgeError, OSError) as error:
            compatibility.append(str(error))
        if platform.system() != "Linux":
            compatibility.append(
                "The current Rust converter supports atomic publication only on Linux."
            )
        runtime = runtime_path(paths)
        runtime_data = runtime.read_bytes()
        limits = runtime_limits(runtime_data, recipe.resources.model_dump())
        report["effective_resource_limits"] = limits
        artifact_pins["runtime"] = file_pin(directory / "general.toml", runtime_data)
        report["runtime_origin"] = str(runtime)
        documents.append((directory / "general.toml", runtime_data))
        parquet = parquet_settings(runtime_data)
        external_binding = prepared_binding(external) if external else None

        entries: list[dict[str, Any]] = []
        for publication in sources:
            selected = references(publication, external, recipe.reference_stream_ids)
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
                    "engine_validation": "not_run",
                }
            )
        upstream_data = await asyncio.to_thread(Path(upstream.declaration.path).read_bytes)
        if hashlib.sha256(upstream_data).hexdigest() != upstream.declaration.sha256:
            raise MarketForgeError("Upstream recipe changed during conversion preparation.")
        for name, filename, data in (
            ("catalog_snapshot", "instruments.json", snapshot_data),
            ("recipe_snapshot", "recipe.toml", recipe_data),
            ("upstream_snapshot", "upstream.toml", upstream_data),
        ):
            path = directory / filename
            artifact_pins[name] = file_pin(path, data)
            documents.append((path, data))
        collection_path = directory / "convert.json"
        if dry_run:
            report.update(
                status="generated",
                job_path=None,
                compatibility_blockers=compatibility,
                validation="Dry run computed job specifications without writing or invoking the Engine.",
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

        if binary is not None and "executable" in artifact_pins:
            for entry in entries:
                try:
                    entry["validation_output"] = await asyncio.to_thread(
                        validate_prepared_job, binary, Path(entry["path"])
                    )
                    entry["engine_validation"] = "passed"
                except (MarketForgeError, OSError, subprocess.TimeoutExpired) as error:
                    entry["engine_validation"] = "failed"
                    compatibility.append(str(error))

        pinned = {
            "version": 1,
            "type": COLLECTION,
            "environment": "testing",
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
        set(document)
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
        or document.get("environment") != "testing"
        or not isinstance(document.get("jobs"), list)
        or not document["jobs"]
    ):
        raise MarketForgeError(
            "Expected a retained Forge conversion collection or its exact per-job document."
        )
    return collection, document, report
