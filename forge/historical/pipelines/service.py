"""Durable, fixed acquire → normalize → merge pipeline."""

import asyncio
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from rich.text import Text

from forge.historical.archives.operation import OperationReport, run_acquisition
from forge.historical.archives.progress import ProgressSnapshot
from forge.historical.archives.selection import ArchiveSelection, ArchiveSelectionError
from forge.historical.archives.storage.files import contained
from forge.historical.conversion import recipes as conversion_recipes
from forge.historical.conversion import service as conversion
from forge.historical.conversion.evidence import inspect_publication_source
from forge.historical.conversion.execution import execute as execute_conversion
from forge.historical.engine.managed import checksum, inventory, resolve
from forge.historical.engine.process import EngineRunner, PreparationObserver, joined_io, supervise
from forge.historical.engine.runtime import compose_pipeline
from forge.historical.merge import service as merge
from forge.historical.normalization import execution as normalize
from forge.historical.normalization.batch import (
    BatchPlan,
    allocate,
    prepare_batch,
    publish_batch_async,
    used_ids,
)
from forge.historical.normalization.batch import (
    validate_candidate as validate_normalization_candidate,
)
from forge.historical.normalization.service import default_output_paths, write_job
from forge.historical.pipelines.config import PipelineRecipe, load_recipe
from forge.historical.pipelines.progress import Activity, PipelineSnapshot, StageSnapshot
from forge.historical.pipelines.terminal import (
    STAGE_LABELS,
    EngineRequest,
    StageWindows,
    WindowCompletion,
    WindowLost,
    WindowUnavailable,
)
from forge.orchestration.artifacts import RunArtifacts
from forge.orchestration.registry import begin, complete_call, show_index, status_of
from forge.shared.config.models import AppSettings
from forge.shared.database.connection import (
    DatabaseError,
    database_connection,
    database_dsn,
    database_environment,
    database_purpose,
    pinned_registry,
    require_registry,
    selected_registry_id,
)
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation, new_attempt_id, new_run_id, parse_run_id
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.acquisition import AcquisitionDisplay
from forge.startup.context import ApplicationContext


class Blocked(MarketForgeError):
    """The run cannot continue without resolving an explicit prerequisite."""


@dataclass(frozen=True, slots=True)
class RegisteredFiles:
    ids: tuple[int, ...]

    @classmethod
    def from_receipt(cls, receipt: dict[str, Any]) -> "RegisteredFiles":
        if receipt.get("registration") != "registered" or not receipt.get("catalogue_dataset_ids"):
            raise Blocked("Required publication registration is incomplete; see the stage receipt.")
        ids = tuple(sorted(receipt["catalogue_dataset_ids"].values()))
        if any(type(value) is not int or value <= 0 for value in ids) or len(set(ids)) != len(ids):
            raise Blocked("Registration returned invalid primary-file identities.")
        return cls(ids)


def now() -> str:
    return datetime.now(UTC).isoformat()


def read(path: Path) -> dict[str, Any]:
    return merge.read_json(path)


def output_present(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def retained(path: Path, digest: str) -> Path:
    if not path.is_file() or checksum(path) != digest:
        raise Blocked(f"Retained artifact changed or is unavailable: {path}")
    return path


async def registry_identity(paths: ProjectPaths) -> str:
    return await selected_registry_id(paths.root)


def artifacts_for(paths: ProjectPaths, run_id: str) -> RunArtifacts:
    try:
        identity = parse_run_id(run_id)
    except ValueError as error:
        raise Blocked("Expected a retained pipeline run ID.") from error
    if identity.operation not in {None, RunOperation.PIPELINE}:
        raise Blocked("Expected a pipeline run reference.")
    return RunArtifacts.for_run(paths, identity)


def status(paths: ProjectPaths, run_id: str) -> dict[str, Any]:
    report = read(artifacts_for(paths, run_id).report)
    if report.get("kind") != "historical_pipeline" or report.get("schema_version") != 1:
        raise Blocked("Expected a version 1 historical pipeline report.")
    return report


def retained_recipe_metadata(paths: ProjectPaths, report: dict[str, Any]) -> dict[str, Any]:
    """Read the fingerprinted resolved plan for human inspection when available."""
    if not report.get("resolved_sha256"):
        return {}
    try:
        artifacts = artifacts_for(paths, report["run_id"])
        return read(retained(artifacts.resolved_plan, report["resolved_sha256"])).get("recipe", {})
    except (MarketForgeError, OSError, KeyError):
        return {}


async def execution_index(paths: ProjectPaths, result: dict[str, Any]) -> str:
    """Inspect bookkeeping without replacing the retained processing outcome."""
    if result.get("execution_registry_status") == "finalized":
        return "Finalized"
    if result.get("execution_registry_warning"):
        return "reconciliation required"
    purpose = result.get("environment")
    if purpose not in {"testing", "production"} or not result.get("registry_id"):
        return "Registry identity unavailable"
    try:
        with (
            database_environment(testing=purpose == "testing"),
            pinned_registry(result["registry_id"]),
        ):
            record = await show_index(paths, result["run_id"])
    except (MarketForgeError, OSError):
        return "Registry unavailable"
    if record is None:
        return "Run absent from the selected index"
    if record["status"] == "running" and result["state"] != "running":
        return "reconciliation required"
    if record["status"] == "running":
        return "Running"
    if record["status"] != status_of(result, record["action"]):
        return f"Indexed · {record['status']} (differs from retained processing state)"
    return f"Finalized · {record['status']}"


class PipelineRun:
    def __init__(
        self,
        context: ApplicationContext,
        artifacts: RunArtifacts,
        report: dict[str, Any],
        recipe: PipelineRecipe,
    ) -> None:
        self.context = context
        self.paths = context.paths
        self.artifacts = artifacts
        self.report = report
        self.recipe = recipe
        self.acquisition: ProgressSnapshot | None = None
        self.interrupting = False
        self.windows: StageWindows | None = None
        self.stage_window = False
        self.window_stage: str | None = None
        self.window_completion: WindowCompletion | None = None
        self.window_task: asyncio.Task[dict[str, Any]] | None = None
        self.acquisition_display: AcquisitionDisplay | None = None
        self.historical = artifacts.root / "historical"

    def snapshot(self) -> PipelineSnapshot:
        report = read(self.artifacts.report) if self.window_stage == "acquire" else self.report
        stages: list[StageSnapshot] = []
        for name, stage in report["stages"].items():
            counts = stage.get("result", {}).get("counts", {})
            verified = (
                (counts["verified_publications"], counts["requested"])
                if ("verified_publications" in counts and "requested" in counts)
                else None
            )
            terminal = "Closed" if stage.get("terminal_outcome") else None
            if self.windows and self.window_stage == name:
                launcher = self.windows.launcher.name
                terminal = (
                    f"{launcher} window"
                    if self.windows.started_stage == name
                    else f"Opening {launcher}"
                )
            stages.append(
                StageSnapshot(
                    name,
                    stage["state"],
                    stage.get("phase", "pending"),
                    stage.get("started_at"),
                    stage.get("finished_at"),
                    stage.get("summary", ""),
                    verified,
                    terminal,
                )
            )
        return PipelineSnapshot(
            report["name"],
            report["run_id"],
            report["environment"],
            (self.recipe.interval.start, self.recipe.interval.end),
            "interrupting"
            if self.interrupting and report["state"] == "running"
            else report["state"],
            report["started_at"],
            report.get("finished_at"),
            tuple(stages),
            self.window_stage or report.get("active_stage"),
            tuple(
                Activity(item["stage"], item["phase"], item["state"]) for item in report["recent"]
            ),
            self.acquisition,
            report.get("error"),
        )

    def save(self) -> None:
        with tempfile.TemporaryDirectory(prefix=".state-", dir=self.artifacts.root) as temporary:
            snapshot = Path(temporary) / "report.json"
            write_job(snapshot, self.report)
            os.replace(snapshot, self.artifacts.report)
            descriptor = os.open(self.artifacts.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def stage(self, name: str) -> dict[str, Any]:
        return self.report["stages"][name]

    def transition(self, name: str, phase: str, state: str, **evidence: Any) -> None:
        stage = self.stage(name)
        timestamp = now()
        if stage.get("phase") != phase:
            previous = stage.get("phase")
            if previous and stage["state"] == "running":
                stage["phases"][previous].update(state="complete", finished_at=timestamp)
            stage["phases"][phase] = {"state": state, "started_at": timestamp}
        stage["phases"][phase]["state"] = state
        if state != "running":
            stage["phases"][phase]["finished_at"] = timestamp
            stage["finished_at"] = timestamp
        else:
            stage.pop("finished_at", None)
        stage.setdefault("started_at", timestamp)
        stage.update(phase=phase, state=state, **evidence)
        if self.stage_window and (name != "acquire" or state != "running"):
            self.context.error_console.print(
                Text(f"{STAGE_LABELS[name]} · {phase.replace('_', ' ')} · {state}")
            )
        self.report["active_stage"] = name
        event = {"timestamp": timestamp, "stage": name, "phase": phase, "state": state}
        activity = self.report["recent"]
        activity.append(event)
        del activity[:-16]
        self.save()
        with self.artifacts.events.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def new_attempt(self, name: str) -> Path:
        attempt = str(new_attempt_id())
        directory = self.artifacts.steps_dir / name / attempt
        self.stage(name).setdefault("attempts", []).append(
            {"id": attempt, "directory": str(directory)}
        )
        self.save()
        return directory

    def engine(self, name: str) -> Path:
        item = self.report["engines"][name]
        return retained(Path(item["path"]), item["sha256"])

    def freeze(self) -> None:
        self.historical.mkdir(parents=True, exist_ok=True)
        snapshots: dict[str, str] = {}
        profile_root = self.historical / "profiles"
        profile_root.mkdir(exist_ok=True)
        for item in normalize.profile_inventory(self.paths, None):
            source = Path(item["location"])
            target = profile_root / item["id"].rsplit(".", 1)[1] / source.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            if checksum(target) != item["sha256"]:
                raise Blocked(f"Profile changed during the snapshot: {source}")
            snapshots[str(target)] = item["sha256"]
        engines: dict[str, Any] = {}
        installations = {item["target"]: item for item in inventory(self.paths)["targets"]}
        targets = [name for name in self.recipe.stage_order() if name != "acquire"]
        for name in targets:
            source = resolve(self.paths, name)
            target = self.artifacts.root / source.name
            digest = checksum(source)
            shutil.copyfile(source, target)
            target.chmod(0o755)
            if checksum(target) != digest:
                raise Blocked(f"Managed {name} executable changed during the snapshot.")
            engines[name] = {
                "path": str(target),
                "sha256": digest,
                "monitor_tui": "monitor-tui"
                in installations[name]["installation"].get("features", []),
            }
        runtime = compose_pipeline(
            self.paths,
            self.historical,
            {name: Path(engines[name]["path"]) for name in ("normalize", "merge")},
        )
        if self.recipe.convert is not None:
            source = self.paths.runtime_config("convert")
            data = source.read_bytes()
            conversion.load_runtime(source, data)
            (self.historical / "convert.toml").write_bytes(data)
        for name in engines:
            path = self.historical / f"{name}.toml"
            snapshots[str(path)] = checksum(path)
        self.report.update(settings_files=snapshots, engines=engines, runtime=runtime)
        self.save()

    def inspect_preparation(self, stage: dict[str, Any]) -> Path:
        past = stage["prepared"]
        retained(Path(past["report"]), past["report_sha256"])
        return retained(Path(past["job"]), past["job_sha256"])

    def on_candidate(self, name: str) -> PreparationObserver:
        def save(job: Path, report: Path) -> None:
            stage = self.stage(name)
            reference = {
                "job": str(job),
                "job_sha256": checksum(job),
                "report": str(report),
                "report_sha256": checksum(report),
            }
            stage["candidate"] = reference
            stage.setdefault("candidates", []).append(reference)
            self.save()

        return save

    def candidate(self, name: str) -> tuple[Path, dict[str, Any]] | None:
        reference = self.stage(name).get("candidate")
        if reference is None:
            return None
        job = retained(Path(reference["job"]), reference["job_sha256"])
        generated = retained(Path(reference["report"]), reference["report_sha256"])
        completed = job.parent / "prepare-report.json"
        if completed.is_file():
            report = read(completed)
            candidate = read(generated)
            mutable = {
                "status",
                "report_path",
                "engine_ready",
                "engine_compatibility",
                "compatibility",
            }
            if any(
                report.get(key) != value for key, value in candidate.items() if key not in mutable
            ):
                raise Blocked("Completed preparation disagrees with the retained candidate.")
            # The final report can exist if Forge stopped between validation and checkpointing.
            return job, report
        return job, read(generated)

    def save_preparation(self, stage: dict[str, Any], result: dict[str, Any], job_key: str) -> None:
        job, report = Path(result[job_key]), Path(result["report_path"])
        stage["prepared"] = {
            "job": str(job),
            "job_sha256": checksum(job),
            "report": str(report),
            "report_sha256": checksum(report),
        }
        stage.setdefault("preparations", []).append(stage["prepared"])
        stage.pop("candidate", None)
        stage.pop("execution_intent", None)
        stage.pop("execution_intent_sha256", None)
        self.save()

    async def acquire(self) -> ArchiveSelection:
        stage = self.stage("acquire")
        if stage["state"] != "complete":
            prior = None
            if stage.get("child_report"):
                prior_path = retained(Path(stage["child_report"]), stage["child_report_sha256"])
                prior = cast(OperationReport, read(prior_path))
            self.new_attempt("acquire").mkdir(parents=True)
            self.transition("acquire", "discover_and_acquire", "running")
            try:
                result = await run_acquisition(
                    self.context,
                    self.recipe.acquisition(),
                    dsn=database_dsn(self.paths.root),
                    download=True,
                    observer=self.observe_acquisition,
                    resume_report=prior,
                )
            except asyncio.CancelledError:
                if self.acquisition and self.acquisition.report:
                    self.save_acquisition_reference(Path(self.acquisition.report))
                raise
            child = Path(result["report"])
            self.save_acquisition_reference(child)
            stage["child_run_id"] = result["run_id"]
            self.save()
            if result["status"] != "complete":
                raise Blocked(f"Acquisition did not complete; inspect {child}.")
            selection = ArchiveSelection.model_validate_json(json.dumps(result["selection"]))
            if not selection.archives:
                raise Blocked(
                    "Discovery completed without ready archives; inspect the acquisition report."
                )
            selection_path = self.artifacts.steps_dir / "acquired.json"
            write_job(selection_path, selection.model_dump(mode="json"))
            self.transition(
                "acquire",
                "discover_and_acquire",
                "complete",
                child_report=str(child),
                child_report_sha256=checksum(child),
                selection=str(selection_path),
                selection_sha256=checksum(selection_path),
                summary=f"{len({item.local_archive_id for item in selection.archives})} ready archives",
            )
        retained(Path(stage["child_report"]), stage["child_report_sha256"])
        selection_path = retained(Path(stage["selection"]), stage["selection_sha256"])
        return ArchiveSelection.model_validate_json(selection_path.read_bytes())

    def observe_acquisition(self, snapshot: ProgressSnapshot) -> None:
        self.acquisition = snapshot
        if self.acquisition_display:
            self.acquisition_display.update(snapshot)

    def save_acquisition_reference(self, child: Path) -> None:
        stage = self.stage("acquire")
        reference: dict[str, object] = {
            "child_report": str(child),
            "child_report_sha256": checksum(child),
        }
        stage.update(reference)
        stage["attempts"][-1].update(reference)
        write_job(Path(stage["attempts"][-1]["directory"]) / "acquisition.json", reference)
        self.save()

    def on_engine(self, name: str) -> Callable[[str], None]:
        def phase(value: str) -> None:
            stage = self.stage(name)
            if value == "execute":
                intent = Path(stage["execution_intent"])
                stage["execution_intent_sha256"] = checksum(intent)
                stage["attempts"][-1]["execution_intent_sha256"] = stage["execution_intent_sha256"]
            self.transition(name, value, "running")

        return phase

    def inspect_intent(self, stage: dict[str, Any]) -> Path | None:
        if "execution_intent" not in stage:
            return None
        intent = Path(stage["execution_intent"])
        if "execution_intent_sha256" in stage:
            retained(intent, stage["execution_intent_sha256"])
        return intent if intent.is_file() else None

    def start_execution(self, name: str) -> Path:
        stage = self.stage(name)
        directory = self.new_attempt(name)
        stage.pop("execution_intent_sha256", None)
        intent = str(directory / "execution-intent.json")
        stage["execution_intent"] = intent
        stage["attempts"][-1]["execution_intent"] = intent
        self.transition(name, "validate", "running")
        return directory

    def finish(
        self, name: str, result: dict[str, Any], *, recovered: bool = False
    ) -> RegisteredFiles:
        stage = self.stage(name)
        stage["result"] = result
        if stage.get("attempts"):
            attempt = stage["attempts"][-1]
            key = "recovery_receipts" if recovered else "execution_report"
            if recovered:
                attempt.setdefault(key, []).append(result["registration_receipt"])
            elif result.get("report_path"):
                attempt[key] = result["report_path"]
        self.save()
        if result.get("status") == "interrupted":
            raise asyncio.CancelledError
        if not recovered and result.get("status") != "complete":
            message = str(
                result.get("registration_error")
                or result.get("error")
                or "Processing did not complete; inspect the retained execution receipt."
            )
            if result.get("verification") == "verified" and result.get("registration") in {
                "failed",
                "pending",
            }:
                raise Blocked(message)
            raise MarketForgeError(message)
        if not recovered and result.get("verification") != "verified":
            raise MarketForgeError("Required publication verification is incomplete.")
        files = RegisteredFiles.from_receipt(result)
        previous = stage.get("primary_file_ids")
        if previous is not None and previous != list(files.ids):
            raise Blocked("Recovered registration disagrees with retained primary-file identities.")
        self.transition(
            name,
            "register",
            "complete",
            primary_file_ids=list(files.ids),
            summary=f"{len(files.ids)} registered files",
        )
        return files

    async def retry_normalization(self, source: Path, stage: dict[str, Any]) -> Path:
        """Reallocate outputs from retained jobs without reranking or resolving inputs."""
        batch = read(source)
        report = read(Path(stage["prepared"]["report"]))
        identity = f"normalize-retry-{new_attempt_id()}"
        async with database_connection(database_dsn(self.paths.root)) as connection:
            used = await used_ids(connection, self.paths)
        directory = self.paths.runs_dir / identity
        jobs: dict[int, dict[str, object]] = {}
        for job, summary in zip(batch["jobs"], report["job_summaries"], strict=True):
            job_id = allocate(f"{identity}:{job['job_id']}:job", used, self.paths)
            dataset_id = allocate(f"{identity}:{job['dataset_id']}:dataset", used, self.paths)
            staging, output = default_output_paths(self.paths, job_id, dataset_id)
            job.update(
                job_id=job_id,
                dataset_id=dataset_id,
                output={"staging_path": str(staging), "dataset_path": str(output)},
            )
            summary.update(
                job_id=job_id,
                dataset_id=dataset_id,
                staging_path=str(staging),
                dataset_path=str(output),
            )
            jobs[job_id] = job
        common = {
            "run_id": identity,
            "run_directory": str(directory),
            "batch_path": str(directory / "normalize-batch.json"),
        }
        batch.update(common)
        report.update(common, report_path=str(directory / "prepare-report.json"))
        binary = self.engine("normalize")
        await publish_batch_async(
            self.paths,
            BatchPlan(report, batch, jobs, directory),
            report,
            self.historical,
            binary,
            lifecycle=self.on_engine("normalize"),
            on_candidate=self.on_candidate("normalize"),
        )
        if not report["engine_ready"] or report["status"] != "prepared":
            raise Blocked(f"Retained normalization retry failed validation; inspect {directory}.")
        self.save_preparation(stage, report, "batch_path")
        return self.inspect_preparation(stage)

    async def normalize(self, selection: ArchiveSelection, *, stop: bool) -> RegisteredFiles | None:
        name = "normalize"
        stage = self.stage(name)
        candidate = self.candidate(name)
        if candidate is not None or "prepared" not in stage:
            self.transition(name, "prepare", "running")
            if candidate is not None:
                job, result = candidate
                if not result["engine_ready"]:
                    result = await validate_normalization_candidate(
                        self.paths,
                        job,
                        result,
                        self.historical,
                        self.engine(name),
                        lifecycle=self.on_engine(name),
                    )
            else:
                result = await prepare_batch(
                    self.paths,
                    self.recipe.normalization(),
                    pinned=selection,
                    allocation_seed=self.report["run_id"],
                    historical=self.historical,
                    executable=self.engine(name),
                    lifecycle=self.on_engine(name),
                    on_candidate=self.on_candidate(name),
                )
            if result["status"] != "prepared" or not result["engine_ready"]:
                stage["child_report"] = result["report_path"]
                raise Blocked(
                    f"Normalization preparation is incomplete; inspect {result['report_path']}."
                )
            self.save_preparation(stage, result, "batch_path")
            self.transition(
                name,
                "validate",
                "prepared",
                summary=f"{result['prepared_job_count']} validated jobs",
            )
        source = self.inspect_preparation(stage)
        if stop:
            return None
        expected = {
            str(contained(self.paths.raw_dir, item.relative_path).resolve()): (
                item.local_archive_id,
                item.sha256,
            )
            for item in selection.archives
        }
        intent = self.inspect_intent(stage)
        if intent is not None:
            if (prior := read(intent)).get("requested_sha256", prior["submitted_sha256"]) != stage[
                "prepared"
            ]["job_sha256"]:
                raise Blocked("Normalization intent differs from the retained submitted batch.")
            document = read(intent.parent / "submitted-batch.json")
            outputs = [self.paths.root / job["output"]["dataset_path"] for job in document["jobs"]]
            if all(output_present(path) for path in outputs):
                await normalize.pin_raw_inputs(self.paths, read(source)["jobs"], expected=expected)
                return self.finish(
                    name,
                    await normalize.recover(
                        self.paths, intent, require_all=True, lifecycle=self.on_engine(name)
                    ),
                    recovered=True,
                )
            if stage["state"] == "complete":
                raise Blocked("A retained normalized publication is unavailable.")
            source = await self.retry_normalization(source, stage)
        elif stage["state"] == "complete":
            raise Blocked(
                "Normalization registration recovery requires the original execution intent."
            )
        directory = self.start_execution(name)
        result = await normalize.execute_async(
            self.paths,
            source,
            directory=directory,
            historical_root=self.historical,
            executable=self.engine(name),
            lifecycle=self.on_engine(name),
            expected_raw=expected,
            tui=self.stage_monitor(name),
            engine_runner=self.engine_runner(name),
        )
        return self.finish(name, result)

    async def merge(self, files: RegisteredFiles) -> None:
        name = "merge"
        stage = self.stage(name)
        if stage.get("upstream_file_ids", list(files.ids)) != list(files.ids):
            raise Blocked("Merge dependency differs from the retained normalization selection.")
        stage["upstream_file_ids"] = list(files.ids)
        candidate = self.candidate(name)
        if candidate is not None:
            job, result = candidate
            if result["status"] != "prepared":
                result = await merge.validate_candidate(
                    self.paths, job, result, self.engine(name), lifecycle=self.on_engine(name)
                )
            if result["status"] != "prepared":
                stage["child_report"] = result["report_path"]
                raise Blocked(f"Retained merge validation failed; inspect {result['report_path']}.")
            if sorted(item["database_id"] for item in result["inputs"]) != list(files.ids):
                raise Blocked("Retained merge candidate differs from its exact upstream file IDs.")
            if "prepared" in stage:
                self.inspect_preparation(stage)
                previous = read(Path(stage["prepared"]["report"]))
                if not merge.same_inputs(previous["inputs"], result["inputs"]):
                    raise Blocked("Retained merge inputs changed before a new execution attempt.")
            self.save_preparation(stage, result, "job_path")
            self.transition(name, "validate", "prepared", summary="Validated merge job")
        intent = self.inspect_intent(stage)
        if intent is not None:
            prior = read(intent)
            reused = stage.get("result", {}).get("disposition") == "reused"
            if (
                not reused
                and prior["submitted_sha256"] != stage["prepared"]["job_sha256"]
                or reused
                and checksum(Path(stage["result"]["requested_submission_path"]))
                != stage["prepared"]["job_sha256"]
            ):
                raise Blocked("Merge intent differs from the retained submitted job.")
            if await joined_io(lambda: output_present(Path(prior["path"]))):
                self.inspect_preparation(stage)
                self.finish(
                    name,
                    await merge.register(
                        self.paths, intent, lifecycle=self.on_engine(name), require_pinned=True
                    ),
                    recovered=True,
                )
                return
        elif stage["state"] == "complete":
            raise Blocked("Merge registration recovery requires the original execution intent.")
        if "prepared" not in stage or intent is not None:
            self.transition(name, "prepare", "running")
            recipe = self.recipe.merging(list(files.ids))
            if "prepared" in stage:
                self.inspect_preparation(stage)
                previous = read(Path(stage["prepared"]["report"]))
                recipe = recipe.model_copy(
                    update={
                        "stream_ranks": {
                            item["stream_id"]: item["stream_rank"] for item in previous["streams"]
                        },
                        "timeline_ordering": previous["timeline_ordering"],
                    }
                )
            else:
                previous = None
            result = await merge.prepare_direct(
                self.paths,
                recipe,
                self.report["name"],
                historical=self.historical,
                executable=self.engine(name),
                lifecycle=self.on_engine(name),
                on_candidate=self.on_candidate(name),
            )
            if result["status"] != "prepared":
                stage["child_report"] = result["report_path"]
                raise Blocked(f"Merge preparation failed; inspect {result['report_path']}.")
            if previous is not None and not merge.same_inputs(previous["inputs"], result["inputs"]):
                raise Blocked("Retained merge inputs changed before a new execution attempt.")
            self.save_preparation(stage, result, "job_path")
            self.transition(name, "validate", "prepared", summary="Validated merge job")
        source = self.inspect_preparation(stage)
        directory = self.start_execution(name)
        result = await merge.execute_async(
            self.paths,
            source,
            execution_directory=directory,
            historical=self.historical,
            executable=self.engine(name),
            lifecycle=self.on_engine(name),
            tui=self.stage_monitor(name),
            engine_runner=self.engine_runner(name),
        )
        if original_intent := result.get("execution_intent_reference"):
            stage["execution_intent"] = original_intent
            stage["execution_intent_sha256"] = checksum(Path(original_intent))
        self.finish(name, result)

    def stage_monitor(self, name: str) -> bool:
        return self.windows is not None and self.report["engines"][name].get("monitor_tui", False)

    async def convert(self) -> None:
        name = "convert"
        stage = self.stage(name)
        options = self.recipe.convert
        assert options is not None
        publications = self.stage("merge")["result"].get("publications", {})
        if len(publications) != 1:
            raise Blocked("Conversion requires exactly one registered upstream merge publication.")
        publication_id = next(iter(publications.values()))["publication_id"]
        self.transition(name, "prepare", "running")
        if "prepared" in stage:
            source = self.inspect_preparation(stage)
        else:
            binding = await inspect_publication_source(self.paths, publication_id)
            recipe = conversion_recipes.Recipe(
                version=1,
                source=binding,
                interval=binding.interval,
                selection=conversion_recipes.Selection(whole_publication=True),
                conversion=conversion_recipes.Policy.model_validate(
                    options.model_dump(
                        exclude={"reference_stream_ids", "reference_publication_id", "resources"}
                    )
                ),
                reference_stream_ids=options.reference_stream_ids,
                reference_publication_id=options.reference_publication_id,
                resources=options.resources,
            )
            path = self.historical / "conversion-recipe.toml"
            path.write_text(
                conversion_recipes.dumps(recipe.model_dump(mode="json", exclude_none=True))
            )
            result = await conversion.prepare(
                self.paths,
                path,
                executable=self.engine(name),
                runtime_path=self.historical / "convert.toml",
                lifecycle=lambda phase: self.transition(name, phase, "running"),
            )
            if result["status"] == "complete" and result.get("disposition") == "reused":
                self.finish_conversion(result)
                return
            if result.get("compatibility") != "validated":
                raise Blocked(
                    f"Conversion preparation did not validate; inspect {result.get('report_path')}."
                )
            self.save_preparation(stage, result, "job_path")
            source = self.inspect_preparation(stage)
        self.new_attempt(name).mkdir(parents=True)
        result = await execute_conversion(
            self.paths,
            source,
            executable=self.engine(name),
            tui=self.stage_monitor(name),
            engine_runner=self.engine_runner(name),
            lifecycle=lambda phase: self.transition(name, phase, "running"),
        )
        self.finish_conversion(result)

    def finish_conversion(self, result: dict[str, Any]) -> None:
        stage = self.stage("convert")
        stage["result"] = result
        if stage.get("attempts"):
            stage["attempts"][-1]["execution_report"] = result.get("report_path")
        self.save()
        if result["status"] == "interrupted":
            raise asyncio.CancelledError
        if result["status"] != "complete":
            jobs = result.get("jobs", ())
            detail = next((job["error"] for job in jobs if job.get("error")), None)
            raise Blocked(
                detail or f"Conversion did not complete; inspect {result.get('report_path')}."
            )
        self.transition(
            "convert",
            "index",
            "complete",
            summary="Reused verified output"
            if result.get("disposition") == "reused"
            else "Verified output indexed",
        )

    async def acquire_with_display(self) -> ArchiveSelection:
        with AcquisitionDisplay(
            self.context.error_console,
            download=True,
            name=self.report["name"],
            interval=self.recipe.interval,
        ) as display:
            self.acquisition_display = display
            return await self.acquire()

    async def acquire_stage(self) -> ArchiveSelection:
        windows = self.windows
        if windows is None or self.stage("acquire")["state"] == "complete":
            return await self.acquire()
        self.window_stage = "acquire"
        self.report["active_stage"] = "acquire"
        try:
            outcome = await windows.dispatch(
                self.report,
                self.artifacts.report,
                "acquire",
                save=self.save,
            )
        except WindowUnavailable as error:
            if not windows.fallback:
                raise Blocked(str(error)) from error
            windows.diagnostic(f"{error} Continuing inline.")
            self.windows = None
            outcome = None
        finally:
            self.window_stage = None
            self.report = status(self.paths, self.report["run_id"])
        if outcome is not None:
            self.stage("acquire")["terminal_outcome"] = outcome["outcome_path"]
            self.save()
            if outcome["state"] == "blocked":
                raise Blocked(outcome["error"])
            if outcome["state"] == "interrupted":
                raise asyncio.CancelledError
            if outcome["state"] == "failed":
                raise MarketForgeError(outcome["error"])
            if self.stage("acquire")["state"] != "complete":
                raise Blocked("The acquisition worker did not retain a complete stage checkpoint.")
        return await self.acquire()

    def engine_runner(self, name: str) -> EngineRunner | None:
        windows = self.windows
        if windows is None:
            return None

        async def run(argv: list[str], root: Path, directory: Path, tui: bool) -> tuple[int, bool]:
            self.window_stage = name
            completion = WindowCompletion(
                asyncio.get_running_loop().create_future(), asyncio.Event()
            )
            self.window_completion = completion
            request = EngineRequest(
                argv, str(directory), checksum(directory / "execution-intent.json"), tui
            )
            self.window_task = task = asyncio.create_task(
                windows.dispatch(
                    self.report,
                    self.artifacts.report,
                    name,
                    save=self.save,
                    engine=request,
                    completion=completion,
                )
            )
            try:
                done, _ = await asyncio.wait(
                    {completion.result, task}, return_when=asyncio.FIRST_COMPLETED
                )
                result = task.result() if task in done else completion.result.result()
                self.stage(name)["terminal_outcome"] = result["outcome_path"]
                self.save()
                return result["exit_code"], result["interrupted"]
            except WindowUnavailable as error:
                self.window_task = None
                self.window_completion = None
                self.window_stage = None
                if not windows.fallback:
                    # The existing engine services release a claim on launch failure.
                    raise OSError(str(error)) from error
                windows.diagnostic(f"{error} Continuing inline.")
                self.windows = None
                return await supervise(argv, root, directory)
            except asyncio.CancelledError:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
                if not completion.result.done():
                    raise WindowLost(
                        "Stage interrupted without a retained worker outcome. Resume to reconcile its work claim."
                    ) from None
                self.stage(name)["terminal_outcome"] = completion.result.result()["outcome_path"]
                self.save()
                return 130, True

        return run

    async def close_stage_window(self) -> None:
        task, completion = self.window_task, self.window_completion
        try:
            if task is None or completion is None or task.cancelled():
                return
            completion.release.set()
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                with suppress(WindowUnavailable, WindowLost, asyncio.CancelledError):
                    await task
                raise
            except (WindowUnavailable, WindowLost):
                # The execution boundary has already checked the worker outcome.
                pass
        finally:
            self.window_task = None
            self.window_completion = None
            self.window_stage = None

    async def execute_engine_stage[T](self, name: str, call: Callable[[], Awaitable[T]]) -> T:
        try:
            return await call()
        except BaseException as error:
            if self.window_stage == name:
                state = (
                    "interrupted"
                    if isinstance(error, (asyncio.CancelledError, KeyboardInterrupt))
                    else "blocked"
                    if isinstance(error, (Blocked, DatabaseError))
                    else "failed"
                )
                self.transition(
                    name,
                    self.stage(name).get("phase", "execute"),
                    state,
                    summary=str(error) or state,
                )
            raise
        finally:
            await self.close_stage_window()

    async def run(self, *, stop: bool = False) -> dict[str, Any]:
        identity = parse_run_id(self.report["run_id"])
        prior = self.report if self.report.get("registry_id") is not None else None
        if self.report.get("environment") != database_purpose() or (
            prior is None
            and any(stage["state"] != "pending" for stage in self.report["stages"].values())
        ):
            raise Blocked("The pipeline requires its original database environment and identity.")
        if self.report.get("state") == "complete":
            if prior is None:
                raise Blocked(
                    "The original registry identity is unavailable for this completed pipeline."
                )
            require_registry(self.report, await registry_identity(self.paths))
            return self.report
        # A prior registration may have committed just before the initial report update.
        indexed = (
            await show_index(self.paths, str(identity))
            if prior is None and identity.operation == RunOperation.PIPELINE
            else None
        )
        execution = await begin(
            self.paths,
            RunOperation.PIPELINE,
            "resume" if prior or indexed else "run",
            identity=identity,
            recipe=self.report["name"],
            source=Path(self.report["source"])
            if self.report.get("source")
            else self.artifacts.submitted_config,
            resume=prior is not None or indexed is not None,
            evidence=prior,
        )
        self.report["registry_id"] = execution.registry
        return await complete_call(execution, lambda: self._run(stop=stop))

    async def _run(self, *, stop: bool = False) -> dict[str, Any]:
        self.report.update(state="running", error=None, finished_at=None)
        self.save()
        try:
            identity = await registry_identity(self.paths)
            if self.report.get("registry_id") is not None:
                require_registry(self.report, identity)
            elif self.report.get("environment") != database_purpose() or any(
                stage["state"] != "pending" for stage in self.report["stages"].values()
            ):
                raise Blocked("The original database identity cannot be established for this run.")
            self.report["registry_id"] = identity
            self.save()
            if "engines" not in self.report:
                await joined_io(self.freeze)
            for path, digest in self.report["settings_files"].items():
                retained(Path(path), digest)
            with pinned_registry(identity):
                selection = await self.acquire_stage()
                files = await self.execute_engine_stage(
                    "normalize",
                    lambda: self.normalize(selection, stop=stop),
                )
                if files is not None:
                    await self.execute_engine_stage("merge", lambda: self.merge(files))
                    if (
                        self.recipe.convert is not None
                        and self.stage("convert")["state"] != "complete"
                    ):
                        await self.execute_engine_stage("convert", self.convert)
            self.report["state"] = "stopped" if stop else "complete"
        except (asyncio.CancelledError, KeyboardInterrupt):
            self.end_error(
                "interrupted", "Interrupted. Resume this run to reconcile any committed output."
            )
        except WindowLost as error:
            self.end_error("interrupted" if self.interrupting else "failed", str(error))
        except (Blocked, ArchiveSelectionError, DatabaseError) as error:
            self.end_error("blocked", str(error))
        except (MarketForgeError, OSError, ValueError, KeyError, TypeError) as error:
            self.end_error("failed", str(error))
        self.interrupting = False
        self.report["finished_at"] = now()
        self.save()
        return self.report

    def end_error(self, state: str, message: str) -> None:
        self.report.update(state=state, error=message)
        name = self.report.get("active_stage")
        if name is not None and self.stage(name)["state"] != "complete":
            self.transition(name, self.stage(name).get("phase", "terminal"), state)


def new(
    context: ApplicationContext, name: str, submitted: bytes, *, source: Path | None = None
) -> PipelineRun:
    recipe = load_recipe(submitted)
    run_id = new_run_id(RunOperation.PIPELINE)
    artifacts = RunArtifacts.for_run(context.paths, run_id)
    artifacts.steps_dir.mkdir(parents=True, exist_ok=False)
    artifacts.submitted_config.write_bytes(submitted)
    resolved: dict[str, Any] = {
        "schema_version": 1,
        "recipe": recipe.model_dump(mode="json"),
        "settings": context.settings.model_dump(mode="json"),
        "workspace": str(context.paths.root),
        "storage": str(context.paths.data_root),
    }
    write_job(artifacts.resolved_plan, resolved)
    report: dict[str, Any] = {
        "schema_version": 1,
        "kind": "historical_pipeline",
        "state": "pending",
        "name": name,
        "source": str(source) if source else None,
        "run_id": str(run_id),
        "environment": "testing" if context.testing else "production",
        "interval": resolved["recipe"]["interval"],
        "started_at": now(),
        "report_path": str(artifacts.report),
        "resolved_plan": str(artifacts.resolved_plan),
        "event_log": str(artifacts.events),
        "submitted_sha256": hashlib.sha256(submitted).hexdigest(),
        "resolved_sha256": checksum(artifacts.resolved_plan),
        "stages": {stage: {"state": "pending", "phases": {}} for stage in recipe.stage_order()},
        "recent": [],
    }
    pipeline = PipelineRun(context, artifacts, report, recipe)
    pipeline.save()
    return pipeline


def resume(context: ApplicationContext, run_id: str) -> PipelineRun:
    artifacts = artifacts_for(context.paths, run_id)
    report = status(context.paths, run_id)
    retained(artifacts.resolved_plan, report["resolved_sha256"])
    original = retained(artifacts.submitted_config, report["submitted_sha256"])
    saved = read(artifacts.resolved_plan)
    if saved["workspace"] != str(context.paths.root) or saved["storage"] != str(
        context.paths.data_root
    ):
        raise Blocked("Resume requires the original workspace and storage root.")
    settings = AppSettings.model_validate_json(json.dumps(saved["settings"]))
    context = replace(context, settings=settings, settings_file=None)
    return PipelineRun(context, artifacts, report, load_recipe(original.read_bytes()))
