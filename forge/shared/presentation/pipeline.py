"""Read-only Pipeline projections for human-facing clients."""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from shlex import join
from typing import cast

from forge.historical.pipelines.config import PipelineRecipe, load_recipe_file
from forge.markets.ranking.policy import DEFAULT_TURNOVER_POLICY
from forge.orchestration.run_summary import mapping, when
from forge.shared.config.library import resolve_config_file
from forge.shared.errors import MarketForgeError
from forge.shared.identity import PIPELINE_STAGE_LABELS, PIPELINE_STAGES

Fields = tuple[tuple[str, str], ...]


def value(item: object) -> str:
    return str(item) if item is not None and item != "" else "—"


def timestamp(item: object) -> str:
    return f"{dt.isoformat(sep=' ')}" if (dt := when(item)) else value(item)


def interval(item: object) -> str:
    bounds = mapping(item)
    return f"{timestamp(bounds.get('start'))} → {timestamp(bounds.get('end'))} (end exclusive)"


def duration(record: Mapping[str, object]) -> str:
    start, end = when(record.get("started_at")), when(record.get("finished_at"))
    if start is None or end is None:
        return "—"
    seconds = max(0, int((end - start).total_seconds()))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02}:{m:02}:{s:02}"


def entries(item: object) -> Iterator[Mapping[str, object]]:
    if isinstance(item, list):
        for entry in cast(list[object], item):
            yield mapping(entry)


@dataclass(frozen=True, slots=True)
class RecipeLine:
    name: str
    stages: str
    requests: str
    requested: str
    target: str
    state: str
    diagnostic: str = ""


def library_entry(name: str, directory: Path) -> RecipeLine:
    try:
        source = resolve_config_file(directory, name)
        recipe = load_recipe_file(source, source.read_bytes())
    except (OSError, ValueError, MarketForgeError) as error:
        return RecipeLine(name, "—", "—", "—", "—", "invalid", str(error))
    return recipe_line(name, recipe)


def recipe_line(name: str, recipe: PipelineRecipe) -> RecipeLine:
    return RecipeLine(
        name,
        str(len(recipe.stage_order())),
        str(len(recipe.requests)),
        interval(recipe.interval.model_dump()),
        recipe.convert.target_quote if recipe.convert else "—",
        "valid",
    )


@dataclass(frozen=True, slots=True)
class StageLine:
    name: str
    state: str
    duration: str
    detail: str


@dataclass(frozen=True, slots=True)
class PipelineView:
    action: str
    environment: str
    state: str
    identity: Fields
    stages: tuple[StageLine, ...]
    requests: tuple[tuple[str, ...], ...]
    inventory: tuple[tuple[str, str, str], ...]
    outputs: Fields
    artifacts: Fields
    index: Fields
    diagnostics: tuple[str, ...]
    commands: tuple[str, ...]


def project(record: Mapping[str, object], action: str, *, index_state: str = "") -> PipelineView:
    source = record.get("source")
    name = record.get("name") or (Path(str(source)).stem if source else "—")
    state = value(record.get("state"))
    environment = str(record.get("environment") or "")
    recipe = mapping(record.get("recipe"))
    run_id = record.get("run_id")
    flag = f"--{environment}" if environment in {"testing", "production"} else ""
    identity: list[tuple[str, str]] = [("Recipe", value(name))]
    if source:
        identity.append(("Configuration", str(source)))
    if run_id:
        identity.extend((("Run ID", str(run_id)), ("Duration", duration(record))))
    if bounds := record.get("interval") or recipe.get("interval"):
        identity.append(("Requested interval", interval(bounds)))
    if action in {"validate", "plan"}:
        identity.append(
            (
                "Scope",
                "Local recipe structure"
                if action == "validate"
                else "Read-only registry and runtime evidence",
            )
        )

    requested = tuple(
        (
            value(req.get("exchange")),
            f"{value(req.get('instrument_type'))} / {value(req.get('market_category'))}",
            value(mapping(req.get("scope")).get("identifier")),
            value(req.get("data_type")),
        )
        for req in entries(recipe.get("requests"))
    )
    inventory = tuple(
        (
            value(mapping(mapping(row.get("request")).get("scope")).get("identifier")),
            value(mapping(row.get("request")).get("data_type")),
            value(mapping(row.get("registered_evidence")).get("total")),
        )
        for row in entries(record.get("inventory"))
    )
    stages: list[StageLine] = []
    outputs: list[tuple[str, str]] = []
    artifacts: list[tuple[str, str]] = []
    commands: list[str] = []
    diagnostics = [
        str(x) for x in (record.get("error"), record.get("execution_registry_warning")) if x
    ]
    blockers = record.get("blockers")
    if isinstance(blockers, list):
        diagnostics.extend(str(item) for item in cast(list[object], blockers))
    if run_id and flag:
        commands.append(join(["mf", "runs", "report", str(run_id), flag]))

    stage_data = mapping(record.get("stages"))
    options = mapping(recipe.get("convert"))
    configurations = {
        "acquire": f"{len(requested)} acquisition requests",
        "normalize": f"Ranking policy {value(mapping(recipe.get('normalize')).get('ranking_policy'))}",
        "merge": f"Timeline {mapping(recipe.get('merge')).get('timeline_ordering') or 'selected from inputs'}",
        "convert": (
            f"Target {value(options.get('target_quote'))}, {value(options.get('price_source'))}, "
            f"maximum age {value(options.get('max_staleness_ns'))} ns, "
            f"maximum hops {value(options.get('max_hops'))}, paths {value(options.get('path_policy'))}"
        ),
    }
    for name_key in PIPELINE_STAGES:
        name_label = PIPELINE_STAGE_LABELS[name_key]
        raw = stage_data.get(name_key)
        if action in {"validate", "plan"} and recipe and (name_key != "convert" or options):
            note = configurations[name_key]
            if isinstance(raw, str):
                note += f". {raw}"
            stages.append(
                StageLine(name_label, "deferred" if action == "plan" else "enabled", "—", note)
            )
            continue
        if raw is None:
            continue
        stage = mapping(raw)
        result = mapping(stage.get("result"))
        detail = value(stage.get("summary"))
        if name_key == "acquire":
            detail = detail.replace("ready archives", "raw archives ready")
        if name_key in {"normalize", "merge"}:
            files = stage.get("primary_file_ids")
            if isinstance(files, list) and files:
                suffix = "" if len(cast(list[object], files)) == 1 else "s"
                detail = (
                    f"{len(cast(list[object], files))} primary file{suffix} registered"
                    if result.get("disposition") != "reused"
                    else f"{len(cast(list[object], files))} registered primary file{suffix} reused"
                )
        if name_key == "merge" and result.get("disposition") != "reused":
            publications = len(mapping(result.get("publications")))
            if publications:
                label = "publication" if publications == 1 else "publications"
                detail = f"{publications} merged {label} registered · {detail}"
        if result.get("disposition") == "reused" and "reused" not in detail.lower():
            detail += " · Reused"
        if phase := stage.get("phase"):
            detail += f" · Phase {str(phase).replace('_', ' ')}"
        stages.append(StageLine(name_label, value(stage.get("state")), duration(stage), detail))
        if child := stage.get("child_run_id"):
            artifacts.append((f"{name_label} child run", str(child)))
        if job := mapping(stage.get("prepared")).get("job"):
            artifacts.append((f"{name_label} preparation", str(job)))
        if execution := result.get("execution_id"):
            artifacts.append((f"{name_label} Engine execution", str(execution)))
        for publication in mapping(result.get("publications")).values():
            if pub := mapping(publication).get("publication_id"):
                outputs.append((f"{name_label} registry publication", str(pub)))
                if flag:
                    commands.append(
                        join(["mf", "datasets", "show", "--publication-id", str(pub), flag])
                    )
        for job_result in entries(result.get("jobs")):
            if (
                job_result.get("indexing") == "complete"
                and job_result.get("status") == "complete"
                and job_result.get("publication")
            ):
                if ref := job_result.get("work_key"):
                    outputs.append(("Indexed conversion output", str(ref)))
                    if flag:
                        commands.append(
                            join(["mf", "datasets", "show", "--output-ref", str(ref), flag])
                        )
            if q := mapping(job_result.get("publication")).get("target_quote"):
                outputs.append(("Target quote", str(q)))
        if error := result.get("error") or stage.get("error"):
            diagnostics.append(f"{name_label} · {error}")

    for key, label in (
        ("report_path", "Retained report"),
        ("resolved_plan", "Frozen settings and recipe"),
    ):
        if path := record.get(key):
            artifacts.append((label, str(path)))
    for binary, pin in mapping(record.get("engines")).items():
        pin_info = mapping(pin)
        artifacts.append(
            (
                f"Pinned Engine · {binary}",
                value(pin_info.get("path") or pin_info.get("sha256") or pin),
            )
        )
    if options and action in {"validate", "plan"}:
        selection = mapping(options.get("reference_selection"))
        references = options.get("reference_stream_ids")
        mode = selection.get("mode") or (
            "automatic" if recipe.get("version") == 2 and references is None else "manual"
        )
        identity.append(("Reference selection", str(mode).capitalize()))
        if mode == "automatic":
            identity.append(("Reference streams", "Resolved after Merge registers its publication"))
            identity.append(
                ("Ranking policy", str(selection.get("ranking_policy") or DEFAULT_TURNOVER_POLICY))
            )
        else:
            identity.append(
                (
                    "Reference streams",
                    ", ".join(map(str, cast(list[object], references)))
                    if isinstance(references, list) and references
                    else "No explicit reference streams",
                )
            )
        if external := options.get("reference_publication_id"):
            identity.append(("External reference registry publication", str(external)))
    if run_id and options.get("target_quote"):
        outputs.append(("Target quote", str(options["target_quote"])))
    if action == "plan" and (runtime := mapping(record.get("runtime_availability"))):
        identity.extend(
            (f"{name.capitalize()} runtime", value(status)) for name, status in runtime.items()
        )
    if run_id and flag:
        if state in {"stopped", "interrupted", "blocked", "failed"}:
            commands.append(join(["mf", "pipeline", "resume", str(run_id), flag]))
        if index_state == "reconciliation required":
            commands.append(join(["mf", "runs", "reconcile", "--run-id", str(run_id), flag]))
    return PipelineView(
        action,
        environment,
        state,
        tuple(identity),
        tuple(stages),
        requested,
        inventory,
        tuple(dict.fromkeys(outputs)),
        tuple(artifacts),
        (("Run history", index_state),) if index_state else (),
        tuple(dict.fromkeys(diagnostics)),
        tuple(dict.fromkeys(commands)),
    )
