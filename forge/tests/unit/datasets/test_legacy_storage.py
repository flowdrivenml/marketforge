"""Legacy path proofs fail closed and distinguish known identities from guesses."""

import json
from pathlib import Path

import pytest

from forge.datasets.legacy_storage import classify_references, ownership
from forge.datasets.publication_cleanup import review_tree
from forge.historical.engine.managed import checksum
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def test_relative_root_guesses_do_not_establish_exclusive_ownership(tmp_path: Path) -> None:
    old = tmp_path / "historical"
    copy = tmp_path / "relocated"
    target = copy / "data" / "processed" / "88"
    target.mkdir(parents=True)
    raw = [
        {"id": 88, "path": "data/processed/88"},
        {"id": 129, "path": str(old / "data/processed/129")},
    ]
    records = classify_references(raw, None)
    assert records[0]["classification"] == "relative_with_plausible_unverified_anchors"
    assert "resolved_path" not in records[0]
    assert records[1]["classification"] == "missing_physical_target"
    evidence = {"state": "legacy_catalogue_without_verified_registry", "references": records}
    assert ownership(evidence, [target])["state"] == "unresolved_legacy_references"
    evidence["references"] = classify_references(raw, str(copy))
    assert evidence["references"][0]["classification"] == "relative_with_verified_historical_anchor"
    assert ownership(evidence, [target])["known_reference_ids"] == [88]


def test_known_alias_and_disjoint_absolute_path_are_publication_specific(tmp_path: Path) -> None:
    target = tmp_path / "testing"
    independent = tmp_path / "separate"
    target.mkdir()
    independent.mkdir()
    alias = tmp_path / "production"
    alias.symlink_to(target)
    records = classify_references([{"id": 1, "path": str(alias)}], None)
    evidence = {"state": "legacy_catalogue_without_verified_registry", "references": records}
    assert ownership(evidence, [target])["state"] == "known_foreign_reference"
    assert ownership(evidence, [independent])["state"] == "verified_disjoint"


def test_missing_or_nonstandard_paths_do_not_qualify_for_risk_acknowledgement(
    tmp_path: Path,
) -> None:
    records = classify_references([{"id": 1, "path": "../anything"}], None)
    evidence = {"state": "legacy_catalogue_without_verified_registry", "references": records}
    assert ownership(evidence, [tmp_path])["state"] == "unsupported_legacy_reference"


def test_review_includes_only_exact_committed_legacy_sidecars(tmp_path: Path) -> None:
    paths = ProjectPaths(tmp_path, tmp_path / "data")
    root = paths.datasets_dir / "normalized" / "5"
    stream = {"stream_id": "fixture", "directory": "streams/fixture/trades", "sources": []}
    sidecar = root / stream["directory"] / "sources.json"
    sidecar.parent.mkdir(parents=True)
    sidecar.write_text(json.dumps(stream))
    (root / "manifest.json").write_text(json.dumps({"streams": [stream]}))
    (root / "processing-job.json").write_text("{}")
    (root / "publication-checksums.json").write_text("{}")
    job_hash = checksum(root / "processing-job.json")
    (root / "processing-job.sha256").write_text(f"{job_hash}  processing-job.json\n")
    candidate = {
        "operation": "normalize",
        "path": str(root),
        "inventory_sha256": {
            "manifest.json": checksum(root / "manifest.json"),
            "processing-job.json": job_hash,
        },
        "checksum_index_sha256": checksum(root / "publication-checksums.json"),
    }
    assert "processing-job.sha256" in review_tree(paths, candidate)["files"]
    assert str(sidecar.relative_to(root)) in review_tree(paths, candidate)["files"]
    sidecar.write_text("{}")
    with pytest.raises(MarketForgeError, match="provenance disagrees"):
        review_tree(paths, candidate)


@pytest.mark.parametrize(
    "options",
    [
        ["--production", "--force", "--apply", "--plan", "report.json"],
        ["--testing", "--apply", "--plan", "report.json"],
        ["--testing", "--force", "--apply"],
        ["--testing", "--force", "--plan", "report.json"],
    ],
)
def test_acknowledgement_requires_the_complete_destructive_invocation(options: list[str]) -> None:
    import argparse
    from typing import cast

    from forge.cli.commands.datasets import configure, run_deduplication
    from forge.startup.context import ApplicationContext

    parser = argparse.ArgumentParser()
    configure(parser)
    arguments = parser.parse_args(
        ["deduplicate", *options, "--accept-unverified-legacy-references"]
    )
    # Rejection must occur before inspecting the application or opening a registry.
    with pytest.raises(MarketForgeError, match="requires --testing --force --apply --plan"):
        run_deduplication(arguments, cast(ApplicationContext, None))
