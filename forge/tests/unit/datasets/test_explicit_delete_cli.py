import argparse
from typing import cast

import pytest

from forge.cli.commands.datasets import configure, run_explicit_delete
from forge.shared.errors import MarketForgeError
from forge.startup.context import ApplicationContext


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    configure(result)
    return result


def test_preview_accepts_exact_distinct_target_and_retained_id_lists() -> None:
    parsed = parser().parse_args(
        [
            "delete",
            "--testing",
            "--publication-id",
            "481",
            "--publication-id",
            "482",
            "--retain-publication-id",
            "479",
            "--retain-publication-id",
            "480",
        ]
    )
    assert parsed.publication_id == [481, 482]
    assert parsed.retain_publication_id == [479, 480]
    assert not parsed.apply and not parsed.cascade


def test_invalid_publication_id_fails_at_the_parser() -> None:
    with pytest.raises(SystemExit):
        parser().parse_args(["delete", "--testing", "--publication-id", "0"])


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["--production", "--publication-id", "481"], "requires explicit --testing"),
        (["--testing", "--apply", "--publication-id", "481"], "requires --plan"),
        (
            ["--testing", "--apply", "--plan", "/fixture/report.json", "--cascade"],
            "from the reviewed plan",
        ),
        (
            ["--testing", "--publication-id", "481", "--accept-unverified-legacy-references"],
            "require --apply",
        ),
    ],
)
def test_delete_rejects_expanded_or_unreviewed_application(
    arguments: list[str], message: str
) -> None:
    args = parser().parse_args(["delete", *arguments])
    with pytest.raises(MarketForgeError, match=message):
        run_explicit_delete(args, cast(ApplicationContext, None))
