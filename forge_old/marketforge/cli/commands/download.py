from __future__ import annotations

import argparse

from marketforge.acquisition.client import MarketForgeClient
from marketforge.cli.arguments import request_from_args
from marketforge.cli.ui.console import console
from marketforge.cli.ui.plan import print_plan
from marketforge.cli.ui.progress import DownloadProgress
from marketforge.cli.ui.results import print_results


def run(
    args: argparse.Namespace,
) -> int:
    """Plan and execute a historical-data acquisition."""

    request = request_from_args(args)

    with MarketForgeClient(data_root=args.data_root) as client:
        plan = client.plan(request)

        print_plan(plan)

        if plan.file_count == 0:
            console.print()
            console.print(
                "No archives to download.",
                style="muted",
            )
            return 0

        console.print()
        console.print(
            "Downloading",
            style="heading",
        )
        console.print()

        with DownloadProgress() as progress:
            results = client.download(
                plan,
                progress=progress.callback,
            )

    print_results(results)

    return 0
