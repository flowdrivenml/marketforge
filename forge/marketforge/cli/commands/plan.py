from __future__ import annotations

import argparse

from marketforge.acquisition.client import MarketForgeClient
from marketforge.cli.arguments import request_from_args
from marketforge.cli.ui.plan import print_plan


def run(
    args: argparse.Namespace,
) -> int:
    """Run the acquisition planning command."""

    request = request_from_args(args)

    with MarketForgeClient(data_root=args.data_root) as client:
        plan = client.plan(request)

    print_plan(plan)

    return 0
