from __future__ import annotations

from typing import Any

import requests

BASE_URL = "https://api.bybit.com"


def get(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    response = requests.get(
        f"{BASE_URL}{path}",
        params=params,
        timeout=timeout,
    )

    response.raise_for_status()

    data = response.json()

    if data.get("retCode") != 0:
        raise RuntimeError(
            f"Bybit API error {data.get('retCode')}: " f"{data.get('retMsg')}"
        )

    return data
