from __future__ import annotations

from typing import Any

import requests

BASE_URL = "https://api.bitget.com"


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

    if data.get("code") != "00000":
        raise RuntimeError(
            f"Bitget API error {data.get('code')}: " f"{data.get('msg')}"
        )

    return data
