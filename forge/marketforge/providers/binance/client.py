from __future__ import annotations

from typing import Any

import requests

SPOT_BASE_URL = "https://api.binance.com"
LINEAR_BASE_URL = "https://fapi.binance.com"
INVERSE_BASE_URL = "https://dapi.binance.com"


def get(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = 30.0,
    base_url: str = SPOT_BASE_URL,
) -> dict[str, Any]:
    response = requests.get(
        f"{base_url}{path}",
        params=params,
        timeout=timeout,
    )

    response.raise_for_status()

    return response.json()
