from __future__ import annotations

from typing import Any

import requests

BASE_URL = "https://api.gateio.ws"


def get(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> Any:
    response = requests.get(
        f"{BASE_URL}{path}",
        params=params,
        timeout=timeout,
    )

    response.raise_for_status()

    return response.json()
