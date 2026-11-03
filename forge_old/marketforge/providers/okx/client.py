from __future__ import annotations

from typing import Any

import requests

BASE_URL = "https://www.okx.com"


BASE_URL = "https://www.okx.com"


class OKXAPIError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
    ) -> None:
        self.code = code
        self.message = message

        super().__init__(f"OKX API error {code}: {message}")


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

    # OKX may return useful API error information
    # together with a non-2xx HTTP status.
    try:
        data = response.json()
    except ValueError:
        response.raise_for_status()
        raise

    code = str(data.get("code", ""))

    if code != "0":
        raise OKXAPIError(
            code=code,
            message=str(data.get("msg", "")),
        )

    response.raise_for_status()

    return data
