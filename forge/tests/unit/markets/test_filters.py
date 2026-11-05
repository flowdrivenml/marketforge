"""Only user-facing glob metacharacters become SQL wildcards."""

import pytest

from forge.markets.filters import symbol_pattern


@pytest.mark.parametrize(
    ("glob", "sql"),
    [
        (None, None),
        ("", ""),
        ("ETH*", "ETH%"),
        ("*BTC", "%BTC"),
        ("eth?usdt", "eth_usdt"),
        ("ETH_USDT", "ETH!_USDT"),
        ("ETH%USDT", "ETH!%USDT"),
        ("A!B*", "A!!B%"),
        ("ETH[ABC]", "ETH[ABC]"),
        ("A\\B", "A\\B"),
        ("' OR true --*", "' OR true --%"),
    ],
)
def test_native_symbol_glob(glob: str | None, sql: str | None) -> None:
    assert symbol_pattern(glob) == sql
