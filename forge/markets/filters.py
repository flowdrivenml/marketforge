"""Native-symbol globs compiled for bound PostgreSQL ILIKE patterns."""


def symbol_pattern(pattern: str | None) -> str | None:
    """Only * and ? are wildcards; SQL %, _ and escape ! remain literal."""
    if pattern is None:
        return None
    return "".join(
        {"!": "!!", "%": "!%", "_": "!_", "*": "%", "?": "_"}.get(char, char) for char in pattern
    )
