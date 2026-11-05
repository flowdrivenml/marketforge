"""Stored spot reference valuations: deterministic, at most one same-venue bridge."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Literal

from forge.markets.ranking.policy import ManualRate, RankingPolicy, currency

PRECISION = 80
ALGORITHM = "turnover24h-spotfx-rational80-v2"
Method = Literal["identity", "manual", "equivalence", "direct", "bridged"]


def decimal_ratio(value: Fraction) -> Decimal:
    """Round once to 80 significant decimal digits; retain the exact ratio too."""
    with localcontext() as context:
        context.prec = PRECISION
        return Decimal(value.numerator) / Decimal(value.denominator)


@dataclass(frozen=True)
class SpotPrice:
    instrument_id: int
    reference: str
    exchange: str
    base: str
    quote: str
    observation_id: int | None
    price: Decimal | None
    fetched_at: datetime | None
    source_observed_at: datetime | None
    window_end: datetime | None
    complete: bool

    @property
    def time(self) -> datetime | None:
        return self.source_observed_at or self.window_end


@dataclass(frozen=True)
class Edge:
    source: SpotPrice
    inverted: bool

    @property
    def start(self) -> str:
        return self.source.quote if self.inverted else self.source.base

    @property
    def end(self) -> str:
        return self.source.base if self.inverted else self.source.quote


@dataclass(frozen=True)
class Conversion:
    source_currency: str
    comparison_currency: str
    target_exchange: str
    method: Method
    numerator: int
    denominator: int
    path: tuple[str, ...]
    legs: tuple[Edge, ...] = ()
    as_of: datetime | None = None
    source: str | None = None

    @property
    def rate(self) -> Decimal:
        return decimal_ratio(Fraction(self.numerator, self.denominator))

    def score(self, native: Decimal) -> Decimal:
        return decimal_ratio(Fraction(native) * Fraction(self.numerator, self.denominator))

    def document(self) -> dict[str, object]:
        return {
            "algorithm_version": ALGORITHM,
            "valuation": "reference_price_turnover",
            "convention": "Aggregate native turnover valued at reference prices; not execution-time conversion of individual trades.",
            "method": self.method,
            "from_currency": self.source_currency,
            "comparison_currency": self.comparison_currency,
            "target_exchange": self.target_exchange,
            "rate": self.rate,
            "numerator": str(self.numerator),
            "denominator": str(self.denominator),
            "precision": PRECISION,
            "score_formula": "round80(Fraction(native_turnover) * numerator / denominator)",
            "path": list(self.path),
            "as_of": self.as_of,
            "source": self.source,
            "legs": [
                {
                    "instrument_id": e.source.instrument_id,
                    "instrument_reference": e.source.reference,
                    "exchange": e.source.exchange,
                    "instrument_type": "spot",
                    "market_category": "spot",
                    "base_asset": e.source.base,
                    "quote_asset": e.source.quote,
                    "observation_id": e.source.observation_id,
                    "original_price": e.source.price,
                    "inverted": e.inverted,
                    "from_currency": e.start,
                    "to_currency": e.end,
                    "fetched_at": e.source.fetched_at,
                    "source_observed_at": e.source.source_observed_at,
                    "window_end": e.source.window_end,
                    "effective_observed_at": e.source.time,
                }
                for e in self.legs
            ],
        }


class ConversionUnavailable(ValueError):
    """A specific, safe exclusion reason; never implies a zero monetary value."""


class ConversionResolver:
    def __init__(self, policy: RankingPolicy, now: datetime, prices: list[SpotPrice]) -> None:
        self.policy = policy
        self.now = now
        self.edges: dict[tuple[str, str, str], list[Edge]] = {}
        self.neighbors: dict[tuple[str, str], set[str]] = {}
        self.cache: dict[tuple[str, str, datetime | None], Conversion | str] = {}
        for price in prices:
            if price.base == price.quote:
                continue
            try:
                currency(price.base)
                currency(price.quote)
            except ValueError:
                continue
            for inverted in (False, True):
                edge = Edge(price, inverted)
                self.edges.setdefault((price.exchange, edge.start, edge.end), []).append(edge)
                self.neighbors.setdefault((price.exchange, edge.start), set()).add(edge.end)

    def resolve(self, unit: str, exchange: str, window_end: datetime | None) -> Conversion:
        key = (unit, exchange, window_end)
        if key not in self.cache:
            try:
                self.cache[key] = self._resolve(unit, exchange, window_end)
            except ConversionUnavailable as error:
                self.cache[key] = str(error)
        result = self.cache[key]
        if isinstance(result, str):
            raise ConversionUnavailable(result)
        return result

    def _resolve(self, unit: str, exchange: str, window_end: datetime | None) -> Conversion:
        p = self.policy
        target = p.comparison_currency
        if unit == target:
            return Conversion(unit, target, exchange, "identity", 1, 1, (unit,))
        manual = p.conversion.manual_rates.get(unit)
        if manual is None and unit in p.conversion_rates:
            if p.conversion_as_of is None or p.conversion_source is None:
                raise ConversionUnavailable("manual conversion lacks timestamp/source provenance")
            manual = ManualRate(
                rate=p.conversion_rates[unit], as_of=p.conversion_as_of, source=p.conversion_source
            )
        if manual is not None:
            return self._manual(unit, exchange, manual)
        if unit in p.conversion.equivalent_currencies:
            return Conversion(
                unit,
                target,
                exchange,
                "equivalence",
                1,
                1,
                (unit, target),
                source="Explicit policy 1:1 equivalence assumption",
            )
        if not p.conversion.automatic:
            raise ConversionUnavailable("no explicit conversion for " + unit)
        return self._market(unit, exchange, window_end)

    def _manual(self, unit: str, exchange: str, manual: ManualRate) -> Conversion:
        age = self.policy.conversion.max_price_age_seconds or self.policy.max_age_seconds
        if manual.as_of > self.now:
            raise ConversionUnavailable(
                "future manual conversion timestamp for "
                + unit
                + "; correct the override as_of timestamp"
            )
        if self.now - manual.as_of > timedelta(seconds=age):
            raise ConversionUnavailable(
                "stale manual conversion for "
                + unit
                + "; update or remove the override intentionally; automatic rates do not bypass it"
            )
        ratio = Fraction(manual.rate)
        return Conversion(
            unit,
            self.policy.comparison_currency,
            exchange,
            "manual",
            ratio.numerator,
            ratio.denominator,
            (unit, self.policy.comparison_currency),
            as_of=manual.as_of,
            source=manual.source,
        )

    def _issue(self, edge: Edge, window: datetime | None) -> str | None:
        s = edge.source
        p = self.policy
        if s.observation_id is None or s.price is None or not s.price.is_finite() or s.price <= 0:
            return "missing or invalid conversion price"
        if not s.complete:
            return "incomplete conversion source refresh"
        if (
            s.fetched_at is None
            or s.time is None
            or s.fetched_at.tzinfo is None
            or s.time.tzinfo is None
        ):
            return "missing conversion observation timestamps"
        if max(s.fetched_at, s.time) > self.now + timedelta(seconds=p.max_window_skew_seconds):
            return "future conversion observation"
        if min(s.fetched_at, s.time) < self.now - timedelta(
            seconds=p.conversion.max_price_age_seconds or p.max_age_seconds
        ):
            return "stale conversion observations"
        if window is not None and abs(s.time - window) > timedelta(
            seconds=p.max_window_skew_seconds
        ):
            return "conversion/turnover timestamp skew"
        return None

    def _market(self, unit: str, exchange: str, window: datetime | None) -> Conversion:
        from collections.abc import Iterator

        p = self.policy
        target = p.comparison_currency

        def paths() -> Iterator[tuple[int, tuple[Edge, ...]]]:
            for venue in p.conversion.source_exchanges:
                for edge in self.edges.get((venue, unit, target), []):
                    yield -1, (edge,)
                if p.conversion.max_hops == 2:
                    bridges = (
                        list(enumerate(p.conversion.bridge_assets))
                        if p.conversion.bridge_assets
                        else [
                            (0, asset)
                            for asset in sorted(
                                self.neighbors.get((venue, unit), set())
                                & self.neighbors.get((venue, target), set())
                            )
                        ]
                    )
                    for bridge_index, bridge in bridges:
                        if bridge in {unit, target}:
                            continue
                        for a in self.edges.get((venue, unit, bridge), []):
                            for b in self.edges.get((venue, bridge, target), []):
                                if a.source.observation_id != b.source.observation_id:
                                    yield bridge_index, (a, b)

        disallowed = False
        issues: set[str] = set()
        # Sort keys have uniform concrete element types; never use price/turnover as preference.
        best_key: tuple[int, int, int, int, int, tuple[tuple[str, int, bool], ...]] | None = None
        selected: tuple[Edge, ...] | None = None
        for bridge_index, path in paths():
            path_issues = [issue for e in path if (issue := self._issue(e, window)) is not None]
            if path_issues:
                issues.update(path_issues)
                continue
            times = [e.source.time for e in path if e.source.time is not None]
            if len(path) == 2 and max(times) - min(times) > timedelta(
                seconds=p.conversion.max_leg_skew_seconds
                if p.conversion.max_leg_skew_seconds is not None
                else p.max_window_skew_seconds
            ):
                issues.add("conversion leg timestamp skew")
                continue
            venue = path[0].source.exchange
            if venue != exchange and not p.conversion.allow_cross_exchange:
                disallowed = True
                continue
            time = min(times)
            delta = time - datetime(1970, 1, 1, tzinfo=UTC)
            micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
            key = (
                int(venue != exchange),
                len(path),
                {str(e): n for n, e in enumerate(p.conversion.source_exchanges)}[venue],
                -micros,
                bridge_index,
                tuple((e.source.reference, e.source.observation_id or 0, e.inverted) for e in path),
            )
            if best_key is None or key < best_key:
                best_key, selected = key, path
        if selected is None:
            reason = (
                "cross-exchange conversion disallowed"
                if disallowed
                else next(
                    (
                        i
                        for i in (
                            "future conversion observation",
                            "stale conversion observations",
                            "conversion leg timestamp skew",
                            "conversion/turnover timestamp skew",
                            "missing or invalid conversion price",
                            "incomplete conversion source refresh",
                            "missing conversion observation timestamps",
                        )
                        if i in issues
                    ),
                    "no eligible conversion pair/path",
                )
            )
            raise ConversionUnavailable(
                f"{reason} for {unit} -> {target}; refresh stored spot statistics with marketforge markets refresh --market spot --testing"
            )
        ratio = Fraction(1)
        for edge in selected:
            assert edge.source.price is not None
            value = Fraction(edge.source.price)
            ratio *= 1 / value if edge.inverted else value
        return Conversion(
            unit,
            target,
            exchange,
            "direct" if len(selected) == 1 else "bridged",
            ratio.numerator,
            ratio.denominator,
            (unit, *(e.end for e in selected)),
            selected,
        )
