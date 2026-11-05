"""Efficient retained inputs from a caller-owned repeatable-read snapshot."""

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, class_row

from forge.markets.ranking.conversion import SpotPrice
from forge.markets.ranking.policy import RankingPolicy


async def turnover_rows(
    c: AsyncConnection[TupleRow], identities: list[int], *, refresh_id: int | None = None
) -> dict[int, TupleRow]:
    result: dict[int, TupleRow] = {}
    for start in range(0, len(identities), 1000):
        cur = await c.execute(
            """SELECT i.id,o.id,o.turnover,o.turnover_unit,o.turnover_estimated,o.fetched_at,o.window_end,o.scope_complete,o.mapping_version,o.raw_json,
            o.base_volume,o.contract_volume,o.base_unit,o.quote_unit,o.last_price,o.window_start
            FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id
            LEFT JOIN LATERAL (
                SELECT obs.id,obs.turnover,obs.turnover_unit,obs.turnover_estimated,obs.fetched_at,obs.window_end,obs.mapping_version,obs.raw_json,
                obs.base_volume,obs.contract_volume,obs.base_unit,obs.quote_unit,obs.last_price,obs.window_start,
                (r.status='complete' OR EXISTS(SELECT 1 FROM jsonb_array_elements(r.source_outcomes) s
                 WHERE s->>'exchange'=e.code AND s->>'market'=i.market_category AND s->>'status'='complete')) AS scope_complete
                FROM catalog.metric_observations obs JOIN catalog.metric_refreshes r ON r.id=obs.refresh_id
                WHERE obs.instrument_id=i.id AND r.status IN ('complete','partial','cancelled')
                  AND (%s::bigint IS NULL OR r.id=%s)
                ORDER BY obs.window_end DESC,obs.fetched_at DESC,obs.id DESC LIMIT 1
            ) o ON true WHERE i.id=ANY(%s)""",
            (refresh_id, refresh_id, identities[start : start + 1000]),
        )
        for row in await cur.fetchall():
            result[row[0]] = row[1:]
    return result


async def spot_prices(
    c: AsyncConnection[TupleRow],
    policy: RankingPolicy,
    currencies: set[str],
    *,
    refresh_id: int | None = None,
) -> list[SpotPrice]:
    if not policy.conversion.automatic:
        return []
    assets = sorted(
        currencies | {policy.comparison_currency} | set(policy.conversion.bridge_assets)
    )
    result: list[SpotPrice] = []
    after = 0
    while True:
        async with c.cursor(row_factory=class_row(SpotPrice)) as cur:
            await cur.execute(
                """SELECT i.id AS instrument_id,e.code||'.spot.'||i.symbol AS reference,e.code AS exchange,
                COALESCE(o.base_unit,i.base_asset) AS base,COALESCE(o.quote_unit,i.quote_asset) AS quote,o.id AS observation_id,o.last_price AS price,
                o.fetched_at,o.source_observed_at,o.window_end,(o.id IS NOT NULL) AS complete
                FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id
                LEFT JOIN LATERAL (
                  SELECT obs.id,obs.base_unit,obs.quote_unit,obs.last_price,obs.fetched_at,obs.source_observed_at,obs.window_end
                  FROM catalog.metric_observations obs JOIN catalog.metric_refreshes r ON r.id=obs.refresh_id
                  WHERE obs.instrument_id=i.id AND r.status IN ('complete','partial','cancelled')
                  AND (%s::bigint IS NULL OR r.id=%s)
                    AND (r.status='complete' OR EXISTS(SELECT 1 FROM jsonb_array_elements(r.source_outcomes) s
                     WHERE s->>'exchange'=e.code AND s->>'market'='spot' AND s->>'status'='complete'))
                  ORDER BY obs.window_end DESC,obs.fetched_at DESC,obs.id DESC LIMIT 1
                ) o ON true
                WHERE i.instrument_type='spot' AND i.market_category='spot'
                  AND i.status IN ('active','TRADING','Trading','live','tradable','online')
                  AND ((COALESCE(o.base_unit,i.base_asset)=ANY(%s)
                        AND COALESCE(o.quote_unit,i.quote_asset)=ANY(%s))
                    OR (%s AND (COALESCE(o.base_unit,i.base_asset)=ANY(%s)
                        OR COALESCE(o.quote_unit,i.quote_asset)=ANY(%s))))
                  AND e.code=ANY(%s) AND i.id>%s
                ORDER BY i.id LIMIT 1000""",
                (
                    refresh_id,
                    refresh_id,
                    assets,
                    assets,
                    not policy.conversion.bridge_assets and policy.conversion.max_hops == 2,
                    sorted(currencies | {policy.comparison_currency}),
                    sorted(currencies | {policy.comparison_currency}),
                    list(policy.conversion.source_exchanges),
                    after,
                ),
            )
            page = list(await cur.fetchall())
        if not page:
            return result
        result.extend(page)
        after = page[-1].instrument_id
