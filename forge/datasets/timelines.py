"""Historical timeline segmentation over retained membership observations."""

import base64
import binascii
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Self, cast
from uuid import UUID

from forge.shared.errors import MarketForgeError

# Each observation is an inclusive first/last event pair. Equality joins two
# endpoints; a one-nanosecond separation is still a different observed span.
# Unknown bounds get a separate segment. The sweep uses the running maximum so
# a nested observation cannot split an otherwise overlapping cluster.
SEGMENTS = """
, timeline_evidence AS (
    SELECT operation,ref,publication_id,engine_dataset_id,symbol,status,integrity,requested,id,
           CASE WHEN lo<=hi THEN lo END AS lo, CASE WHEN lo<=hi THEN hi END AS hi,
           jsonb_build_array(operation,instrument_namespace,unresolved_namespace_output,
               exchange,instrument_id,instrument_type,market_category,data_type,target_quote,
               variant,representation,stream_id) AS identity
    FROM observations WHERE exchange IS NOT NULL AND exchange <> '' AND instrument_id <> ''
), spans AS (
    SELECT DISTINCT identity,ref,publication_id,engine_dataset_id,symbol,lo,hi,status,integrity,requested,id
    FROM timeline_evidence
    WHERE (%(timeline_identity)s::text IS NULL OR
           encode(sha256(convert_to(identity::text,'UTF8')),'hex')=%(timeline_identity)s)
), sweep AS (
    SELECT *, max(hi) OVER (
        PARTITION BY identity, (lo IS NULL OR hi IS NULL)
        ORDER BY lo,hi,ref,id,symbol ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS preceding_end
    FROM spans
), numbered AS (
    SELECT *, sum(CASE WHEN lo IS NOT NULL AND hi IS NOT NULL
                       AND (preceding_end IS NULL OR lo>preceding_end) THEN 1 ELSE 0 END)
        OVER (PARTITION BY identity,(lo IS NULL OR hi IS NULL)
              ORDER BY lo,hi,ref,id,symbol ROWS UNBOUNDED PRECEDING) AS segment
    FROM sweep
), contributors AS (
    SELECT identity,segment,ref,publication_id,engine_dataset_id,
           max(symbol) AS symbol, min(lo) FILTER (WHERE segment<>0) AS lo,
           max(hi) FILTER (WHERE segment<>0) AS hi, requested,
           CASE WHEN count(DISTINCT status)=1 THEN min(status) ELSE 'mixed' END AS status,
           CASE WHEN count(DISTINCT integrity)=1 THEN min(integrity) ELSE 'mixed' END AS integrity,
           count(DISTINCT id) AS matching_primary_files
    FROM numbered GROUP BY identity,segment,ref,publication_id,engine_dataset_id,requested
), grouped AS (
    SELECT identity,segment,max(symbol) AS symbol,
           min(lo) AS start_timestamp_ns,max(hi) AS end_timestamp_ns,
           count(*) AS publication_count,coalesce(max(publication_id),0) AS watermark,
           sum(matching_primary_files)::bigint AS matching_primary_files,
           CASE WHEN count(DISTINCT status)=1 THEN min(status) ELSE 'mixed' END AS status,
           CASE WHEN count(DISTINCT integrity)=1 THEN min(integrity) ELSE 'mixed' END AS integrity,
           bool_or((%(timeline_publication)s::bigint IS NULL OR publication_id=%(timeline_publication)s)
               AND (%(timeline_engine)s::text IS NULL OR engine_dataset_id=%(timeline_engine)s)) AS selected
    FROM contributors GROUP BY identity,segment
), with_neighbors AS (
    SELECT *, lag(end_timestamp_ns) OVER (PARTITION BY identity ORDER BY segment) AS prior_observed_end_ns
    FROM grouped
), inventory AS (
    SELECT *,identity->>0 AS operation,identity->>3 AS exchange,identity->>4 AS instrument_id
    FROM with_neighbors
    WHERE selected
      AND (%(timeline_identity)s::text IS NULL OR
           start_timestamp_ns IS NOT DISTINCT FROM %(timeline_lo)s::bigint
           AND end_timestamp_ns IS NOT DISTINCT FROM %(timeline_hi)s::bigint)
), selected_page AS (
    SELECT *, row_number() OVER (ORDER BY {order}) AS position
    FROM inventory ORDER BY {order} LIMIT %(limit)s OFFSET %(offset)s
), page AS (
    SELECT p.*, refs.publications, encode(sha256(convert_to(p.identity::text,'UTF8')),'hex') AS identity_key
    FROM selected_page p
    CROSS JOIN LATERAL (
        SELECT jsonb_agg(jsonb_build_object(
            'registry_publication_id',c.publication_id,'engine_publication_id',c.engine_dataset_id,
            'output_ref',CASE WHEN c.publication_id IS NULL THEN c.ref END,
            'start_timestamp_ns',c.lo,'end_timestamp_ns',c.hi,
            'requested_interval',c.requested,'status',c.status,'integrity',c.integrity,
            'matching_primary_files',CASE WHEN c.publication_id IS NOT NULL THEN c.matching_primary_files END
        ) ORDER BY c.publication_id NULLS LAST,c.ref) AS publications
        FROM contributors c WHERE (c.identity,c.segment)=(p.identity,p.segment)
    ) refs
), timeline_totals AS (
    SELECT count(DISTINCT i.identity) AS identities,
           count(*) FILTER (WHERE i.segment<>0) AS observed_segments,
           count(*) FILTER (WHERE i.segment=0) AS unknown_segments,
           count(*) FILTER (WHERE i.publication_count>1) AS multi_publication_segments,
           count(*) FILTER (WHERE i.prior_observed_end_ns < i.start_timestamp_ns) AS observed_separations
    FROM inventory i
), contributing_outputs AS (
    SELECT DISTINCT i.operation,c.ref FROM inventory i JOIN contributors c USING(identity,segment)
)
"""

IDENTITY_FIELDS = (
    "operation",
    "instrument_namespace",
    "unresolved_namespace_output",
    "exchange",
    "instrument_id",
    "instrument_type",
    "market_category",
    "data_type",
    "target_quote",
    "variant",
    "representation",
    "stream_id",
)


@dataclass(frozen=True, slots=True)
class TimelineReference:
    registry: str
    operation: str
    identity: str
    watermark: int
    lo: int | None
    hi: int | None
    fingerprint: str
    status: str | None
    start: int | None
    end: int | None
    search: str | None
    output: str | None
    filters: list[str | None]

    def membership_filters(self) -> dict[str, Any]:
        return dict(
            zip(
                ("exchange", "instrument", "instrument_type", "market_category", "data_type"),
                self.filters,
                strict=True,
            )
        )

    def encode(self) -> str:
        return "tl1." + base64.urlsafe_b64encode(
            json.dumps(list(asdict(self).values()), separators=(",", ":")).encode()
        ).decode().rstrip("=")

    @classmethod
    def decode(cls, token: str) -> Self:
        try:
            if len(token) > 8192 or not re.fullmatch(r"tl1\.[A-Za-z0-9_-]+", token):
                raise ValueError
            raw = token[4:]
            decoded: object = json.loads(
                base64.b64decode(raw + "=" * (-len(raw) % 4), altchars=b"-_", validate=True)
            )
            if not isinstance(decoded, list):
                raise ValueError
            values = cast(list[Any], decoded)
            (
                registry,
                operation,
                identity,
                watermark,
                lo,
                hi,
                fingerprint,
                status,
                start,
                end,
                search,
                output,
                filters,
            ) = values
            if (
                str(UUID(registry)) != registry
                or operation not in ("normalize", "merge", "convert")
                or any(
                    not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value)
                    for value in (identity, fingerprint, output if output is not None else identity)
                )
                or type(watermark) is not int
                or not 0 <= watermark < 2**63
                or any(
                    value is not None and (type(value) is not int or not -(2**63) <= value < 2**63)
                    for value in (lo, hi, start, end)
                )
                or not isinstance(filters, list)
                or len(cast(list[Any], filters)) != 5
                or any(
                    value is not None and not isinstance(value, str)
                    for value in (status, search, *cast(list[Any], filters))
                )
            ):
                raise ValueError
            return cls(*values)
        except (ValueError, TypeError, AttributeError, binascii.Error) as error:
            raise MarketForgeError(
                "Use an exact tl1 timeline reference from the selected registry."
            ) from error


def publication_fingerprint(publications: list[dict[str, Any]]) -> str:
    from hashlib import sha256

    pins = [
        [
            row["registry_publication_id"],
            row["output_ref"],
            row["start_timestamp_ns"],
            row["end_timestamp_ns"],
            row["matching_primary_files"],
        ]
        for row in publications
    ]
    return sha256(json.dumps(pins, separators=(",", ":")).encode()).hexdigest()
