"""Summarize preparation evidence without changing input selection or validation."""

from dataclasses import dataclass, field

from forge.markets.models import array, obj


def blocker_category(reason: str) -> str:
    if reason.startswith("No immutable historical specification"):
        return "missing_historical_specifications"
    if reason.startswith("Historical family catalogue prerequisite missing"):
        return "missing_historical_family_authority"
    if reason.startswith("No persisted canonical namespace mapping"):
        return "missing_canonical_identity"
    return "input_resolution_error"


@dataclass
class _Group:
    context: dict[str, object]
    category: str
    severity: str
    local_ids: set[int] = field(default_factory=lambda: set[int]())
    request_indices: set[int] = field(default_factory=lambda: set[int]())
    reasons: set[str] = field(default_factory=lambda: set[str]())


def summarize(requests: list[dict[str, object]]) -> dict[str, object]:
    """Keep every original diagnostic; add deterministic, deduplicated group counts.

    Coverage annotations are warnings, not the input-resolution exceptions or
    nominal interval gaps that actually mark the batch partial.
    """
    groups: dict[tuple[str, ...], _Group] = {}
    matched_ids: set[int] = set()
    blocked_ids: set[int] = set()
    warning_ids: set[int] = set()
    gap_requests: list[int] = []
    gap_count = 0

    def add(
        context: dict[str, object],
        request_index: int,
        category: str,
        severity: str,
        reason: str,
        identities: set[int],
    ) -> None:
        key = (*[str(value or "") for value in context.values()], category, severity)
        group = groups.setdefault(key, _Group(context, category, severity))
        group.local_ids.update(identities)
        group.request_indices.add(request_index)
        group.reasons.add(reason)
        (blocked_ids if severity == "blocker" else warning_ids).update(identities)

    for entry in requests:
        request_index = int(str(entry["request_index"]))
        request = obj(entry.get("request", {}))
        scope = obj(request.get("scope", {}))
        options = obj(request.get("source_options") or {})
        context = {
            key: request.get(key)
            for key in ("exchange", "instrument_type", "market_category", "data_type")
        }
        context.update(
            instrument=scope.get("identifier"),
            scope_kind=scope.get("kind"),
            feed=options.get("feed"),
        )
        matched_ids.update(
            int(str(obj(item)["local_id"]))
            for item in [
                *array(entry.get("matched_archives", [])),
                *array(entry.get("selected_archives", [])),
            ]
        )

        for value in array(entry.get("blockers", [])):
            blocker = obj(value)
            reason = str(blocker["reason"])
            add(
                context,
                request_index,
                blocker_category(reason),
                "blocker",
                reason,
                {int(str(blocker["local_id"]))},
            )
        coverage = obj(entry.get("coverage", {}))
        count = int(str(coverage.get("gap_count") or 0))
        if count:
            gap_requests.append(request_index)
            gap_count += count
        for reason, identities in obj(coverage.get("diagnostics", {})).items():
            category = (
                "unknown_acquisition_feed"
                if reason == "unknown feed identity"
                else "unresolved_asset_metadata"
                if reason in {"unknown or ambiguous base_asset", "unknown or ambiguous quote_asset"}
                else "coverage_annotation"
            )
            add(
                context,
                request_index,
                category,
                "warning",
                reason,
                {int(str(identity)) for identity in array(identities)},
            )
    category_ids: dict[tuple[str, str], set[int]] = {}
    for group in groups.values():
        category_ids.setdefault((group.severity, group.category), set()).update(group.local_ids)
    return {
        "matched_archive_count": len(matched_ids),
        "blocking_archive_count": len(blocked_ids),
        "warning_archive_count": len(warning_ids),
        "coverage_gap_request_indices": gap_requests,
        "coverage_gap_count": gap_count,
        "category_totals": [
            {"severity": severity, "category": category, "archive_count": len(ids)}
            for (severity, category), ids in sorted(category_ids.items())
        ],
        "groups": [
            {
                **group.context,
                "category": group.category,
                "severity": group.severity,
                "archive_count": len(group.local_ids),
                "local_ids": sorted(group.local_ids),
                "request_indices": sorted(group.request_indices),
                "reasons": sorted(group.reasons),
            }
            for _, group in sorted(groups.items())
        ],
    }
