"""Choose force-cleanup survivors by distinct downstream outputs, across groups."""

from itertools import product
from math import prod


def optimize(
    groups: list[tuple[int, ...]],
    affected: dict[int, set[str]],
    baseline: tuple[int, ...],
    *,
    search_limit: int = 4096,
) -> tuple[tuple[int, ...], dict[str, int]]:
    """Solve each independent impact component exactly when bounded.

    Large components use deterministic coordinate descent from the old selection.
    An impact token represents one downstream output regardless of how many edges
    reach it. Dependent physical outputs are retained, so physical tie-breaking
    reduces to retaining the existing canonical decisions.
    """
    memberships: dict[str, set[int]] = {}
    for index, candidates in enumerate(groups):
        for key in set[str]().union(*(affected[item] for item in candidates)):
            memberships.setdefault(key, set()).add(index)
    neighbours = [set[int]() for _ in groups]
    for members in memberships.values():
        for index in members:
            neighbours[index].update(members)
    unseen = set(range(len(groups)))
    chosen = list(baseline)
    approximate = 0
    interacting_groups = 0
    components = 0
    while unseen:
        component = {min(unseen)}
        pending = list(component)
        while pending:
            fresh = neighbours[pending.pop()] - component
            component.update(fresh)
            pending.extend(sorted(fresh))
        unseen.difference_update(component)
        components += 1
        if len(component) > 1:
            interacting_groups += len(component)
        indices = sorted(component)
        choices = [groups[index] for index in indices]
        initial = tuple(baseline[index] for index in indices)

        def cost(
            selection: tuple[int, ...],
            choices: list[tuple[int, ...]] = choices,
            initial: tuple[int, ...] = initial,
        ) -> tuple[int, int, tuple[int, ...]]:
            invalidated = set[str]().union(
                *(
                    affected[item]
                    for candidates, keep in zip(choices, selection, strict=True)
                    for item in candidates
                    if item != keep
                )
            )
            return (
                len(invalidated),
                sum(pick != prior for pick, prior in zip(selection, initial, strict=True)),
                tuple(
                    candidates.index(item)
                    for candidates, item in zip(choices, selection, strict=True)
                ),
            )

        if prod(map(len, choices)) <= search_limit:
            best = min(product(*choices), key=cost)
        else:
            approximate += 1
            best = initial
            for _ in range(len(choices)):
                before = best
                for position, candidates in enumerate(choices):
                    best = min(
                        (best[:position] + (item,) + best[position + 1 :] for item in candidates),
                        key=cost,
                    )
                if before == best:
                    break
        for index, item in zip(indices, best, strict=True):
            chosen[index] = item

    def losses(selection: list[int] | tuple[int, ...]) -> set[str]:
        return set[str]().union(
            *(
                affected[item]
                for candidates, keep in zip(groups, selection, strict=True)
                for item in candidates
                if item != keep
            )
        )

    universe = set[str]().union(*(affected[item] for group in groups for item in group))
    old, new = losses(baseline), losses(chosen)
    assert len(new) <= len(old)
    return tuple(chosen), {
        "downstream_outputs": len(universe),
        "baseline_preserved": len(universe - old),
        "optimized_preserved": len(universe - new),
        "baseline_affected": len(old),
        "optimized_affected": len(new),
        "approximate_components": approximate,
        "components": components,
        "interacting_groups": interacting_groups,
    }
