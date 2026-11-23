from forge.datasets.canonical import optimize
from forge.datasets.force_cleanup import graph_blockers


def test_larger_id_can_preserve_more_transitive_work() -> None:
    choices, result = optimize([(58, 156)], {58: {"one"}, 156: {"two", "three", "four"}}, (58,))
    assert choices == (156,)
    assert (result["baseline_preserved"], result["optimized_preserved"]) == (1, 3)


def test_global_choice_counts_shared_multi_parent_outputs_once() -> None:
    # Independent choices prefer losing one local output per group. Losing
    # the same two shared outputs across all three groups is globally better.
    graph = {
        10: {"shared-1", "shared-2"},
        11: {"local-a"},
        20: {"shared-1", "shared-2"},
        21: {"local-b"},
        30: {"shared-1", "shared-2"},
        31: {"local-c"},
    }
    choices, result = optimize([(10, 11), (20, 21), (30, 31)], graph, (10, 20, 30))
    assert choices == (11, 21, 31)
    assert result["downstream_outputs"] == 5
    assert result["optimized_affected"] == 2


def test_smaller_id_wins_on_impact_and_existing_choice_wins_exact_ties() -> None:
    assert optimize([(1, 2)], {1: {"a", "b"}, 2: {"c"}}, (2,))[0] == (1,)
    assert optimize([(1, 2)], {1: {"a"}, 2: {"b"}}, (2,))[0] == (2,)


def test_bounded_search_is_deterministic_and_never_worse_than_baseline() -> None:
    groups = [(1, 2), (3, 4), (5, 6)]
    graph = {1: {"a"}, 2: {"b", "c"}, 3: {"b"}, 4: {"d", "c"}, 5: {"a"}, 6: {"e", "c"}}
    first = optimize(groups, graph, (1, 3, 5), search_limit=1)
    assert first == optimize(groups, graph, (1, 3, 5), search_limit=1)
    assert first[1]["approximate_components"] == 1
    assert first[1]["optimized_affected"] <= first[1]["baseline_affected"]


def test_cyclic_lineage_blocks_the_branch() -> None:
    graph = {
        "edges": [{"parent": 1, "child": 2}, {"parent": 2, "child": 3}, {"parent": 3, "child": 2}],
        "dependents": [],
        "conversion_references": [],
    }
    assert "Downstream registry lineage contains a cycle." in graph_blockers(graph, set())
