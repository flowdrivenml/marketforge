"""Resolved work hashes exclude destination identities and retain semantic order."""

from forge.historical.engine.work import convert_key, merge_key, normalize_key


def test_normalization_keeps_raw_and_interpretation_evidence() -> None:
    batch = {"run_id": "old", "instrument_namespace": "catalogue"}
    job = {
        "job_id": 1,
        "dataset_id": 2,
        "tasks": [{"task_id": 7}, {"task_id": 8}],
        "output": {"dataset_path": "old", "staging_path": "old.tmp"},
    }
    runtime = {"resources": {"parquet": {"file_target_bytes": 200}}, "diagnostics": {"mode": "off"}}
    raw = {"raw.csv": "digest"}
    profiles = [{"id": "spot.trades", "sha256": "profile-fingerprint"}]
    old = normalize_key("registry", "build", batch, job, raw, runtime, profiles)
    new_job = {**job, "job_id": 9, "dataset_id": 10, "output": {"dataset_path": "new"}}
    assert old == normalize_key(
        "registry",
        "build",
        {**batch, "run_id": "new"},
        new_job,
        raw,
        {**runtime, "diagnostics": {"mode": "detailed"}},
        profiles,
    )
    assert old != normalize_key("registry", "different", batch, job, raw, runtime, profiles)
    assert old != normalize_key("registry", "build", batch, job, {}, runtime, profiles)
    assert old != normalize_key("registry", "build", batch, job, raw, {}, profiles)
    assert old != normalize_key("registry", "build", batch, job, raw, runtime, [])


def test_merge_keeps_input_precedence_and_stream_rank() -> None:
    inputs = [{"id": 1, "stream_rank": 3}, {"id": 2, "stream_rank": 4}]
    job = {"job_id": 3, "output": {"dataset_path": "old"}, "inputs": inputs}
    old = merge_key("registry", "build", job, {}, inputs)
    assert old == merge_key(
        "registry", "build", {**job, "job_id": 9, "output": {"dataset_path": "new"}}, {}, inputs
    )
    assert old != merge_key("registry", "build", {**job, "inputs": inputs[::-1]}, {}, inputs[::-1])
    assert old != merge_key(
        "registry",
        "build",
        {**job, "inputs": [{**inputs[0], "stream_rank": 0}, inputs[1]]},
        {},
        inputs,
    )


def test_conversion_keeps_valuation_settings() -> None:
    job = {"job_id": 3, "output": {"dataset_path": "old"}, "conversion": {"max_hops": 2}}
    old = convert_key("registry", "build", job)
    assert old == convert_key(
        "registry", "build", {**job, "job_id": 9, "output": {"dataset_path": "new"}}
    )
    assert old != convert_key("registry", "build", {**job, "conversion": {"max_hops": 3}})
    assert old != convert_key("different-registry", "build", job)
