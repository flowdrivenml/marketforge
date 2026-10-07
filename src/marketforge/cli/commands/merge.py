from __future__ import annotations

import argparse

from marketforge.cli.arguments import date_to_ns
from marketforge.database import connect
from marketforge.processing.datasets import DatasetRepository
from marketforge.processing.merge import MergePlanner, write_merge_job


def run(
    args: argparse.Namespace,
) -> int:
    """
    Plan a canonical dataset merge and generate merge.json.

    Rust execution is intentionally separate.
    """

    with connect() as conn:
        planner = MergePlanner(conn)

        plan = planner.plan(
            args.dataset_ids,
            start_timestamp_ns=date_to_ns(args.start),
            end_timestamp_ns=date_to_ns(args.end),
        )

        repository = DatasetRepository(conn)

        dataset = repository.create(
            exchange_id=None,
            instrument_id=None,
            data_type=plan.data_type,
            start_timestamp_ns=plan.start_timestamp_ns,
            end_timestamp_ns=plan.end_timestamp_ns,
            storage_format="parquet",
            storage_path=None,
            status="pending",
        )

        dataset_path = args.data_root / "processed" / str(dataset.id)

        repository.set_storage_path(
            dataset.id,
            dataset_path,
        )

        repository.add_inputs(
            dataset_id=dataset.id,
            input_dataset_ids=args.dataset_ids,
        )

        job_path = write_merge_job(
            job_id=dataset.id,
            dataset_id=dataset.id,
            plan=plan,
            conn=conn,
            profile_name=args.profile,
            data_root=args.data_root,
        )

        conn.commit()

    print("Merge Job")
    print("=" * 72)
    print(f"Dataset : {dataset.id}")
    print(f"Inputs  : {len(plan.inputs)}")
    print(f"Type    : {plan.data_type}")
    print(f"Output  : {dataset_path}")
    print(f"Job     : {job_path}")

    return 0
