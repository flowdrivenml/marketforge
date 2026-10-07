from __future__ import annotations

import argparse

from marketforge.database import connect
from marketforge.processing.archives import scan_raw_archives, select_raw_archives
from marketforge.processing.datasets import DatasetRepository
from marketforge.processing.metadata import ProcessingMetadataResolver
from marketforge.processing.planning import plan_process_dataset, write_process_job


def run(
    args: argparse.Namespace,
) -> int:
    """
    Select downloaded raw archives and generate process.json.

    Rust execution is intentionally separate.
    """

    archives = scan_raw_archives(
        data_root=args.data_root,
    )

    selected = select_raw_archives(
        archives,
        exchange=args.exchange,
        instrument_type=args.instrument_type,
        market_category=args.category,
        data_type=args.data_type,
        symbol=args.symbol,
        start=args.start,
        end=args.end,
    )

    if not selected:
        raise ValueError("No downloaded raw archives match the requested selection")

    with connect() as conn:
        resolver = ProcessingMetadataResolver(conn)

        metadata = resolver.resolve(
            exchange=args.exchange,
            symbol=args.symbol,
            instrument_type=args.instrument_type,
            market_category=args.category,
            dataset=args.dataset,
            profile=args.profile,
        )

        dataset_plan = plan_process_dataset(
            archives=selected,
            metadata=metadata,
        )

        repository = DatasetRepository(conn)

        dataset = repository.create(
            exchange_id=dataset_plan.exchange_id,
            instrument_id=dataset_plan.instrument_id,
            data_type=dataset_plan.data_type,
            start_timestamp_ns=(dataset_plan.start_timestamp_ns),
            end_timestamp_ns=(dataset_plan.end_timestamp_ns),
            storage_format="parquet",
            storage_path=None,
            status="pending",
        )

        dataset_path = args.data_root / "processed" / str(dataset.id)

        repository.set_storage_path(
            dataset.id,
            dataset_path,
        )

        job_path = write_process_job(
            job_id=dataset.id,
            dataset_id=dataset.id,
            archives=selected,
            metadata=metadata,
            data_root=args.data_root,
        )

        conn.commit()

    print("Process Job")
    print("=" * 72)
    print(f"Dataset : {dataset.id}")
    print(f"Archives: {len(selected)}")
    print(f"Output  : {dataset_path}")
    print(f"Job     : {job_path}")

    return 0
