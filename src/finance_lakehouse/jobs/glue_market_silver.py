from __future__ import annotations

import json
import sys
from collections.abc import Iterable

from finance_lakehouse.config import settings
from finance_lakehouse.jobs.common import iso_timestamp
from finance_lakehouse.jobs.delta import (
    DeltaTableLayout,
    default_delta_options,
    merge_keys_for_dataset,
)
from finance_lakehouse.jobs.glue_support import (
    merge_condition,
    register_delta_table_sql,
    silver_delta_write_options,
)
from finance_lakehouse.jobs.runtime import RuntimeArgs, default_runtime_args, parse_glue_args
from finance_lakehouse.jobs.silver_curated import curate_market_records


def default_args() -> RuntimeArgs:
    return default_runtime_args(
        job_name="glue-market-silver",
        raw_bucket=settings.aws.raw_bucket,
        processed_bucket=settings.aws.processed_bucket,
        glue_database=settings.aws.glue_database,
        bronze_table="market_data_bronze",
        silver_table="market_data_silver",
        source_name="market_data",
    )


def market_silver_layout(bucket: str, database: str) -> DeltaTableLayout:
    return DeltaTableLayout(
        database=database,
        table="market_data_silver",
        bucket=bucket,
        layer="silver",
        dataset="market_data",
    )


def build_silver_payload(records: Iterable[dict], args: RuntimeArgs) -> dict:
    curated = curate_market_records(records)
    layout = market_silver_layout(args.processed_bucket, args.glue_database)
    return {
        "job_name": args.job_name,
        "source_table": f"{args.glue_database}.{args.bronze_table}",
        "target_table": layout.table_name,
        "target_path": layout.s3_path,
        "merge_keys": merge_keys_for_dataset("market_data"),
        "merge_condition": merge_condition(merge_keys_for_dataset("market_data")),
        "register_sql": register_delta_table_sql(layout.table_name, layout.s3_path),
        "delta_options": default_delta_options(),
        "write_options": silver_delta_write_options(),
        "records": [
            {
                **record,
                "processing_timestamp": iso_timestamp(args.execution_date),
            }
            for record in curated
        ],
    }


def run_job(input_records: Iterable[dict], argv: list[str] | None = None) -> dict:
    args = parse_glue_args(argv or sys.argv[1:], default_args())
    return build_silver_payload(input_records, args)


if __name__ == "__main__":
    sample = [
        {
            "symbol": "AAPL",
            "price": 190.25,
            "volume": 1000000,
            "source_system": "market_api",
            "extracted_at": "2026-04-21T00:00:00+00:00",
        }
    ]
    print(json.dumps(run_job(sample), indent=2))
