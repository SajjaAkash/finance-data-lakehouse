from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Iterable

from finance_lakehouse.config import settings
from finance_lakehouse.jobs.common import (
    S3Target,
    ingestion_partition,
    iso_date,
    iso_timestamp,
    normalize_cik,
    normalize_ticker,
)
from finance_lakehouse.jobs.glue_support import bronze_json_write_options
from finance_lakehouse.jobs.runtime import RuntimeArgs, default_runtime_args, parse_glue_args


def build_sec_submission_payload(
    ciks: Iterable[str],
    tickers: Iterable[str],
    *,
    filing_date: str | None = None,
    extraction_time: datetime | None = None,
) -> list[dict]:
    extracted_at = extraction_time or datetime.now(timezone.utc)
    effective_filing_date = filing_date or iso_date(extracted_at)
    records: list[dict] = []
    for index, (cik, ticker) in enumerate(zip(ciks, tickers, strict=True)):
        records.append(
            {
                "cik": normalize_cik(cik),
                "ticker": normalize_ticker(ticker),
                "filing_url": f"{settings.sources.sec_submissions_url}{normalize_cik(cik)}.json",
                "form_type": "10-K" if index % 2 == 0 else "10-Q",
                "filing_date": effective_filing_date,
                "source_system": "sec_submissions",
                "extracted_at": iso_timestamp(extracted_at),
            }
        )
    return records


def bronze_target(bucket: str, execution_date: datetime | None = None) -> S3Target:
    return S3Target(
        bucket=bucket,
        prefix=f"bronze/sec_submissions/{ingestion_partition(execution_date)}",
    )


def render_manifest(ciks: list[str], tickers: list[str], bucket: str | None = None) -> str:
    manifest = {
        "target": bronze_target(bucket or settings.aws.raw_bucket).uri,
        "records": build_sec_submission_payload(ciks, tickers),
    }
    return json.dumps(manifest, indent=2)


def default_args() -> RuntimeArgs:
    return default_runtime_args(
        job_name="glue-sec-bronze",
        raw_bucket=settings.aws.raw_bucket,
        processed_bucket=settings.aws.processed_bucket,
        glue_database=settings.aws.glue_database,
        bronze_table="sec_submissions_bronze",
        silver_table="sec_submissions_silver",
        source_name="sec_submissions",
    )


def build_bronze_records(args: RuntimeArgs) -> list[dict]:
    return build_sec_submission_payload(
        settings.sources.tracked_ciks,
        settings.sources.tracked_tickers,
        filing_date=iso_date(args.execution_date),
        extraction_time=args.execution_date,
    )


def run_job(argv: list[str] | None = None) -> dict:
    args = parse_glue_args(argv or sys.argv[1:], default_args())
    payload = {
        "job_name": args.job_name,
        "database": args.glue_database,
        "table": args.bronze_table,
        "target_path": bronze_target(args.raw_bucket, args.execution_date).uri,
        "write_mode": "append",
        "format": "json",
        "write_options": bronze_json_write_options(),
        "records": build_bronze_records(args),
    }
    return payload


if __name__ == "__main__":
    print(json.dumps(run_job(), indent=2))
