from __future__ import annotations

import json
from datetime import datetime, timezone

from finance_lakehouse.config import settings
from finance_lakehouse.jobs.common import S3Target, ingestion_partition, normalize_ticker


def build_sec_submission_payload(ciks: list[str], tickers: list[str]) -> list[dict]:
    extraction_time = datetime.now(timezone.utc).isoformat()
    return [
        {
            "cik": cik.zfill(10),
            "ticker": normalize_ticker(ticker),
            "filing_url": f"{settings.sec_submissions_url}{cik.zfill(10)}.json",
            "source_system": "sec_submissions",
            "extracted_at": extraction_time,
        }
        for cik, ticker in zip(ciks, tickers, strict=True)
    ]


def bronze_target() -> S3Target:
    return S3Target(
        bucket=settings.raw_bucket,
        prefix=f"bronze/sec_submissions/{ingestion_partition()}",
    )


def render_manifest(ciks: list[str], tickers: list[str]) -> str:
    manifest = {
        "target": bronze_target().uri,
        "records": build_sec_submission_payload(ciks, tickers),
    }
    return json.dumps(manifest, indent=2)


if __name__ == "__main__":
    print(render_manifest(["320193", "789019"], ["AAPL", "MSFT"]))
