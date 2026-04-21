from __future__ import annotations

import json
from datetime import datetime, timezone

from finance_lakehouse.config import settings
from finance_lakehouse.jobs.common import S3Target, ingestion_partition, normalize_ticker


def build_market_snapshot_payload(symbols: list[str]) -> list[dict]:
    extraction_time = datetime.now(timezone.utc).isoformat()
    return [
        {
            "symbol": normalize_ticker(symbol),
            "price": None,
            "volume": None,
            "source_system": "market_api",
            "extracted_at": extraction_time,
        }
        for symbol in symbols
    ]


def bronze_target() -> S3Target:
    return S3Target(
        bucket=settings.raw_bucket,
        prefix=f"bronze/market_data/{ingestion_partition()}",
    )


def render_manifest(symbols: list[str]) -> str:
    manifest = {
        "target": bronze_target().uri,
        "records": build_market_snapshot_payload(symbols),
    }
    return json.dumps(manifest, indent=2)


if __name__ == "__main__":
    print(render_manifest(["AAPL", "MSFT", "BRK.B"]))
