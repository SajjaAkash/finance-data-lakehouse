from __future__ import annotations

from collections.abc import Iterable

from finance_lakehouse.jobs.common import deduplicate_records, normalize_ticker


def curate_market_records(records: Iterable[dict]) -> list[dict]:
    curated: list[dict] = []
    normalized_records = [
        {
            **record,
            "symbol": normalize_ticker(record["symbol"]),
        }
        for record in records
    ]

    for record in deduplicate_records(normalized_records, key="symbol"):
        curated.append(
            {
                "symbol": record["symbol"],
                "close_price": record.get("price"),
                "trade_volume": record.get("volume"),
                "source_system": record.get("source_system", "market_api"),
                "extracted_at": record.get("extracted_at"),
            }
        )
    return curated


def curate_sec_records(records: Iterable[dict]) -> list[dict]:
    curated: list[dict] = []
    for record in deduplicate_records(records, key="filing_url"):
        curated.append(
            {
                "cik": str(record["cik"]).zfill(10),
                "ticker": normalize_ticker(record["ticker"]),
                "filing_url": record["filing_url"],
                "source_system": record.get("source_system", "sec_submissions"),
                "extracted_at": record.get("extracted_at"),
            }
        )
    return curated
