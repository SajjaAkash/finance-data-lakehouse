from datetime import datetime, timezone

from finance_lakehouse.jobs.common import deduplicate_records, ingestion_partition, normalize_ticker


def test_ingestion_partition_uses_expected_format() -> None:
    value = ingestion_partition(datetime(2026, 4, 21, tzinfo=timezone.utc))
    assert value == "ingestion_date=2026-04-21"


def test_normalize_ticker_uppercases_and_replaces_dot() -> None:
    assert normalize_ticker(" brk.b ") == "BRK-B"


def test_deduplicate_records_removes_duplicates_and_empty_keys() -> None:
    records = [
        {"symbol": "AAPL"},
        {"symbol": ""},
        {"symbol": "AAPL"},
        {"symbol": "MSFT"},
    ]
    assert deduplicate_records(records, "symbol") == [{"symbol": "AAPL"}, {"symbol": "MSFT"}]
