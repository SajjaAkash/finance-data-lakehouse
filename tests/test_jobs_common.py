from datetime import datetime, timezone

import pytest

from finance_lakehouse.jobs.common import (
    chunked,
    deduplicate_records,
    ingestion_partition,
    iso_date,
    iso_timestamp,
    normalize_cik,
    normalize_ticker,
)


def test_ingestion_partition_uses_expected_format() -> None:
    value = ingestion_partition(datetime(2026, 4, 21, tzinfo=timezone.utc))
    assert value == "ingestion_date=2026-04-21"


def test_normalize_ticker_uppercases_and_replaces_dot() -> None:
    assert normalize_ticker(" brk.b ") == "BRK-B"


def test_normalize_cik_zero_pads_value() -> None:
    assert normalize_cik("320193") == "0000320193"


def test_iso_date_formats_utc_date() -> None:
    value = iso_date(datetime(2026, 4, 21, 13, 15, tzinfo=timezone.utc))
    assert value == "2026-04-21"


def test_iso_timestamp_preserves_offset() -> None:
    value = iso_timestamp(datetime(2026, 4, 21, 13, 15, tzinfo=timezone.utc))
    assert value.endswith("+00:00")


def test_deduplicate_records_removes_duplicates_and_empty_keys() -> None:
    records = [
        {"symbol": "AAPL"},
        {"symbol": ""},
        {"symbol": "AAPL"},
        {"symbol": "MSFT"},
    ]
    assert deduplicate_records(records, "symbol") == [{"symbol": "AAPL"}, {"symbol": "MSFT"}]


def test_chunked_splits_values_into_even_groups() -> None:
    assert chunked(["A", "B", "C", "D"], 2) == [["A", "B"], ["C", "D"]]


def test_chunked_keeps_remainder_group() -> None:
    assert chunked(["A", "B", "C"], 2) == [["A", "B"], ["C"]]


def test_chunked_raises_for_invalid_size() -> None:
    with pytest.raises(ValueError):
        chunked(["A"], 0)
