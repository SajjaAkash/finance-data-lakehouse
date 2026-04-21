from finance_lakehouse.jobs.silver_curated import curate_market_records, curate_sec_records


def test_curate_market_records_shapes_payload() -> None:
    records = [
        {"symbol": "aapl", "price": 190.1, "volume": 100, "extracted_at": "2026-04-21T00:00:00Z"},
        {"symbol": "AAPL", "price": 190.1, "volume": 100, "extracted_at": "2026-04-21T00:00:00Z"},
    ]

    assert curate_market_records(records) == [
        {
            "symbol": "AAPL",
            "close_price": 190.1,
            "trade_volume": 100,
            "source_system": "market_api",
            "extracted_at": "2026-04-21T00:00:00Z",
        }
    ]


def test_curate_market_records_keeps_first_distinct_symbol() -> None:
    records = [
        {"symbol": "msft", "price": 410.2, "volume": 120, "extracted_at": "2026-04-21T00:00:00Z"},
        {"symbol": "aapl", "price": 190.1, "volume": 100, "extracted_at": "2026-04-21T00:00:00Z"},
    ]

    assert [record["symbol"] for record in curate_market_records(records)] == ["MSFT", "AAPL"]


def test_curate_sec_records_shapes_payload() -> None:
    records = [
        {
            "cik": "320193",
            "ticker": "aapl",
            "filing_url": "https://data.sec.gov/submissions/0000320193.json",
            "form_type": "10-K",
            "filing_date": "2026-04-21",
            "extracted_at": "2026-04-21T00:00:00Z",
        }
    ]

    assert curate_sec_records(records) == [
        {
            "cik": "0000320193",
            "ticker": "AAPL",
            "filing_url": "https://data.sec.gov/submissions/0000320193.json",
            "form_type": "10-K",
            "filing_date": "2026-04-21",
            "source_system": "sec_submissions",
            "extracted_at": "2026-04-21T00:00:00Z",
        }
    ]


def test_curate_sec_records_deduplicates_same_filing_url() -> None:
    records = [
        {
            "cik": "320193",
            "ticker": "aapl",
            "filing_url": "https://data.sec.gov/submissions/0000320193.json",
            "form_type": "10-K",
            "filing_date": "2026-04-21",
            "extracted_at": "2026-04-21T00:00:00Z",
        },
        {
            "cik": "0000320193",
            "ticker": "AAPL",
            "filing_url": "https://data.sec.gov/submissions/0000320193.json",
            "form_type": "10-K",
            "filing_date": "2026-04-21",
            "extracted_at": "2026-04-21T00:00:00Z",
        },
    ]

    assert len(curate_sec_records(records)) == 1
