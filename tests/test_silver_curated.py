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


def test_curate_sec_records_shapes_payload() -> None:
    records = [
        {
            "cik": "320193",
            "ticker": "aapl",
            "filing_url": "https://data.sec.gov/submissions/0000320193.json",
            "extracted_at": "2026-04-21T00:00:00Z",
        }
    ]

    assert curate_sec_records(records) == [
        {
            "cik": "0000320193",
            "ticker": "AAPL",
            "filing_url": "https://data.sec.gov/submissions/0000320193.json",
            "source_system": "sec_submissions",
            "extracted_at": "2026-04-21T00:00:00Z",
        }
    ]
