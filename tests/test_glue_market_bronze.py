from datetime import datetime, timezone

from finance_lakehouse.jobs.glue_market_bronze import (
    bronze_target,
    build_market_snapshot_payload,
    default_args,
    run_job,
)


def test_build_market_snapshot_payload_creates_expected_market_records() -> None:
    records = build_market_snapshot_payload(
        ["AAPL", "MSFT"],
        price_date="2026-04-21",
        extraction_time=datetime(2026, 4, 21, tzinfo=timezone.utc),
    )
    assert [record["symbol"] for record in records] == ["AAPL", "MSFT"]
    assert records[0]["price_date"] == "2026-04-21"


def test_bronze_target_builds_market_path() -> None:
    target = bronze_target("raw-bucket", datetime(2026, 4, 21, tzinfo=timezone.utc))
    assert target.uri == "s3://raw-bucket/bronze/market_data/ingestion_date=2026-04-21"


def test_default_args_uses_market_job_identity() -> None:
    args = default_args()
    assert args.job_name == "glue-market-bronze"
    assert args.source_name == "market_data"


def test_run_job_contains_write_options_and_records() -> None:
    payload = run_job(["--execution_date", "2026-04-21T00:00:00Z"])
    assert payload["format"] == "json"
    assert payload["write_options"]["compression"] == "gzip"
    assert payload["records"]
