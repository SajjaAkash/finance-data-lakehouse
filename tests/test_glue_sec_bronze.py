from datetime import datetime, timezone

from finance_lakehouse.jobs.glue_sec_bronze import (
    bronze_target,
    build_sec_submission_payload,
    default_args,
    run_job,
)


def test_build_sec_submission_payload_creates_expected_records() -> None:
    records = build_sec_submission_payload(
        ["320193"],
        ["AAPL"],
        filing_date="2026-04-21",
        extraction_time=datetime(2026, 4, 21, tzinfo=timezone.utc),
    )
    assert records[0]["cik"] == "0000320193"
    assert records[0]["form_type"] == "10-K"


def test_bronze_target_builds_sec_path() -> None:
    target = bronze_target("raw-bucket", datetime(2026, 4, 21, tzinfo=timezone.utc))
    assert target.uri == "s3://raw-bucket/bronze/sec_submissions/ingestion_date=2026-04-21"


def test_default_args_uses_sec_job_identity() -> None:
    args = default_args()
    assert args.job_name == "glue-sec-bronze"
    assert args.source_name == "sec_submissions"


def test_run_job_contains_sec_bronze_records() -> None:
    payload = run_job(["--execution_date", "2026-04-21T00:00:00Z"])
    assert payload["format"] == "json"
    assert payload["records"][0]["filing_url"].endswith(".json")
