from datetime import datetime, timezone

from finance_lakehouse.jobs.runtime import default_runtime_args, parse_glue_args


def make_defaults():
    return default_runtime_args(
        job_name="test-job",
        raw_bucket="raw-bucket",
        processed_bucket="processed-bucket",
        glue_database="finance_lakehouse",
        bronze_table="bronze_table",
        silver_table="silver_table",
        source_name="market_data",
    )


def test_default_runtime_args_uses_passed_values() -> None:
    defaults = make_defaults()
    assert defaults.job_name == "test-job"
    assert defaults.raw_bucket == "raw-bucket"
    assert defaults.silver_table == "silver_table"


def test_parse_glue_args_returns_defaults_when_empty() -> None:
    defaults = make_defaults()
    parsed = parse_glue_args([], defaults)
    assert parsed == defaults


def test_parse_glue_args_overrides_fields() -> None:
    defaults = make_defaults()
    parsed = parse_glue_args(
        [
            "--job_name",
            "override-job",
            "--raw_bucket",
            "override-raw",
            "--processed_bucket",
            "override-processed",
            "--execution_date",
            "2026-04-21T00:00:00Z",
        ],
        defaults,
    )
    assert parsed.job_name == "override-job"
    assert parsed.raw_bucket == "override-raw"
    assert parsed.processed_bucket == "override-processed"
    assert parsed.execution_date == datetime(2026, 4, 21, tzinfo=timezone.utc)
