from finance_lakehouse.jobs.glue_sec_silver import (
    build_silver_payload,
    default_args,
    sec_silver_layout,
)


def sample_sec_records() -> list[dict]:
    return [
        {
            "cik": "320193",
            "ticker": "aapl",
            "filing_url": "https://data.sec.gov/submissions/0000320193.json",
            "form_type": "10-K",
            "filing_date": "2026-04-21",
            "source_system": "sec_submissions",
            "extracted_at": "2026-04-21T00:00:00Z",
        }
    ]


def test_sec_silver_layout_uses_silver_path() -> None:
    layout = sec_silver_layout("processed-bucket", "finance_lakehouse")
    assert layout.s3_path == "s3://processed-bucket/silver/sec_submissions"


def test_build_sec_silver_payload_contains_merge_metadata() -> None:
    args = default_args()
    payload = build_silver_payload(sample_sec_records(), args)
    assert payload["merge_keys"] == ["cik", "filing_date", "form_type"]
    assert "target.cik = source.cik" in payload["merge_condition"]
    assert payload["write_options"]["overwriteSchema"] == "true"


def test_build_sec_silver_payload_keeps_form_metadata() -> None:
    args = default_args()
    payload = build_silver_payload(sample_sec_records(), args)
    assert payload["records"][0]["form_type"] == "10-K"


def test_build_sec_silver_payload_targets_delta_table() -> None:
    args = default_args()
    payload = build_silver_payload(sample_sec_records(), args)
    assert payload["target_table"] == "finance_lakehouse.sec_submissions_silver"
