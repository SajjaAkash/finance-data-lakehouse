from finance_lakehouse.jobs.glue_market_silver import (
    build_silver_payload,
    default_args,
    market_silver_layout,
)


def sample_market_records() -> list[dict]:
    return [
        {
            "symbol": "aapl",
            "price": 190.25,
            "volume": 1000000,
            "source_system": "market_api",
            "extracted_at": "2026-04-21T00:00:00Z",
        }
    ]


def test_market_silver_layout_uses_silver_path() -> None:
    layout = market_silver_layout("processed-bucket", "finance_lakehouse")
    assert layout.s3_path == "s3://processed-bucket/silver/market_data"


def test_build_silver_payload_contains_merge_metadata() -> None:
    args = default_args()
    payload = build_silver_payload(sample_market_records(), args)
    assert payload["merge_keys"] == ["symbol", "price_date"]
    assert "target.symbol = source.symbol" in payload["merge_condition"]
    assert payload["write_options"]["mergeSchema"] == "true"


def test_build_silver_payload_adds_processing_timestamp() -> None:
    args = default_args()
    payload = build_silver_payload(sample_market_records(), args)
    assert payload["records"][0]["processing_timestamp"]


def test_build_silver_payload_targets_delta_table() -> None:
    args = default_args()
    payload = build_silver_payload(sample_market_records(), args)
    assert payload["target_table"] == "finance_lakehouse.market_data_silver"
