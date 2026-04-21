from finance_lakehouse.jobs.delta import (
    DeltaTableLayout,
    default_delta_options,
    merge_keys_for_dataset,
)


def test_delta_table_layout_formats_table_name() -> None:
    layout = DeltaTableLayout("finance", "market_data_silver", "bucket", "silver", "market_data")
    assert layout.table_name == "finance.market_data_silver"


def test_delta_table_layout_formats_s3_path() -> None:
    layout = DeltaTableLayout("finance", "market_data_silver", "bucket", "silver", "market_data")
    assert layout.s3_path == "s3://bucket/silver/market_data"


def test_default_delta_options_enables_core_features() -> None:
    options = default_delta_options()
    assert options["delta.enableChangeDataFeed"] == "true"
    assert options["delta.autoOptimize.optimizeWrite"] == "true"


def test_merge_keys_for_market_dataset() -> None:
    assert merge_keys_for_dataset("market_data") == ["symbol", "price_date"]


def test_merge_keys_for_sec_dataset() -> None:
    assert merge_keys_for_dataset("sec_submissions") == ["cik", "filing_date", "form_type"]
