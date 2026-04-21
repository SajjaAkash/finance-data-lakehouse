from finance_lakehouse.jobs.glue_support import (
    bronze_json_write_options,
    merge_condition,
    register_delta_table_sql,
    silver_delta_write_options,
)


def test_bronze_json_write_options_uses_gzip() -> None:
    assert bronze_json_write_options() == {"compression": "gzip"}


def test_silver_delta_write_options_enable_schema_evolution() -> None:
    options = silver_delta_write_options()
    assert options["mergeSchema"] == "true"
    assert options["overwriteSchema"] == "true"


def test_register_delta_table_sql_contains_table_and_location() -> None:
    sql = register_delta_table_sql("finance.market_data_silver", "s3://bucket/silver/market_data")
    assert "finance.market_data_silver" in sql
    assert "s3://bucket/silver/market_data" in sql


def test_merge_condition_joins_multiple_keys() -> None:
    assert merge_condition(["symbol", "price_date"]) == (
        "target.symbol = source.symbol AND target.price_date = source.price_date"
    )
