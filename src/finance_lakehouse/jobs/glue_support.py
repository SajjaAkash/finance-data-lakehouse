from __future__ import annotations

from collections.abc import Iterable


def bronze_json_write_options() -> dict[str, str]:
    return {
        "compression": "gzip",
    }


def silver_delta_write_options() -> dict[str, str]:
    return {
        "mergeSchema": "true",
        "overwriteSchema": "true",
    }


def register_delta_table_sql(table_name: str, location: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} "
        "USING DELTA "
        f"LOCATION '{location}'"
    )


def merge_condition(keys: Iterable[str]) -> str:
    return " AND ".join(f"target.{key} = source.{key}" for key in keys)
