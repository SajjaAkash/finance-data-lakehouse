from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DeltaTableLayout:
    database: str
    table: str
    bucket: str
    layer: str
    dataset: str

    @property
    def table_name(self) -> str:
        return f"{self.database}.{self.table}"

    @property
    def s3_path(self) -> str:
        return f"s3://{self.bucket}/{self.layer}/{self.dataset}"


def default_delta_options() -> dict[str, str]:
    return {
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.autoOptimize.optimizeWrite": "true",
    }


def merge_keys_for_dataset(dataset: str) -> list[str]:
    keys = {
        "market_data": ["symbol", "price_date"],
        "sec_submissions": ["cik", "filing_date", "form_type"],
    }
    return keys[dataset]
