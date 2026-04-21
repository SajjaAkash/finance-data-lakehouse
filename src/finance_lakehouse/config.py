from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    raw_bucket: str = os.getenv("RAW_BUCKET", "finance-lakehouse-raw")
    processed_bucket: str = os.getenv("PROCESSED_BUCKET", "finance-lakehouse-processed")
    market_data_api_url: str = os.getenv("MARKET_DATA_API_URL", "https://example.com/market-data")
    sec_submissions_url: str = os.getenv("SEC_SUBMISSIONS_URL", "https://data.sec.gov/submissions/")


settings = Settings()
