from __future__ import annotations

import os
from dataclasses import dataclass, field


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


@dataclass(frozen=True)
class AwsSettings:
    region: str = os.getenv("AWS_REGION", "us-east-1")
    raw_bucket: str = os.getenv("RAW_BUCKET", "finance-lakehouse-raw")
    processed_bucket: str = os.getenv("PROCESSED_BUCKET", "finance-lakehouse-processed")
    athena_output: str = os.getenv(
        "ATHENA_OUTPUT",
        "s3://finance-lakehouse-athena-results/",
    )
    glue_database: str = os.getenv("GLUE_DATABASE", "finance_lakehouse")
    lakeformation_enabled: bool = _bool_env("LAKEFORMATION_ENABLED", False)


@dataclass(frozen=True)
class SourceSettings:
    market_data_api_url: str = os.getenv("MARKET_DATA_API_URL", "https://example.com/market-data")
    sec_submissions_url: str = os.getenv("SEC_SUBMISSIONS_URL", "https://data.sec.gov/submissions/")
    tracked_tickers: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            ticker.strip()
            for ticker in os.getenv("TRACKED_TICKERS", "AAPL,MSFT,NVDA,AMZN,GOOGL").split(",")
            if ticker.strip()
        )
    )
    tracked_ciks: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            cik.strip()
            for cik in os.getenv("TRACKED_CIKS", "320193,789019,1045810,1018724,1652044").split(",")
            if cik.strip()
        )
    )


@dataclass(frozen=True)
class PlatformSettings:
    project_name: str = os.getenv("PROJECT_NAME", "finance-data-lakehouse")
    environment: str = os.getenv("ENVIRONMENT", "dev")
    dbt_target: str = os.getenv("DBT_TARGET", "dev")
    mwaa_environment: str = os.getenv("MWAA_ENVIRONMENT", "finance-lakehouse-mwaa")


@dataclass(frozen=True)
class Settings:
    aws: AwsSettings = field(default_factory=AwsSettings)
    sources: SourceSettings = field(default_factory=SourceSettings)
    platform: PlatformSettings = field(default_factory=PlatformSettings)


settings = Settings()
