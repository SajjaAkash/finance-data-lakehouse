from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class MarketRecord:
    symbol: str
    close_price: float
    trade_volume: int
    price_date: str
    source_system: str
    extracted_at: str


@dataclass(frozen=True)
class SecSubmissionRecord:
    cik: str
    ticker: str
    filing_url: str
    form_type: str
    filing_date: str
    source_system: str
    extracted_at: str


@dataclass(frozen=True)
class JobContext:
    job_name: str
    execution_date: datetime
    bronze_table: str
    silver_table: str
    gold_selector: str
