from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable


@dataclass(frozen=True)
class S3Target:
    bucket: str
    prefix: str

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}".rstrip("/")


def ingestion_partition(execution_date: datetime | None = None) -> str:
    dt = execution_date or datetime.now(timezone.utc)
    return dt.strftime("ingestion_date=%Y-%m-%d")


def normalize_ticker(value: str) -> str:
    return value.strip().upper().replace(".", "-")


def deduplicate_records(records: Iterable[dict], key: str) -> list[dict]:
    seen: set[str] = set()
    unique: list[dict] = []

    for record in records:
        token = str(record.get(key, "")).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        unique.append(record)

    return unique
