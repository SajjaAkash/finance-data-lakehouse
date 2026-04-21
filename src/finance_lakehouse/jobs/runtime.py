from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class RuntimeArgs:
    job_name: str
    execution_date: datetime
    raw_bucket: str
    processed_bucket: str
    glue_database: str
    bronze_table: str
    silver_table: str
    source_name: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def default_runtime_args(
    *,
    job_name: str,
    raw_bucket: str,
    processed_bucket: str,
    glue_database: str,
    bronze_table: str,
    silver_table: str,
    source_name: str,
) -> RuntimeArgs:
    return RuntimeArgs(
        job_name=job_name,
        execution_date=utc_now(),
        raw_bucket=raw_bucket,
        processed_bucket=processed_bucket,
        glue_database=glue_database,
        bronze_table=bronze_table,
        silver_table=silver_table,
        source_name=source_name,
    )


def parse_glue_args(argv: list[str], defaults: RuntimeArgs) -> RuntimeArgs:
    if not argv:
        return defaults

    provided: dict[str, Any] = {}
    key = None
    for token in argv:
        if token.startswith("--"):
            key = token[2:].lower()
            continue
        if key:
            provided[key] = token
            key = None

    execution_date = provided.get("execution_date")
    parsed_dt = (
        datetime.fromisoformat(execution_date.replace("Z", "+00:00"))
        if execution_date
        else defaults.execution_date
    )

    return RuntimeArgs(
        job_name=str(provided.get("job_name", defaults.job_name)),
        execution_date=parsed_dt,
        raw_bucket=str(provided.get("raw_bucket", defaults.raw_bucket)),
        processed_bucket=str(provided.get("processed_bucket", defaults.processed_bucket)),
        glue_database=str(provided.get("glue_database", defaults.glue_database)),
        bronze_table=str(provided.get("bronze_table", defaults.bronze_table)),
        silver_table=str(provided.get("silver_table", defaults.silver_table)),
        source_name=str(provided.get("source_name", defaults.source_name)),
    )
