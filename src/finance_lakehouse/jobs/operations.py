from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone


@dataclass(frozen=True)
class BackfillBatch:
    batch_id: int
    start_date: str
    end_date: str
    partition_keys: tuple[str, ...]


@dataclass(frozen=True)
class QuarantineResult:
    accepted: list[dict[str, object]]
    quarantined: list[dict[str, object]]


def build_backfill_plan(
    start_date: str, end_date: str, batch_size_days: int = 3
) -> list[BackfillBatch]:
    if batch_size_days <= 0:
        raise ValueError("batch_size_days must be positive")

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")

    cursor = start
    batch_id = 1
    batches: list[BackfillBatch] = []
    while cursor <= end:
        batch_end = min(cursor + timedelta(days=batch_size_days - 1), end)
        partitions = []
        partition_cursor = cursor
        while partition_cursor <= batch_end:
            partitions.append(f"ingestion_date={partition_cursor.isoformat()}")
            partition_cursor += timedelta(days=1)
        batches.append(
            BackfillBatch(
                batch_id=batch_id,
                start_date=cursor.isoformat(),
                end_date=batch_end.isoformat(),
                partition_keys=tuple(partitions),
            )
        )
        batch_id += 1
        cursor = batch_end + timedelta(days=1)
    return batches


def classify_records_for_quarantine(
    records: Iterable[dict[str, object]], required_fields: tuple[str, ...]
) -> QuarantineResult:
    accepted: list[dict[str, object]] = []
    quarantined: list[dict[str, object]] = []
    for record in records:
        missing = [
            field_name
            for field_name in required_fields
            if record.get(field_name) in (None, "")
        ]
        if missing:
            quarantined.append({**record, "quarantine_reason": f"missing:{','.join(missing)}"})
        else:
            accepted.append(record)
    return QuarantineResult(accepted=accepted, quarantined=quarantined)


def build_replay_manifest(
    *,
    pipeline_name: str,
    execution_time: datetime | None,
    requested_window_start: str,
    requested_window_end: str,
    successful_partitions: list[str],
    failed_partitions: list[str],
) -> dict[str, object]:
    occurred_at = execution_time or datetime.now(timezone.utc)
    return {
        "pipeline_name": pipeline_name,
        "generated_at": occurred_at.isoformat(),
        "requested_window_start": requested_window_start,
        "requested_window_end": requested_window_end,
        "successful_partitions": successful_partitions,
        "failed_partitions": failed_partitions,
        "rerun_required": bool(failed_partitions),
    }
