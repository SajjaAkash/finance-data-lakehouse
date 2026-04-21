from datetime import datetime, timezone

import pytest

from finance_lakehouse.jobs.operations import (
    build_backfill_plan,
    build_replay_manifest,
    classify_records_for_quarantine,
)


def test_build_backfill_plan_breaks_window_into_batches() -> None:
    plan = build_backfill_plan("2026-04-01", "2026-04-05", batch_size_days=2)
    assert [batch.batch_id for batch in plan] == [1, 2, 3]
    assert plan[0].partition_keys == ("ingestion_date=2026-04-01", "ingestion_date=2026-04-02")
    assert plan[-1].end_date == "2026-04-05"


def test_build_backfill_plan_rejects_invalid_batch_size() -> None:
    with pytest.raises(ValueError):
        build_backfill_plan("2026-04-01", "2026-04-05", batch_size_days=0)


def test_classify_records_for_quarantine_separates_bad_rows() -> None:
    result = classify_records_for_quarantine(
        [
            {"symbol": "AAPL", "price_date": "2026-04-01"},
            {"symbol": "", "price_date": "2026-04-01"},
        ],
        ("symbol", "price_date"),
    )
    assert len(result.accepted) == 1
    assert result.quarantined[0]["quarantine_reason"] == "missing:symbol"


def test_build_replay_manifest_flags_failed_partitions() -> None:
    manifest = build_replay_manifest(
        pipeline_name="finance_lakehouse",
        execution_time=datetime(2026, 4, 21, tzinfo=timezone.utc),
        requested_window_start="2026-04-18",
        requested_window_end="2026-04-20",
        successful_partitions=["ingestion_date=2026-04-18"],
        failed_partitions=["ingestion_date=2026-04-19"],
    )
    assert manifest["rerun_required"] is True
    assert manifest["generated_at"].endswith("+00:00")
