"""Tests for the bench result data model and JSON round-trip."""

from pathlib import Path

from bench.core.results import (
    SCHEMA_VERSION,
    CellResult,
    LatencyStats,
    Round,
    RunResult,
)


def _cell(status: str = "ok") -> CellResult:
    latency = LatencyStats.from_durations([0.001, 0.002, 0.003, 0.004, 0.005])
    rounds = [
        Round(elapsed_s=0.5, operations=100, ops_per_sec=200.0, latency=latency, counters={}),
        Round(elapsed_s=0.4, operations=100, ops_per_sec=250.0, latency=latency, counters={}),
        Round(
            elapsed_s=0.6,
            operations=100,
            ops_per_sec=166.0,
            latency=latency,
            counters={"conflicts": 3},
        ),
    ]
    return CellResult(
        scenario="store.append_batch",
        interface="store",
        backend="memory",
        metric="throughput",
        params={"batch_size": 10, "payload": "small"},
        status=status,
        reason=None,
        rounds=rounds,
    )


def test_latency_stats_from_durations() -> None:
    stats = LatencyStats.from_durations([0.001, 0.002, 0.003, 0.004, 0.010])
    assert stats.min_ms == 1.0
    assert stats.p50_ms == 3.0
    assert stats.p99_ms > stats.p50_ms
    assert stats.mean_ms == 4.0


def test_cell_id_is_stable_and_param_sorted() -> None:
    cell = _cell()
    assert cell.cell_id == "store.append_batch[memory](batch_size=10,payload=small)"


def test_median_round_selected_by_ops_per_sec() -> None:
    cell = _cell()
    median = cell.median_round
    assert median is not None
    assert median.ops_per_sec == 200.0


def test_median_round_none_when_no_rounds() -> None:
    cell = _cell(status="skipped")
    cell.rounds = []
    assert cell.median_round is None


def test_json_round_trip(tmp_path: Path) -> None:
    run = RunResult(
        schema_version=SCHEMA_VERSION,
        metadata={"commit": "abc123", "python": "3.11"},
        cells=[_cell()],
    )
    path = tmp_path / "run.json"
    run.save(path)
    loaded = RunResult.from_json(path.read_text())
    assert loaded.schema_version == SCHEMA_VERSION
    assert loaded.metadata["commit"] == "abc123"
    assert loaded.cells[0].cell_id == run.cells[0].cell_id
    assert loaded.cells[0].rounds[2].counters == {"conflicts": 3}
    assert loaded.cells[0].rounds[0].latency is not None
    assert loaded.cells[0].rounds[0].latency.p50_ms == 3.0
