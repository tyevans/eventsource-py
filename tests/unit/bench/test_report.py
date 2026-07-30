"""Report generator tests against a hand-built RunResult."""

from bench.core.report import render_markdown
from bench.core.results import (
    SCHEMA_VERSION,
    CellResult,
    LatencyStats,
    Round,
    RunResult,
)


def _round(ops: float = 100.0, conflicts: int = 0) -> Round:
    return Round(
        elapsed_s=1.0,
        operations=100,
        ops_per_sec=ops,
        latency=LatencyStats(p50_ms=2.0, p95_ms=4.0, p99_ms=5.0, mean_ms=2.5, min_ms=1.0),
        counters={"conflicts": conflicts} if conflicts else {},
    )


def _run() -> RunResult:
    return RunResult(
        schema_version=SCHEMA_VERSION,
        metadata={"commit": "abc1234", "platform": "linux-test", "timestamp": "t"},
        cells=[
            CellResult(
                scenario="store.append_batch",
                interface="store",
                backend="memory",
                metric="throughput",
                params={"batch_size": 1},
                status="ok",
                rounds=[_round(500.0)],
            ),
            CellResult(
                scenario="store.append_batch",
                interface="store",
                backend="postgresql",
                metric="throughput",
                params={"batch_size": 1},
                status="skipped",
                reason="postgres unreachable",
            ),
            CellResult(
                scenario="store.contended_append",
                interface="store",
                backend="memory",
                metric="throughput",
                params={"writers": 10},
                status="ok",
                rounds=[_round(200.0, conflicts=25)],
            ),
        ],
    )


def test_report_contains_metadata_and_sections() -> None:
    text = render_markdown(_run())
    assert text.index("abc1234") < text.index("## store")
    assert "### store.append_batch" in text
    assert "| batch_size=1 |" in text


def test_report_renders_throughput_skip_and_conflicts() -> None:
    text = render_markdown(_run())
    assert "500/s" in text
    assert "skipped: postgres unreachable" in text
    assert "[20% conflicts]" in text  # 25 / (100 + 25)


def test_report_latency_metric_uses_percentiles() -> None:
    run = _run()
    run.cells[0].metric = "latency"
    text = render_markdown(run)
    assert "2.00ms" in text and "p95 4.00ms" in text and "p99 5.00ms" in text
