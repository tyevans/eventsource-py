"""Run every store scenario against the memory adapter with tiny budgets."""

import pytest

from bench.adapters.stores import MemoryStoreAdapter
from bench.core.runner import RunnerConfig, run_cell
from bench.scenarios.stores import STORE_SCENARIOS

TINY = RunnerConfig(
    rounds=1,
    warmup_iterations=1,
    calibration_iterations=1,
    target_round_seconds=0.02,
    max_iterations=5,
    cell_timeout_seconds=30.0,
)

SMALLEST_PARAMS = {
    "store.append_batch": {"batch_size": 1, "payload": "small"},
    "store.read_stream": {"stream_length": 100},
    "store.concurrent_append": {"writers": 2},
    "store.contended_append": {"writers": 2},
}


@pytest.mark.parametrize("scenario", STORE_SCENARIOS, ids=lambda s: s.name)
async def test_store_scenario_runs_on_memory(scenario) -> None:  # type: ignore[no-untyped-def]
    adapter = MemoryStoreAdapter()
    await adapter.setup()
    cell = await run_cell(adapter, scenario, SMALLEST_PARAMS[scenario.name], TINY)
    await adapter.teardown()
    assert cell.status == "ok", cell.reason
    assert cell.rounds and cell.rounds[0].operations > 0


def test_grids_match_spec() -> None:
    by_name = {s.name: s for s in STORE_SCENARIOS}
    assert by_name["store.append_batch"].grid == {
        "batch_size": [1, 10, 100, 1000],
        "payload": ["small", "large"],
    }
    assert by_name["store.read_stream"].grid == {"stream_length": [100, 1000, 10000]}
    assert by_name["store.concurrent_append"].grid == {"writers": [1, 10, 50]}
    assert by_name["store.contended_append"].grid == {"writers": [1, 10, 50]}
