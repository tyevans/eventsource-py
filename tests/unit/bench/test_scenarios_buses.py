"""Run every bus scenario against the memory adapter with tiny budgets."""

import pytest

from bench.adapters.buses import MemoryBusAdapter
from bench.core.runner import RunnerConfig, run_cell
from bench.scenarios.buses import BUS_SCENARIOS

TINY = RunnerConfig(
    rounds=1,
    warmup_iterations=1,
    calibration_iterations=1,
    target_round_seconds=0.02,
    max_iterations=5,
    cell_timeout_seconds=30.0,
)

SMALLEST_PARAMS = {
    "bus.publish_throughput": {"batch_size": 1},
    "bus.fanout": {"subscribers": 1},
    "bus.roundtrip": {},
}


@pytest.mark.parametrize("scenario", BUS_SCENARIOS, ids=lambda s: s.name)
async def test_bus_scenario_runs_on_memory(scenario) -> None:  # type: ignore[no-untyped-def]
    adapter = MemoryBusAdapter()
    await adapter.setup()
    cell = await run_cell(adapter, scenario, SMALLEST_PARAMS[scenario.name], TINY)
    await adapter.teardown()
    assert cell.status == "ok", cell.reason
    assert cell.rounds and cell.rounds[0].operations > 0


def test_grids_match_spec() -> None:
    by_name = {s.name: s for s in BUS_SCENARIOS}
    assert by_name["bus.publish_throughput"].grid == {"batch_size": [1, 10, 100]}
    assert by_name["bus.fanout"].grid == {"subscribers": [1, 10, 50]}
    assert by_name["bus.roundtrip"].grid == {}
