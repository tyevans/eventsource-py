"""Snapshot and end-to-end scenarios against memory backends."""

import pytest

from bench.adapters.e2e import make_e2e_adapters
from bench.adapters.snapshots import MemorySnapshotAdapter
from bench.core.runner import RunnerConfig, run_cell
from bench.scenarios.aggregate import E2E_SCENARIOS
from bench.scenarios.snapshots import SNAPSHOT_SCENARIOS

TINY = RunnerConfig(
    rounds=1,
    warmup_iterations=1,
    calibration_iterations=1,
    target_round_seconds=0.02,
    max_iterations=5,
    cell_timeout_seconds=30.0,
)


@pytest.mark.parametrize("scenario", SNAPSHOT_SCENARIOS, ids=lambda s: s.name)
async def test_snapshot_scenario_runs_on_memory(scenario) -> None:  # type: ignore[no-untyped-def]
    adapter = MemorySnapshotAdapter()
    await adapter.setup()
    cell = await run_cell(adapter, scenario, {"size": "small"}, TINY)
    await adapter.teardown()
    assert cell.status == "ok", cell.reason


@pytest.mark.parametrize("snapshots", ["none", "threshold"])
async def test_e2e_scenario_runs_on_memory(snapshots: str) -> None:
    adapter = next(a for a in make_e2e_adapters() if a.name == "memory")
    await adapter.setup()
    scenario = E2E_SCENARIOS[0]
    params = {"stream_length": 100, "snapshots": snapshots}
    cell = await run_cell(adapter, scenario, params, TINY)
    await adapter.teardown()
    assert cell.status == "ok", cell.reason
    assert cell.rounds and cell.rounds[0].latency is not None


def test_grids_match_spec() -> None:
    by_name = {s.name: s for s in SNAPSHOT_SCENARIOS + E2E_SCENARIOS}
    assert by_name["snapshot.save"].grid == {"size": ["small", "medium", "large"]}
    assert by_name["snapshot.load"].grid == {"size": ["small", "medium", "large"]}
    assert by_name["e2e.load_mutate_save"].grid == {
        "stream_length": [100, 1000, 10000],
        "snapshots": ["none", "threshold"],
    }
