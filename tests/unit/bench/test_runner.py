"""Runner tests using a fake in-process adapter and scenario."""

import asyncio
from typing import Any

from bench.adapters.base import BenchAdapter
from bench.core.runner import (
    Measurement,
    RunnerConfig,
    Scenario,
    expand_grid,
    run_cell,
    run_matrix,
)


class FakeAdapter(BenchAdapter[dict[str, Any]]):
    name = "fake"

    def __init__(self, reason: str | None = None) -> None:
        self.reason = reason
        self.setup_calls = 0
        self.teardown_calls = 0

    async def available(self) -> str | None:
        return self.reason

    async def setup(self) -> None:
        self.setup_calls += 1

    async def teardown(self) -> None:
        self.teardown_calls += 1

    async def create(self) -> dict[str, Any]:
        return {}


async def _fast_scenario_func(
    resource: dict[str, Any], params: dict[str, Any], iterations: int, prepared: Any
) -> Measurement:
    durations = [0.001] * iterations
    return Measurement(elapsed_s=0.001 * iterations, operations=iterations, durations_s=durations)


FAST = Scenario(
    name="fake.fast",
    interface="store",
    metric="latency",
    grid={"size": [1, 2]},
    func=_fast_scenario_func,
)

TINY_CONFIG = RunnerConfig(
    rounds=2,
    warmup_iterations=1,
    calibration_iterations=1,
    target_round_seconds=0.01,
    max_iterations=10,
    cell_timeout_seconds=5.0,
)


def test_expand_grid_full_and_quick() -> None:
    grid = {"a": [1, 2], "b": ["x", "y"]}
    assert len(expand_grid(grid, quick=False)) == 4
    assert expand_grid(grid, quick=True) == [{"a": 1, "b": "x"}]


async def test_run_cell_produces_rounds_and_latency() -> None:
    cell = await run_cell(FakeAdapter(), FAST, {"size": 1}, TINY_CONFIG)
    assert cell.status == "ok"
    assert len(cell.rounds) == 2
    assert cell.rounds[0].latency is not None
    assert cell.rounds[0].ops_per_sec > 0


async def test_run_cell_failure_is_captured() -> None:
    async def boom(
        resource: dict[str, Any], params: dict[str, Any], iterations: int, prepared: Any
    ) -> Measurement:
        raise RuntimeError("kaboom")

    scenario = Scenario(name="fake.boom", interface="store", metric="latency", grid={}, func=boom)
    cell = await run_cell(FakeAdapter(), scenario, {}, TINY_CONFIG)
    assert cell.status == "failed"
    assert cell.reason is not None and "kaboom" in cell.reason


async def test_run_cell_timeout_is_captured() -> None:
    async def hang(
        resource: dict[str, Any], params: dict[str, Any], iterations: int, prepared: Any
    ) -> Measurement:
        await asyncio.sleep(60)
        return Measurement(elapsed_s=0.0, operations=0)

    scenario = Scenario(name="fake.hang", interface="store", metric="latency", grid={}, func=hang)
    config = RunnerConfig(
        rounds=1,
        warmup_iterations=1,
        calibration_iterations=1,
        target_round_seconds=0.01,
        max_iterations=1,
        cell_timeout_seconds=0.2,
    )
    cell = await run_cell(FakeAdapter(), scenario, {}, config)
    assert cell.status == "failed"
    assert cell.reason is not None and "timeout" in cell.reason.lower()


async def test_run_cell_bounds_hanging_destroy() -> None:
    class HangingDestroyAdapter(FakeAdapter):
        name = "hanging-destroy"

        async def destroy(self, resource: dict[str, Any]) -> None:
            await asyncio.sleep(60)

    config = RunnerConfig(
        rounds=1,
        warmup_iterations=1,
        calibration_iterations=1,
        target_round_seconds=0.01,
        max_iterations=10,
        cell_timeout_seconds=5.0,
        destroy_timeout_seconds=0.05,
    )
    cell = await asyncio.wait_for(
        run_cell(HangingDestroyAdapter(), FAST, {"size": 1}, config), timeout=2.0
    )
    assert cell.status == "ok"


class BadAdapter(FakeAdapter):
    name = "bad"


async def test_run_cell_applies_iteration_cap() -> None:
    capped = Scenario(
        name="fake.capped",
        interface="store",
        metric="latency",
        grid={"size": [1]},
        func=_fast_scenario_func,
        iteration_cap=lambda params: 2,
    )
    cell = await run_cell(FakeAdapter(), capped, {"size": 1}, TINY_CONFIG)
    assert cell.status == "ok"
    assert all(round_.operations <= 2 for round_ in cell.rounds)


async def test_run_matrix_skips_unavailable_and_sets_metadata() -> None:
    good = FakeAdapter()
    bad = BadAdapter(reason="service down")
    result = await run_matrix([FAST], {"store": [good, bad]}, TINY_CONFIG)
    ok_cells = [c for c in result.cells if c.status == "ok"]
    skipped = [c for c in result.cells if c.status == "skipped"]
    assert len(ok_cells) == 2  # grid size 2 for the good adapter
    assert len(skipped) == 2 and skipped[0].reason == "service down"
    assert good.setup_calls == 1 and good.teardown_calls == 1
    assert bad.setup_calls == 0
    assert "python" in result.metadata and "timestamp" in result.metadata
