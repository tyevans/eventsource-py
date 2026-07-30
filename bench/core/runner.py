"""Matrix runner: warmup -> calibration -> timed rounds per cell.

All measurement for a cell happens inside the caller's event loop --
never a per-measurement asyncio.run() (spec: engine decision).
"""

import asyncio
import contextlib
import gc
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from itertools import product
from typing import Any

from bench.adapters.base import BenchAdapter
from bench.core.meta import collect_metadata
from bench.core.results import (
    SCHEMA_VERSION,
    CellResult,
    LatencyStats,
    Round,
    RunResult,
)


@dataclass(frozen=True)
class Measurement:
    elapsed_s: float
    operations: int
    durations_s: list[float] | None = None
    counters: dict[str, int] = field(default_factory=dict)


PrepareFunc = Callable[[BenchAdapter[Any], Any, dict[str, Any]], Awaitable[Any]]
ScenarioFunc = Callable[[Any, dict[str, Any], int, Any], Awaitable[Measurement]]


@dataclass(frozen=True)
class Scenario:
    name: str
    interface: str  # "store" | "bus" | "snapshot" | "e2e"
    metric: str  # "latency" | "throughput"
    grid: dict[str, list[Any]]
    func: ScenarioFunc
    prepare: PrepareFunc | None = None
    iteration_cap: Callable[[dict[str, Any]], int] | None = None


@dataclass(frozen=True)
class RunnerConfig:
    rounds: int = 3
    warmup_iterations: int = 3
    calibration_iterations: int = 5
    target_round_seconds: float = 2.0
    max_iterations: int = 10_000
    cell_timeout_seconds: float = 60.0
    # Bounded separately from cell_timeout_seconds: destroy() runs in the
    # `finally` block after the cell timeout has already elapsed, so a wedged
    # teardown (e.g. a hung broker connection close) needs its own bound to
    # avoid hanging the whole matrix run.
    destroy_timeout_seconds: float = 10.0
    quick: bool = False

    def effective(self) -> "RunnerConfig":
        if not self.quick:
            return self
        return replace(self, rounds=1, target_round_seconds=0.2)


def expand_grid(grid: dict[str, list[Any]], quick: bool) -> list[dict[str, Any]]:
    if not grid:
        return [{}]
    keys = list(grid)
    values = [[grid[k][0]] if quick else grid[k] for k in keys]
    return [dict(zip(keys, combo, strict=True)) for combo in product(*values)]


def _round_from_measurement(measurement: Measurement) -> Round:
    ops_per_sec = (
        measurement.operations / measurement.elapsed_s if measurement.elapsed_s > 0 else 0.0
    )
    latency = (
        LatencyStats.from_durations(measurement.durations_s) if measurement.durations_s else None
    )
    return Round(
        elapsed_s=measurement.elapsed_s,
        operations=measurement.operations,
        ops_per_sec=ops_per_sec,
        latency=latency,
        counters=measurement.counters,
    )


async def run_cell(
    adapter: BenchAdapter[Any],
    scenario: Scenario,
    params: dict[str, Any],
    config: RunnerConfig,
) -> CellResult:
    config = config.effective()
    cell = CellResult(
        scenario=scenario.name,
        interface=scenario.interface,
        backend=adapter.name,
        metric=scenario.metric,
        params=params,
        status="ok",
    )
    resource: Any = None
    try:
        async with asyncio.timeout(config.cell_timeout_seconds):
            resource = await adapter.create()
            prepared: Any = None
            if scenario.prepare is not None:
                prepared = await scenario.prepare(adapter, resource, params)

            await scenario.func(resource, params, config.warmup_iterations, prepared)

            calibration = await scenario.func(
                resource, params, config.calibration_iterations, prepared
            )
            per_iteration = max(calibration.elapsed_s / config.calibration_iterations, 1e-9)
            iterations = max(
                1,
                min(
                    int(config.target_round_seconds / per_iteration),
                    config.max_iterations,
                ),
            )
            if scenario.iteration_cap is not None:
                iterations = max(1, min(iterations, scenario.iteration_cap(params)))

            for _ in range(config.rounds):
                gc.collect()
                measurement = await scenario.func(resource, params, iterations, prepared)
                cell.rounds.append(_round_from_measurement(measurement))
    except TimeoutError:
        cell.status = "failed"
        cell.reason = f"timeout after {config.cell_timeout_seconds}s"
    except Exception as exc:  # noqa: BLE001 - a failed cell must not kill the run
        cell.status = "failed"
        cell.reason = "".join(traceback.format_exception_only(type(exc), exc)).strip()
    finally:
        if resource is not None:
            with contextlib.suppress(Exception):
                async with asyncio.timeout(config.destroy_timeout_seconds):
                    await adapter.destroy(resource)
    return cell


async def run_matrix(
    scenarios: list[Scenario],
    adapters: dict[str, list[BenchAdapter[Any]]],
    config: RunnerConfig,
) -> RunResult:
    cells: list[CellResult] = []
    for interface, interface_adapters in adapters.items():
        interface_scenarios = [s for s in scenarios if s.interface == interface]
        if not interface_scenarios:
            continue
        for adapter in interface_adapters:
            reason = await adapter.available()
            if reason is not None:
                for scenario in interface_scenarios:
                    for params in expand_grid(scenario.grid, config.quick):
                        cells.append(
                            CellResult(
                                scenario=scenario.name,
                                interface=scenario.interface,
                                backend=adapter.name,
                                metric=scenario.metric,
                                params=params,
                                status="skipped",
                                reason=reason,
                            )
                        )
                continue
            await adapter.setup()
            try:
                for scenario in interface_scenarios:
                    for params in expand_grid(scenario.grid, config.quick):
                        cells.append(await run_cell(adapter, scenario, params, config))
            finally:
                await adapter.teardown()
    return RunResult(
        schema_version=SCHEMA_VERSION,
        metadata=collect_metadata(),
        cells=cells,
    )
