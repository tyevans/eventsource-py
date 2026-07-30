"""Scenario registry. Later tasks append bus, snapshot, and e2e scenarios."""

from bench.core.runner import Scenario
from bench.scenarios.buses import BUS_SCENARIOS
from bench.scenarios.stores import STORE_SCENARIOS


def all_scenarios() -> list[Scenario]:
    return [*STORE_SCENARIOS, *BUS_SCENARIOS]
