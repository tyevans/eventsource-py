"""Scenario registry."""

from bench.core.runner import Scenario
from bench.scenarios.aggregate import E2E_SCENARIOS
from bench.scenarios.buses import BUS_SCENARIOS
from bench.scenarios.snapshots import SNAPSHOT_SCENARIOS
from bench.scenarios.stores import STORE_SCENARIOS


def all_scenarios() -> list[Scenario]:
    return [*STORE_SCENARIOS, *BUS_SCENARIOS, *SNAPSHOT_SCENARIOS, *E2E_SCENARIOS]
