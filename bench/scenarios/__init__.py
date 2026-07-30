"""Scenario registry. Later tasks append bus, snapshot, and e2e scenarios."""

from bench.core.runner import Scenario
from bench.scenarios.stores import STORE_SCENARIOS


def all_scenarios() -> list[Scenario]:
    return [*STORE_SCENARIOS]
