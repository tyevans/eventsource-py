"""Fixtures and test doubles for projection replay tests."""

from tests.unit.application.projections.test_replay._fixtures import (
    _REGISTRY,
    TENANT_A,
    TENANT_B,
    Collecting,
    InvoiceIssued,
    NonAdvancingFeed,
    OrderPlaced,
    PositionlessFeed,
    RecordingFeed,
    RejectsEverything,
    RejectsOne,
    _append,
    _envelope,
    _store_with,
)

__all__ = [  # pragma: allowlist secret
    "Collecting",
    "InvoiceIssued",
    "NonAdvancingFeed",
    "OrderPlaced",
    "PositionlessFeed",
    "RecordingFeed",
    "RejectsEverything",
    "RejectsOne",
    "TENANT_A",
    "TENANT_B",
    "_REGISTRY",
    "_append",
    "_envelope",
    "_store_with",
]
