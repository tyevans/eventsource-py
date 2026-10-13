"""JSON serialization unit tests package."""

from __future__ import annotations

from .test_contract import TestJsonEncoderContract
from .test_dumps_loads import (
    TestJsonDumps,
    TestJsonDumpsPydanticModels,
    TestJsonDumpsUnsupportedTypes,
    TestJsonLoads,
)
from .test_encoder import (
    TestNewModuleExports,
)
from .test_non_finite_floats import TestDomainEventRejectsNonFiniteFloats

__all__ = [
    "TestDomainEventRejectsNonFiniteFloats",
    "TestJsonDumps",
    "TestJsonDumpsPydanticModels",
    "TestJsonDumpsUnsupportedTypes",
    "TestJsonEncoderContract",
    "TestJsonLoads",
    "TestNewModuleExports",
]
