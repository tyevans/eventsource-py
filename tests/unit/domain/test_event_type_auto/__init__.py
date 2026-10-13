"""Barrel export facade for automatic event_type derivation tests."""

from tests.unit.domain.test_event_type_auto.test_event_type_auto_behavior import (
    TestEventTypeMismatchWarning,
    TestInheritanceBehavior,
    TestSubclassingDoesNotCorruptParent,
    TestSuppressEventTypeWarningAttribute,
)
from tests.unit.domain.test_event_type_auto.test_event_type_auto_construction import (
    TestDictConstruction,
    TestEdgeCases,
    TestModelValidatorBehavior,
)
from tests.unit.domain.test_event_type_auto.test_event_type_auto_derivation import (
    TestBackwardCompatibility,
    TestEventTypeAutoDerivation,
    TestExplicitEventTypePreservation,
    TestIntegrationWithEventRegistry,
)

__all__ = [  # pragma: allowlist secret
    "TestBackwardCompatibility",
    "TestDictConstruction",
    "TestEdgeCases",
    "TestEventTypeAutoDerivation",
    "TestEventTypeMismatchWarning",
    "TestExplicitEventTypePreservation",
    "TestInheritanceBehavior",
    "TestIntegrationWithEventRegistry",
    "TestModelValidatorBehavior",
    "TestSubclassingDoesNotCorruptParent",
    "TestSuppressEventTypeWarningAttribute",
]
