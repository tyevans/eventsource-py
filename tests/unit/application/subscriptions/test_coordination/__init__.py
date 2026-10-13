"""Tests for LeaderElector protocol, InMemoryLeaderElector, and work redistribution coordination."""

from tests.unit.application.subscriptions.test_coordination._fixtures import (
    IncompleteElector,
    MockElector,
    MockLeaderElector,
    mock_leader_elector,
    shared_leader_state,
    work_coordinator,
)
from tests.unit.application.subscriptions.test_coordination.test_coordination_topics_and_notifications import (
    TestModuleExportsP3003,
    TestShutdownIntent,
    TestShutdownNotification,
    TestTopicConstants,
)
from tests.unit.application.subscriptions.test_coordination.test_in_memory_leader_elector import (
    TestInMemoryLeaderElector,
    TestModuleExportsWithInMemory,
    TestSharedLeaderState,
)
from tests.unit.application.subscriptions.test_coordination.test_leader_protocol import (
    TestLeaderElectorProtocol,
    TestMockElectorBehavior,
    TestModuleExports,
)
from tests.unit.application.subscriptions.test_coordination.test_messages_and_peers import (
    TestHeartbeatMessage,
    TestPeerInfo,
    TestWorkAssignment,
)
from tests.unit.application.subscriptions.test_coordination.test_peer_tracking import (
    TestLeadershipHandoff,
    TestPeerTracking,
)
from tests.unit.application.subscriptions.test_coordination.test_work_redistribution import (
    TestWorkRedistributionCoordinator,
)

__all__ = [  # pragma: allowlist secret
    "IncompleteElector",
    "MockElector",
    "MockLeaderElector",
    "TestHeartbeatMessage",
    "TestInMemoryLeaderElector",
    "TestLeaderElectorProtocol",
    "TestLeadershipHandoff",
    "TestMockElectorBehavior",
    "TestModuleExports",
    "TestModuleExportsP3003",
    "TestModuleExportsWithInMemory",
    "TestPeerInfo",
    "TestPeerTracking",
    "TestSharedLeaderState",
    "TestShutdownIntent",
    "TestShutdownNotification",
    "TestTopicConstants",
    "TestWorkAssignment",
    "TestWorkRedistributionCoordinator",
    "mock_leader_elector",
    "shared_leader_state",
    "work_coordinator",
]
