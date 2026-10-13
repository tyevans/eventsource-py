"""Shared fixtures and helper functions for SubscriptionMigrator tests."""

from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.position_mapper import TranslationResult
from eventsource.application.migration.subscription_migrator import SubscriptionMigrator
from eventsource.ports.checkpoints import CheckpointData
from eventsource.ports.positions import Position

SOURCE_STORE_ID = "source-store"
TARGET_STORE_ID = "target-store"


def source_pos(n: int) -> Position:
    """A source-store position token.

    The migrator is token-native end to end: checkpoints, translations, and
    results all carry Position objects, never ints.
    """
    return Position(store_id=SOURCE_STORE_ID, key=(n,))


def target_pos(n: int) -> Position:
    """A target-store position token, symmetric to source_pos."""
    return Position(store_id=TARGET_STORE_ID, key=(n,))


_SENTINEL = object()


def make_checkpoint_data(
    projection_name: str = "OrderProjection",
    last_event_id: Any = _SENTINEL,
    last_event_type: str | None = "OrderCreated",
    position: Position | None = None,
) -> CheckpointData:
    """Create a CheckpointData instance for testing."""
    return CheckpointData(
        projection_name=projection_name,
        last_event_id=uuid4() if last_event_id is _SENTINEL else last_event_id,
        last_event_type=last_event_type,
        position=position if position is not None else source_pos(1000),
    )


def exact_translation(src: int = 1000, dst: int = 500) -> TranslationResult:
    """Create an exact TranslationResult for testing."""
    return TranslationResult(
        source_position=source_pos(src),
        target_position=target_pos(dst),
        is_exact=True,
    )


@pytest.fixture
def mock_position_mapper() -> MagicMock:
    """Create a mock position mapper."""
    return MagicMock()


@pytest.fixture
def mock_checkpoint_repo() -> MagicMock:
    """Create a mock checkpoint repository."""
    return MagicMock()


@pytest.fixture
def migrator(
    mock_position_mapper: MagicMock,
    mock_checkpoint_repo: MagicMock,
) -> SubscriptionMigrator:
    """Create a migrator with mock dependencies."""
    return SubscriptionMigrator(
        position_mapper=mock_position_mapper,
        checkpoint_repo=mock_checkpoint_repo,
        enable_tracing=False,
    )


class SubscriptionMigratorTestBase:
    """Shared base class providing common mock dependencies for migrator tests."""

    __test__ = False

    @pytest.fixture
    def mock_position_mapper(self) -> MagicMock:
        """Create a mock position mapper."""
        return MagicMock()

    @pytest.fixture
    def mock_checkpoint_repo(self) -> MagicMock:
        """Create a mock checkpoint repository."""
        return MagicMock()

    @pytest.fixture
    def migrator(
        self,
        mock_position_mapper: MagicMock,
        mock_checkpoint_repo: MagicMock,
    ) -> SubscriptionMigrator:
        """Create a migrator with mock dependencies."""
        return SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=mock_checkpoint_repo,
            enable_tracing=False,
        )
