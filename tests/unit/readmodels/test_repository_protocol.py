"""Unit tests for ReadModelRepository protocol."""

from collections.abc import Iterable
from typing import Any
from uuid import UUID

from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.repository import ReadModelRepository


class TestReadModelRepositoryProtocol:
    """Tests for ReadModelRepository protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """Test that protocol can be used with isinstance()."""
        assert getattr(ReadModelRepository, "_is_runtime_protocol", False)

    def test_protocol_methods_exist(self) -> None:
        """Test that all required methods are defined."""
        required_methods = [
            "get",
            "get_many",
            "save",
            "save_many",
            "delete",
            "soft_delete",
            "restore",
            "get_deleted",
            "find_deleted",
            "exists",
            "find",
            "find_one",
            "count",
            "truncate",
            "save_with_version_check",
        ]
        for method in required_methods:
            assert hasattr(ReadModelRepository, method)

    def test_mock_implementation_satisfies_protocol(self) -> None:
        """Test that a mock implementation satisfies the protocol."""

        class SimpleModel(ReadModel):
            name: str

        class MockRepository:
            """Mock implementation for protocol compliance testing."""

            async def get(self, id: UUID) -> SimpleModel | None:
                return None

            async def get_many(self, ids: Iterable[UUID]) -> list[SimpleModel]:
                return []

            async def save(self, model: SimpleModel) -> None:
                pass

            async def save_many(self, models: Iterable[SimpleModel]) -> None:
                pass

            async def delete(self, id: UUID) -> bool:
                return False

            async def soft_delete(self, id: UUID) -> bool:
                return False

            async def restore(self, id: UUID) -> bool:
                return False

            async def get_deleted(self, id: UUID) -> SimpleModel | None:
                return None

            async def find_deleted(self, query: object = None) -> list[SimpleModel]:
                return []

            async def exists(self, id: UUID) -> bool:
                return False

            async def find(self, query: object = None, **eq_filters: Any) -> list[SimpleModel]:
                return []

            async def find_one(self, query: object = None, **eq_filters: Any) -> SimpleModel | None:
                return None

            async def count(self, query: object = None, **eq_filters: Any) -> int:
                return 0

            async def truncate(self) -> int:
                return 0

            async def save_with_version_check(self, model: SimpleModel) -> None:
                pass

        assert isinstance(MockRepository(), ReadModelRepository)

    def test_incomplete_implementation_does_not_satisfy_protocol(self) -> None:
        """Test that incomplete implementation does not satisfy protocol."""

        class IncompleteRepository:
            """Repository missing most methods."""

            async def get(self, id: UUID) -> None:
                return None

        assert not isinstance(IncompleteRepository(), ReadModelRepository)
