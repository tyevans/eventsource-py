"""Unit tests for InMemoryReadModelRepository."""

from tests.unit.readmodels.test_in_memory._fixtures import OrderSummary, repo
from tests.unit.readmodels.test_in_memory.test_in_memory_crud import (
    TestInMemoryReadModelCrud,
)
from tests.unit.readmodels.test_in_memory.test_in_memory_pagination import (
    TestInMemoryReadModelPagination,
)
from tests.unit.readmodels.test_in_memory.test_in_memory_query import (
    TestInMemoryReadModelQuery,
)


class TestInMemoryReadModelRepository(
    TestInMemoryReadModelCrud,
    TestInMemoryReadModelQuery,
    TestInMemoryReadModelPagination,
):
    """Composite test class for backward compatibility."""

    __test__ = False


__all__ = [
    "OrderSummary",
    "TestInMemoryReadModelCrud",
    "TestInMemoryReadModelPagination",
    "TestInMemoryReadModelQuery",
    "TestInMemoryReadModelRepository",
    "repo",
]
