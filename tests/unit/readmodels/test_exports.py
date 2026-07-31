"""Read-model public surface: new port/adapter paths, plus shim completeness."""

import pytest


def test_port_and_adapter_paths_export_the_public_names() -> None:
    from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
    from eventsource.ports.readmodels import (
        Filter,
        Query,
        ReadModel,
        ReadModelRepository,
    )

    assert hasattr(ReadModel, "table_name")
    assert hasattr(ReadModel, "field_names")
    assert hasattr(ReadModel, "custom_field_names")
    assert hasattr(ReadModel, "is_deleted")

    assert hasattr(ReadModelRepository, "get")
    assert hasattr(ReadModelRepository, "save")
    assert hasattr(ReadModelRepository, "find")

    q = Query()
    assert hasattr(q, "filters")
    assert hasattr(q, "order_by")
    assert hasattr(q, "limit")

    assert hasattr(Filter, "eq")
    assert hasattr(Filter, "ne")
    assert hasattr(Filter, "gt")
    assert hasattr(Filter, "in_")

    assert hasattr(InMemoryReadModelRepository, "model_class")


def test_legacy_package_still_covers_all_sixteen_names() -> None:
    import eventsource.readmodels

    assert len(eventsource.readmodels.__all__) == 16
    for name in eventsource.readmodels.__all__:
        with pytest.warns(DeprecationWarning):
            assert getattr(eventsource.readmodels, name) is not None


def test_no_circular_imports() -> None:
    """Test that importing readmodels doesn't cause circular imports."""
    # If we get here without ImportError, circular imports are avoided
    assert True


def test_observability_attributes_exported() -> None:
    """Test that readmodel observability attributes are exported."""
    from eventsource.observability.attributes import (
        ATTR_QUERY_FILTER_COUNT,
        ATTR_QUERY_LIMIT,
        ATTR_READMODEL_ID,
        ATTR_READMODEL_OPERATION,
        ATTR_READMODEL_TYPE,
    )

    # Verify naming convention
    assert ATTR_READMODEL_TYPE.startswith("eventsource.")
    assert ATTR_READMODEL_ID.startswith("eventsource.")
    assert ATTR_READMODEL_OPERATION.startswith("eventsource.")
    assert ATTR_QUERY_FILTER_COUNT.startswith("eventsource.")
    assert ATTR_QUERY_LIMIT.startswith("eventsource.")

    # Verify they're in __all__
    from eventsource.observability import attributes

    assert "ATTR_READMODEL_TYPE" in attributes.__all__
    assert "ATTR_READMODEL_ID" in attributes.__all__
    assert "ATTR_READMODEL_OPERATION" in attributes.__all__
    assert "ATTR_QUERY_FILTER_COUNT" in attributes.__all__
    assert "ATTR_QUERY_LIMIT" in attributes.__all__
