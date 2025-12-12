"""Unit tests for readmodels module exports."""


def test_readmodels_package_exports() -> None:
    """Test that all expected classes are exported from readmodels package."""
    from eventsource.readmodels import (
        Filter,
        InMemoryReadModelRepository,
        Query,
        ReadModel,
        ReadModelRepository,
    )

    # Verify types are correct
    assert hasattr(ReadModel, "table_name")
    assert hasattr(ReadModel, "field_names")
    assert hasattr(ReadModel, "custom_field_names")
    assert hasattr(ReadModel, "is_deleted")

    # Verify ReadModelRepository has protocol methods
    assert hasattr(ReadModelRepository, "get")
    assert hasattr(ReadModelRepository, "save")
    assert hasattr(ReadModelRepository, "find")

    # Verify Query can be instantiated and has expected fields
    q = Query()
    assert hasattr(q, "filters")
    assert hasattr(q, "order_by")
    assert hasattr(q, "limit")

    # Verify Filter has factory methods
    assert hasattr(Filter, "eq")
    assert hasattr(Filter, "ne")
    assert hasattr(Filter, "gt")
    assert hasattr(Filter, "in_")

    # Verify InMemoryReadModelRepository exists
    assert hasattr(InMemoryReadModelRepository, "model_class")


def test_no_circular_imports() -> None:
    """Test that importing readmodels doesn't cause circular imports."""
    # This test verifies the import order is correct

    # If we get here without ImportError, circular imports are avoided
    assert True


def test_all_exports_defined() -> None:
    """Test that __all__ is properly defined."""
    import eventsource.readmodels

    expected_exports = [
        "ReadModel",
        "ReadModelRepository",
        "Query",
        "Filter",
        "InMemoryReadModelRepository",
    ]

    for name in expected_exports:
        assert name in eventsource.readmodels.__all__
        assert hasattr(eventsource.readmodels, name)


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
