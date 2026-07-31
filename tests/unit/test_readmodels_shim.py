"""The `eventsource.readmodels` deprecation shim resolves every legacy name."""

import warnings

import pytest

MOVED = {
    "ReadModel": "eventsource.ports.readmodels.model",
    "ReadModelRepository": "eventsource.ports.readmodels.repository",
    "Query": "eventsource.ports.readmodels.query",
    "Filter": "eventsource.ports.readmodels.query",
    "ReadModelError": "eventsource.ports.readmodels.exceptions",
    "OptimisticLockError": "eventsource.ports.readmodels.exceptions",
    "ReadModelNotFoundError": "eventsource.ports.readmodels.exceptions",
    "InMemoryReadModelRepository": "eventsource.adapters.memory.readmodels",
    "PostgreSQLReadModelRepository": "eventsource.adapters.postgresql.readmodels",
    "SQLiteReadModelRepository": "eventsource.adapters.sqlite.readmodels",
    "ReadModelProjection": "eventsource.adapters.sql.readmodel_projection",
    "generate_schema": "eventsource.adapters.sql.readmodel_schema",
    "generate_indexes": "eventsource.adapters.sql.readmodel_schema",
    "generate_full_schema": "eventsource.adapters.sql.readmodel_schema",
    "POSTGRESQL_TYPE_MAP": "eventsource.adapters.sql.readmodel_schema",
    "SQLITE_TYPE_MAP": "eventsource.adapters.sql.readmodel_schema",
}


def test_every_legacy_name_is_covered() -> None:
    import eventsource.readmodels as shim

    assert set(shim.__all__) == set(MOVED)


@pytest.mark.parametrize(("name", "new_path"), sorted(MOVED.items()))
def test_legacy_name_resolves_with_a_deprecation_warning(name: str, new_path: str) -> None:
    import eventsource.readmodels as shim

    with pytest.warns(DeprecationWarning, match=new_path):
        assert getattr(shim, name) is not None


def test_dir_lists_every_moved_name() -> None:
    import eventsource.readmodels as shim

    assert set(MOVED) <= set(dir(shim))


def test_unknown_attribute_raises_attribute_error() -> None:
    import eventsource.readmodels as shim

    with pytest.raises(AttributeError):
        shim.NotAThing  # noqa: B018


def test_top_level_projection_comes_from_the_sql_adapter() -> None:
    import eventsource
    from eventsource.adapters.sql.readmodel_projection import ReadModelProjection

    assert eventsource.ReadModelProjection is ReadModelProjection


def test_importing_the_shim_emits_no_warning_by_itself() -> None:
    import importlib

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        importlib.import_module("eventsource.readmodels")
