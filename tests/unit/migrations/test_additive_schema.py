"""The additive-fragment composition path (spec §11 risk 3).

`position_token` is added by a new fragment file appended at composition
time, never by editing a base schema file (migrations/ is append-only by
file). These cases pin that the column reaches every composed schema.
"""

from eventsource.migrations import get_all_schemas, get_schema


class TestPositionTokenReachesComposedSchemas:
    def test_postgres_checkpoints_schema_carries_the_column(self) -> None:
        assert "position_token" in get_schema("checkpoints")

    def test_sqlite_checkpoints_schema_carries_the_column(self) -> None:
        assert "position_token" in get_schema("checkpoints", backend="sqlite")

    def test_postgres_all_schema_carries_the_column(self) -> None:
        assert "position_token" in get_all_schemas()

    def test_sqlite_all_schema_carries_the_column(self) -> None:
        assert "position_token" in get_all_schemas(backend="sqlite")

    def test_base_schema_files_are_unmodified(self) -> None:
        """The column must come from a fragment, never from an edited base file."""
        from eventsource.migrations import _SCHEMAS_DIR, _TEMPLATES_DIR

        for path in (
            _SCHEMAS_DIR / "all.sql",
            _SCHEMAS_DIR / "sqlite_all.sql",
            _TEMPLATES_DIR / "checkpoints.sql",
            _TEMPLATES_DIR / "sqlite" / "checkpoints.sql",
        ):
            assert "position_token" not in path.read_text(), path
