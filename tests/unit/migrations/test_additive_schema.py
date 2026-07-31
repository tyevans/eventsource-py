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


class TestMigrationPositionTokensReachComposedSchema:
    """The migration schema's token columns arrive via the same additive
    fragment mechanism, but only for the PostgreSQL backend (no SQLite
    migration schema exists to compose against).
    """

    _TOKEN_COLUMNS = (
        "source_position_token",
        "target_position_token",
        "last_source_position_token",
        "last_target_position_token",
    )

    def test_postgres_migration_schema_carries_the_token_columns(self) -> None:
        schema = get_schema("migration")

        for column in self._TOKEN_COLUMNS:
            assert column in schema, column

    def test_base_migration_template_is_unmodified(self) -> None:
        """The column must come from a fragment, never from an edited base file."""
        from eventsource.migrations import _TEMPLATES_DIR

        text = (_TEMPLATES_DIR / "migration.sql").read_text()
        for column in self._TOKEN_COLUMNS:
            assert column not in text, column

    def test_non_additive_migration_schema_omits_the_token_columns(self) -> None:
        schema = get_schema("migration", additive=False)

        for column in self._TOKEN_COLUMNS:
            assert column not in schema, column

    def test_sqlite_migration_schema_still_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError):
            get_schema("migration", backend="sqlite")


class TestEventsTxidReachesComposedSchemas:
    """The feed-horizon column arrives by fragment, PostgreSQL only."""

    def test_base_events_files_are_unmodified(self) -> None:
        from eventsource.migrations import _SCHEMAS_DIR, _TEMPLATES_DIR

        for path in (
            _SCHEMAS_DIR / "all.sql",
            _SCHEMAS_DIR / "events.sql",
            _TEMPLATES_DIR / "events.sql",
            _TEMPLATES_DIR / "events_partitioned.sql",
        ):
            assert "txid" not in path.read_text(), path

    def test_operator_script_exists_with_the_split_alter(self) -> None:
        from eventsource.migrations import _PACKAGE_DIR

        script = (_PACKAGE_DIR / "updates" / "004_add_events_txid.sql").read_text()
        assert "ADD COLUMN IF NOT EXISTS txid xid8" in script
        assert "ALTER COLUMN txid SET DEFAULT pg_current_xact_id()" in script
        assert "rewrite" in script  # the rationale for splitting the two statements
