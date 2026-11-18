"""The snapshots.tenant_id column (ADR-0113) arrives via additive fragments.

The column is added by fragment files appended at composition time, never by
editing a base schema file. Existing PostgreSQL databases get it from the
operator script updates/005_add_snapshot_tenant.sql.
"""

from pathlib import Path

import pytest

from eventsource.adapters.sql.schemas import (
    _PACKAGE_DIR,
    _SCHEMAS_DIR,
    _TEMPLATES_DIR,
    get_all_schemas,
    get_schema,
)

_BASE_FILES = (
    _SCHEMAS_DIR / "all.sql",
    _SCHEMAS_DIR / "sqlite_all.sql",
    _TEMPLATES_DIR / "snapshots.sql",
    _TEMPLATES_DIR / "sqlite" / "snapshots.sql",
)


def _snapshots_table(text: str) -> str:
    """Return the snapshots CREATE TABLE statement; other tables may carry tenant_id."""
    start = text.index("CREATE TABLE IF NOT EXISTS snapshots")
    return text[start : text.index(");", start)]


class TestSnapshotTenantReachesComposedSchemas:
    def test_postgres_snapshots_schema_carries_the_column(self) -> None:
        assert "ADD COLUMN IF NOT EXISTS tenant_id UUID" in get_schema("snapshots")

    def test_sqlite_snapshots_schema_stays_idempotent_without_the_column(self) -> None:
        """The SQLite snapshot store applies the column with a PRAGMA guard, not by composition."""
        assert "tenant_id" not in get_schema("snapshots", backend="sqlite")

    def test_postgres_all_schema_carries_the_column(self) -> None:
        assert "ADD COLUMN IF NOT EXISTS tenant_id UUID" in get_all_schemas()

    def test_sqlite_all_schema_carries_the_column(self) -> None:
        assert "ADD COLUMN tenant_id TEXT" in get_all_schemas(backend="sqlite")

    def test_non_additive_snapshots_schema_omits_the_column(self) -> None:
        assert "tenant_id" not in get_schema("snapshots", additive=False)
        assert "tenant_id" not in get_schema("snapshots", backend="sqlite", additive=False)

    @pytest.mark.parametrize("path", _BASE_FILES, ids=lambda p: p.name)
    def test_base_snapshot_files_are_unmodified(self, path: Path) -> None:
        """The column must come from a fragment, never from an edited base file."""
        assert "tenant_id" not in _snapshots_table(path.read_text()), path


class TestSnapshotTenantFragmentFiles:
    def test_postgres_fragment_is_idempotent(self) -> None:
        fragment = (_PACKAGE_DIR / "additive" / "snapshots_tenant_id.sql").read_text()
        assert "ADD COLUMN IF NOT EXISTS tenant_id UUID" in fragment

    def test_sqlite_fragment_is_the_plain_add_column(self) -> None:
        fragment = (_PACKAGE_DIR / "additive" / "sqlite" / "snapshots_tenant_id.sql").read_text()
        assert "ALTER TABLE snapshots ADD COLUMN tenant_id TEXT;" in fragment
        assert "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS" not in fragment

    def test_postgres_operator_script_adds_the_same_column(self) -> None:
        script = (_PACKAGE_DIR / "updates" / "005_add_snapshot_tenant.sql").read_text()
        assert "ADD COLUMN IF NOT EXISTS tenant_id UUID" in script
