"""The dead_letter_queue.tenant_id column (ADR-0114) arrives via additive fragments.

The column is added by fragment files appended at composition time, never by
editing a base schema file. Existing PostgreSQL databases get it from the
operator script updates/007_add_dlq_tenant.sql.
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
    _TEMPLATES_DIR / "dlq.sql",
    _TEMPLATES_DIR / "sqlite" / "dlq.sql",
)


def _dlq_table(text: str) -> str:
    """Return the dead_letter_queue CREATE TABLE statement; other tables may carry tenant_id."""
    start = text.index("CREATE TABLE IF NOT EXISTS dead_letter_queue")
    return text[start : text.index(");", start)]


class TestDLQTenantReachesComposedSchemas:
    def test_postgres_dlq_schema_carries_the_column(self) -> None:
        assert "ADD COLUMN IF NOT EXISTS tenant_id UUID" in get_schema("dlq")

    def test_sqlite_dlq_schema_carries_the_column(self) -> None:
        assert "ADD COLUMN tenant_id TEXT" in get_schema("dlq", backend="sqlite")

    def test_postgres_all_schema_carries_the_column(self) -> None:
        assert "ADD COLUMN IF NOT EXISTS tenant_id UUID" in get_all_schemas()

    def test_sqlite_all_schema_carries_the_column(self) -> None:
        assert "ADD COLUMN tenant_id TEXT" in get_all_schemas(backend="sqlite")

    def test_composed_schemas_carry_the_tenant_index(self) -> None:
        assert "idx_dlq_tenant_id" in get_schema("dlq")
        assert "idx_dlq_tenant_id" in get_schema("dlq", backend="sqlite")

    def test_non_additive_schemas_omit_the_column(self) -> None:
        assert "tenant_id" not in _dlq_table(get_schema("dlq", additive=False))
        assert "idx_dlq_tenant_id" not in get_schema("dlq", additive=False)
        assert "idx_dlq_tenant_id" not in get_schema("dlq", backend="sqlite", additive=False)

    @pytest.mark.parametrize("path", _BASE_FILES, ids=lambda p: p.name)
    def test_base_dlq_files_are_unmodified(self, path: Path) -> None:
        """The column must come from a fragment, never from an edited base file."""
        text = path.read_text()
        assert "idx_dlq_tenant_id" not in text, path
        if "CREATE TABLE IF NOT EXISTS dead_letter_queue" in text:
            assert "tenant_id" not in _dlq_table(text), path


class TestDLQTenantFragmentFiles:
    def test_postgres_fragment_is_idempotent(self) -> None:
        fragment = (_PACKAGE_DIR / "additive" / "dlq_tenant_id.sql").read_text()
        assert "ADD COLUMN IF NOT EXISTS tenant_id UUID" in fragment

    def test_sqlite_fragment_is_the_plain_add_column(self) -> None:
        fragment = (_PACKAGE_DIR / "additive" / "sqlite" / "dlq_tenant_id.sql").read_text()
        assert "ALTER TABLE dead_letter_queue ADD COLUMN tenant_id TEXT;" in fragment
        assert "ALTER TABLE dead_letter_queue ADD COLUMN IF NOT EXISTS" not in fragment

    def test_postgres_operator_script_adds_the_same_column(self) -> None:
        script = (_PACKAGE_DIR / "updates" / "007_add_dlq_tenant.sql").read_text()
        assert "ADD COLUMN IF NOT EXISTS tenant_id UUID" in script
        assert "CREATE INDEX IF NOT EXISTS idx_dlq_tenant_id" in script
