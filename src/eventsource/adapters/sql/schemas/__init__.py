"""Database migration and SQL schema template utilities for postgresql and sqlite."""

from pathlib import Path
from typing import Literal

SchemaName = Literal[
    "events",
    "events_partitioned",
    "outbox",
    "checkpoints",
    "dlq",
    "snapshots",
    "migration",
    "all",
]
BackendName = Literal["postgresql", "sqlite"]

_PACKAGE_DIR = Path(__file__).parent
_TEMPLATES_DIR = _PACKAGE_DIR / "templates"
_SCHEMAS_DIR = _PACKAGE_DIR / "schemas"
_ADDITIVE_DIR = _PACKAGE_DIR / "additive"

# Schema fragments appended at composition time (base schema files are append-only by file).
_ADDITIVE_FRAGMENTS: dict[tuple[str, str], tuple[str, ...]] = {
    ("checkpoints", "postgresql"): ("checkpoints_position_token",),
    ("checkpoints", "sqlite"): ("checkpoints_position_token",),
    ("all", "postgresql"): ("checkpoints_position_token", "events_txid"),
    ("all", "sqlite"): ("checkpoints_position_token",),
    ("migration", "postgresql"): ("migration_position_tokens",),
    ("events", "postgresql"): ("events_txid",),
    ("events_partitioned", "postgresql"): ("events_txid",),
}


def _get_backend_additive_dir(backend: BackendName) -> Path:
    """Get the additive-fragment directory for a specific backend."""
    return _ADDITIVE_DIR if backend == "postgresql" else _ADDITIVE_DIR / backend


def _get_backend_templates_dir(backend: BackendName) -> Path:
    """Get the templates directory for a specific backend."""
    return _TEMPLATES_DIR if backend == "postgresql" else _TEMPLATES_DIR / backend


def _get_backend_schema_file(backend: BackendName) -> Path:
    """Get the combined schema file path for a specific backend."""
    filename = "all.sql" if backend == "postgresql" else f"{backend}_all.sql"
    return _SCHEMAS_DIR / filename


def get_template_path(name: SchemaName, backend: BackendName = "postgresql") -> Path:
    """Get the path to a SQL template file for the given schema name and backend."""
    templates_dir = _get_backend_templates_dir(backend)
    path = templates_dir / f"{name}.sql"

    if not path.exists():
        if backend != "postgresql":
            raise ValueError(
                f"Schema '{name}' is not available for backend '{backend}'. "
                f"Available schemas: {list_schemas(backend)}"
            )
        raise FileNotFoundError(f"Schema template not found: {path}")
    return path


def get_schema(
    name: SchemaName, backend: BackendName = "postgresql", *, additive: bool = True
) -> str:
    """Load a SQL schema template by name and backend, optionally appending additive fragments."""
    if name == "all":
        path = _get_backend_schema_file(backend)
    else:
        path = _get_backend_templates_dir(backend) / f"{name}.sql"

    if not path.exists():
        if backend != "postgresql":
            available = list_schemas(backend)
            if name == "all":
                raise FileNotFoundError(
                    f"Combined schema file not found for backend '{backend}': {path}"
                )
            raise ValueError(
                f"Schema '{name}' is not available for backend '{backend}'. "
                f"Available schemas: {available}"
            )
        raise FileNotFoundError(f"Schema file not found: {path}")

    text = path.read_text()
    if not additive:
        return text

    fragments = _ADDITIVE_FRAGMENTS.get((name, backend), ())
    if not fragments:
        return text

    additive_dir = _get_backend_additive_dir(backend)
    parts = [text]
    for fragment_name in fragments:
        fragment_path = additive_dir / f"{fragment_name}.sql"
        if not fragment_path.exists():
            raise FileNotFoundError(f"Additive fragment not found: {fragment_path}")
        parts.append(fragment_path.read_text())

    return "\n\n".join(parts)


def get_all_schemas(backend: BackendName = "postgresql") -> str:
    """Load the combined SQL schema containing all tables for the given backend."""
    return get_schema("all", backend=backend)


def list_schemas(backend: BackendName = "postgresql") -> list[str]:
    """List all available schema templates for a backend."""
    templates_dir = _get_backend_templates_dir(backend)
    if not templates_dir.exists():
        return []
    return sorted(p.stem for p in templates_dir.glob("*.sql"))


def list_backends() -> list[str]:
    """List all available database backends that have schema templates."""
    backends = ["postgresql"]
    for subdir in _TEMPLATES_DIR.iterdir():
        if subdir.is_dir() and subdir.name != "alembic" and list(subdir.glob("*.sql")):
            backends.append(subdir.name)
    return sorted(backends)


def get_alembic_template(name: str) -> str:
    """Load an Alembic migration template by name."""
    path = _TEMPLATES_DIR / "alembic" / f"{name}.py.template"
    if not path.exists():
        raise FileNotFoundError(f"Alembic template not found: {path}")
    return path.read_text()


def list_alembic_templates() -> list[str]:
    """List all available Alembic migration templates."""
    alembic_dir = _TEMPLATES_DIR / "alembic"
    if not alembic_dir.exists():
        return []
    return sorted(p.stem.replace(".py", "") for p in alembic_dir.glob("*.py.template"))


EVENTS_SCHEMA = "events"
EVENTS_PARTITIONED_SCHEMA = "events_partitioned"
OUTBOX_SCHEMA = "outbox"
CHECKPOINTS_SCHEMA = "checkpoints"
DLQ_SCHEMA = "dlq"
SNAPSHOTS_SCHEMA = "snapshots"
MIGRATION_SCHEMA = "migration"

__all__ = [
    "get_schema",
    "get_all_schemas",
    "get_template_path",
    "list_schemas",
    "list_backends",
    "get_alembic_template",
    "list_alembic_templates",
    "EVENTS_SCHEMA",
    "EVENTS_PARTITIONED_SCHEMA",
    "OUTBOX_SCHEMA",
    "CHECKPOINTS_SCHEMA",
    "DLQ_SCHEMA",
    "SNAPSHOTS_SCHEMA",
    "MIGRATION_SCHEMA",
    "SchemaName",
    "BackendName",
]
