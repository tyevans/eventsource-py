"""
Database migration support for the eventsource library.

This module provides SQL schema templates and utilities for setting up
the database tables required by the eventsource library.

Tables:
    - events: Core event store table (partitioned and non-partitioned versions)
    - event_outbox: Transactional outbox for reliable publishing
    - projection_checkpoints: Projection position tracking
    - dead_letter_queue: Failed event processing storage

Supported backends:
    - postgresql (default): Full-featured PostgreSQL schemas
    - sqlite: SQLite-compatible schemas

Usage:
    from eventsource.migrations import get_schema, get_all_schemas

    # Get a specific schema (PostgreSQL by default)
    events_sql = get_schema("events")

    # Get SQLite-specific schema
    events_sql = get_schema("events", backend="sqlite")

    # Get all schemas combined
    all_sql = get_all_schemas()

    # Get all SQLite schemas combined
    all_sql = get_all_schemas(backend="sqlite")

    # Load and execute in a test database
    async with engine.begin() as conn:
        await conn.execute(text(get_all_schemas()))

For Alembic migrations, see the templates in:
    eventsource.migrations.templates.alembic
"""

from pathlib import Path
from typing import Literal

# Schema file names
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

# Supported database backends
BackendName = Literal["postgresql", "sqlite"]

# Paths
_PACKAGE_DIR = Path(__file__).parent
_TEMPLATES_DIR = _PACKAGE_DIR / "templates"
_SCHEMAS_DIR = _PACKAGE_DIR / "schemas"


def _get_backend_templates_dir(backend: BackendName) -> Path:
    """
    Get the templates directory for a specific backend.

    Args:
        backend: The database backend (postgresql, sqlite)

    Returns:
        Path to the backend-specific templates directory
    """
    if backend == "postgresql":
        return _TEMPLATES_DIR
    return _TEMPLATES_DIR / backend


def _get_backend_schema_file(backend: BackendName) -> Path:
    """
    Get the combined schema file path for a specific backend.

    Args:
        backend: The database backend (postgresql, sqlite)

    Returns:
        Path to the combined schema file
    """
    if backend == "postgresql":
        return _SCHEMAS_DIR / "all.sql"
    return _SCHEMAS_DIR / f"{backend}_all.sql"


def get_template_path(name: SchemaName, backend: BackendName = "postgresql") -> Path:
    """
    Get the path to a SQL template file.

    Args:
        name: The schema name (events, events_partitioned, outbox, checkpoints, dlq)
        backend: The database backend (postgresql, sqlite). Defaults to postgresql.

    Returns:
        Path to the SQL template file

    Raises:
        FileNotFoundError: If the template file doesn't exist
        ValueError: If the schema is not available for the specified backend
    """
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


def get_schema(name: SchemaName, backend: BackendName = "postgresql") -> str:
    """
    Load a SQL schema template by name and backend.

    Args:
        name: The schema name. One of:
            - "events": Non-partitioned events table
            - "events_partitioned": Partitioned events table for high volume
              (PostgreSQL only)
            - "outbox": Transactional outbox table
            - "checkpoints": Projection checkpoints table
            - "dlq": Dead letter queue table
            - "all": All tables combined
        backend: The database backend. One of:
            - "postgresql": Full-featured PostgreSQL schemas (default)
            - "sqlite": SQLite-compatible schemas

    Returns:
        SQL schema definition as a string

    Raises:
        FileNotFoundError: If the schema file doesn't exist
        ValueError: If the schema is not available for the specified backend

    Example:
        >>> from eventsource.migrations import get_schema
        >>> # PostgreSQL schemas (default)
        >>> events_sql = get_schema("events")
        >>> checkpoints_sql = get_schema("checkpoints")
        >>> # SQLite schemas
        >>> events_sql = get_schema("events", backend="sqlite")
        >>> all_sql = get_schema("all", backend="sqlite")
    """
    if name == "all":
        path = _get_backend_schema_file(backend)
    else:
        templates_dir = _get_backend_templates_dir(backend)
        path = templates_dir / f"{name}.sql"

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

    return path.read_text()


def get_all_schemas(backend: BackendName = "postgresql") -> str:
    """
    Load the combined SQL schema containing all tables.

    This returns the content of the combined schema file which includes:
    - events table
    - event_outbox table
    - projection_checkpoints table
    - dead_letter_queue table

    Args:
        backend: The database backend (postgresql, sqlite). Defaults to postgresql.

    Returns:
        Combined SQL schema definition

    Example:
        >>> from eventsource.migrations import get_all_schemas
        >>> # PostgreSQL (default)
        >>> async with engine.begin() as conn:
        ...     await conn.execute(text(get_all_schemas()))
        >>> # SQLite
        >>> import aiosqlite
        >>> async with aiosqlite.connect(":memory:") as db:
        ...     await db.executescript(get_all_schemas(backend="sqlite"))
    """
    return get_schema("all", backend=backend)


def list_schemas(backend: BackendName = "postgresql") -> list[str]:
    """
    List all available schema templates for a backend.

    Args:
        backend: The database backend (postgresql, sqlite). Defaults to postgresql.

    Returns:
        List of schema names available for the specified backend

    Example:
        >>> from eventsource.migrations import list_schemas
        >>> print(list_schemas())
        ['checkpoints', 'dlq', 'events', 'events_partitioned', 'outbox']
        >>> print(list_schemas(backend="sqlite"))
        ['checkpoints', 'dlq', 'events', 'outbox']
    """
    templates_dir = _get_backend_templates_dir(backend)
    if not templates_dir.exists():
        return []
    templates = templates_dir.glob("*.sql")
    return sorted(p.stem for p in templates)


def list_backends() -> list[str]:
    """
    List all available database backends.

    Returns:
        List of backend names that have schema templates available

    Example:
        >>> from eventsource.migrations import list_backends
        >>> print(list_backends())
        ['postgresql', 'sqlite']
    """
    backends = ["postgresql"]  # Always available (default templates)

    # Check for additional backend directories
    for subdir in _TEMPLATES_DIR.iterdir():
        if subdir.is_dir() and subdir.name != "alembic" and list(subdir.glob("*.sql")):
            backends.append(subdir.name)

    return sorted(backends)


def get_alembic_template(name: str) -> str:
    """
    Load an Alembic migration template.

    Args:
        name: Template name (events, outbox, checkpoints, dlq, all_tables)

    Returns:
        Alembic migration template content

    Raises:
        FileNotFoundError: If the template doesn't exist

    Example:
        >>> from eventsource.migrations import get_alembic_template
        >>> template = get_alembic_template("events")
        >>> # Customize template with revision ID
        >>> migration = template.replace("${revision_id}", "abc123")
    """
    alembic_dir = _TEMPLATES_DIR / "alembic"
    path = alembic_dir / f"{name}.py.template"

    if not path.exists():
        raise FileNotFoundError(f"Alembic template not found: {path}")

    return path.read_text()


def list_alembic_templates() -> list[str]:
    """
    List all available Alembic migration templates.

    Returns:
        List of template names available

    Example:
        >>> from eventsource.migrations import list_alembic_templates
        >>> print(list_alembic_templates())
        ['all_tables', 'checkpoints', 'dlq', 'events', 'outbox']
    """
    alembic_dir = _TEMPLATES_DIR / "alembic"
    if not alembic_dir.exists():
        return []
    templates = alembic_dir.glob("*.py.template")
    return sorted(p.stem.replace(".py", "") for p in templates)


# Convenience exports
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
