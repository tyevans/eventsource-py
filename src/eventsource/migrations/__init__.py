"""
Database migration support for the eventsource library.

This module provides SQL schema templates and utilities for setting up
the database tables required by the eventsource library.

Tables:
    - events: Core event store table (partitioned and non-partitioned versions)
    - event_outbox: Transactional outbox for reliable publishing
    - projection_checkpoints: Projection position tracking
    - dead_letter_queue: Failed event processing storage

Usage:
    from eventsource.migrations import get_schema, get_all_schemas

    # Get a specific schema
    events_sql = get_schema("events")

    # Get all schemas combined
    all_sql = get_all_schemas()

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
    "all",
]

# Paths
_PACKAGE_DIR = Path(__file__).parent
_TEMPLATES_DIR = _PACKAGE_DIR / "templates"
_SCHEMAS_DIR = _PACKAGE_DIR / "schemas"


def get_template_path(name: SchemaName) -> Path:
    """
    Get the path to a SQL template file.

    Args:
        name: The schema name (events, events_partitioned, outbox, checkpoints, dlq)

    Returns:
        Path to the SQL template file

    Raises:
        FileNotFoundError: If the template file doesn't exist
    """
    path = _TEMPLATES_DIR / f"{name}.sql"
    if not path.exists():
        raise FileNotFoundError(f"Schema template not found: {path}")
    return path


def get_schema(name: SchemaName) -> str:
    """
    Load a SQL schema template by name.

    Args:
        name: The schema name. One of:
            - "events": Non-partitioned events table
            - "events_partitioned": Partitioned events table for high volume
            - "outbox": Transactional outbox table
            - "checkpoints": Projection checkpoints table
            - "dlq": Dead letter queue table
            - "all": All tables combined (from schemas/all.sql)

    Returns:
        SQL schema definition as a string

    Raises:
        FileNotFoundError: If the schema file doesn't exist

    Example:
        >>> from eventsource.migrations import get_schema
        >>> events_sql = get_schema("events")
        >>> checkpoints_sql = get_schema("checkpoints")
    """
    path = _SCHEMAS_DIR / "all.sql" if name == "all" else _TEMPLATES_DIR / f"{name}.sql"

    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")

    return path.read_text()


def get_all_schemas() -> str:
    """
    Load the combined SQL schema containing all tables.

    This returns the content of schemas/all.sql which includes:
    - events table
    - event_outbox table
    - projection_checkpoints table
    - dead_letter_queue table

    Returns:
        Combined SQL schema definition

    Example:
        >>> from eventsource.migrations import get_all_schemas
        >>> async with engine.begin() as conn:
        ...     await conn.execute(text(get_all_schemas()))
    """
    return get_schema("all")


def list_schemas() -> list[str]:
    """
    List all available schema templates.

    Returns:
        List of schema names available

    Example:
        >>> from eventsource.migrations import list_schemas
        >>> print(list_schemas())
        ['checkpoints', 'dlq', 'events', 'events_partitioned', 'outbox']
    """
    templates = _TEMPLATES_DIR.glob("*.sql")
    return sorted(p.stem for p in templates)


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

__all__ = [
    "get_schema",
    "get_all_schemas",
    "get_template_path",
    "list_schemas",
    "get_alembic_template",
    "list_alembic_templates",
    "EVENTS_SCHEMA",
    "EVENTS_PARTITIONED_SCHEMA",
    "OUTBOX_SCHEMA",
    "CHECKPOINTS_SCHEMA",
    "DLQ_SCHEMA",
]
