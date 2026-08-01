"""Unit tests for the `SnapshotStore` / `SnapshotTypeInvalidation` Protocols.

`SnapshotStore` moved from an ABC with concrete default bodies
(`snapshot_exists`) and a `NotImplementedError`-raising
`delete_snapshots_by_type` to a `@runtime_checkable` Protocol with no
implementation code at all; the optional bulk-invalidation capability is
now its own Protocol, `SnapshotTypeInvalidation`. These tests assert the
Protocol contract (structural satisfaction, `isinstance` behavior) and
that the port module contains no implementation code, rather than the
old ABC-subclassing/instantiation behavior.
"""

import ast
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

import eventsource.ports.snapshots as _snapshots_port
from eventsource.ports.snapshots import Snapshot, SnapshotStore, SnapshotTypeInvalidation


def test_snapshot_store_protocol_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError, match="Protocols cannot be instantiated"):
        SnapshotStore()  # type: ignore[abstract]


def test_snapshot_type_invalidation_protocol_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError, match="Protocols cannot be instantiated"):
        SnapshotTypeInvalidation()  # type: ignore[abstract]


class _FullSnapshotStore:
    """Structural implementation: no inheritance, matches both Protocols."""

    def __init__(self) -> None:
        self._snapshots: dict[tuple, Snapshot] = {}

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        self._snapshots[(snapshot.aggregate_id, snapshot.aggregate_type)] = snapshot

    async def get_snapshot(self, aggregate_id, aggregate_type):
        return self._snapshots.get((aggregate_id, aggregate_type))

    async def delete_snapshot(self, aggregate_id, aggregate_type) -> bool:
        key = (aggregate_id, aggregate_type)
        if key in self._snapshots:
            del self._snapshots[key]
            return True
        return False

    async def snapshot_exists(self, aggregate_id, aggregate_type) -> bool:
        return (aggregate_id, aggregate_type) in self._snapshots

    async def delete_snapshots_by_type(self, aggregate_type, schema_version_below=None) -> int:
        to_delete = [key for key in self._snapshots if key[1] == aggregate_type]
        for key in to_delete:
            del self._snapshots[key]
        return len(to_delete)


class _CoreOnlySnapshotStore:
    """Satisfies SnapshotStore but not SnapshotTypeInvalidation."""

    async def save_snapshot(self, snapshot: Snapshot) -> None: ...

    async def get_snapshot(self, aggregate_id, aggregate_type):
        return None

    async def delete_snapshot(self, aggregate_id, aggregate_type) -> bool:
        return False

    async def snapshot_exists(self, aggregate_id, aggregate_type) -> bool:
        return False


class _IncompleteSnapshotStore:
    """Missing get_snapshot and delete_snapshot -- does not satisfy the port."""

    async def save_snapshot(self, snapshot: Snapshot) -> None: ...


def test_structural_implementation_satisfies_snapshot_store() -> None:
    store = _FullSnapshotStore()
    assert isinstance(store, SnapshotStore)


def test_structural_implementation_satisfies_snapshot_type_invalidation() -> None:
    store = _FullSnapshotStore()
    assert isinstance(store, SnapshotTypeInvalidation)


def test_core_only_store_does_not_satisfy_type_invalidation() -> None:
    store = _CoreOnlySnapshotStore()
    assert isinstance(store, SnapshotStore)
    assert not isinstance(store, SnapshotTypeInvalidation)


def test_incomplete_store_does_not_satisfy_snapshot_store() -> None:
    store = _IncompleteSnapshotStore()
    assert not isinstance(store, SnapshotStore)


async def test_snapshot_exists_true_when_present() -> None:
    store = _FullSnapshotStore()
    aggregate_id = uuid4()
    snapshot = Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type="Order",
        version=1,
        state={},
        schema_version=1,
        created_at=datetime.now(UTC),
    )
    await store.save_snapshot(snapshot)

    assert await store.snapshot_exists(aggregate_id, "Order") is True


async def test_snapshot_exists_false_when_missing() -> None:
    store = _FullSnapshotStore()
    assert await store.snapshot_exists(uuid4(), "Order") is False


async def test_snapshot_exists_distinguishes_aggregate_types() -> None:
    store = _FullSnapshotStore()
    aggregate_id = uuid4()
    snapshot = Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type="Order",
        version=1,
        state={},
        schema_version=1,
        created_at=datetime.now(UTC),
    )
    await store.save_snapshot(snapshot)

    assert await store.snapshot_exists(aggregate_id, "Order") is True
    assert await store.snapshot_exists(aggregate_id, "User") is False


async def test_delete_snapshots_by_type_only_available_on_capable_stores() -> None:
    capable = _FullSnapshotStore()
    snapshot = Snapshot(
        aggregate_id=uuid4(),
        aggregate_type="Order",
        version=1,
        state={},
        schema_version=1,
        created_at=datetime.now(UTC),
    )
    await capable.save_snapshot(snapshot)

    count = await capable.delete_snapshots_by_type("Order")

    assert count == 1

    incapable = _CoreOnlySnapshotStore()
    assert not hasattr(incapable, "delete_snapshots_by_type")


def test_ports_snapshots_module_contains_no_implementation_code() -> None:
    """`ports/snapshots.py` must contain zero method bodies beyond `...`.

    Mirrors the ast-based purity check in
    `tests/unit/ports/test_readmodels_port_surface.py`: ports own contracts,
    never behavior. Every `async def` inside `SnapshotStore` or
    `SnapshotTypeInvalidation` must have a body that is exactly a docstring
    (optional) followed by a single `Ellipsis` expression statement -- no
    `pass`, no `raise NotImplementedError`, no real logic.
    """
    source_path = Path(_snapshots_port.__file__)
    tree = ast.parse(source_path.read_text(), filename=str(source_path))

    protocol_names = {"SnapshotStore", "SnapshotTypeInvalidation"}
    violations: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name in protocol_names:
            for item in node.body:
                if not isinstance(item, ast.AsyncFunctionDef):
                    continue
                body = item.body
                # Drop a leading docstring, if any.
                if (
                    body
                    and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)
                ):
                    body = body[1:]
                is_ellipsis_only = (
                    len(body) == 1
                    and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and body[0].value.value is Ellipsis
                )
                if not is_ellipsis_only:
                    violations.append(f"{node.name}.{item.name} has a non-empty body")

    assert not violations, violations


def test_ports_snapshots_module_does_not_import_abc() -> None:
    """No `abc.ABC`/`abstractmethod` machinery -- the port is a Protocol now.

    Checked at the AST level (not a raw substring search) so the module's
    own prose docstrings -- which are free to *talk about* ABC or
    NotImplementedError as things the port deliberately no longer uses --
    don't trip the assertion.
    """
    source_path = Path(_snapshots_port.__file__)
    tree = ast.parse(source_path.read_text(), filename=str(source_path))

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "abc":
            pytest.fail(f"unexpected `from abc import ...`: {ast.dump(node)}")
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "abc":
                    pytest.fail("unexpected `import abc`")
        if isinstance(node, ast.Name) and node.id in {"ABC", "abstractmethod"}:
            pytest.fail(f"unexpected reference to {node.id!r}")
        if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call):
            func = node.exc.func
            if isinstance(func, ast.Name) and func.id == "NotImplementedError":
                pytest.fail("unexpected `raise NotImplementedError`")
