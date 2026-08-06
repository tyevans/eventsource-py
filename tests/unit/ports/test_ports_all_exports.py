"""Every module under `eventsource.ports` declares its public surface.

A module with no `__all__` has an implicit surface: under a strict type
checker (`--no-implicit-reexport`) a name the module merely imported is not
importable from it, while a name it defined is -- and nothing in this repo
fails when the two diverge from what the docs tell consumers to import.
These tests make the surface explicit and keep it in sync with the module.

The criterion `__all__` encodes here: a ports module exports exactly the
public names it *defines*. Names imported for annotations or delegation are
incidental -- they keep their single documented home (`eventsource.ports`
for the port vocabulary, `eventsource.domain` for domain types).
"""

import ast
import importlib
import pkgutil
from pathlib import Path

import pytest

import eventsource.ports


def _ports_modules() -> list[str]:
    names = [eventsource.ports.__name__]
    names.extend(
        info.name
        for info in pkgutil.walk_packages(
            eventsource.ports.__path__, prefix=f"{eventsource.ports.__name__}."
        )
    )
    return sorted(names)


PORTS_MODULES = _ports_modules()


def _is_type_parameter(node: ast.Assign) -> bool:
    """`T = TypeVar(...)` and friends -- a type parameter, not public API."""
    call = node.value
    return (
        isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id in {"TypeVar", "ParamSpec", "TypeVarTuple"}
    )


def _defined_public_names(path: Path) -> set[str]:
    """Top-level public classes, functions and assignments in a module."""
    tree = ast.parse(path.read_text())
    defined: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            defined.add(node.name)
        elif isinstance(node, ast.Assign):
            if _is_type_parameter(node):
                continue
            defined.update(target.id for target in node.targets if isinstance(target, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            defined.add(node.target.id)
    return {name for name in defined if not name.startswith("_")}


@pytest.mark.parametrize("module_name", PORTS_MODULES)
def test_ports_module_declares_all(module_name: str) -> None:
    module = importlib.import_module(module_name)
    assert hasattr(module, "__all__"), (
        f"{module_name} has no __all__; its public surface is implicit and "
        "strict-typed consumers cannot rely on it"
    )


@pytest.mark.parametrize("module_name", PORTS_MODULES)
def test_ports_all_entries_resolve(module_name: str) -> None:
    module = importlib.import_module(module_name)
    exported: list[str] = list(getattr(module, "__all__", []))
    assert len(set(exported)) == len(exported), f"{module_name}.__all__ has duplicates"
    for name in exported:
        assert hasattr(module, name), f"{module_name}.__all__ names missing {name!r}"


@pytest.mark.parametrize("module_name", PORTS_MODULES)
def test_ports_all_covers_every_defined_public_name(module_name: str) -> None:
    """A public name defined in a ports module is exported from it.

    Catches the drift case: a port protocol or value object added to a
    module and never added to `__all__`, which type-checks here and fails
    at the consumer.
    """
    module = importlib.import_module(module_name)
    path = Path(module.__file__ or "")
    if path.name == "__init__.py":
        pytest.skip("package __init__ curates its re-exports by hand")
    missing = _defined_public_names(path) - set(getattr(module, "__all__", []))
    assert not missing, f"{module_name} defines {sorted(missing)} but does not export them"
