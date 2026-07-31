"""`eventsource.ports.readmodels` is importable without sqlalchemy."""

import subprocess
import sys


def test_public_names_import_from_the_port() -> None:
    from eventsource.ports.readmodels import (
        Filter,
        OptimisticLockError,
        Query,
        ReadModel,
        ReadModelError,
        ReadModelNotFoundError,
        ReadModelRepository,
        ReadModelRepositoryProtocol,
    )

    assert ReadModelRepositoryProtocol is ReadModelRepository
    assert issubclass(OptimisticLockError, ReadModelError)
    assert issubclass(ReadModelNotFoundError, ReadModelError)
    assert hasattr(ReadModel, "table_name")
    assert hasattr(Query, "with_filter")
    assert hasattr(Filter, "eq")


def test_no_submodule_pulls_in_sqlalchemy() -> None:
    """Import every port submodule in a fresh interpreter and assert sqlalchemy stayed out.

    A subprocess rather than an in-process check: by the time this test runs,
    another test module has almost certainly imported an adapter, so
    `sys.modules` in this process says nothing.
    """
    program = (
        "import sys\n"
        "import eventsource.ports.readmodels\n"
        "import eventsource.ports.readmodels.model\n"
        "import eventsource.ports.readmodels.query\n"
        "import eventsource.ports.readmodels.repository\n"
        "import eventsource.ports.readmodels.exceptions\n"
        "assert 'sqlalchemy' not in sys.modules, sorted(\n"
        "    m for m in sys.modules if m.startswith('sqlalchemy')\n"
        ")\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", program], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
