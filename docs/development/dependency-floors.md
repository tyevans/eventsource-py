# Dependency floors

`pyproject.toml` declares a lower bound for every dependency -- `pydantic>=2.8.0`,
`sqlalchemy>=2.0.0`, and so on. Those bounds are a promise: install
`eventsource-py` alongside anything satisfying them and the library works.

Nothing in an ordinary CI run tests that promise.

## Why the floors are invisible

Every job in `.github/workflows/ci.yml` installs with
`uv sync --all-extras --locked`, and `make check` uses the same flags, so both
run against the one exact version set pinned in `uv.lock`. That set is always
near the *top* of each declared range -- it is what a fresh `uv lock` resolves.

The `>=` bound is therefore the single point in the supported range that nothing
ever executes. A floor that is too low cannot fail anything: it passes CI, it
ships, and it breaks for the first user who installs at the bottom of the range.
The failure surfaces in *their* traceback, far from the declaration that caused
it.

This is not a hypothetical. A downstream consumer declared `eventsource-py>=0.9.1`
while calling a constructor parameter that only existed from 0.10.0; their suite
passed throughout, because their CI installed 0.11. When the same question was
turned on this repository, five of our own declared floors turned out to be
**uninstallable on the only Python version we support**.

Note the shape of that consumer's bug: a *constructor signature*. It imports
perfectly. A smoke test that imports the package would have reported success.

## The gate

```bash
make floors
```

`scripts/check_dependency_floors.sh` builds a throwaway virtualenv, installs the
project with `uv pip install --resolution lowest-direct` so every declared
dependency lands exactly on its `>=` bound, and then runs the existing unit suite
against it.

It takes a few minutes and needs network access. In CI it is
`.github/workflows/dependency-floors.yml`, which runs on any pull request that
touches `pyproject.toml` or the script itself, weekly on a schedule, and on
demand via `workflow_dispatch`.

### It is not part of `make check`

`make check` is CI parity, and every gate in it runs against `uv.lock`. This one
exists precisely to resolve *differently*, and it needs the network to do so.
Putting it in the parity set would make the fast local loop slow and
network-dependent for a property that only changes when someone edits a version
bound. It has its own workflow rather than a step in `ci.yml` for the same
reason: the absence of `--locked` is deliberate here, and a separate file makes
that legible instead of looking like an oversight.

## Design notes

### `lowest-direct`, not `lowest`

`uv` offers both. `--resolution lowest` floors transitive dependencies as well as
direct ones; `--resolution lowest-direct` floors only what we declare.

We use `lowest-direct`. Transitive versions are not something this project
declares, controls, or promises anything about -- `greenlet`, `typing-extensions`
and friends are chosen by our dependencies, not by us. Flooring them produces
failures in other projects' decade-old releases, which is noise rather than a bug
in our bounds. The `>=` bounds we publish are what the gate is there to verify,
and those are exactly the set `lowest-direct` pins.

### No floor-specific assertions

The gate runs the unit suite that already exists. It deliberately does not ship
its own list of "things to check at the floor".

Such a list would be a second, partial copy of the public surface, and copies
drift -- the failure mode catalogued in `.claude/rules/recurring-defects.md`
section 2. It would also have to *anticipate* which API the next wrong floor
breaks. The suite already covers the surface; pointing it at a different
resolution is the whole idea.

### Test tooling is pinned, not floored

`pytest`, `hypothesis` and the rest are installed under a constraint file
exported from `uv.lock`. They are contributor tooling, not part of the runtime
contract we publish, and flooring them would fail this gate for reasons having
nothing to do with the bounds under test. Pinning them means the runtime floors
are the only thing that differs between `make floors` and `make test`.

### What is and is not exercised

All extras are *installed* at their floors -- installing `asyncpg` does not
require a PostgreSQL server -- so an uninstallable or unimportable floor is
caught for every one of them.

The suite selection excludes service-backed tests, the same exclusion
`make test` and the `test` CI job use, because this job starts no containers.
So the floors for `asyncpg`, `redis`, `aiokafka`, `aio-pika` and
`confluent-kafka` are verified as far as *installs, imports, and passes the
adapter unit tests*, and no further; a floor of theirs that breaks only against
a live broker would still get through. The core dependencies and the `sqlite`
and `telemetry` extras get the full unit suite.

Raising that ceiling means running the floor resolution against the
`integration` and `broker-tests` service matrices, which roughly triples the
job. It has not been worth it: every floor defect found so far failed at
install or import.

## When a floor is wrong

Raise it in `pyproject.toml` to the lowest version that actually works, and say
in the commit message how you determined that. Do not raise it to the currently
locked version out of caution -- an unnecessarily high floor is a real cost to
downstream consumers with their own constraints to satisfy.

Two distinct reasons a floor fails, worth telling apart:

- **Uninstallable.** No wheel for our `requires-python`, and the source build
  fails. This is what caught five of ours: `requires-python = ">=3.13"` combined
  with a bound predating Python 3.13 describes an environment that cannot exist.
  Whenever the Python floor rises, every dependency floor needs rechecking
  against it -- that coupling is easy to miss.
- **Installable but too old.** The version resolves and imports, but lacks an
  API the library calls. This is the consumer's constructor-signature case and
  the reason the gate runs a test suite rather than an import.
- **Installed, imported, and silently inert.** The sharpest case found so far.
  `sqlalchemy` gates its `greenlet` dependency behind a platform marker that
  excluded Python 3.13 until 2.0.37, so a bare `sqlalchemy` install on 3.13
  resolved, imported, and passed a smoke test -- and then failed *every* async
  call with `the greenlet library is required`. The fix was to declare
  `sqlalchemy[asyncio]`, which states the requirement we actually have instead
  of inheriting it from someone else's marker. Only running the suite finds
  this shape.
- **A new call outrunning the floor.** With the greenlet problem fixed, the
  same gate immediately found the next one: `adapters/_sql/engine.py` calls
  `Dialect.detect_autocommit_setting()`, added in SQLAlchemy 2.0.43, against a
  declared floor of 2.0. This is the ordinary way floors go wrong and the
  reason the gate is wired to pull requests that touch `pyproject.toml` --
  though note it will *not* fire on the PR that introduces the call, only on
  one that edits the declaration. Reaching for a newly added API of a
  dependency is worth a deliberate look at that dependency's `>=` bound.
