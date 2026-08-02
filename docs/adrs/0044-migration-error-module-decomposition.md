# 0044. Migration Error Module Decomposition

`application/migration/exceptions.py` had grown to 1533 lines and was an
exceptions module in name only — it held a circuit breaker, an error
handler, a retry-config system, and a classification vocabulary alongside
the `MigrationError` taxonomy. It splits into four single-responsibility
modules, layered as a one-way DAG so that the mutual dependency between the
taxonomy and the vocabulary — each needing the other — resolves without a
cycle.

## Status

**Accepted.** Implemented across the four tasks of the migration-error
decomposition wave.

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0034](0034-migration-ring-and-layers-contract.md) | Amended — the fourteen-module list that settled `application/migration/`'s contents (Decision Table) gains three new modules from this split; the ring placement and layers contract ADR 0034 decided are unaffected. |
| [0041](0041-infrastructure-exceptions-to-ports.md) | Stands — this ADR's placement calls (`CircuitBreakerOpenError` staying with the taxonomy) follow ADR 0041's one-taxonomy-per-area convention rather than revising it. |

ADR 0034's Status section carries an "Amended by ADR 0044" pointer.

## Context

`application/migration/exceptions.py` had grown to 1533 lines and was an
exceptions module in name only: alongside `MigrationError` and its
subclasses it held `CircuitBreaker` and its supporting types, `ErrorHandler`
and `classify_exception()`, a `RetryConfig` system, and the
`ErrorSeverity`/`ErrorRecoverability`/`ErrorClassification` vocabulary those
retry configs and the handler both depend on. The 2026-08-01
DDD/dogfooding review flagged it as the largest remaining structural defect
in the codebase after the domain-ring hardening work — a single module
mixing a taxonomy, a runtime state machine, a bridge function, and a value
vocabulary, each with a different reason to change.

Splitting it naively deadlocks. `MigrationError._default_classification`
needs `ErrorClassification`, `ErrorSeverity`, `ErrorRecoverability`, and
`RetryConfig` to compute a default classification for an unclassified
error. `classify_exception()` needs `MigrationError` — it is the function
that maps an arbitrary exception onto the migration error taxonomy. Put the
vocabulary and the taxonomy in the same module and it is still one
oversized file; put them in two modules that import each other and the
Python import system deadlocks the first time either module is imported
directly.

## Decision

The module decomposes into four modules forming a one-way DAG: vocabulary →
taxonomy → circuit breaker → handling. Each module may import only the
modules to its left; none may import a module to its right, and none may
import a sibling out of order.

| Module | Contents | Lines | Imports (siblings) |
| --- | --- | --- | --- |
| `error_classification.py` | `ErrorSeverity`, `ErrorRecoverability`, `ErrorClassification`, `RetryConfig`, `TRANSIENT_RETRY_CONFIG`, `CONNECTIVITY_RETRY_CONFIG`, `CUTOVER_RETRY_CONFIG` | 334 | none |
| `exceptions.py` | `MigrationError` + 13 subclasses, including `CircuitBreakerOpenError` | 710 (down from 1533) | `error_classification.py` |
| `circuit_breaker.py` | `CircuitState`, `CircuitBreakerConfig`, `CircuitBreaker`, `CircuitBreakerContext` | 254 | `exceptions.py` |
| `error_handling.py` | `ErrorHandler`, `classify_exception()` | 308 | `error_classification.py`, `exceptions.py`, `circuit_breaker.py` |

The vocabulary carries no sibling imports at all, which is what breaks the
cycle: `error_classification.py` sits underneath the taxonomy rather than
beside it, so `exceptions.py` can depend downward on the vocabulary without
the vocabulary needing anything back from the taxonomy.

Two placement calls were made explicitly, not by default:

**`CircuitBreakerOpenError` stays with the taxonomy, not the circuit
breaker.** It is a `MigrationError` subclass — a member of the exception
hierarchy the taxonomy owns — even though the circuit breaker is the only
thing that raises it. Splitting it into `circuit_breaker.py` would put half
of one taxonomy in each of two modules, one taxonomy per area rather than
one taxonomy per module — the same convention ADR 0041 established when it
moved infrastructure-meaning exceptions out of `domain/exceptions.py` as
complete groups, not individually by raiser.

**`classify_exception()` goes to `error_handling.py`, not
`exceptions.py`.** It depends on both the vocabulary (to build a
classification) and the taxonomy (to match exception types), which places
it downstream of both by construction. It is also, in nature, a runtime
bridge function — mapping an arbitrary caught exception onto the migration
error taxonomy at the moment of handling — not a member of the taxonomy
itself, so it belongs beside `ErrorHandler`, the other piece of runtime
error-handling logic, rather than inside the type hierarchy it classifies
against.

### The cycle

The mutual dependency is real, not incidental: `MigrationError`'s default
classification needs the vocabulary layer, and `classify_exception()` needs
the taxonomy layer. The decomposition above resolves it by making the
dependency one-directional at the module level — the vocabulary never
imports the taxonomy, only the taxonomy imports the vocabulary — while both
directions of the *conceptual* relationship (a `MigrationError` self-reports
a classification; a bridge function classifies an arbitrary exception
against the taxonomy) remain expressible, just from different modules.

The layering is enforced by
`tests/unit/application/migration/test_module_layering.py`, which AST-parses
each module's sibling imports and fails on any import that violates the
DAG. A `TYPE_CHECKING`-guarded import was explicitly considered and
rejected as a way to satisfy the test while keeping a cycle at the type
level: the test does not exempt `TYPE_CHECKING` blocks, on the reasoning
that a cycle hidden behind a type-checking guard is still a cycle in the
design, invisible to the layering contract only because it never executes.
This forced a real decision mid-wave — see Consequences.

## Consequences

### Positive

- `exceptions.py` returns to being what its name says: the `MigrationError`
  taxonomy alone, at 710 lines, down from 1533.
- Each of the four modules has one reason to change: the vocabulary changes
  when classification semantics change, the taxonomy when a new migration
  failure mode is named, the circuit breaker when its state machine changes,
  and the handler when the bridge or dispatch logic changes.
- The layering test fails loudly, at commit time, if a future change
  reintroduces a cycle — the DAG is enforced mechanically, not just by
  convention.

### Negative

- **Mid-wave episode, not a regression:** while `ErrorHandler` still lived
  in `exceptions.py` (before the handler moved to its own module), its
  `circuit_breaker` constructor parameter had to be temporarily widened from
  `CircuitBreaker | None` to `Any | None`, because at that point in the
  sequence `exceptions.py` was not permitted to import `circuit_breaker.py`
  — importing it would have violated the DAG one step early. Once
  `ErrorHandler` relocated to `error_handling.py`, which is downstream of
  `circuit_breaker.py`, the parameter was restored to its real type with a
  genuine import. No type regression shipped in the final state; the
  episode is recorded here because it demonstrates the layering test
  actively constraining design mid-implementation rather than being a
  decorative check added after the fact.
- Import sites inside the package that named `exceptions.py` for something
  now living elsewhere (`CircuitBreaker`, `ErrorHandler`,
  `classify_exception`, the vocabulary types) need updating to the specific
  new module. This is confined to internal call sites — see Non-breaking
  below.

### Non-breaking

`application/migration/__init__.py`'s `__all__` is byte-identical to its
pre-decomposition state across all four implementation commits. Every name
this ADR relocates is still exported from `eventsource.application.migration`
at the same top-level path; only a caller using a direct submodule import
(`from eventsource.application.migration.exceptions import CircuitBreaker`)
needs to retarget it to the new module.

## Alternatives Considered

**Keep the vocabulary and taxonomy in one module, split only the circuit
breaker and handler out.** Rejected: this halves the line count but leaves
the taxonomy module mixing a value vocabulary with an exception hierarchy —
two things with different reasons to change, the same complaint the DDD
review made about the original file, just at smaller scale.

**Use a `TYPE_CHECKING`-guarded import to let `error_classification.py`
reference `MigrationError` for type hints without a runtime cycle.**
Rejected: the layering test does not exempt `TYPE_CHECKING` blocks, by
design — a cycle that only fails to execute because of a type-checking
guard is still a cycle in the dependency graph, and permitting it would
let a future change reintroduce a real runtime cycle behind the same guard
with nothing to catch it.

## References

- `src/eventsource/application/migration/error_classification.py` — the
  vocabulary layer
- `src/eventsource/application/migration/exceptions.py` — the
  `MigrationError` taxonomy
- `src/eventsource/application/migration/circuit_breaker.py` — the circuit
  breaker
- `src/eventsource/application/migration/error_handling.py` — `ErrorHandler`
  and `classify_exception()`
- `tests/unit/application/migration/test_module_layering.py` — the
  AST-based layering contract enforcing the DAG
- `tests/unit/application/migration/test_circuit_breaker.py`,
  `test_error_handling.py`, `test_exceptions.py`,
  `test_error_classification.py` — realigned test modules; collected test
  count (819) unchanged throughout the wave
- [ADR 0034](0034-migration-ring-and-layers-contract.md) — settled the
  fourteen-module `application/migration/` layout this ADR adds three
  modules to
- [ADR 0041](0041-infrastructure-exceptions-to-ports.md) — the
  one-taxonomy-per-area convention this ADR's `CircuitBreakerOpenError`
  placement follows

## Related

- `CHANGELOG.md` — `[Unreleased]` entry for this decomposition
- `.claude/rules/architecture.md` — the migration-package module list,
  updated for this ADR
