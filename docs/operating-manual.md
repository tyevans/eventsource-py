# eventsource-py Agent Operating Manual

Welcome to **eventsource-py**, managed via **SpecOps**—the opinionated, autonomous Project Management as Code (PMaC) engine for human architects and AI coding assistants.

All specifications, user stories, tasks, and architectural decisions are version-locked directly in git alongside implementation code.

---

## Hard Invariants

These rules are non-negotiable. Autonomous agents and human contributors must follow them without exception:

1. **File Length Limit (<500 lines)**:
   - Source files over ~500 lines are strictly forbidden. Decompose large files into focused, single-responsibility modules.
   - Enforced by `uv run spec-ops health` (warns proactively at >=400 lines). Governed by ADR-0002.
2. **Blackbox Frontdoor Verification**:
   - Tests must exercise public interfaces (CLI commands, public module entry points, domain models) rather than reaching into private internals or backdoor state manipulation.
   - Governed by ADR-0003.
3. **Strict Backlog Isolation**:
   - Multi-agent workers execute in isolated git worktrees (`.worktrees/<task-id>`) on dedicated task branches (`task/<task-id>` or `feat/<task-id>`).
   - Shared backlog files (`docs/project/backlog/`) must never be modified directly on feature branches; transitions are synchronized upon integration.
   - Governed by ADR-0005.
4. **UV Workspace Package Management**:
   - All Python tools and dependencies are managed through root UV workspace (`uv run pytest`, `uv run pytest -m "not integration and not postgres and not redis and not e2e and not benchmark"`, `uv run spec-ops ...`). Never invoke bare `pip` or create ad-hoc virtual environments.
5. **Specification as Code (PMaC)**:
   - Every requirement, persona, architectural decision, and work item lives under `docs/project/` as Markdown with YAML frontmatter.
   - Governed by ADR-0001.
6. **Executable BDD User Stories & Frontdoor Testing**:
   - User stories in `docs/project/user_stories/accepted/` must provide executable Gherkin scenarios.
   - All acceptance tests must verify observable outcomes without private mock backdoors.
   - Governed by ADR-0006.
7. **Domain-Driven Design (DDD) & Bounded Contexts**:
   - Code is segmented into explicit bounded contexts with pure domain models isolated from infrastructure.
   - Governed by ADR-0007.
8. **Property-Based Testing (Hypothesis) & Mutation Testing (Mutmut)**:
   - Domain models, state machines, parsers, and health algorithms must maintain generative property tests (`@given(...)`).
   - Core domain modules must maintain a minimum 80% mutation kill score under `mutmut`.
   - Governed by ADR-0009.
9. **Security & Supply-Chain Hard Invariants**:
   - Autonomous agents are strictly forbidden from hardcoding credentials, modifying unapproved lockfiles, or executing non-allowlisted shell commands.
   - Enforced by `uv run spec-ops health --security` and preflight secret scanners. Governed by ADR-0010, ADR-0011, and ADR-0012.

---

## Security & Supply-Chain Hard Invariants

These security and supply-chain guardrails are non-negotiable across all autonomous worker streams:
1. **No Hardcoded Credentials**: Autonomous agents are strictly forbidden from hardcoding credentials, API keys, tokens, or high-entropy secrets in source code, tests, or git commits.
2. **Lockfile Immutability**: Autonomous agents are strictly forbidden from modifying unapproved lockfiles (`uv.lock`, `package-lock.json`) without explicit human architectural approval.
3. **Allowlisted Command Execution**: Autonomous agents are strictly forbidden from executing non-allowlisted shell commands outside approved development toolchains.

<!-- BEGIN CUSTOM INVARIANTS -->
10. **Dependency Management with UV**:
   - Never edit `pyproject.toml` dependency tables by hand — use `uv add`, `uv add --optional <extra>`, or `uv remove`.
   - Always re-sync with `uv sync --all-extras` after any dependency modification.
11. **Ring Layering & Invariant Isolation**:
   - Ring layering: `adapters` over `application` over `ports` over `domain`.
   - `domain` and `ports` must not import `observability`.
   - Inner rings must not import the `testing` toolkit.
   - Governed by ADR-0007, ADR-0134 (legacy 0034), and ADR-0140 (legacy 0040).

### Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **Record remaining work** - Add anything that needs follow-up to `BACKLOG.md`
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update the backlog** - Remove finished items, note in-progress ones
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

### Orchestration Failure Protocol (Dogfooding SpecOps)
When using the SpecOps orchestrator skill on this project, any orchestration failure is an actionable task:
- **Immediate Bug Documentation**: Orchestrators must document all failures as high-priority bugs/tasks in the backlog (`docs/project/backlog/proposed/` or active queue).
- **Dispatch Remediation**: Orchestrators must dispatch through them to resolve root causes and improve the life of all future maintainers.
<!-- END CUSTOM INVARIANTS -->

---

## Design Principles

- **Version-Locked Specifications**: Requirements, user stories, and tasks live in the exact same git commit history as implementation code.
- **Thin Vertical Slicing**: Decompose PRDs into thin, single-pass vertical slices and architectural spikes rather than speculative horizontal layers.
- **Just-In-Time (JIT) Refinement**: Maintain a lean buffer of ~10 ready tasks in `refined/` to prevent specification rot before work begins.
- **Living Relational Graph**: Maintain bidirectional traceability from Personas -> PRDs -> Stories -> Tasks -> ADRs -> Commits.

---

## Project Structure & Navigation

All project management specifications live under `docs/project/`:

| Directory | Purpose |
|---|---|
| `docs/project/user_stories/PERSONAS.md` | Core user personas defining user needs and pain points |
| `docs/project/product/` | PRDs progressing from `idea/` to `shaped/`, `accepted/`, and `shipped/` |
| `docs/project/user_stories/` | Gherkin user stories defining end-to-end user journeys |
| `docs/project/adrs/` | Architectural Decision Records organized with `REGISTRY.md` |
| `docs/project/backlog/` | Work items in `complete/`, `refined/`, and `proposed/` |
| `docs/project/backlog/PRIORITY.md` | Strict sequential priority queue for engineering tasks |
| `docs/project/backlog/ROADMAP.md` | High-level delivery milestones |

---

## Documentation Directives (Diataxis Standards)

All system documentation outside `docs/project/` follows the **Diataxis framework** (`tutorials/`, `how-to/`, `reference/`, `explanation/`):

1. **Consult Existing Docs**: Search `docs/` before implementing changes or adding new conventions.
2. **Fix Stale Documentation**: Update inaccurate or outdated documentation discovered during your work.
3. **Document Reusable Capabilities**: When introducing or modifying public CLI flags, APIs, or architectural patterns, author corresponding how-to recipes or reference specs in `docs/`.

---

## Definition of Ready (DoR)

A backlog task or feature may only be transitioned to `refined/` and pulled into active development when:

1. **Task Metadata Complete**: Task frontmatter contains `id`, `title`, `status: Refined`, `target_bc`, and explicit dependencies.
2. **Governing Artifacts Linked**: Reference PRD in `docs/project/product/accepted/`, Persona in `docs/project/user_stories/PERSONAS.md`, and governing ADRs in `docs/project/adrs/accepted/` are cited.
3. **Executable BDD Specification**: Governing user story in `docs/project/user_stories/accepted/` provides executable Gherkin scenarios (`Given ... When ... Then`) whose test setup is achievable strictly through public frontdoors without backdoor tampering (ADR-0006).
4. **Generative Property Invariants Identified**: Core domain state spaces, combinatorial parsers, and health algorithms identify invariant properties for Hypothesis `@given(...)` testing (ADR-0009).
5. **Mutation Testing Scope Defined**: Target modules for Mutmut mutation testing identified with target >=80% mutant kill score (ADR-0009).
6. **INVEST Criteria Satisfied**: Validated as a thin vertical slice (Independent, Negotiable, Valuable, Estimable, Small [<500 lines per file], Testable) with zero speculative horizontal layers.
7. **Documentation Review**: Relevant existing documentation in `docs/` reviewed to prevent conflicting conventions.

---

## Definition of Done (DoD)

Work is complete and ready for integration into `main` only when:

1. **Blackbox Frontdoor Verification**: 100% test pass rate (`uv run pytest`) verifying observable contracts through public frontdoors with zero private mock backdoors (ADR-0003).
2. **Executable BDD Scenarios Passing**: All Gherkin acceptance criteria executed via `pytest-bdd` pass cleanly without mock backdoors (ADR-0006).
3. **Hypothesis Property Tests Passing**: Generative property tests verify domain invariants across randomized inputs without shrinking failures (ADR-0009).
4. **Mutmut Mutation Score Attained**: Target domain modules achieve >=80% mutant kill score under `mutmut` (ADR-0009).
5. **Codebase Health Check**: `uv run spec-ops health` reports 0 file limit violations (<500 lines) and 0 proactive warnings (<400 lines), and verifies `PRIORITY.md` sync (ADR-0002).
6. **Lockfile Integrity**: `uv lock --check` passes cleanly without unstaged dependency drifts.
7. **Documentation Integrity (Diataxis)**:
   - Inaccurate or stale docs discovered during work are corrected.
   - Reusable patterns, CLI commands, and architectural changes documented in `docs/how-to/` or `docs/reference/`.
   - Documentation builds cleanly (`uv run spec-ops docs build`).
8. **Strict Backlog Progression**: Task is moved from `refined/` to `complete/` (or via `spec-ops queue complete <task-id>`) and `PRIORITY.md` updated atomically upon integration (ADR-0005).
9. **Commit Provenance**: Commits include structured trailers referencing governing tasks and stories (`SpecOps-Task: TASK-XXXX`).

---

## Task Execution Workflow

When picking up engineering work:

1. **Select Task**: Always select the highest-priority unassigned task in `docs/project/backlog/PRIORITY.md` located in `refined/`.
2. **Review Invariants & DoR**: Verify task meets the Definition of Ready (DoR); read governing ADRs, PRDs, and user stories cited in the frontmatter.
3. **Implement**: Develop the solution using test-driven development through public frontdoors. Maintain BDD scenarios and Hypothesis property tests alongside feature code.
4. **Preflight Verification**:
   - Run `uv run spec-ops health` (verify 0 file limit violations, 0 warnings, and PRIORITY.md sync).
   - Run `uv run pytest` (verify 100% test pass rate across unit, BDD, and Hypothesis property tests).
   - Run `uv run pytest -m "not integration and not postgres and not redis and not e2e and not benchmark"` (verify quality preflight passes).
   - Run `uv run mutmut run` (verify mutant kill score on mutated domain modules).
   - Run `uv lock --check` (verify lockfile synchronization).
5. **Complete**: Verify all Definition of Done (DoD) criteria; move task to `complete/` or use `spec-ops queue complete <task-id>`, update `PRIORITY.md`, and link commit or PR.
