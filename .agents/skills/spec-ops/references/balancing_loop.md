# Autonomous Continuous Balancing Loop & Tri-Directional Discovery

This reference defines the operational mechanics, discovery heuristics, and looping protocols for the SpecOps autonomous orchestrator. It establishes how an orchestrator continuously drives development momentum across architecture, product value, user experience, and documentation without stalling or requiring manual step-by-step steering.

---

## 1. Operating Philosophy: Continuous Momentum & Tri-Directional Equilibrium

Traditional agent workflows stall after every sub-action, awaiting human prodding to decide what to do next. The SpecOps Continuous Balancing Loop eliminates this inertia by operating as an autonomous, self-balancing engine:

1. **Continuous Looping Momentum**:
   - The orchestrator never terminates into an idle prompt when work remains in the system.
   - Upon completion and integration of any task, the orchestrator immediately triggers the next discovery audit and executes the next slice.

2. **Tri-Directional Equilibrium**:
   - Development is not merely writing feature code. A healthy software system maintains strict balance across:
     - **Architecture (ADRs)**: Non-negotiable invariants, bounded contexts, mutation resistance, and security constraints.
     - **Product & Journeys (PRDs & User Stories)**: Measurable customer value, observable outcomes, and executable Gherkin journeys.
     - **Living Knowledge (Diataxis Docs)**: Up-to-date tutorials, how-to guides, reference specs, and architecture explanations.

3. **Strict Invariant Governance**:
   - Every vertical slice must respect hard invariants: file length <500 lines (proactive warning at >=400 lines), blackbox frontdoor verification without backdoor mocks, git worktree isolation, and lockfile immutability.

---

## 2. Tri-Directional Discovery Audit Protocol

At the start of every cycle (or whenever the ready buffer needs replenishment), the orchestrator conducts a systematic **Tri-Directional Discovery Audit** across four key vectors:

```
                  ┌───────────────────────────────┐
                  │      Accepted PRDs (Value)    │
                  │   - Checkable Outcomes        │
                  │   - Unmapped Capabilities     │
                  └───────────────┬───────────────┘
                                  │
                                  ▼
┌─────────────────────────┐               ┌─────────────────────────┐
│   Accepted ADRs (Arch)  │   BALANCE     │  Accepted Stories (BDD) │
│ - Architectural Gaps    │◄─────────────►│ - Uncompleted Scenarios │
│ - Low Task Coverage     │     AUDIT     │ - Persona Journey Paths │
└─────────────────────────┘               └─────────────────────────┘
                                  ▲
                                  │
                  ┌───────────────┴───────────────┐
                  │    Diataxis Documentation     │
                  │   - Tutorials & How-Tos       │
                  │   - Reference & Explanation   │
                  └───────────────────────────────┘
```

### Vector 1: PRD Outcome Coverage (Product Value)
- **Objective**: Ensure that all promised business and product capabilities in `docs/project/product/accepted/` are decomposed into observable implementations.
- **Audit Heuristics**:
  1. Parse all files in `docs/project/product/accepted/*.md`.
  2. Extract the `## Checkable Outcomes` or `## Requirements` checklists.
  3. Scan all backlog tasks (`complete/`, `refined/`, `proposed/`) for matching `governing_prds: [PRD-XXXX]`.
  4. Cross-reference completed PRD checkboxes against actual frontdoor acceptance tests.
  5. **Discovery Action**: For any unchecked outcome lacking an active story or task, synthesize a candidate vertical slice.

### Vector 2: User Story Inspection (User Journeys)
- **Objective**: Ensure end-to-end user journeys defined in `docs/project/user_stories/accepted/` are realized and verified through public interfaces.
- **Audit Heuristics**:
  1. Inspect all accepted user stories (`docs/project/user_stories/accepted/us-XXXX-*.md`).
  2. Inspect Gherkin scenarios (`Scenario:` or `Scenario Outline:`).
  3. Check test implementation coverage under `tests/test_bdd_*.py`.
  4. Cross-reference against target personas in `docs/project/user_stories/PERSONAS.md` to identify neglected user roles or workflow edges.
  5. **Discovery Action**: Prioritize uncompleted scenarios into INVEST-compliant task candidates.

### Vector 3: ADR Architectural Coverage (Structural Health)
- **Objective**: Prevent architectural erosion by ensuring accepted architectural decisions in `docs/project/adrs/accepted/` have concrete, enforceable verification tasks.
- **Audit Heuristics**:
  1. Parse `docs/project/adrs/REGISTRY.md` and accepted ADR files (`docs/project/adrs/accepted/adr-XXXX-*.md`).
  2. Scan all backlog tasks across stages (`complete/`, `refined/`, `proposed/`) and aggregate counts of `governing_adrs: [ADR-XXXX]`.
  3. Identify "zero-coverage" or "low-coverage" ADRs:
     - ADRs with 0 or 1 associated tasks.
     - ADRs specifying quality attributes (e.g. mutation kill rate, AST seam boundaries, property testing) that lack automated CI/health check enforcement.
  4. **Discovery Action**: Synthesize architectural hardening tasks (e.g., property tests via Hypothesis, mutation kill benchmarks via Mutmut, health check rules) citing the unrepresented ADRs.

### Vector 4: Diataxis Documentation Balance (System Knowledge)
- **Objective**: Ensure documentation expands concurrently with codebase capabilities, adhering to the Diataxis framework (`tutorials/`, `how-to/`, `reference/`, `explanation/`).
- **Audit Heuristics**:
  1. Inspect recent CLI subcommands, domain events, or core modules added to the codebase.
  2. Verify whether corresponding user documentation exists:
     - New workflows -> `docs/how-to/`
     - New concepts / architectures -> `docs/explanation/`
     - New CLI flags / API surfaces -> `docs/reference/`
     - Getting-started paths -> `docs/tutorials/`
  3. **Discovery Action**: Include documentation sync items directly within feature tasks or generate dedicated documentation tasks when coverage lags behind implementation.

---

## 3. Synthesizing Balanced Batches of Work

To avoid skewing purely toward features (accumulating architectural debt) or purely toward refactoring (delivering zero user value), the orchestrator synthesizes batches following a disciplined portfolio ratio:

| Category | Target Ratio | Focus Areas |
|---|---|---|
| **Product Value** | ~40% | PRD checkable outcomes, persona pain points, customer-facing CLI subcommands |
| **Architectural Hardening** | ~30% | Low-coverage ADR implementation, AST decomposition, property tests, mutation kill scores |
| **Diataxis & Journey Docs** | ~30% | Executable BDD scenarios, how-to recipes, CLI reference guides, architecture explanations |

### Task Slicing Rules (INVEST Standards)
- **File Length Guardrail**: Every task must be small enough that no created or modified file exceeds 400 lines (hard limit: 500 lines).
- **Public Frontdoor Only**: Every task acceptance criteria must be testable through public module interfaces or CLI commands without backdoor state mutation.
- **Atomic Independence**: Tasks within a batch should minimize overlapping file edits to permit clean parallel execution across isolated worktrees.

---

## 4. The Continuous Balancing Loop Execution Protocol

The orchestrator executes an infinite balancing loop that only pauses when the backlog is clean, all PRD outcomes are checked, and health checks report 0 warnings:

```
┌─────────────────────────────────────────────────────────────────┐
│                      CONTINUOUS BALANCING LOOP                  │
└────────────────────────────────┬────────────────────────────────┘
                                 │
  ┌──────────────────────────────┴──────────────────────────────┐
  ▼                                                             │
[Stage 1: Tri-Directional Discovery Audit]                      │
  - Audit PRDs, Stories, ADRs, Diataxis docs                    │
  - Check buffer depth (target ~10 refined tasks)               │
  │                                                             │
  ▼                                                             │
[Stage 2: JIT Refinement & Task Slicing]                        │
  - Scaffold proposed tasks to balance portfolio ratio          │
  - Curate and refine candidates (`spec-ops curate --infer`)     │
  - Enforce Definition of Ready (DoR)                           │
  │                                                             │
  ▼                                                             │
[Stage 3: In-Worktree Implementation]                           │
  - Pull highest priority task from `PRIORITY.md`               │
  - Create isolated worktree (`spec-ops worktree create`)       │
  - Dispatch subagent with governing ADRs & stories             │
  - Frontdoor TDD: Write tests, implement code, keep <500 lines │
  │                                                             │
  ▼                                                             │
[Stage 4: Preflight Verification & Integration]                 │
  - Verify `spec-ops health` (0 violations, 0 warnings)         │
  - Verify test suite (`pytest`) and lockfile (`uv lock --check`)│
  - Integrate branch under merge lock                           │
  - Mark task complete and sync `PRIORITY.md`                   │
  │                                                             │
  ▼                                                             │
[Stage 5: Diataxis Documentation Sync]                          │
  - Update affected how-tos, references, and explanation docs   │
  - Re-verify documentation build (`spec-ops docs build`)       │
  │                                                             │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
                          [Repeat Loop]
```

### Transition & Non-Blocking Rules:
1. **Never Stall on Completion**: Immediately upon completing Stage 5, the orchestrator begins Stage 1 of the next cycle without prompting the user for permission to continue.
2. **Autonomous Error Triage**: If an implementation step fails or stalls:
   - Run `uv run spec-ops rescue inspect TASK-XXXX` to extract diagnostics.
   - If recoverable, salvage and retry with anti-loop memory.
   - If an architectural or toolchain defect is identified, document an immediate high-priority remediation task and prioritize it first.
3. **Graceful Quiescence**: The loop pauses only when:
   - All accepted PRD outcomes are verified complete.
   - All accepted user stories pass 100% of their BDD scenarios.
   - All accepted ADRs maintain verified architectural task coverage.
   - All documentation is synchronized and codebase health reports 0 violations.
