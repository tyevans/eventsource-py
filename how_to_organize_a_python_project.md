# How to Organize a Python Project

*Principles for structuring a DDD project to maximize agentic coding capability.*

This document synthesizes research from library science, information architecture, knowledge management, software engineering, and AI agent interaction patterns into a coherent set of organizational principles for Python projects. The goal: a project structure that humans can navigate intuitively and AI agents can work with effectively.

Throughout this guide, we'll use a **Learning Management System (LMS) with a Learning Record Store (LRS)** as our running example. This LMS tracks learning activities using the xAPI/Experience API specification.

**Note on scope:** While this document covers universal project organization principles applicable to any Python codebase, it illustrates them through a Domain-Driven Design (DDD) monorepo approach. Sections 1-2 (information scent, screaming architecture) and 5-7 (controlled vocabulary, progressive disclosure, wayfinding) apply to all projects. Sections 3-4 and 8-14 demonstrate these principles in a DDD context with bounded contexts, aggregates, and anti-corruption layers. Readers not using DDD can extract the underlying organizational patterns while skipping the domain-specific implementation details.

---

## Table of Contents

1. [The Core Insight: Information Scent](#1-the-core-insight-information-scent)
2. [Screaming Architecture](#2-screaming-architecture)
3. [The Monorepo Structure](#3-the-monorepo-structure)
4. [Internal Package Convention](#4-internal-package-convention)
5. [Controlled Vocabulary](#5-controlled-vocabulary)
6. [Progressive Disclosure](#6-progressive-disclosure)
7. [Wayfinding](#7-wayfinding)
8. [Boundary Architecture](#8-boundary-architecture)
9. [Agentic Coding Optimization](#9-agentic-coding-optimization)
10. [Documentation Organization](#10-documentation-organization)
11. [Knowledge Externalization](#11-knowledge-externalization)
12. [Verification](#12-verification)
13. [Principles Summary](#13-principles-summary)
14. [Adoption Strategy](#14-adoption-strategy)
15. [Concrete Structure](#15-concrete-structure)
16. [Sources](#16-sources)

---

## 1. The Core Insight: Information Scent

Information foraging theory (Pirolli & Card, Xerox PARC) shows that developers navigate codebases the way animals forage for food -- by following **scent cues** that signal proximity to what they need. Every navigable element in a project emits scent: directory names, file names, module docstrings, function signatures, import paths, link text.

**Strong scent** means a developer (or AI agent) looking for "how course prerequisites work" sees `courses/` → `domain/` → `prerequisite.py` and knows they're on the right path before opening a single file.

**Weak scent** means they see `core/` → `services/` → `manager.py` and have no idea whether they're close or miles away. They fall back to grep. AI agents burn context tokens on exploratory reads.

This single concept -- **information scent** -- is the fundamental unit of discoverability. It governs naming, structure, documentation, and every other organizational decision in this document.

**The rule:** Every directory, file, module, class, and function name should emit accurate scent about what it contains. Generic names (`utils`, `helpers`, `core`, `common`, `base`, `manager`, `service`) emit zero scent and should be avoided or qualified.

Related concept: **knowledge crystallization** (Russell, Stefik, Pirolli & Card) -- the process of taking scattered information and compacting it into the representation most useful for a given task. Good project organization is knowledge crystallization applied to code: raw implementation details are organized into a structure that directly supports the developer's task of understanding, modifying, and extending the system.

---

## 2. Screaming Architecture

Robert C. Martin's screaming architecture principle: looking at a project's top-level structure should immediately tell you what the system *does*, not what framework it uses.

**Wrong** (screams "framework"):
```
src/
  models/
  services/
  repositories/
  controllers/
  config/
```

**Right** (screams "domain"):
```
packages/
  lms-courses/          # Course catalog, curriculum, prerequisites
  lms-enrollment/       # Student registration, cohorts, waitlists
  lms-content/          # Lessons, media, SCORM, adaptive paths
  lms-assessment/       # Quizzes, assignments, grading, proctoring
  lms-records/          # xAPI statements, activity streams, LRS
  lms-analytics/        # Dashboards, completion, outcomes, predictive
  lms-identity/         # Users, roles, organizations, OAuth/SSO
  lms-notifications/    # Email, push, in-app, digests
  lms-protocols/        # Shared contracts
  lms/                  # Composition root
```

A new developer (or AI agent) opening this repo immediately understands: this system is about courses, enrollment, content delivery, assessment, and learning records. The *what* is visible before reading a single line of code.

This aligns with library science's subject-based classification systems: organize by the domain subject matter (courses, enrollment, assessment), not by technical format (models, controllers, services). Just as library classification organizes books by subject rather than by media type, good architecture makes the domain visible in the structure.

---

## 3. The Monorepo Structure

A well-structured DDD project defines a tier hierarchy with explicit extraction targets. Each becomes a **uv workspace member** with its own `pyproject.toml`, dependencies, and tests.

```
lms-project/                      # Repository root
├── pyproject.toml                # Root: [tool.uv.workspace] members
├── uv.lock                       # Shared lockfile
├── packages/
│   ├── lms-protocols/            # Tier -1: stdlib-only contracts
│   │   ├── pyproject.toml
│   │   ├── src/
│   │   │   └── lms_protocols/
│   │   └── tests/
│   ├── lms-eventsource/          # Tier 0: event store seedwork
│   ├── lms-courses/              # Tier 1: course catalog & curriculum
│   ├── lms-enrollment/           # Tier 1: registration & cohorts
│   ├── lms-content/              # Tier 1: content delivery
│   ├── lms-assessment/           # Tier 1: quizzes & grading
│   ├── lms-records/              # Tier 1: xAPI LRS
│   ├── lms-analytics/            # Tier 1: reporting & outcomes
│   ├── lms-identity/             # Tier 1: users & authentication
│   ├── lms-notifications/        # Tier 1: messaging & alerts
│   ├── lms-orchestration/        # Tier 2: workflow coordination
│   └── lms/                      # Tier 3: composition root
├── docs/
│   ├── architecture/             # Bounded context specifications
│   └── how_to_organize_a_python_project.md
├── CLAUDE.md
├── AGENTS.md
└── .claude/
```

**Why uv workspaces:**
- Shared lockfile across all packages (no version drift)
- Local dependencies via `workspace = true` in `[tool.uv.sources]`
- Independent per-package operations: `uv run --package lms-courses pytest`
- Tier enforcement: each package declares only its tier-appropriate dependencies
- Natural evolution path from monolith → modular monorepo → microservices

**Versioning strategy:**
Workspace members can share a single version (simpler, recommended for tightly-coupled monorepos like this LMS) or have independent versions (needed when packages have external consumers). uv workspaces support both patterns via the root `pyproject.toml` configuration.

**Dev dependency management:**
Development dependencies (pytest, ruff, mypy, etc.) are typically declared in the workspace root `pyproject.toml` and shared across all members, while runtime dependencies go in each package's own `pyproject.toml`. This centralizes tooling versions while keeping domain dependencies explicit per package.

**Trade-offs:**
Multi-package workspaces introduce operational costs: more `pyproject.toml` files to maintain, cross-package version alignment work, and CI matrix complexity. However, uv's unified lockfile and workspace dependency resolution mitigate most of these concerns. The isolation benefits for agent work and architectural enforcement typically outweigh the maintenance overhead.

**Why src layout** (e.g., `packages/lms-courses/src/lms_courses/`):
- Prevents accidental import of in-development code
- Standard recommendation from PyPA, pyOpenSci, and the Python packaging community
- Essential in monorepos to prevent import confusion between workspace members

**Why py.typed marker** (PEP 561):
- Signals to type checkers (mypy, pyright, etc.) that the package provides inline type annotations
- Without this marker, type checkers ignore annotations when the package is installed as a dependency
- Critical in monorepos where workspace members import each other's types
- Must be an empty file at the package root (next to `__init__.py`)

---

## 4. Internal Package Convention

Every bounded context package follows the same internal structure. This is convention-over-configuration applied to DDD: given a concept name, a developer can *predict* the file path without searching.

```
packages/lms-courses/
├── pyproject.toml
├── README.md                     # Purpose, tier, consumers, stability
├── src/
│   └── lms_courses/
│       ├── __init__.py           # Public API re-exports only
│       ├── domain/
│       │   ├── __init__.py
│       │   ├── course.py         # Course aggregate
│       │   ├── prerequisite.py   # Prerequisite aggregate
│       │   ├── events.py         # Domain events
│       │   ├── commands.py       # Commands (if BC-internal)
│       │   ├── value_objects.py  # CourseCode, Duration, etc.
│       │   └── repository.py     # Repository protocol (abstract)
│       ├── application/
│       │   ├── __init__.py
│       │   ├── handlers.py       # Command handlers
│       │   └── queries.py        # Query handlers
│       └── infrastructure/
│           ├── __init__.py
│           └── postgres_course_repository.py  # Repository impl
└── tests/
    ├── domain/
    │   ├── test_course.py
    │   └── test_prerequisite.py
    ├── application/
    │   └── test_handlers.py
    └── infrastructure/
        └── test_postgres_course_repository.py
```

**The convention:**

| You're looking for... | You'll find it at... |
|---|---|
| Aggregate `Foo` | `domain/foo.py` |
| Domain events | `domain/events.py` |
| Value objects | `domain/value_objects.py` |
| Repository interface | `domain/repository.py` |
| Command handlers | `application/handlers.py` |
| Query handlers | `application/queries.py` |
| Repository implementation | `infrastructure/<store>_<name>_repository.py` |
| External adapter | `infrastructure/<service>_adapter.py` |
| Tests for any of the above | `tests/<layer>/test_<filename>.py` |

This is the same insight behind Rails' `app/controllers/`, Django's `models.py`, and Nx's "public API per package" -- predictable placement eliminates search. Structure becomes documentation.

**Example:** Looking for how enrollment waitlists work? You know immediately to check `packages/lms-enrollment/src/lms_enrollment/domain/waitlist.py` without asking anyone or searching.

**Note on thin bounded contexts:** The domain/application/infrastructure convention provides a default internal structure. Individual BCs may deviate when their domain warrants it (e.g., a simple lookup service may not need an infrastructure layer, or a notification BC might have only domain + infrastructure with no application layer), provided they maintain the boundary contract via their `__init__.py` public API. Structure serves the domain, not the reverse.

---

## 5. Controlled Vocabulary

Library science's strongest lesson: **uncontrolled naming causes retrieval failure.** If the same concept is called "student" in one place, "learner" in another, and "user" in a third, no search finds all of them. No AI agent can reason about them consistently.

DDD's **ubiquitous language** is the software equivalent of a library's controlled vocabulary. For your project, this means (hereafter "bounded context" abbreviated as BC):

- Every bounded context, aggregate, event, command, and protocol has **one canonical name**
- That name is used identically in code (class names, module names), documentation, CLI commands, event types, and conversation
- Synonyms are explicitly documented and redirected: "if you're looking for X, it's called Y here"
- The vocabulary lives in one authoritative place (e.g., `docs/architecture/GLOSSARY.md`) and is enforced in review
- Automated enforcement options include custom ruff rules, pre-commit hooks checking directory/module names against a vocabulary list, or CI scripts that validate naming consistency

**Anti-patterns to avoid:**
- `CourseManager` vs `CourseService` vs `CourseOrchestrator` for the same concept
- `handle_event` vs `process_event` vs `on_event` for the same operation
- `lesson` vs `unit` vs `module` vs `section` for the same curriculum component

**Example vocabulary for our LMS:**
- **Student** (not learner, user, participant) -- someone enrolled in courses
- **Instructor** (not teacher, facilitator, educator) -- someone delivering courses
- **Course** (not class, program) -- a complete learning experience with objectives
- **Lesson** (not unit, module) -- a single learning session within a course
- **Assessment** (not test, quiz, exam) -- generic term; specific types are Quiz, Assignment, Exam
- **xAPI Statement** (not learning event, activity record) -- the LRS's atomic unit

**What Ranganathan's Five Laws teach us here:**
1. Code is for use -- optimize for developer consumption, not storage elegance
2. Every developer has their module -- ensure all team members can find relevant code
3. Every module has its developer -- clear ownership and discoverability
4. Save the time of the developer -- minimize navigation, maximize findability
5. A codebase is a growing organism -- expect the vocabulary to evolve; update it when it does

---

## 6. Progressive Disclosure

Progressive disclosure reveals complexity in layers: show the most important information first, provide pathways to deeper detail on demand. Never dump all detail at one level. Never hide detail behind an opaque wall.

Applied to a project, this creates a **zoom experience:**

```
Level 0: Repository root
  → CLAUDE.md (30-second orientation), AGENTS.md, README
  → packages/ (the bounded contexts, visible at a glance)

Level 1: Package
  → README.md (purpose, tier, API summary, stability)
  → __init__.py (public API -- what you can import)

Level 2: Layer
  → domain/ (the business rules)
  → application/ (the use cases)
  → infrastructure/ (the wiring)

Level 3: Module
  → Docstrings (what this class/function does and why)
  → Type signatures (what it accepts and returns)

Level 4: Implementation
  → The actual code
  → Inline comments only where logic isn't self-evident

Level 5: Tests
  → Usage examples, edge cases, invariant verification
```

Each level is self-contained enough to answer its level of question. A developer asking "what does the analytics system do?" gets their answer at Level 1 without reading implementation code. A developer asking "how is course completion calculated?" drills to Level 3-4.

**For agent context files specifically:**
- Root `CLAUDE.md`: <300 lines. Project overview, key conventions, quick reference
- `.claude/rules/*.md`: Topic-specific deep dives loaded on demand
- Per-BC context: BC-specific invariants, patterns, and constraints (in `AGENTS.md` or package README)

Research (Jaroslawicz et al., 2025, arXiv 2507.11538) shows primacy bias in AI agents intensifies around 150-200 system prompt instructions, with later instructions increasingly likely to be dropped. At 250 instructions, even the best models achieve only 73-85% accuracy. Progressive disclosure keeps the always-loaded context lean while making detailed guidance available when working in specific areas.

**LMS example:**
- Level 0: "This is an LMS with xAPI-compliant learning record storage"
- Level 1: `lms-records/README.md`: "This package implements the xAPI LRS specification"
- Level 2: `domain/statement.py`: "xAPI Statement aggregate with actor/verb/object"
- Level 3: Type signature shows `Statement` accepts `Actor`, `Verb`, `Object`, optional `Context`
- Level 4: Implementation validates against xAPI 1.0.3 spec requirements
- Level 5: Tests show examples of valid/invalid statements, edge cases for nested contexts

---

## 7. Wayfinding

Kevin Lynch's urban planning research identified five elements that make spaces navigable. They map directly to codebases:

| Urban Element | Codebase Equivalent | LMS Example |
|---|---|---|
| **Landmarks** | Well-known entry points | `main.py`, `lms-protocols/`, `docs/architecture/INDEX.md` |
| **Paths** | Import chains, doc breadcrumbs | `from lms_protocols.courses import CourseRepository` |
| **Districts** | Bounded contexts (areas with distinct identity) | `lms-courses/` feels different from `lms-assessment/` |
| **Edges** | Boundaries between areas | Protocol interfaces, `__init__.py` public APIs |
| **Nodes** | Decision points | `__init__.py` files where you choose which import to follow |

**Practical applications:**

**Multiple access paths.** Peter Morville's information architecture work (*Information Architecture for the World Wide Web*) shows that findability requires overlapping ways to reach the same information. Librarians provide subject headings, classification numbers, title indexes, author indexes, and full-text search -- all pointing to the same resource. For code: directory browsing, module re-exports, documentation cross-references, and consistent naming should all lead to the same file.

**Browsability + searchability.** Browsing works for newcomers (they don't know what to search for). Searching works for experts (they know the term). Both degrade without the other. Keep directory trees shallow and well-named (browsable: max ~7-10 items per level). Keep naming consistent and grep-friendly (searchable: all course events contain "Course" in the name).

**Cross-references solve the colocation problem.** A file can only live in one directory, but concepts span multiple bounded contexts. Libraries solve this with "see also" cards. Codebases solve it with: `__init__.py` re-exports, cross-reference sections in documentation, and an index that maps concepts to their locations across BCs.

**LMS example:** A developer needs to understand how course completion triggers certificate generation.
- **Browsing path:** `packages/` → `lms-courses/` → `domain/events.py` → `CourseCompleted` event
- **Search path:** grep for "CourseCompleted" finds the event definition and all subscribers
- **Import path:** `from lms_courses.domain.events import CourseCompleted`
- **Documentation path:** `docs/architecture/event-flows.md` shows the completion → certificate workflow
- **Cross-reference:** `CourseCompleted` docstring links to certificate generation in `lms-records/`

All five paths lead to understanding the same behavior.

---

## 8. Boundary Architecture

### Boundary Objects

Star & Griesemer's **boundary objects** are artifacts flexible enough to adapt to local needs but robust enough to maintain identity across communities. In a Python DDD project, your `protocols` package serves as standardized interface contracts at boundaries (not "boundary objects" in the sociological sense): the `CourseRepository` protocol means something specific in the courses BC (the persistence layer) and something different in the analytics BC (a data source to query), but maintains a shared contract across both.

Making boundary objects explicit:
- The protocols package is the primary boundary layer
- Events are boundary objects: produced in one BC, consumed in another
- Commands cross BC boundaries via the protocol layer
- Document which artifacts serve as boundary objects and what they mean in each context

**LMS example:** An `xAPIStatement` is a boundary object:
- In `lms-content/`: emitted when a student completes a lesson
- In `lms-records/`: stored as an immutable LRS record
- In `lms-analytics/`: aggregated for completion dashboards
- In `lms-content/`: input signal for adaptive learning paths (via adaptive_path.py)

All four bounded contexts share the same `xAPIStatement` protocol but use it differently.

### Public API Enforcement

Each package exposes its contract through `__init__.py`. Internal modules are implementation details.

```python
# lms_courses/__init__.py -- this IS the public API
__all__ = [
    "Course",
    "Prerequisite",
    "CoursePublished",
    "CourseRetired",
    "CourseCode",
    "Duration",
]

from lms_courses.domain.course import Course
from lms_courses.domain.prerequisite import Prerequisite
from lms_courses.domain.events import CoursePublished, CourseRetired
from lms_courses.domain.value_objects import CourseCode, Duration
```

Consumers import from `lms_courses`, never from `lms_courses.infrastructure.postgres_course_repository`. Combined with import-linter, this creates hard, enforceable boundaries.

### The Legibility/Metis Balance

James C. Scott warns that systems imposing **legibility** (standardization, simplification) can destroy valuable **metis** (practical wisdom, local adaptation). Over-standardizing module structure suppresses adaptations that make each BC work well in its own domain.

**The balance:** Standardize the interfaces (protocols, event schemas, naming, the domain/application/infrastructure convention). Allow bounded contexts internal freedom in implementation patterns. The courses BC might genuinely need different patterns than the assessment BC. The convention provides the skeleton; the BC fills it with domain-appropriate flesh.

**Orchestration tier coupling caveat:** The Tier 2 orchestration layer coordinates cross-BC workflows (see Section 3). If this layer knows about every BC's internals, it becomes a god object that couples the entire system. The orchestration tier should only depend on BC public APIs (protocols and events), never on internal implementations. Workflows coordinate via event subscriptions and protocol-defined commands, maintaining loose coupling across boundaries.

**LMS example:**
- **Standardized:** All BCs use the `domain/events.py` convention, all events inherit from `DomainEvent`, all use ISO 8601 timestamps
- **Locally adapted:**
  - `lms-assessment/` has a `grading/` subdirectory in `domain/` for complex rubric logic
  - `lms-content/` has a `scorm/` infrastructure adapter for SCORM package integration
  - `lms-records/` has a `xapi/` validation module for spec compliance checking

The standard structure is visible; the domain-specific adaptations don't break it.

**Database migration placement:**
Database migrations (Alembic, Django migrations, etc.) typically live in the infrastructure layer of the BC that owns the relevant tables. For shared database schemas, migrations can be placed in a dedicated `migrations/` package at Tier 0. In the concrete structure example (Section 15), `lms-eventsource/infrastructure/` would contain EventStoreDB-related migrations, while each BC's infrastructure layer holds BC-specific migrations.

---

## 9. Agentic Coding Optimization

This section covers three related topics: how AI agents navigate codebases, strategies for organizing agent-specific context files, and documentation practices that serve both human and machine readers.

### How AI Agents Navigate

AI coding agents reset their working memory each session. While modern tooling provides persistent context mechanisms (Claude Code's MEMORY.md, Cursor's .cursor_rules, session transcripts), the agent's active working context still starts fresh. They rely on:

1. **Directory structure** -- `ls` and glob to understand project shape
2. **File names** -- naming conventions that signal content without opening files
3. **Import graphs** -- following `from X import Y` to trace dependencies
4. **Explicit metadata** -- CLAUDE.md, AGENTS.md, README files, docstrings
5. **grep/ripgrep** -- content search as fallback when structure doesn't guide them

Aider's repository map (tree-sitter + PageRank) identifies the most-referenced identifiers to build an efficient overview. The implication: well-connected code (clear imports, explicit dependencies) is more navigable than isolated code.

### Agent Context Hierarchy

Hierarchical context files scope instructions to relevant workspaces, preventing primacy bias from instruction overload:

```
CLAUDE.md                              # Always loaded (keep concise - primacy bias degrades later instructions)
├── .claude/rules/commits.md           # Loaded when committing
├── .claude/rules/testing.md           # Loaded when testing
├── .claude/agents/*.md                # Agent-specific instructions
AGENTS.md                              # Cross-tool compatibility (Copilot, Cursor, etc.)
├── packages/lms-courses/AGENTS.md     # BC-specific context
├── packages/lms-assessment/AGENTS.md  # BC-specific context
└── ...
```

Both CLAUDE.md and AGENTS.md support **hierarchical nesting** -- the nearest file in the directory tree takes precedence. For a DDD project, each BC directory can have its own agent context with BC-specific invariants, patterns, and constraints. An agent working on course code loads the course-specific context without irrelevant assessment details.

**LMS example context hierarchy:**

```markdown
# Root CLAUDE.md
- Python 3.13+, uv, ruff
- DDD with event sourcing
- xAPI 1.0.3 compliance for learning records
- See packages/<bc>/AGENTS.md for BC-specific rules

# packages/lms-records/AGENTS.md
- xAPI Statement is immutable once stored
- Validate against xAPI 1.0.3 spec before persistence
- LRS must support statement retrieval by ID, actor, verb, activity
- See docs/architecture/xapi-compliance.md for full requirements
```

An agent working on the LRS loads both contexts; an agent working on course prerequisites only loads the root.

### What Makes Codebases Hard for AI

| Anti-pattern | Why it hurts | Fix |
|---|---|---|
| Large files (1000+ lines) | Exceeds context, forces truncation | Split by concept |
| Generic names (`utils.py`) | Zero information scent | Name by domain concept |
| Implicit conventions | Agent guesses wrong, compounds errors | Document in CLAUDE.md |
| Scattered concerns | Multi-hop reasoning across files | Co-locate by feature |
| Missing type annotations | Agent can't infer contracts | Type all public APIs |
| Circular dependencies | Agent can't trace clean paths | Enforce with import-linter |

**LMS anti-pattern example:**
```python
# BAD: lms_common/utils.py
def calculate(data, type):  # What does this calculate?
    ...

# GOOD: lms_analytics/domain/completion_calculator.py
def calculate_course_completion_rate(
    enrollments: list[Enrollment],
    completions: list[CourseCompleted]
) -> Decimal:
    """Calculate completion rate as (completions / enrollments)."""
    ...
```

The good version emits strong scent (completion, course, rate), has explicit types, and lives in the right BC.

### Documentation Strategy

Good documentation serves both humans (browsing, learning, reference) and agents (context loading, task grounding). The Diataxis framework (covered in Section 10) provides the taxonomy: tutorials, how-tos, reference, and explanation. For agent work, explanation docs (architecture specs, ADRs) and reference docs (event catalogs, API schemas) are most valuable. Keep context files concise and link to detailed specs rather than inlining large architecture documents.

### Agent-Specific File Conventions

Agent context files (CLAUDE.md, AGENTS.md, .claude/rules/*.md) follow conventions that maximize effectiveness:
- Root context files stay under 300 lines to avoid primacy bias
- Per-BC AGENTS.md files capture BC-specific invariants, not general patterns
- Link to architecture specs in `docs/` for detailed domain knowledge
- Use progressive disclosure: brief overview in context file, full details in linked docs

### Context Budget

Treat the context window like OS memory: budget, compact, intelligently page. As a practical guideline for approximate allocation:

- System instructions (CLAUDE.md + rules): 10-15%
- Tool/function context: 15-20%
- Domain knowledge (specs, docs): 30-40%
- Conversation history: 20-30%
- Reasoning buffer: 10-15%

As a practical guideline, sub-agents should return condensed summaries (1-2K tokens), not raw conversation. Spec-driven development helps: feed only the relevant spec section per task, not the entire architecture.

### Spec-Driven Development

The emerging pattern (Thoughtworks, GitHub Spec Kit, Addy Osmani): **Specify → Plan → Tasks → Implement.**

Specs are persistent, reviewable artifacts that preserve context across sessions. They define input/output mappings, invariants, interface contracts, and success criteria. For a Python project, this might mean architecture documents in `docs/architecture/` that define each bounded context's responsibilities, events, aggregates, and integration points.

**LMS example spec:** `docs/architecture/enrollment-waitlist.md`
```markdown
## Enrollment Waitlist Specification

### Purpose
Manage waitlists when courses reach capacity.

### Aggregates
- `Waitlist`: tracks students waiting for a course
- `WaitlistEntry`: a single student's waitlist position

### Invariants
- Waitlist position is determined by enrollment timestamp (FIFO)
- Student can only appear once per course waitlist
- When seat opens, highest-priority waitlist entry is offered enrollment
- Offer expires after 48 hours if not accepted

### Events
- `StudentAddedToWaitlist(student_id, course_id, position, timestamp)`
- `WaitlistOfferSent(student_id, course_id, offer_expires_at)`
- `WaitlistOfferAccepted(student_id, course_id, enrollment_id)`
- `WaitlistOfferExpired(student_id, course_id, next_offer_to)`

### Commands
- `AddToWaitlist(student_id, course_id)`
- `AcceptWaitlistOffer(student_id, course_id)`

### Integration Points
- Listens for: `CourseCapacityIncreased`, `StudentDroppedCourse`
- Publishes to: `lms-enrollment` (for enrollment creation)
- Queries: `lms-identity` for student contact info (email notification)
```

An agent implementing this has a clear contract to code against. No ambiguity about behavior.

The FAIR principles (Findable, Accessible, Interoperable, Reusable) apply to specs and all project artifacts:
- **Findable**: Unique identifiers, rich metadata, clear naming
- **Accessible**: Retrievable via standard mechanisms (imports, documented paths)
- **Interoperable**: Shared vocabulary, standard formats, protocol contracts
- **Reusable**: Type annotations, documented invariants, clear provenance

---

## 10. Documentation Organization

### Diataxis Framework

The Diataxis framework identifies four distinct documentation needs:

| Quadrant | Purpose | LMS Example |
|---|---|---|
| **Tutorials** | Learning-oriented | "Build your first LMS course" walkthrough |
| **How-to guides** | Task-oriented | "Integrate with SCORM content provider" |
| **Reference** | Information-oriented | xAPI event schema catalog, API docs |
| **Explanation** | Understanding-oriented | Architecture decision records, BC specs |

Most projects start with "explanation" docs. As the project grows, separating (or at least tagging) the four types prevents the common conflation problem where a reference doc tries to also be a tutorial.

**LMS documentation structure:**
```
docs/
├── architecture/           # Explanation: bounded context specs
│   ├── INDEX.md
│   ├── DECISIONS.md        # ADRs
│   ├── courses.md
│   ├── enrollment.md
│   ├── lrs-xapi.md
│   └── event-flows.md
├── tutorials/              # Learning-oriented
│   ├── getting-started.md
│   └── first-course.md
├── how-to/                 # Task-oriented
│   ├── scorm-integration.md
│   ├── sso-setup.md
│   └── custom-grading-rubric.md
└── reference/              # Information-oriented
    ├── api/
    ├── events.md           # Full event catalog
    └── xapi-statement-schema.md
```

### Maps of Content (MOC)

Your `docs/architecture/INDEX.md` should evolve from a flat list into a **Map of Content** -- a navigation hub that groups documents by multiple dimensions:

- **By tier**: Protocol-level docs → Infrastructure docs → Domain docs → Orchestration docs → Application docs
- **By cross-cutting concern**: Event flows, authentication integration, multi-tenancy, data retention
- **By stability**: Draft / Evolving / Stable / Deprecated

A document can appear in multiple groupings. This is the file-system equivalent of faceted classification -- multiple access paths to the same resource.

**LMS INDEX.md excerpt:**
```markdown
# LMS Architecture Documentation

## By Bounded Context
- [Courses](courses.md) -- catalog, curriculum, prerequisites
- [Enrollment](enrollment.md) -- registration, cohorts, waitlists
- [Assessment](assessment.md) -- quizzes, grading, proctoring
- [Learning Records](lrs-xapi.md) -- xAPI LRS implementation

## By Cross-Cutting Concern
- [Authentication & Authorization](auth-integration.md) -- OAuth, SAML, RBAC
- [Event Flows](event-flows.md) -- course completion → certificate workflow
- [Multi-Tenancy](multi-tenancy.md) -- organization isolation strategy

## By Tier
- Tier -1: [Protocols](protocols.md)
- Tier 0: [Event Sourcing Infrastructure](eventsource.md)
- Tier 1: Domain BCs (courses, enrollment, assessment, records, etc.)
- Tier 2: [Orchestration](orchestration.md)
- Tier 3: [Application](application.md)
```

### Cross-References

Every bounded context doc should link to related BCs. `courses.md` discusses prerequisites; it should link to `enrollment.md` (can't enroll without prerequisites). The integration map defines these relationships at runtime; cross-reference links make them navigable at reading time.

### Document Maturity

Add a status indicator to doc headers:

```markdown
---
status: evolving    # draft | evolving | stable | deprecated
---
```

This prevents developers (and agents) from treating a draft sketch as a stable reference.

---

## 11. Knowledge Externalization

The SECI model (Nonaka & Takeuchi) identifies the critical transition for growing projects: **externalization** -- converting tacit knowledge ("why we did it this way") into explicit artifacts (decision records, documented invariants, architecture docs).

**Practice this discipline:**
- Every significant "why" gets captured: not just architectural decisions but naming choices, organizational rationale, and rejected alternatives
- Decision records include context, options considered, decision, and consequences
- "Why not" is as valuable as "why" -- it prevents future contributors from re-litigating settled questions

**LMS example ADR:**
```markdown
# ADR-007: Store xAPI Statements as Immutable Event Log

## Status
Accepted

## Context
xAPI LRS specification requires statement immutability once stored.
We considered three approaches:
1. Traditional CRUD database with update restrictions
2. Append-only event log (event sourcing)
3. Immutable document store (like MongoDB with write-once collections)

## Decision
Use append-only event log (approach 2) implemented with EventStoreDB.

## Consequences
**Positive:**
- Natural fit for xAPI immutability requirements
- Supports temporal queries ("what did student know on date X?")
- Enables event replay for analytics recalculation
- Aligns with event-sourced architecture across the system

**Negative:**
- Cannot "fix" incorrect statements, must void + resubmit
- Storage grows indefinitely (mitigated by retention policies)
- Query complexity for current state (mitigated by read projections)

## Alternatives Rejected
- Approach 1: CRUD restrictions are easy to bypass accidentally
- Approach 3: MongoDB write-once doesn't support efficient temporal queries
```

Wurman's **LATCH framework** provides a completeness check. There are exactly five ways to organize information:

| Axis | Codebase Application | LMS Provision |
|---|---|---|
| **Location** | Which service/host runs what | Deployment docs, infra diagrams |
| **Alphabet** | A-Z sorted reference | API docs, event catalog, glossary |
| **Time** | Chronological history | Git log, changelog, ADRs |
| **Category** | By domain concept | Directory structure (BCs) |
| **Hierarchy** | By importance/dependency | Tier hierarchy, domain model diagrams |

For each axis, the project should provide at least one way to access information organized that way.

---

## 12. Verification

### Import-Linter

import-linter enforces architectural boundaries at CI time. Define contracts:

- **Independence**: Tier 1 BCs never import each other (courses doesn't import assessment)
- **Layers**: Within each BC, domain never imports from infrastructure or application (domain is the innermost layer)
- **Forbidden**: Nothing imports from `lms` application layer except `lms` itself

**Example `.importlinter` config for LMS:**
```ini
[importlinter]
root_packages =
    lms_protocols
    lms_eventsource
    lms_courses
    lms_enrollment
    lms_content
    lms_assessment
    lms_records
    lms_analytics
    lms_identity
    lms_notifications
    lms_orchestration
    lms
include_external_packages = True

[importlinter:contract:tier-1-independence]
name = Tier 1 BCs must not import each other
type = independence
modules =
    lms_courses
    lms_enrollment
    lms_content
    lms_assessment
    lms_records
    lms_analytics
    lms_identity
    lms_notifications

[importlinter:contract:ddd-layer-ordering]
name = DDD layer ordering (infrastructure may import app+domain, app may import domain, domain imports nothing)
type = layers
layers =
    lms_courses.infrastructure
    lms_courses.application
    lms_courses.domain

[importlinter:contract:protocol-purity]
name = Protocols package must only use stdlib
type = forbidden
source_modules =
    lms_protocols
forbidden_modules =
    sqlalchemy
    pydantic
    httpx
```

**Incremental adoption:** Start with just the independence contract between Tier 1 BCs. Add layer contracts as each BC matures and establishes clear domain/application/infrastructure separation. Add forbidden contracts for external dependencies once the core architecture stabilizes.

Run in CI: `uv run lint-imports` fails the build if violations exist.

Kraken Technologies (as of 2024: 500+ developers, 4M+ lines of Python) uses import-linter to track architectural compliance as a metric.

### Conformance Tests

**Conformance suites** -- tests derived directly from specs that any implementation must pass -- are recommended for protocol-driven architecture. Simon Willison coined the term "conformance suites" for project structure enforcement in the context of AI-assisted coding. For this architecture, conformance tests verify that implementations satisfy their Protocol contracts, independent of implementation details.

**LMS example:**
```python
# packages/lms-protocols/tests/test_course_repository_conformance.py
"""Conformance tests for CourseRepository protocol.

Any implementation of CourseRepository must pass these tests.
"""
from lms_protocols.courses import CourseRepository
import pytest

def test_course_repository_conformance(
    repository: CourseRepository  # Fixture provides any impl
):
    """Test that repository satisfies the protocol contract."""
    # Create
    course = Course(code="CS101", title="Intro to CS")
    saved = repository.save(course)
    assert saved.id is not None

    # Retrieve
    retrieved = repository.get_by_id(saved.id)
    assert retrieved.code == "CS101"

    # List
    all_courses = repository.list_by_instructor(instructor_id="prof-1")
    assert isinstance(all_courses, list)
```

Each repository implementation runs these tests as part of its own test suite.

### Three-Tier Boundary System

This three-tier approach synthesizes protocol contracts, architectural verification, and testing into a unified boundary enforcement strategy. Document explicit boundaries for both humans and AI agents:

| Tier | Description | Example |
|---|---|---|
| **Always** | No approval needed | Run tests, read any file, format code |
| **Ask first** | Human review required | Modify shared protocols, change event schemas |
| **Never** | Hard stops | Delete LRS data, modify production config |

Place this in `.claude/rules/boundaries.md`:
```markdown
# Architectural Boundaries

## Always Safe (Green Light)
- Add new aggregates within existing bounded contexts
- Add new tests
- Refactor within a single module (same public API)
- Update documentation
- Run linters and formatters

## Ask First (Yellow Light)
- Add new bounded context packages
- Modify event schemas (breaks consumers)
- Change protocol interfaces (breaks contracts)
- Modify database migrations
- Change xAPI statement schema (LRS compliance)

## Never Without Explicit Instruction (Red Light)
- Delete or void xAPI statements (immutability requirement)
- Modify tier dependencies (breaks architecture)
- Skip test runs before commits
- Commit secrets or credentials
```

---

## 13. Principles Summary

1. **Information scent is king.** Every name, path, and docstring should emit accurate scent about its contents. Zero-scent names (`utils`, `core`, `helpers`) are organizational debt.

2. **Structure screams domain.** Top-level organization communicates bounded contexts, not technical layers. A stranger should understand what the system does from `ls packages/`.

3. **Convention eliminates search.** Given a concept name, a developer should predict the file path. Predictable placement scales better than documentation.

4. **Progressive disclosure layers complexity.** Root → package → layer → module → function → test. Each level answers its level of question.

5. **Multiple paths to the same destination.** Directory browsing, imports, cross-references, and search should all lead to the same file. If you can only find something by knowing its exact path, findability has failed.

6. **Standardize interfaces, free implementations.** Protocols, event schemas, and naming are standardized. Bounded contexts have internal autonomy. Legibility at boundaries, metis within them.

7. **Externalize tacit knowledge.** Decision records, architecture docs, and "why" comments are not overhead -- they're the mechanism that prevents knowledge loss and re-litigation.

8. **FAIR artifacts.** Every project artifact should be findable (named well), accessible (importable or linked), interoperable (uses shared vocabulary), and reusable (typed and documented).

9. **Design for both audiences.** Humans browse and build mental maps. AI agents grep and follow imports. Good organization serves both: clear hierarchy for browsing, consistent naming for searching, explicit metadata for orientation.

10. **A codebase is a growing organism.** The organization will evolve. Design for extension (new BCs, new events) not perfection. Ranganathan's Fifth Law.

---

## 14. Adoption Strategy

The conventional wisdom for organizing software projects: **start simple, add structure as you grow**. Begin with a flat module structure, extract abstractions when patterns emerge, introduce layers only when pain demands it. This is YAGNI applied to architecture -- don't build for scale you don't have.

**For agentic projects, this advice inverts.**

### Why Start Complete

When AI agents are the primary developers, bounded contexts are not organizational abstractions -- they are **concurrency primitives**. The full structure becomes the right Day 1 default because:

**1. Bounded contexts prevent accidental coupling that agents can't see.**

A human developer maintaining mental boundaries can recognize when they're about to make an inappropriate cross-boundary call. They feel the architectural friction. An AI agent operating on a flat directory structure sees no such signal. Without file system boundaries, agents will take the shortest import path, creating tangled dependencies that violate your conceptual boundaries.

Example: In a flat structure, an agent implementing "send notification when course completes" might directly import `CourseRepository` into the notification module. In a bounded-context structure, the courses and notifications packages are separated -- the agent must go through the protocol layer or event bus, preserving the architectural boundary.

**2. uv workspaces provide pytest isolation.**

Each bounded context package has its own test suite that can be run independently: `uv run --package lms-courses pytest`. When Agent A is testing course prerequisite logic and Agent B is testing waitlist promotion, their test runs don't interfere. They don't share fixtures, database state, or side effects. This isolation is critical for parallel agent work.

In a flat structure, tests share a single pytest session. Agents working on different features can introduce test pollution -- fixture conflicts, shared database state, import caching issues -- that break each other's work.

**3. Separate packages prevent cross-boundary imports at the tooling level.**

import-linter can enforce that `lms_courses` never imports `lms_enrollment`. This is a hard boundary -- the tooling will fail CI if an agent violates it. Without workspace separation, import-linter contracts are harder to express and easier to subvert.

An agent can't accidentally couple course logic to assessment logic when those packages are physically separated in the workspace. The import path `from lms_assessment import GradingRubric` is forbidden in `lms_courses/` -- the agent gets immediate feedback from the linter.

**4. Per-BC `__init__.py` public APIs stabilize interfaces for agent consumers.**

Each package declares its public API via `__init__.py` re-exports. An agent consuming the courses BC sees only `Course`, `Prerequisite`, `CoursePublished` -- not internal implementation details like `PostgresCourseRepository`. This forces interface stability. When Agent A refactors course internals, Agent B's code consuming the courses API doesn't break because the public interface remains stable.

In a flat structure, every module is implicitly public. Agents have no signal about what's safe to depend on versus what's internal implementation that might change.

**5. Per-BC `AGENTS.md` context files scope invariants to the relevant workspace.**

An agent working in `lms-records/` loads the LRS-specific invariants (xAPI statement immutability, LRS query requirements) without the unrelated noise of course prerequisite rules or assessment grading logic. Claude Code's hierarchical context loading means the nearest `AGENTS.md` in the directory tree takes precedence.

In a flat structure, all agent context lives in the root `CLAUDE.md`. As the project grows, this file bloats with cross-cutting concerns. Primacy bias (Jaroslawicz et al., 2025) shows that agents start dropping instructions after 150-200 lines. Context localized to workspaces stays lean and focused.

**6. The structure is the specification.**

When an agent is asked to "add analytics for course completion rates," the file system structure tells the agent where this code belongs: `packages/lms-analytics/src/lms_analytics/domain/completion_calculator.py`. The directory tree is the routing table. Agents don't need to hold a conceptual model in context -- the structure externalizes the model.

In a flat structure, placement is a judgment call. The agent guesses. Over time, similar features scatter across inconsistent locations. The codebase loses coherence.

**The conventional "start simple, extract later" advice assumes humans holding boundaries in their heads.** Humans build and refine mental models of system structure. They can navigate a flat codebase by remembering "assessment stuff lives in this area" without needing directory boundaries.

Agents start from zero every session. They have no persistent mental model. They need boundaries materialized in the file system, dependency declarations, and tooling contracts. The structure must be legible to tools, not just to minds.

**This doesn't mean over-engineering.** It means starting with the structure that makes agent work safe and parallelizable. Bounded contexts may have minimal code initially -- a single aggregate, a handful of events. That's fine. The packages provide the growth space. You're not building for scale you don't have; you're building for the cognitive constraints of the developers (agents) who will grow it.

### Migrating Existing Projects

For teams with existing Flask, Django, or FastAPI applications who want to adopt this structure, migration follows a **Strangler Fig pattern** -- incrementally extract bounded contexts from the monolith while maintaining backward compatibility.

#### Phase 1: Identify Bounded Contexts (Week 1-2)

Start with discovery, not refactoring.

**1. Map the domain language.**

Review your existing models, views, services, and repositories. Group them by the business concept they serve, not by technical layer. Look for clusters:
- What concepts have their own lifecycle? (Course, Enrollment, Assignment, User)
- What sets of features form cohesive workflows? (Registration → waitlist → enrollment)
- Where are the transactional boundaries? (You can delete a course without deleting students)
- What data clusters have independent write models? (Course catalog vs student progress)

**2. Look for natural seams.**

Bounded contexts emerge where communication between concepts is primarily event-driven or query-based rather than direct method calls. If CourseService and EnrollmentService communicate by publishing events or querying each other's repositories, that's a seam. If they share mutable state or call each other's internal methods, the boundary isn't clean yet.

**3. Start with 3-5 contexts, not 20.**

Over-partitioning early creates coordination overhead. Choose the contexts with the clearest boundaries and highest agent activity. For an LMS:
- **Core three:** courses, enrollment, identity (authentication/users)
- **Phase two:** assessment, content delivery, notifications
- **Phase three:** analytics, records (LRS), orchestration

**4. Document the target state.**

Write a brief spec for each bounded context: core aggregates, key events, responsibilities, integration points. This lives in `docs/architecture/<context>.md`. Agents implementing the extraction will reference these specs.

#### Phase 2: Extract the Protocols Package (Week 2-3)

Before extracting bounded contexts, create the anti-corruption layer.

**1. Initialize uv workspace.**

```bash
# In your existing project root:
mkdir -p packages/lms-protocols/src/lms_protocols
touch packages/lms-protocols/pyproject.toml
```

Configure the workspace in the root `pyproject.toml`:
```toml
[tool.uv.workspace]
members = ["packages/*"]
```

**2. Define protocol interfaces for one context.**

Start with the simplest bounded context. If "courses" is the cleanest, define `CourseRepository` protocol and key event types in `lms_protocols/courses.py`:

```python
# packages/lms-protocols/src/lms_protocols/courses.py
from typing import Protocol, runtime_checkable
from datetime import datetime

@runtime_checkable
class CourseRepository(Protocol):
    """Repository protocol for course aggregate."""
    def get_by_id(self, course_id: str) -> Course | None: ...
    def save(self, course: Course) -> Course: ...
    def list_active(self) -> list[Course]: ...

class CourseEvent:
    """Base for course domain events."""
    course_id: str
    occurred_at: datetime

class CoursePublished(CourseEvent):
    title: str
    instructor_id: str
```

**3. Gradually expand the protocols package.**

As you extract bounded contexts, add their protocols. Don't try to define every protocol upfront -- extract one context at a time and add its contracts as you go.

#### Phase 3: Extract One Bounded Context (Week 3-5 per context)

Strangler Fig: move code incrementally, maintaining backward compatibility at each step.

**1. Create the workspace package.**

```bash
mkdir -p packages/lms-courses/src/lms_courses
touch packages/lms-courses/pyproject.toml
```

Declare dependencies in `packages/lms-courses/pyproject.toml`:
```toml
[project]
name = "lms-courses"
dependencies = [
    "lms-protocols",
]

[tool.uv.sources]
lms-protocols = { workspace = true }
```

**2. Move domain logic first.**

Extract aggregates, value objects, and domain events. These have the fewest dependencies:
- `Course` model → `lms_courses/domain/course.py`
- `CourseCode` value object → `lms_courses/domain/value_objects.py`
- `CoursePublished` event → `lms_courses/domain/events.py`

Keep the old locations as thin wrappers temporarily:
```python
# Old location: your_app/models/course.py (DEPRECATED)
from lms_courses.domain.course import Course  # Re-export for backward compat
from lms_courses.domain.events import CoursePublished

# TODO: Remove after migration completes
```

**3. Move application logic next.**

Command handlers, query handlers, and use case orchestration move to `lms_courses/application/`. Wire them to call the new domain code.

**4. Adapt infrastructure last.**

Repository implementations, ORM mappings, and external adapters move to `lms_courses/infrastructure/`. These depend on domain abstractions (the repository protocol), so they can only move after domain is stable.

**5. Redirect imports in the old app.**

Your existing Flask/Django app becomes a thin routing layer:
```python
# app/routes/courses.py
from lms_courses import Course, CoursePublished  # Import from new package
from lms_courses.application.handlers import PublishCourseHandler

@app.route("/courses/<id>/publish", methods=["POST"])
def publish_course(id):
    handler = PublishCourseHandler(course_repo=get_course_repository())
    handler.handle(PublishCourse(course_id=id))
    return {"status": "published"}
```

**6. Run tests for both old and new code.**

During migration, the old tests still run against the old structure, but you're adding new tests in `packages/lms-courses/tests/`. Both must pass. Once all consumers migrate to the new package, delete the old tests and remove the deprecated re-exports.

#### Phase 4: Introduce Verification Tooling (Ongoing)

As you extract bounded contexts, add import-linter contracts incrementally.

**1. Start with simple independence contracts.**

After extracting two bounded contexts (say, `lms-courses` and `lms-enrollment`), add a contract:
```ini
[importlinter]
root_packages =
    lms_protocols
    lms_courses
    lms_enrollment

[importlinter:contract:tier-1-independence]
name = Tier 1 BCs must not import each other
type = independence
modules =
    lms_courses
    lms_enrollment
```

**2. Expand contracts as you extract more contexts.**

Each new bounded context gets added to the independence contract. This prevents new coupling as you extract.

**3. Add layer contracts within bounded contexts.**

Once a bounded context has all three layers (domain, application, infrastructure), enforce ordering:
```ini
[importlinter:contract:courses-layer-ordering]
name = Courses BC DDD layer ordering
type = layers
layers =
    lms_courses.infrastructure
    lms_courses.application
    lms_courses.domain
```

#### Phase 5: Migrate the Composition Root (Final phase)

Once all bounded contexts are extracted, the original app becomes `packages/lms/` (Tier 3). This is the last piece to move because it depends on everything:

**1. Move routing and bootstrapping.**

FastAPI routes, Flask blueprints, or Django URLs move to `lms/api/`. Dependency injection wiring moves to `lms/bootstrap/`.

**2. Wire event subscriptions.**

Cross-BC event subscriptions (e.g., "when CourseCompleted, trigger certificate generation") live in `lms/bootstrap/event_subscriptions.py`. This is the orchestration layer.

**3. Configuration becomes a Tier 3 concern.**

Settings, environment variables, and feature flags move to `lms/config/`. Bounded contexts receive configuration via dependency injection at composition time.

#### Maintaining Backward Compatibility

During migration (often 3-6 months for a production app), you'll run hybrid code:

**1. Facade pattern for the old app.**

Keep the old API stable while the internals shift:
```python
# Old code (preserved for external clients)
from app.services.course_service import CourseService

# New code (internal implementation)
from lms_courses.application.handlers import PublishCourseHandler
```

**2. Dual testing.**

Run existing integration tests against the old API and new unit tests against the extracted bounded contexts. Both must pass.

**3. Feature flags for cutover.**

When a bounded context is ready, use feature flags to route traffic:
```python
if feature_flag("use_new_courses_bc"):
    handler = PublishCourseHandler(...)
else:
    service = CourseService(...)  # Old path
```

**4. Incremental deployment.**

Deploy extracted bounded contexts to production while the old code still runs. Monitor for errors. Gradually increase traffic to new code via feature flags. Only delete old code after 100% cutover.

#### When to Introduce Each Element

| Element | Introduce When... |
|---------|-------------------|
| uv workspace root | Immediately (Phase 1) -- sets up the structure |
| protocols package | Before extracting first BC (Phase 2) -- defines contracts |
| First bounded context | After protocols exist (Phase 3) -- courses or identity |
| import-linter | After 2+ BCs exist (Phase 4) -- enforces boundaries |
| Per-BC `AGENTS.md` | When BC has complex invariants or >3 aggregates |
| Tier 0 (eventsource) | When 3+ BCs need shared event infrastructure |
| Tier 2 (orchestration) | When cross-BC workflows become complex (5+ steps) |
| Composition root (Tier 3) | Final phase -- after all BCs extracted |

#### Timeline for a Typical Production App

**Small app (5K-20K lines, 2-3 developers):**
- Weeks 1-2: Identify 3 bounded contexts, create protocols package
- Weeks 3-6: Extract first BC (courses or identity)
- Weeks 7-10: Extract second BC (enrollment or notifications)
- Weeks 11-14: Extract third BC, add import-linter
- Weeks 15-16: Move composition root, delete deprecated code
- **Total:** 3-4 months

**Medium app (20K-100K lines, 5-10 developers):**
- Month 1: Domain mapping, protocols package, first BC extraction starts
- Months 2-4: Extract 2-3 core BCs, add import-linter contracts
- Months 5-7: Extract 3-5 supporting BCs, per-BC agent context
- Month 8: Composition root migration, feature flag cutover
- **Total:** 6-8 months

**Large app (100K+ lines, 10+ developers):**
- Months 1-2: Domain mapping, architecture specs, protocols package
- Months 3-8: Extract core BCs one at a time (1-2 per month), strict verification
- Months 9-14: Extract supporting BCs, orchestration layer, event subscriptions
- Months 15-18: Composition root migration, gradual cutover, deprecation cleanup
- **Total:** 12-18 months

**Key principle:** Each phase delivers value independently. After extracting the first BC, agent work in that area already benefits from isolation, clear boundaries, and focused context. You don't wait until the entire migration completes to see returns.

---

## 15. Concrete Structure

Putting it all together, here is the recommended project layout for a DDD Python project (using our LMS as the example):

```
lms-project/
├── CLAUDE.md                         # Agent context: project overview, conventions
├── AGENTS.md                         # Cross-tool agent compatibility
├── README.md                         # Human onboarding
├── pyproject.toml                    # Root workspace config
├── uv.lock                           # Shared lockfile
│
├── packages/
│   ├── lms-protocols/                # Tier -1: contracts (stdlib-only)
│   │   ├── pyproject.toml
│   │   ├── README.md                 # Purpose, consumers, stability
│   │   ├── AGENTS.md                 # Protocol-specific agent context
│   │   ├── src/
│   │   │   └── lms_protocols/
│   │   │       ├── __init__.py       # Public API
│   │   │       ├── courses.py        # Course protocol definitions
│   │   │       ├── enrollment.py     # Enrollment protocol definitions
│   │   │       ├── assessment.py     # Assessment protocol definitions
│   │   │       ├── xapi.py           # xAPI LRS protocol definitions
│   │   │       ├── events.py         # Event base types
│   │   │       └── commands.py       # Command base types
│   │   └── tests/
│   │       └── test_conformance.py   # Protocol conformance suites
│   │
│   ├── lms-eventsource/              # Tier 0: event store seedwork
│   │   ├── pyproject.toml
│   │   ├── README.md
│   │   ├── src/
│   │   │   └── lms_eventsource/
│   │   │       ├── __init__.py
│   │   │       ├── domain/
│   │   │       │   ├── aggregate.py  # AggregateRoot base
│   │   │       │   ├── events.py     # DomainEventBase
│   │   │       │   └── repository.py # Repository protocol
│   │   │       ├── application/
│   │   │       │   └── projections.py
│   │   │       └── infrastructure/
│   │   │           ├── eventstore_repository.py
│   │   │           └── migrations/   # EventStoreDB-related migrations
│   │   └── tests/
│   │
│   ├── lms-courses/                  # Tier 1: course catalog & curriculum
│   │   ├── pyproject.toml            # Depends on: protocols, eventsource
│   │   ├── README.md
│   │   ├── AGENTS.md                 # Course-specific invariants
│   │   ├── src/
│   │   │   └── lms_courses/
│   │   │       ├── __init__.py       # Exports: Course, Prerequisite, etc.
│   │   │       ├── py.typed          # PEP 561 marker: enables type checking for consumers
│   │   │       ├── domain/
│   │   │       │   ├── course.py     # Course aggregate
│   │   │       │   ├── prerequisite.py
│   │   │       │   ├── events.py     # CoursePublished, etc.
│   │   │       │   ├── commands.py
│   │   │       │   ├── value_objects.py  # CourseCode, Duration
│   │   │       │   └── repository.py
│   │   │       ├── application/
│   │   │       │   ├── handlers.py
│   │   │       │   └── queries.py
│   │   │       └── infrastructure/
│   │   │           ├── postgres_course_repository.py
│   │   │           └── migrations/   # Course-specific database migrations
│   │   └── tests/
│   │       ├── domain/
│   │       ├── application/
│   │       └── infrastructure/
│   │
│   ├── lms-enrollment/               # Tier 1: registration, cohorts, waitlists
│   │   ├── pyproject.toml
│   │   ├── README.md
│   │   ├── AGENTS.md                 # Waitlist FIFO invariants, etc.
│   │   ├── src/
│   │   │   └── lms_enrollment/
│   │   │       ├── domain/
│   │   │       │   ├── enrollment.py
│   │   │       │   ├── waitlist.py   # Waitlist aggregate
│   │   │       │   ├── cohort.py
│   │   │       │   └── events.py
│   │   │       ├── application/
│   │   │       └── infrastructure/
│   │   └── tests/
│   │
│   ├── lms-content/                  # Tier 1: lessons, media, SCORM
│   │   ├── src/
│   │   │   └── lms_content/
│   │   │       ├── domain/
│   │   │       │   ├── lesson.py
│   │   │       │   └── adaptive_path.py
│   │   │       └── infrastructure/
│   │   │           ├── scorm/        # SCORM-specific adapter
│   │   │           └── media_storage.py
│   │   └── ...
│   │
│   ├── lms-assessment/               # Tier 1: quizzes, assignments, grading
│   │   ├── src/
│   │   │   └── lms_assessment/
│   │   │       ├── domain/
│   │   │       │   ├── quiz.py
│   │   │       │   ├── assignment.py
│   │   │       │   ├── grading/      # Complex rubric logic
│   │   │       │   │   ├── rubric.py
│   │   │       │   │   └── auto_grader.py
│   │   │       │   └── events.py
│   │   │       └── ...
│   │   └── ...
│   │
│   ├── lms-records/                  # Tier 1: xAPI LRS
│   │   ├── README.md                 # xAPI 1.0.3 compliance notes
│   │   ├── AGENTS.md                 # Statement immutability rules
│   │   ├── src/
│   │   │   └── lms_records/
│   │   │       ├── domain/
│   │   │       │   ├── statement.py  # xAPI Statement aggregate
│   │   │       │   └── xapi/         # xAPI validation
│   │   │       │       └── validator.py
│   │   │       ├── application/
│   │   │       │   ├── lrs_query.py  # LRS query handlers
│   │   │       │   └── statement_handler.py
│   │   │       └── infrastructure/
│   │   │           └── eventstore_lrs.py
│   │   └── tests/
│   │       └── xapi/
│   │           └── test_compliance.py  # xAPI spec conformance
│   │
│   ├── lms-analytics/                # Tier 1: dashboards, outcomes
│   ├── lms-identity/                 # Tier 1: users, roles, OAuth
│   ├── lms-notifications/            # Tier 1: email, push, alerts
│   │
│   ├── lms-orchestration/            # Tier 2: workflow coordination
│   │   ├── pyproject.toml            # Depends on: all Tier 0-1
│   │   ├── src/
│   │   │   └── lms_orchestration/
│   │   │       ├── workflows/
│   │   │       │   ├── course_completion.py  # Completion → certificate
│   │   │       │   └── waitlist_promotion.py
│   │   │       └── handlers/
│   │   └── ...
│   │
│   └── lms/                          # Tier 3: composition root
│       ├── pyproject.toml            # Depends on: everything
│       ├── src/
│       │   └── lms/
│       │       ├── __init__.py
│       │       ├── api/              # FastAPI routes, grouped by BC
│       │       │   ├── courses.py
│       │       │   ├── enrollment.py
│       │       │   └── lrs.py        # xAPI LRS API endpoints
│       │       ├── cli/              # Click commands
│       │       ├── bootstrap/        # DI container, subscription wiring
│       │       │   ├── container.py
│       │       │   └── event_subscriptions.py
│       │       └── config/           # pydantic-settings
│       └── tests/
│           └── integration/          # End-to-end tests
│
├── docs/
│   ├── architecture/                 # Architecture specs (explanation)
│   │   ├── INDEX.md                  # Map of content
│   │   ├── DECISIONS.md              # ADR-001 through ADR-NNN
│   │   ├── GLOSSARY.md               # Ubiquitous language
│   │   ├── event-flows.md            # Cross-BC event workflows
│   │   ├── courses.md                # Courses BC spec
│   │   ├── enrollment.md             # Enrollment BC spec
│   │   ├── lrs-xapi.md               # LRS BC spec + xAPI compliance
│   │   └── ...
│   ├── tutorials/                    # Learning-oriented
│   │   ├── getting-started.md
│   │   └── first-course.md
│   ├── how-to/                       # Task-oriented
│   │   ├── scorm-integration.md
│   │   └── sso-setup.md
│   ├── reference/                    # Information-oriented
│   │   ├── api/
│   │   ├── events.md                 # Full event catalog
│   │   └── xapi-statement-schema.md
│   └── how_to_organize_a_python_project.md  # This document
│
├── .importlinter                     # Boundary enforcement config
└── .claude/
    ├── rules/                        # Topic-specific agent instructions
    │   ├── commits.md
    │   ├── testing.md
    │   └── boundaries.md             # Always/Ask/Never rules
    └── agents/                       # Agent role definitions
```

---

## 16. Sources

### Library Science & Classification
- Ranganathan, S.R. *Five Laws of Library Science* (1931). [Wikipedia](https://en.wikipedia.org/wiki/Five_laws_of_library_science)
- Ranganathan, S.R. *Colon Classification* (faceted classification). [Wikipedia](https://en.wikipedia.org/wiki/Colon_classification)
- Library of Congress. *Controlled Vocabularies*. [loc.gov](https://www.loc.gov/librarians/controlled-vocabularies/)
- Glushko, R. *The Discipline of Organizing* (2013)

### Information Architecture & Knowledge Management
- Pirolli, P. & Card, S. *Information Foraging Theory* (Xerox PARC). [NNGroup](https://www.nngroup.com/articles/information-foraging/)
- Russell, Stefik, Pirolli & Card. *The Cost Structure of Sensemaking* (knowledge crystallization). [ACM](https://dl.acm.org/doi/10.1145/169059.169209)
- Morville, P. *Ambient Findability* (multiple access paths). [Semantic Studios](https://semanticstudios.com/)
- Wurman, R.S. *LATCH framework* (Location, Alphabet, Time, Category, Hierarchy). [Visual Communication Guy](https://thevisualcommunicationguy.com/2013/07/20/the-five-and-only-five-ways-to-orgaize-information/)
- Nonaka, I. & Takeuchi, H. *SECI model* (knowledge externalization). [Wikipedia](https://en.wikipedia.org/wiki/SECI_model_of_knowledge_dimensions)
- Lynch, K. *The Image of the City* (wayfinding). [Wikipedia](https://en.wikipedia.org/wiki/Kevin_A._Lynch)
- Norman, D. *The Design of Everyday Things* (affordances)
- Scott, J.C. *Seeing Like a State* (legibility vs metis). [Wikipedia](https://en.wikipedia.org/wiki/Seeing_Like_a_State)
- Star, S.L. & Griesemer, J.R. *Boundary Objects* (1989). [Wikipedia](https://en.wikipedia.org/wiki/Boundary_object)
- FAIR Data Principles. [go-fair.org](https://www.go-fair.org/fair-principles/)

### Knowledge Organization Patterns
- Ahrens, S. *How to Take Smart Notes* (Zettelkasten). [Zettelkasten.de](https://zettelkasten.de/introduction/)
- Forte, T. *PARA Method*. [Forte Labs](https://fortelabs.com/blog/para/)
- Milo, N. *Maps of Content*. [Linking Your Thinking](https://www.linkingyourthinking.com/)
- Johnny Decimal. [johnnydecimal.com](https://johnnydecimal.com/)
- Procida, D. *Diataxis documentation framework*. [diataxis.fr](https://diataxis.fr/)

### Software Architecture
- Martin, R.C. *Screaming Architecture*. [Clean Coder Blog](https://blog.cleancoder.com/uncle-bob/2011/09/30/Screaming-Architecture.html)
- Millett, S. & Tune, N. *Patterns, Principles, and Practices of Domain-Driven Design* (2015)
- Feature-Sliced Design. [feature-sliced.design](https://feature-sliced.design/)
- Kraken Technologies. import-linter. [GitHub](https://github.com/seddonym/import-linter)

### AI Agent Patterns
- Anthropic. *Effective Context Engineering for AI Agents*. [anthropic.com](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)
- Anthropic. *Eight Trends Defining How Software Gets Built in 2026*. [claude.com/blog](https://claude.com/blog/eight-trends-defining-how-software-gets-built-in-2026)
- Osmani, A. on scaling frontend architecture. [addyosmani.com](https://addyosmani.com/blog/good-spec/)
- Thoughtworks. *Spec-Driven Development*. [thoughtworks.com](https://www.thoughtworks.com/en-us/insights/blog/agile-engineering-practices/spec-driven-development-unpacking-2025-new-engineering-practices)
- AGENTS.md Standard. [agents.md](https://agents.md/)
- GitHub Spec Kit. [github.com/github/spec-kit](https://github.com/github/spec-kit)
- Aider. *Repository Map*. [aider.chat](https://aider.chat/docs/repomap.html)
- arXiv. *Agent READMEs Empirical Study*. [arxiv.org](https://arxiv.org/html/2511.12884v1)

### Python Packaging
- Python Packaging User Guide. *src layout*. [packaging.python.org](https://packaging.python.org/)
- uv. *Workspaces*. [docs.astral.sh](https://docs.astral.sh/uv/concepts/workspaces/)
