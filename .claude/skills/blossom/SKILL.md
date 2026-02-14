# Blossom Skill

Spike-driven exploration workflow for discovering solutions before committing to implementation.

## Usage

```
/blossom <goal>
```

## Workflow

### Phase 1: Understand

Gather context about the goal:
- What modules/files are involved?
- What interfaces/protocols constrain the solution?
- What tests already exist?

### Phase 2: Spike

Create a minimal throwaway spike to test the approach:
- Write the smallest possible code that validates the idea
- Run it (tests, REPL, or script)
- Capture what you learned

### Phase 3: Evaluate

Assess the spike:
- Does it work? What broke?
- Does it fit the existing architecture patterns?
- What edge cases exist?
- Is the approach worth pursuing?

### Phase 4: Propose

Present findings:
- What worked, what didn't
- Recommended approach with rationale
- Estimated scope (files to change, tests to write)
- Any risks or open questions

## Spike Areas for This Project

- **Event Store backends**: New storage backend implementations
- **Bus integrations**: New event bus implementations (message brokers)
- **Projection patterns**: New projection types or processing strategies
- **Serialization**: Event versioning, schema evolution
- **Multi-tenancy**: Tenant isolation strategies
- **Observability**: Tracing, metrics, logging patterns
- **Migration tooling**: Event store migration utilities

## Rules

- Spikes are throwaway -- never commit spike code directly
- Always run existing tests after spiking to verify nothing broke
- If the spike touches a protocol/interface, check all implementations
