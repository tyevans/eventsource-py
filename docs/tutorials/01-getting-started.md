# Getting Started with eventsource-py

Welcome to eventsource-py! This tutorial guides you through setting up your environment and running your first autonomous feature cycle.

## Prerequisites
- Python 3.11+
- UV package manager (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Git

## Step 1: Install Dependencies
```bash
uv sync
```

## Step 2: Verify System Health
Run the SpecOps invariant scanner:
```bash
uv run spec-ops health
```
