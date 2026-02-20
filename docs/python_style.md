# MergeXO Python Style Guide

## 1. Tooling defaults

1. Type checker: `ty`
   - All production code must be fully type-annotated.
   - New modules should pass `ty` checks before merge.
2. Package and environment manager: `uv`
   - Use `uv` for dependency management, locking, and running tools/tests.
3. Formatter: `ruff`
   - Use `ruff format` as the canonical code formatter.

## 2. Typing policy

1. Prefer precise types over `Any`.
2. Use explicit return types on public functions and methods.
3. Keep domain models strongly typed (for example with typed IDs and enums where useful).
4. Treat type checking as a required quality gate in CI.

## 3. Testing policy (property-based first)

1. Prefer property-based tests using `hypothesis` for core logic.
2. Use example-based tests mainly for:
   - regressions,
   - integration boundaries,
   - user-visible scenarios.
3. For state machines and schedulers, define invariants first, then encode them as properties.

Example invariants:

1. Scheduler never assigns more than `N` concurrent workers.
2. A processed event is never applied twice.
3. Closed or merged PRs never transition back to `running`.

## 4. Immutability policy

1. Prefer immutable data structures from `pyrsistent` (`PMap`, `PVector`, `PSet`, `PClass`).
2. Model state transitions as pure functions:
   - input state + event -> new state + effects.
3. Avoid in-place mutation of shared state in orchestrator logic.
4. If mutation is required for performance, isolate it and document why.

## 5. Practical coding rules

1. Keep side effects at boundaries (GitHub API, git subprocesses, filesystem IO).
2. Keep core decision logic deterministic and easy to replay in tests.
3. Prefer small, composable functions and explicit data flow.
4. Favor total functions over exception-driven control flow where practical.

## 6. Suggested dependency baseline

1. Runtime:
   - `pyrsistent`
2. Dev/test:
   - `hypothesis`
   - `ty`
   - `ruff`
   - `pytest`

## 7. Development workflow

1. Add/update dependencies with `uv`.
2. Format code with `uv run ruff format`.
3. Run tests with `uv run pytest`.
4. Run type checks with `uv run ty`.
5. Only merge when formatter, tests, and type checks pass.
