# Repository Guidelines

## Project Structure & Module Organization

Application code lives in `src/feature_store/`. Keep service entry points (`api.py`,
`worker.py`, and `streaming.py`) thin; put registry, retrieval, and storage behavior in their
dedicated modules. Tests are under `tests/` and mirror public behavior rather than file layout.
The fraud example and its feature definitions live in `examples/fraud/`. Database migrations
belong in `migrations/versions/`, while local infrastructure is defined in `compose.yaml`.

## Build, Test, and Development Commands

- `uv sync --extra dev`: create the environment and install locked development dependencies.
- `uv run feature-store --help`: inspect CLI operations for registry, backfill, and retrieval.
- `cp .env.example .env && docker compose up --build -d`: start the complete local stack.
- `uv run feature-store demo`: seed fraud data, register features, and enqueue a backfill.
- `uv run pytest`: run the test suite with coverage reporting.
- `uv run ruff check .` and `uv run mypy src`: run linting and strict type checks.
- `uv build`: produce the wheel and source distribution in `dist/`.

## Coding Style & Naming Conventions

Use Python 3.12+, four-space indentation, type annotations, and a 100-character line limit.
Ruff enforces formatting, import order, modern Python syntax, and common bug patterns. Use
`snake_case` for modules, functions, feature names, and registry objects; use `PascalCase` for
classes. Feature references must be pinned as `view_name@1.0.0:feature_name`. Keep timestamps
timezone-aware and normalize them to UTC at system boundaries.

## Testing Guidelines

Tests use pytest and should be named `test_<behavior>.py` with functions named
`test_<expected_behavior>`. Add regression tests for point-in-time leakage, TTL boundaries,
version conflicts, replay safety, and job recovery when changing those areas. Prefer local
SQLite and Delta fixtures; reserve Docker-dependent scenarios for explicit integration tests.
No fixed coverage threshold is enforced, but new logic should cover success and failure paths.

## Commit & Pull Request Guidelines

Follow Conventional Commits, as in `feat: build distributed feature store MVP`. Keep commits
focused and run Ruff, mypy, pytest, and `git diff --check` before committing. Pull requests
should explain the behavior change, note schema or migration effects, link relevant issues, and
include exact verification commands. Add screenshots only for rendered documentation or future
UI changes.

## Security & Configuration

Never commit `.env`, credentials, generated data, or object-store contents. Update
`.env.example` when adding configuration. Development credentials in Compose are local-only and
must not be reused in deployed environments.
