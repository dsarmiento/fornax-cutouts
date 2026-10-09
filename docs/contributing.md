---
title: Contributing
description: How to contribute to Fornax Cutouts
---

# Contributing

Thank you for helping improve Fornax Cutouts. This guide covers local development and the conventions we use so changes integrate cleanly with review, CI, and releases.

## Development setup

1. Fork and clone the repository.
2. Install dependencies and hooks:

```bash
uv sync
uv run pre-commit install
```

3. Run checks before pushing:

```bash
uv run pre-commit run --all-files
uv run pytest
```

See [Getting Started](getting-started.md) for running the API and workers locally.

## Pull requests

- Open a PR against `main` with a clear description of the problem and solution.
- Keep changes focused; link related [issues](https://github.com/nasa-fornax/fornax-cutouts/issues) when applicable.
- Ensure **Lint and Format** and **Test** workflows pass on the PR.
- Request review from maintainers when ready.

## Commit messages and PR titles

This project uses **squash merges** on `main`. The **pull request title** becomes the commit message on `main`, so write PR titles in [Conventional Commits](https://www.conventionalcommits.org/) form:

```text
<type>[optional scope]: <description>
```

Examples:

- `feat: add rolling window limit configuration`
- `fix(api): return 404 when job ID is unknown`
- `docs: clarify Redis environment variables`

The [PR title check](https://github.com/nasa-fornax/fornax-cutouts/blob/main/.github/workflows/pr-title.yml) validates titles on pull requests. Ask in the PR if you are unsure which type fits.

### Types and when to use them

| Type       | Use for                                      |
| ---------- | -------------------------------------------- |
| `feat`     | New user-facing behavior or API capability   |
| `fix`      | Bug fixes                                    |
| `perf`     | Performance improvements without API changes |
| `docs`     | Documentation only                           |
| `test`     | Tests only                                   |
| `refactor` | Code structure without behavior change       |
| `chore`    | Tooling, deps, or maintenance                |
| `ci`       | CI/CD workflow changes                       |

Optional **scope** in parentheses (for example `fix(worker):`) helps changelog readers; keep scopes short and consistent.

### Breaking changes

For changes that break existing behavior or public APIs, use a `feat` or `fix` title and include a `BREAKING CHANGE:` paragraph in the PR description (footer-style), describing what changed and how to migrate. See the [Conventional Commits specification](https://www.conventionalcommits.org/en/v1.0.0/#commit-message-with-description-and-breaking-change-footer).

## Versioning and changelog

Package versions follow [Semantic Versioning](https://semver.org/) (`MAJOR.MINOR.PATCH`). While the project is pre-1.0, releases stay on **0.x** and version bumps are derived from merged PR titles via [semantic-release](https://github.com/semantic-release/semantic-release):

| PR title prefix                                | Version bump (0.x)                       |
| ---------------------------------------------- | ---------------------------------------- |
| `feat:`                                        | Minor — `0.(x+1).0`                      |
| `fix:`, `perf:`                                | Patch — `0.x.(y+1)`                      |
| `docs:`, `chore:`, `ci:`, `test:`, `refactor:` | No new release                           |
| `BREAKING CHANGE:` in PR body                  | Treated as a minor bump on 0.x until 1.0 |

User-facing changes appear in [CHANGELOG.md](https://github.com/nasa-fornax/fornax-cutouts/blob/main/CHANGELOG.md) when maintainers cut a release. You do not need to edit the changelog in your PR; a clear conventional title is enough.

## Code style

- Python 3.12+ with type hints on public APIs and Pydantic models.
- [Ruff](https://docs.astral.sh/ruff/) for lint and format (line length 120, double quotes).
- Match existing patterns in the module you are changing.

## Questions

Use [GitHub Issues](https://github.com/nasa-fornax/fornax-cutouts/issues) for bugs and feature discussion, or comment on your PR for implementation questions.
