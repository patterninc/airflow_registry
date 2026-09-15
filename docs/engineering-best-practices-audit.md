# Engineering Best Practices Audit — airflow_registry

| | |
|---|---|
| **Audit date** | 2026-09-15 |
| **Auditor** | Claude — gauge-repo skill |
| **Rubric version** | `item-credit-v1` — 2026-09-04 (`references/best-practices.md`) |

## Repo profile

`airflow_registry` is a small Python library (~600 lines across 7 modules) of custom Apache Airflow operators, hooks, and utilities (`S3ToPostgresOperator`, `SnowflakeToS3Operator`, `SodaCheckOperator`, Slack failure alerts, Soda check helpers) shared across Pattern's Airflow projects. It is distributed as a pip package installed directly from `git+https://github.com/patterninc/airflow_registry@main` — there is no deployment, no owned database, no service API, and no UI surface. Runtime dependencies are two (`apache-airflow-providers-slack`, `soda-core-snowflake`) declared with `>=` ranges in `setup.py`; there is no lockfile and only one release tag (`v1.0`). The repo has no CI workflows, no test suite, and no lint/format/type-check configuration. History shows ~10 contributors with active development in 2022–early 2023, then dormancy until a 2026 Backstage onboarding commit; `backstage.yaml` records ownership by `dev-data-acquisition` (DATA cost center). The GitHub owner is verified as `patterninc` via `gh repo view` (`patterninc/airflow_registry`), so Pattern's inherited Wiz and Toolsmith controls apply. There is no AWS deploy footprint in this repo (AWS access happens via connections configured in consuming Airflow instances), so item 49's `aws[]` sub-check would not be required.

## Scorecard

| Metric | Value |
|--------|-------|
| **Critical gates** | **RED** |
| **Adjusted compliance** | **22.2%** |

Critical gates are RED: five applicable gates are Gaps — AGENTS.md (2), required CI checks (16), unit tests (23), integration tests (24), and reproducible builds (48). Adjusted compliance is calculated independently:

`(6 Met + 0.5 × 0 Partial) / (49 total − 22 justified N/A) = 6 / 27 = 22.2%`

### Status totals

| Status | Items |
|--------|------:|
| Met | 6 |
| Partial | 0 |
| Gap | 21 |
| N/A | 22 |
| **Total** | **49** |

### Per-category breakdown

| Category | Met | Partial | Gap | N/A |
|----------|----:|--------:|----:|----:|
| Documentation & Context | 1 | 0 | 5 | 3 |
| Guardrails & Enforcement | 3 | 0 | 7 | 3 |
| Testing & Feedback Loops | 0 | 0 | 5 | 8 |
| Environment & Tooling | 2 | 0 | 3 | 8 |
| Agent dispatch | 0 | 0 | 1 | 0 |
| **Total** | **6** | **0** | **21** | **22** |

## Documentation & Context

| # | Practice | Status | Evidence | Recommendation / rationale |
|---|----------|--------|----------|----------------------------|
| 1 | Skills / reusable prompt workflows | **Gap** | No `.claude/skills/`, `.claude/commands/`, or equivalent | Add skills for the recurring tasks: adding a new operator, cutting a release, updating consumer install docs. |
| 2 | AGENTS.md | **Gap** | No `AGENTS.md`, `CLAUDE.md`, or `.cursorrules` | Add `AGENTS.md` covering package layout, operator/hook conventions, how to install editable (`pip install -e .`), how to run tests once they exist, and the consumer-facing compatibility constraint (consumers install from `@main`). |
| 3 | Architecture decision records | **Gap** | No `docs/adr/` or design docs | Record the key decisions (why a shared registry vs per-repo operators; why consumers install from `@main`; hook wrapping strategy) in `docs/adr/`. |
| 4 | Runbooks | **Not applicable** | No deployed service | Library consumed directly from `@main`; there are no deploy/rollback/key-rotation operations. Consumer-facing setup steps live in `README.md` and `airflow_registry/utils/README.md`. |
| 5 | API contract docs | **Not applicable** | No wire API | Python library; its contract is the import surface, documented via operator docstrings (`airflow_registry/operators/s3ToPostgresOperator.py`). |
| 6 | README with setup & run instructions | **Met** | `README.md` covers purpose, 3-step install, and a worked DAG example; `airflow_registry/utils/README.md` documents Slack alert setup | Consider adding a short contributor section (editable install, test command) once tooling exists. |
| 7 | Changelog with migration notes | **Gap** | Only tag is `v1.0`; no `CHANGELOG.md` | Consumers install from `@main`, so every merge ships instantly and silently. Add a `CHANGELOG.md` with migration notes, and pair it with tagged releases (see item 48). |
| 8 | On-call playbooks | **Not applicable** | No deployed service | Failures surface as task failures inside consuming Airflow deployments, which own their incident response. |
| 9 | CODEOWNERS | **Gap** | No `.github/CODEOWNERS`; `backstage.yaml` names `dev-data-acquisition` as owner | Add `CODEOWNERS` mapping `*` to the dev-data-acquisition team so shared-library changes auto-assign the owning team for review. |

## Guardrails & Enforcement

| # | Practice | Status | Evidence | Recommendation / rationale |
|---|----------|--------|----------|----------------------------|
| 10 | Linters | **Gap** | No ruff/flake8 config | Adopt `ruff` with a `pyproject.toml` config; run it in CI (item 16). |
| 11 | Formatters | **Gap** | No Black/ruff-format config; mixed naming styles (`postgresCustomHook.py` camelCase filenames) | Adopt `ruff format` (or Black) and format the codebase once. |
| 12 | Type checking | **Gap** | Partial type hints exist (`from __future__ import annotations`, `TYPE_CHECKING` blocks in operators) but no mypy/pyright config or CI check | Add `mypy` with a lenient baseline config and run it in CI. |
| 13 | Pre-commit hooks | **Gap** | No `.pre-commit-config.yaml` | Add pre-commit running ruff lint + format; document `pre-commit install` in the README/AGENTS.md. |
| 14 | Commit message conventions | **Gap** | Ad-hoc history (`git log`); no commitlint or convention doc | Adopt Conventional Commits — it enables changelog automation (item 7) for a package where every merge reaches consumers. |
| 15 | Branch protection rules | **Met** | Org-level active ruleset `require-pr-review` (source: `patterninc` organization) on the default branch: blocks deletion and force-push, requires pull requests | — |
| 16 | Required CI checks before merge | **Gap** | No `.github/workflows/`; the only workflow is the org-injected Copilot PR reviewer | Add a GitHub Actions workflow (lint + type-check + tests) and require it via the ruleset. Nothing currently prevents merging code that doesn't even import. |
| 17 | Dependency allow/deny lists | **Not applicable** | `setup.py` declares exactly two runtime dependencies | At this dependency scale, PR review by the owning team covers new-dependency judgment; a formal allow/deny policy would be ceremony with no value. |
| 18 | License compliance scanning | **Gap** | No license-check job; MIT library (`license.md`) with Apache-2.0 dependencies | Add a lightweight license check (e.g. `pip-licenses` in CI) — the repo is public and MIT-licensed, so a viral-license dependency added later would matter. |
| 19 | Secret scanning | **Met** | Inherited Pattern Wiz policy (owner `patterninc` verified via `gh repo view`) | — |
| 20 | SAST / static analysis gates | **Met** | Inherited Pattern Wiz policy (owner verified) | — |
| 21 | Max complexity limits | **Not applicable** | ~600 lines across 7 small modules; largest file is 125 lines | An enforced complexity ceiling would never bind at this size; adopting a linter (item 10) is the leverage point. |
| 22 | Import boundary enforcement | **Not applicable** | Single flat package (`hooks/`, `operators/`, `utils/`) with one intentional direction (operators import hooks) | No architectural layers to protect. |

## Testing & Feedback Loops

| # | Practice | Status | Evidence | Recommendation / rationale |
|---|----------|--------|----------|----------------------------|
| 23 | Unit tests | **Gap** | No test directory or test files anywhere in the repo | Add `pytest` unit tests for the pure logic: SQL/COPY statement construction in `S3ToPostgresOperator`, Soda check parsing in `soda_check_function.py`, Slack message formatting in `slack_notifications.py`. |
| 24 | Integration tests | **Gap** | None | Add integration tests for `PostgresCustomHook`/`S3CustomHook` against containers (Postgres + LocalStack/minio); mark them so they can run separately from unit tests. |
| 25 | Snapshot / golden-file tests | **Not applicable** | No rendered artifacts | The only generated text is SQL strings, which unit-test assertions (item 23) cover more precisely than golden files. |
| 26 | Contract tests | **Not applicable** | No service API | The library's contract is its Python import surface; no consumer-provider wire contract exists. |
| 27 | End-to-end tests | **Not applicable** | No UI; headless library | Consuming Airflow projects own DAG-level end-to-end validation. |
| 28 | Visual regression tests | **Not applicable** | No visual surface | — |
| 29 | Test coverage thresholds | **Gap** | No tests, no coverage tooling | Once the unit suite (item 23) exists, add `pytest --cov` with a modest enforced floor in CI. |
| 30 | Mutation testing | **Not applicable** | ~600-line dormant utility library | Mutation testing's cost/benefit is aimed at large, actively-evolving suites; it adds no value at this scale even with tests in place. |
| 31 | Load / performance benchmarks | **Not applicable** | Thin wrappers around Airflow hooks | Throughput is owned by the underlying systems (Postgres COPY, S3, Snowflake), not this glue code. |
| 32 | Flaky test quarantine | **Not applicable** | No test suite; single owning team | Quarantine machinery exists to keep large suites from blocking many teams; not needed at this repo's scale. |
| 33 | Structured CI output | **Gap** | No CI | When adding CI (item 16), emit JUnit XML from pytest so failures are machine-parseable. |
| 34 | Deterministic test fixtures | **Gap** | No fixtures | Build the unit/integration suites (items 23–24) on fixed fixture data (sample S3 keys, canned Soda scan results) from the start. |
| 35 | Smoke tests for deploys | **Not applicable** | Nothing is deployed | Library installed from git by consumers; there is no deploy step to smoke-test. |

## Environment & Tooling

| # | Practice | Status | Evidence | Recommendation / rationale |
|---|----------|--------|----------|----------------------------|
| 36 | Devcontainer config | **Gap** | No `.devcontainer/` | Airflow dev environments are notoriously fiddly; a devcontainer pinning Python + Airflow versions would make contributions (human or agent) reproducible. |
| 37 | One-command setup | **Gap** | No `Makefile`/`justfile`; setup is manual `pip install` | Add a `Makefile` with `make dev` (venv + editable install + dev deps) and `make test`. |
| 38 | Seed scripts for local databases | **Not applicable** | No owned database | Operators write to consumer-owned tables; any test data belongs to the test fixtures (item 34). |
| 39 | MCP servers for external tools | **Met** | Toolsmith-managed MCP access (inherited; owner verified `patterninc`) | — |
| 40 | Scoped secrets per environment | **Not applicable** | Repo holds no credentials and deploys nothing | Connections/webhooks are configured inside consuming Airflow instances; `airflow_registry/utils/README.md` explicitly directs webhook credentials into Airflow connections rather than this repo. |
| 41 | Preview environments per PR | **Not applicable** | Nothing to deploy | — |
| 42 | Hot-reload / watch mode | **Not applicable** | Library with no app process | `pip install -e .` gives live-edit iteration; there is no server to reload. |
| 43 | Structured logging (JSON) | **Not applicable** | Code logs via stdlib `logging` (e.g. `s3ToPostgresOperator.py`) | Log formatting and aggregation are owned by the consuming Airflow deployment; a library should not configure formatters. |
| 44 | Observable traces and metrics | **Not applicable** | No runtime service | Task-level observability belongs to the consuming Airflow instances and their Datadog integration. |
| 45 | Feature flags with local overrides | **Not applicable** | Library; behavior is selected by constructor parameters | No runtime service in which to toggle flags. |
| 46 | Database migration tooling | **Not applicable** | No owned schema | Target tables/schemas are owned and versioned by consuming projects. |
| 47 | Dependency update automation | **Met** | Org-wide Wiz (verified Pattern repo) | — |
| 48 | Reproducible builds (lockfiles) | **Gap** | `setup.py` uses open `>=` ranges; no lockfile; README instructs consumers to install from `@main` | Pin or constrain dependencies, add a lockfile (or `constraints.txt`), and move consumers from `@main` to tagged releases (`@v1.x`) so a merge cannot silently change every production pipeline. |

## Agent dispatch

| # | Practice | Status | Evidence | Recommendation / rationale |
|---|----------|--------|----------|----------------------------|
| 49 | Agent-dispatch manifest | **Gap** | No `.agents/pattern-agents.json` | Add the manifest with `schema_version`, `github.repo: patterninc/airflow_registry`, the DATA team's ClickUp list id, Slack channel, and skills plugins. No `aws[]` needed — the repo has no AWS deploy footprint. |

## Prioritized recommendations

1. **[S] Gap — reproducible builds (48):** Pin/constrain dependencies, add a lockfile, and publish tagged releases so consumers stop installing from `@main`; highest blast radius — today any merge instantly reaches every consuming Airflow pipeline.
2. **[S] Gap — AGENTS.md (2):** Write `AGENTS.md` with package conventions, editable-install setup, test command, and the consumers-install-from-main compatibility constraint.
3. **[M] Gap — required CI checks (16):** Add a GitHub Actions workflow (ruff, mypy, pytest, package import check) and require it to pass via the branch ruleset.
4. **[M] Gap — unit tests (23):** Add a pytest suite covering SQL construction, Soda check parsing, and Slack message formatting.
5. **[L] Gap — integration tests (24):** Test the Postgres and S3 hooks against containerized Postgres and LocalStack/minio in a separately-markable suite.
6. **[S] Gap — agent-dispatch manifest (49):** Add `.agents/pattern-agents.json` with GitHub, ClickUp, Slack, and skills metadata for the DATA team.
7. **[S] Gap — linter (10):** Adopt ruff via `pyproject.toml` and wire it into CI.
8. **[S] Gap — formatter (11):** Adopt ruff format (or Black); one-time reformat of the codebase.
9. **[S] Gap — pre-commit hooks (13):** Add `.pre-commit-config.yaml` running lint + format.
10. **[M] Gap — type checking (12):** Add mypy with a lenient baseline; the operators already carry partial type hints.
11. **[S] Gap — CODEOWNERS (9):** Map `*` to the dev-data-acquisition team per `backstage.yaml` ownership.
12. **[S] Gap — changelog (7):** Add `CHANGELOG.md` with migration notes, paired with tagged releases.
13. **[S] Gap — one-command setup (37):** Add a `Makefile` with `make dev` and `make test`.
14. **[S] Gap — structured CI output (33):** Emit JUnit XML from pytest in the CI workflow.
15. **[S] Gap — deterministic fixtures (34):** Build the new test suites on fixed fixture data from the start.
16. **[S] Gap — coverage thresholds (29):** Enforce a modest `pytest --cov` floor once the unit suite lands.
17. **[S] Gap — commit conventions (14):** Adopt Conventional Commits to feed changelog automation.
18. **[S] Gap — license scanning (18):** Add a `pip-licenses` (or equivalent) CI check for this public MIT-licensed package.
19. **[S] Gap — ADRs (3):** Record the shared-registry and distribution-model decisions in `docs/adr/`.
20. **[M] Gap — devcontainer (36):** Pin Python + Airflow versions in a `.devcontainer/` for reproducible contributor environments.
21. **[M] Gap — skills (1):** Add reusable skills for adding operators and cutting releases.

## Declined practices

| # | Practice | Rationale |
|---|----------|-----------|
| 4 | Runbooks | No deployed service; no deploy/rollback/rotation operations exist. Consumer setup steps are documented in the READMEs. |
| 5 | API contract docs | No wire API — the contract is the Python import surface, documented in operator docstrings. |
| 8 | On-call playbooks | Failures surface inside consuming Airflow deployments, which own incident response. |
| 17 | Dependency allow/deny lists | Two runtime dependencies; PR review by the owning team covers new-dependency judgment at this scale. |
| 21 | Max complexity limits | ~600-line codebase; a complexity ceiling would never bind. |
| 22 | Import boundary enforcement | Single flat package with one intentional import direction; no layers to protect. |
| 25 | Snapshot / golden-file tests | Only generated text is SQL strings, better covered by unit assertions. |
| 26 | Contract tests | No consumer-provider wire contract exists. |
| 27 | End-to-end tests | No UI; DAG-level validation belongs to consuming projects. |
| 28 | Visual regression tests | No visual surface. |
| 30 | Mutation testing | Aimed at large, actively-evolving suites; no value at ~600 dormant lines. |
| 31 | Load / performance benchmarks | Thin glue over Postgres COPY / S3 / Snowflake; performance is owned by those systems. |
| 32 | Flaky test quarantine | Quarantine machinery is for large multi-team suites; not this repo's scale. |
| 35 | Smoke tests for deploys | Nothing is deployed. |
| 38 | Seed scripts | No owned database. |
| 40 | Scoped secrets per environment | Repo holds no credentials and deploys nothing; connections live in consuming Airflow instances. |
| 41 | Preview environments | Nothing to deploy per PR. |
| 42 | Hot-reload / watch mode | No app process; `pip install -e .` covers live-edit iteration. |
| 43 | Structured logging | Library logs via stdlib `logging`; formatting is owned by the consuming Airflow deployment. |
| 44 | Traces and metrics | Runtime observability belongs to consuming Airflow instances. |
| 45 | Feature flags | No runtime service; behavior is constructor-parameterized. |
| 46 | Database migration tooling | Target schemas are owned and versioned by consuming projects. |

## Beyond the checklist

- **Backstage onboarding**: `backstage.yaml` registers the component with owner (`dev-data-acquisition`), cost center, and environment labels — discoverable ownership metadata the checklist doesn't name.
- **Consumer-facing how-to docs**: `airflow_registry/utils/README.md` walks consumers through Slack webhook setup end-to-end, including a security-conscious note about where webhook credentials should (and should not) live.
- **Airflow-provider-style docstrings**: operators document parameters and template fields following upstream Airflow provider conventions, which helps both humans and agents use them correctly.
- **Modern typing hygiene in newer code**: `from __future__ import annotations` and `TYPE_CHECKING`-guarded imports in the operators give type-checkers a head start despite no enforced checking yet.
- **Org-level AI review**: the injected Copilot pull-request reviewer workflow is active on this repo.
