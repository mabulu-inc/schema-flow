# Contributing to schema-flow

Thanks for your interest in contributing to schema-flow. This document covers what you need to get started.

## Getting Started

```bash
git clone https://github.com/mabulu-inc/schema-flow.git
cd schema-flow
pnpm install
pnpm run build
```

You'll need a PostgreSQL instance for integration testing. The quickest way is the built-in script, which starts Postgres in-memory (data directory on tmpfs, durability disabled):

```bash
pnpm run test:db
export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
```

This runs Postgres with `--tmpfs /var/lib/postgresql/data` and `-c fsync=off -c full_page_writes=off -c synchronous_commit=off`, so all data lives in RAM and no disk I/O occurs. Tests run significantly faster this way.

Or run the Docker command directly for more control:

```bash
docker run -d \
  --name sf-postgres \
  --tmpfs /var/lib/postgresql/data \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  postgres:latest \
  -c fsync=off \
  -c full_page_writes=off \
  -c synchronous_commit=off

export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
```

Podman works the same way:

```bash
podman run -d \
  --name sf-postgres \
  --tmpfs /var/lib/postgresql/data \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  docker.io/library/postgres:latest \
  -c fsync=off \
  -c full_page_writes=off \
  -c synchronous_commit=off

export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
```

Or point at any existing Postgres instance you have running:

```bash
export TEST_DATABASE_URL="postgresql://user:pass@localhost:5432/yourdb"
```

## Project Structure

```
src/
  cli/          CLI entry point and argument parsing
  core/         Config, database pool, logger, file hash tracker
  introspect/   Reads current Postgres state via pg_catalog
  schema/       YAML parser and type definitions
  planner/      Diffs desired state against live DB, produces SQL
  executor/     Runs phases (pre → migrate → post) with transactions
  scaffold/     Generates scripts, bootstraps from existing DB
examples/
  schema-flow/    Example schema-flow directory
    schema/       Example table YAML files
    pre/          Example pre-migration scripts
    post/         Example post-migration scripts
```

## Running Tests

All integration tests run against a real Postgres database. Tests create and drop isolated databases (`sf_test_<random>`) automatically — your data is never touched.

```bash
# Run the full suite
pnpm test

# Watch mode during development
pnpm run test:watch
```

Tests run sequentially to avoid database contention. The `pretest` script compiles TypeScript before running tests.

When you're done, stop the container:

```bash
pnpm run test:db:stop
```

## Development Workflow

1. Create a branch from `main`
2. Make your changes
3. Run `pnpm run build` — the project must compile cleanly with zero errors
4. Run `pnpm test` — all tests must pass against a real Postgres instance
5. Open a pull request

## CI Pipeline

Every push and PR runs through GitHub Actions:

- **Build** — TypeScript compilation on Node 22
- **Typecheck** — `tsc --noEmit` for strict type safety
- **Test matrix** — Integration tests across Node 20/22 × Postgres 15/16/17, plus a `postgres:latest` run

CI starts each Postgres container in-memory mode (`--tmpfs`, `fsync=off`, `full_page_writes=off`, `synchronous_commit=off`) for fast, deterministic test runs. See `.github/workflows/ci.yml` for details.

## Conventions

### Code

- TypeScript strict mode, no `any` unless absolutely unavoidable
- Use `node:` prefix for built-in modules (`node:path`, `node:fs`, etc.)
- ESM only — no CommonJS
- Keep dependencies minimal. Every new dependency needs a justification.

### Schema YAML

- One table per file, filename matches table name
- Columns are NOT NULL by default (set `nullable: true` to opt in)
- Foreign keys live in the table file that owns the column, not the referenced table
- All schema files go in `schema-flow/schema/`

### SQL Scripts

- Filenames are prefixed with a UTC timestamp (`YYYYMMDDHHMMSS_description.sql`)
- Wrap changes in `BEGIN` / `COMMIT`
- Use the scaffolder: `npx @mabulu-inc/schema-flow new pre <name>`

### Commits

- Use clear, imperative commit messages: `Add partial index support`, not `added some stuff`
- One logical change per commit
- Reference issue numbers where applicable

## What Makes a Good Contribution

### Yes, please

- Bug fixes with a clear description of what was broken
- Support for additional Postgres types or constraint types
- Improvements to the diff engine (fewer false positives, better ALTER detection)
- Better error messages and logging
- Documentation improvements
- Performance work backed by before/after measurements

### Talk to us first

Open an issue before starting work on:

- New commands or CLI flags
- Changes to the YAML schema format
- New dependencies
- Changes to the migration execution order or transaction boundaries

These affect the public API and need design discussion.

### Out of scope

- Support for databases other than PostgreSQL — this is a Postgres tool by design
- ORM integration — schema-flow is deliberately ORM-agnostic
- GUI or web interface

## Reporting Bugs

Open an issue with:

1. What you expected to happen
2. What actually happened
3. The schema YAML and/or SQL scripts involved (sanitized if needed)
4. PostgreSQL version
5. Node.js version
6. `schema-flow` version (`schema-flow --version`)

## Code of Conduct

Be respectful and constructive. We're all here to build something useful.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
