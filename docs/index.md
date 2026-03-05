---
layout: home
hero:
  name: schema-flow
  text: Declarative PostgreSQL Migrations
  tagline: Define your schema in YAML. The tool figures out the SQL. Zero install, zero downtime.
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: YAML Reference
      link: /yaml-reference
    - theme: alt
      text: GitHub
      link: https://github.com/mabulu-inc/schema-flow
features:
  - title: Declarative, Not Imperative
    details: Describe what your schema should look like. schema-flow computes the ALTER statements. No more writing migration files by hand.
  - title: Full PostgreSQL
    details: RLS policies, triggers, partial indexes, GIN indexes, materialized views, cross-schema FKs, roles, grants, generated columns — all as YAML.
  - title: Safe by Default
    details: Only additive operations run unless you pass --allow-destructive. Plan before you apply. Lock and statement timeouts prevent runaway DDL.
  - title: Zero Install
    details: Run via npx or pnpx. No Java, no Go binary, no Docker requirement. Just Node.js and a PostgreSQL connection.
  - title: Drift Detection
    details: Compare your YAML against the live database. Know immediately when the database diverges from what your schema files describe.
  - title: Zero-Downtime Patterns
    details: Built-in expand/contract for column renames. Dual-write triggers, backfill, and cleanup — all managed by the CLI.
---

<div class="vp-doc" style="padding: 0 24px; max-width: 900px; margin: 0 auto;">

## Quick Start

**1. Configure the registry** — schema-flow is on [GitHub Packages](https://github.com/mabulu-inc/schema-flow/pkgs/npm/%40mabulu-inc/schema-flow). Add to `.npmrc`:

```ini
@mabulu-inc:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

Set a [personal access token](https://github.com/settings/tokens/new?scopes=read:packages) with `read:packages` scope, then `export GITHUB_TOKEN=ghp_...`. See [full setup](./getting-started#configure-the-registry).

**2. Generate or init:**

```bash
# Generate YAML from an existing database
DATABASE_URL="postgresql://..." npx @mabulu-inc/schema-flow generate

# Or start fresh
npx @mabulu-inc/schema-flow init
```

Define a table:

```yaml
# schema/tables/users.yaml
table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
    unique: true
  - name: name
    type: varchar(100)
    nullable: true
  - name: created_at
    type: timestamptz
    default: now()
indexes:
  - columns: [email]
    unique: true
```

Preview and apply:

```bash
npx @mabulu-inc/schema-flow plan   # see the SQL
npx @mabulu-inc/schema-flow run    # apply it
```

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](./getting-started) | Installation, first migration, making changes |
| [YAML Reference](./yaml-reference) | Complete specification of every YAML key, type, and default |
| [CLI Commands](./cli) | All commands, flags, and environment variables |
| [Examples](./examples) | Four complete example projects (beginner to advanced) |
| [Switching from imperative tools](./switching-from-imperative) | Guide for Knex, Flyway, Liquibase, dbmate teams |
| [Switching from ORM migrations](./switching-from-orms) | Guide for Prisma, TypeORM, Sequelize, Django, Rails teams |
| [AI Integration](./ai-integration) | CLAUDE.md template and llms.txt for AI coding assistants |

## What schema-flow manages

| Object | Support |
|--------|---------|
| Tables and columns | All PostgreSQL types, NOT NULL, defaults, generated columns |
| Primary keys | Single-column and composite |
| Foreign keys | Including cross-schema (`references.schema`) |
| Unique constraints | Single-column and multi-column |
| Check constraints | Named constraints with arbitrary expressions |
| Indexes | btree, gin, gist, hash, brin, partial, expression, covering (INCLUDE), opclass |
| Enums | Create and add values safely |
| Functions | plpgsql, sql, any PL language, SECURITY DEFINER, EXECUTE grants |
| Views | CREATE OR REPLACE with grants |
| Materialized views | With indexes and grants |
| Row-level security | Enable/force RLS, PERMISSIVE and RESTRICTIVE policies |
| Roles | CREATE ROLE with login, inherit, membership |
| Grants | Table-level, column-level, and function-level with GRANT OPTION |
| Triggers | BEFORE/AFTER/INSTEAD OF, per-row/statement, WHEN conditions |
| Comments | On tables, columns, indexes, constraints, functions, views, enums, triggers, policies |
| Seeds | Declarative row data upserted on every migration |
| Mixins | Reusable column/index/trigger/policy/grant sets with `{table}` interpolation |
| Extensions | CREATE EXTENSION IF NOT EXISTS |
| Expand/contract | Zero-downtime column renames with dual-write triggers and backfill |

## Safety matrix

| Operation | Default | With `--allow-destructive` |
|-----------|---------|---------------------------|
| Add column, index, FK, constraint | Runs | Runs |
| Widen type (e.g. varchar(50) to varchar(100)) | Runs | Runs |
| Add enum value | Runs | Runs |
| Drop column | **Blocked** | Runs |
| Drop table | **Blocked** | Runs |
| Narrow type | **Blocked** | Runs |
| Drop index | **Blocked** | Runs |

All DDL runs with configurable lock timeout (default 5s) and statement timeout (default 30s).

## Switching from another tool?

- **[From Knex, Flyway, Liquibase, dbmate](./switching-from-imperative)** — Stop writing migration files. Point `generate` at your database, `baseline`, and start writing YAML.
- **[From Prisma, TypeORM, Sequelize, Django, Rails](./switching-from-orms)** — Keep your ORM for queries. Let schema-flow manage the schema. Get access to every PostgreSQL feature your ORM can't express.

</div>
