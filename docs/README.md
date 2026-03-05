# schema-flow Documentation

## Getting Started

- [Main README](../README.md) — Quick start, command reference, migration recipes
- [YAML Reference](yaml-reference.md) — Complete specification of every YAML key

## Switching to schema-flow

- [From Imperative Tools](switching-from-imperative.md) — Knex, Flyway, Liquibase, golang-migrate, dbmate, raw SQL
- [From ORM Migrations](switching-from-orms.md) — Prisma, TypeORM, Sequelize, Django, Rails, SQLAlchemy/Alembic

## Examples

- [examples/todo-app](../examples/todo-app/) — Basic tables, enums, FKs, indexes, check constraints, mixins
- [examples/cms](../examples/cms/) — Extensions, roles, RLS, views, materialized views, seeds, grants, GIN indexes
- [examples/multi-tenant-orders](../examples/multi-tenant-orders/) — Tenant isolation, RESTRICTIVE RLS, security functions, generated columns, audit trail
- [examples/multi-schema](../examples/multi-schema/) — Cross-schema FKs, cross-schema views, migration ordering

## AI Integration

- [CLAUDE.md Template](CLAUDE.md.template) — Drop this into your project so Claude understands your schema-flow setup
- [llms.txt](../llms.txt) — AI-discoverable project summary (llmstxt.org standard)

## Contributing

- [CONTRIBUTING.md](../CONTRIBUTING.md) — Development setup, testing, code conventions
