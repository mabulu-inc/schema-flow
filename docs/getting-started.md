# Getting Started

## Install

schema-flow runs via `npx` — no global install required:

```bash
npx @mabulu-inc/schema-flow --version
```

Or install it as a dev dependency:

```bash
npm install -D @mabulu-inc/schema-flow
# or
pnpm add -D @mabulu-inc/schema-flow
```

## Option A: Start from an Existing Database

If you already have a PostgreSQL database, generate YAML files from it:

```bash
DATABASE_URL="postgresql://user:pass@localhost:5432/mydb" npx @mabulu-inc/schema-flow generate
```

This creates one YAML file per table, enum, function, view, and role in the `schema/` directory. The output matches your live database exactly.

Then mark everything as applied:

```bash
npx @mabulu-inc/schema-flow baseline
```

Verify zero drift:

```bash
npx @mabulu-inc/schema-flow plan
# Should show: No changes detected
```

From here, all schema changes go through YAML.

## Option B: Start Fresh

Initialize the directory structure:

```bash
npx @mabulu-inc/schema-flow init
```

This creates:

```
schema/
  tables/       # YAML table definitions
  enums/        # Enum type definitions
  functions/    # Function definitions
  views/        # View definitions
  roles/        # Role definitions
  pre/          # Pre-migration SQL scripts
  post/         # Post-migration SQL scripts
  mixins/       # Reusable schema mixins
```

## Define a Table

Create `schema/tables/users.yaml`:

```yaml
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

## Set the Connection

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/mydb"
```

Or create `schema-flow.config.yaml`:

```yaml
environments:
  development:
    connectionString: postgresql://localhost:5432/mydb_dev
  production:
    connectionString: ${DATABASE_URL}
```

## Preview

```bash
npx @mabulu-inc/schema-flow plan
```

Output:
```
[plan] CREATE TABLE "public"."users" (
  "id" serial NOT NULL,
  "email" varchar(255) NOT NULL,
  "name" varchar(100),
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("id")
);
[plan] CREATE UNIQUE INDEX "idx_users_email" ON "public"."users" ("email");
```

## Apply

```bash
npx @mabulu-inc/schema-flow run
```

## Make a Change

Add a column — just edit the YAML:

```yaml
columns:
  # ... existing columns ...
  - name: role
    type: varchar(20)
    default: "'user'"
```

Preview and apply:

```bash
npx @mabulu-inc/schema-flow plan   # see the ALTER TABLE
npx @mabulu-inc/schema-flow run    # apply it
```

## Check for Drift

Compare YAML against the live database:

```bash
npx @mabulu-inc/schema-flow drift
```

## Next Steps

- [YAML Reference](./yaml-reference) — Every key, type, and default
- [CLI Commands](./cli) — Full command reference
- [Examples](./examples) — Complete example projects
- [Switching from other tools](./switching-from-imperative) — Migration guides
