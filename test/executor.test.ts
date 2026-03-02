// test/executor.test.ts
// Integration tests for the full migration lifecycle

import { describe, it, expect } from "vitest";
import {
  writeSchema,
  writeScript,
  writeMixin,
  tableExists,
  getColumns,
  fkExists,
  triggerExists,
  functionExists,
  policyExists,
  rlsEnabled,
  execSql,
  useTestProject,
} from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { runAll, runPre, runMigrate, runPost, runValidate } from "../src/executor/index.js";
import { logger, LogLevel } from "../src/core/logger.js";
import { closePool } from "../src/core/db.js";

// Suppress logs during tests
logger.setLevel(LogLevel.SILENT);

describe("executor", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("creates tables from YAML schema files", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
    unique: true
  - name: created_at
    type: timestamptz
    default: now()
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);
    expect(result.operationsExecuted).toBeGreaterThan(0);

    expect(await tableExists(ctx.connectionString, "users")).toBe(true);
    const cols = await getColumns(ctx.connectionString, "users");
    expect(cols).toContain("id");
    expect(cols).toContain("email");
    expect(cols).toContain("created_at");
  });

  it("creates tables with foreign keys in correct order", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "authors.yaml",
      `table: authors
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: varchar(100)
`,
    );

    writeSchema(
      ctx.project.schemaDir,
      "books.yaml",
      `table: books
columns:
  - name: id
    type: serial
    primary_key: true
  - name: author_id
    type: integer
    references:
      table: authors
      column: id
      on_delete: CASCADE
  - name: title
    type: varchar(255)
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    expect(await tableExists(ctx.connectionString, "authors")).toBe(true);
    expect(await tableExists(ctx.connectionString, "books")).toBe(true);
    expect(await fkExists(ctx.connectionString, "fk_books_author_id_authors")).toBe(true);
  });

  it("runs pre-migration scripts", async () => {
    // First create the table that the pre-script will alter
    await execSql(ctx.connectionString, `CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL)`);

    writeScript(
      ctx.project.preDir,
      "20260101000000_rename_column.sql",
      `ALTER TABLE "public"."users" RENAME COLUMN "name" TO "display_name";`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runPre(config);
    expect(result.success).toBe(true);
    expect(result.operationsExecuted).toBe(1);

    const cols = await getColumns(ctx.connectionString, "users");
    expect(cols).toContain("display_name");
    expect(cols).not.toContain("name");
  });

  it("runs post-migration scripts", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE roles (id serial PRIMARY KEY, name varchar(50) UNIQUE NOT NULL)`);

    writeScript(
      ctx.project.postDir,
      "20260101000000_seed_roles.sql",
      `INSERT INTO "public"."roles" (name) VALUES ('admin'), ('editor') ON CONFLICT DO NOTHING;`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runPost(config);
    expect(result.success).toBe(true);

    const res = await execSql(ctx.connectionString, `SELECT count(*) as cnt FROM roles`);
    expect(Number(res.rows[0].cnt)).toBe(2);
  });

  it("runs all phases in order: pre → migrate → post", async () => {
    // Pre-script creates a table
    writeScript(
      ctx.project.preDir,
      "20260101000000_create_legacy.sql",
      `CREATE TABLE IF NOT EXISTS legacy_data (id serial PRIMARY KEY, value text);`,
    );

    // Schema defines a new table
    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
`,
    );

    // Post-script seeds data
    writeScript(
      ctx.project.postDir,
      "20260101000000_seed.sql",
      `INSERT INTO users (email) VALUES ('test@example.com');`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const results = await runAll(config);
    expect(results).toHaveLength(3);
    expect(results.every((r) => r.success)).toBe(true);

    expect(await tableExists(ctx.connectionString, "legacy_data")).toBe(true);
    expect(await tableExists(ctx.connectionString, "users")).toBe(true);

    const res = await execSql(ctx.connectionString, `SELECT email FROM users`);
    expect(res.rows[0].email).toBe("test@example.com");
  });

  it("dry run does not modify the database", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: true,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);
    expect(result.dryRun).toBe(true);
    expect(result.operationsExecuted).toBeGreaterThan(0);

    // Table should NOT exist
    expect(await tableExists(ctx.connectionString, "users")).toBe(false);
  });

  it("skips unchanged files on second run", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const first = await runMigrate(config);
    expect(first.success).toBe(true);
    expect(first.operationsExecuted).toBeGreaterThan(0);

    // Reset the app pool between runs
    await closePool();

    const second = await runMigrate(config);
    expect(second.success).toBe(true);
    expect(second.operationsExecuted).toBe(0); // Nothing to do
  });

  it("stops on pre-script failure and does not run migrate", async () => {
    writeScript(ctx.project.preDir, "20260101000000_bad.sql", `SELECT 1 FROM nonexistent_table;`);

    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const results = await runAll(config);
    expect(results[0].success).toBe(false); // pre failed
    expect(results).toHaveLength(1); // stopped early, no migrate or post

    expect(await tableExists(ctx.connectionString, "users")).toBe(false);
  });

  it("blocks column drops in safe mode and reports them", async () => {
    // Create table with extra column
    await execSql(
      ctx.connectionString,
      `CREATE TABLE items (id serial PRIMARY KEY, name text NOT NULL, obsolete text NOT NULL)`,
    );

    // Schema file does NOT include the 'obsolete' column
    writeSchema(
      ctx.project.schemaDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
      allowDestructive: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);
    expect(result.blockedDestructive).toBe(1);

    // The 'obsolete' column should STILL exist (not dropped)
    const cols = await getColumns(ctx.connectionString, "items");
    expect(cols).toContain("obsolete");
  });

  it("drops columns when allowDestructive is true", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE items (id serial PRIMARY KEY, name text NOT NULL, obsolete text NOT NULL)`,
    );

    writeSchema(
      ctx.project.schemaDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
      allowDestructive: true,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);
    expect(result.blockedDestructive).toBe(0);

    // The 'obsolete' column should be gone
    const cols = await getColumns(ctx.connectionString, "items");
    expect(cols).not.toContain("obsolete");
    expect(cols).toContain("name");
  });

  it("creates functions from fn_ files and triggers from YAML", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "fn_update_timestamp.yaml",
      `name: update_timestamp
language: plpgsql
returns: trigger
body: |
  BEGIN
    NEW.updated_at = now();
    RETURN NEW;
  END;
`,
    );

    writeSchema(
      ctx.project.schemaDir,
      "events.yaml",
      `table: events
columns:
  - name: id
    type: serial
    primary_key: true
  - name: updated_at
    type: timestamptz
    default: now()
triggers:
  - name: set_events_updated_at
    timing: BEFORE
    events: [UPDATE]
    function: update_timestamp
    for_each: ROW
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    expect(await functionExists(ctx.connectionString, "update_timestamp")).toBe(true);
    expect(await tableExists(ctx.connectionString, "events")).toBe(true);
    expect(await triggerExists(ctx.connectionString, "set_events_updated_at", "events")).toBe(true);
  });

  it("expands mixins and creates tables with mixin columns and triggers", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "fn_update_timestamp.yaml",
      `name: update_timestamp
language: plpgsql
returns: trigger
body: |
  BEGIN
    NEW.updated_at = now();
    RETURN NEW;
  END;
`,
    );

    writeMixin(
      ctx.project.mixinsDir,
      "timestamps.yaml",
      `mixin: timestamps
columns:
  - name: created_at
    type: timestamptz
    default: now()
  - name: updated_at
    type: timestamptz
    default: now()
triggers:
  - name: set_{table}_updated_at
    timing: BEFORE
    events: [UPDATE]
    function: update_timestamp
    for_each: ROW
`,
    );

    writeSchema(
      ctx.project.schemaDir,
      "products.yaml",
      `table: products
use: [timestamps]
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: varchar(100)
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    expect(await tableExists(ctx.connectionString, "products")).toBe(true);
    const cols = await getColumns(ctx.connectionString, "products");
    expect(cols).toContain("created_at");
    expect(cols).toContain("updated_at");
    expect(cols).toContain("name");
    expect(await triggerExists(ctx.connectionString, "set_products_updated_at", "products")).toBe(true);
  });

  it("dry-run shows trigger operations", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "fn_update_timestamp.yaml",
      `name: update_timestamp
language: plpgsql
returns: trigger
body: |
  BEGIN
    NEW.updated_at = now();
    RETURN NEW;
  END;
`,
    );

    writeSchema(
      ctx.project.schemaDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: updated_at
    type: timestamptz
    default: now()
triggers:
  - name: set_items_updated
    timing: BEFORE
    events: [UPDATE]
    function: update_timestamp
    for_each: ROW
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: true,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);
    expect(result.dryRun).toBe(true);
    // Should include the trigger creation op in the count
    expect(result.operationsExecuted).toBeGreaterThanOrEqual(2); // create table + create trigger

    // Table should NOT exist (dry run)
    expect(await tableExists(ctx.connectionString, "items")).toBe(false);
  });

  it("creates table with RLS and policies", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "orders.yaml",
      `table: orders
rls: true
columns:
  - name: id
    type: serial
    primary_key: true
  - name: user_id
    type: integer
policies:
  - name: users_see_own_orders
    for: SELECT
    using: "user_id = 1"
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    expect(await tableExists(ctx.connectionString, "orders")).toBe(true);
    expect(await rlsEnabled(ctx.connectionString, "orders")).toBe(true);
    expect(await policyExists(ctx.connectionString, "users_see_own_orders", "orders")).toBe(true);
  });

  it("creates function with SECURITY DEFINER", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "fn_get_user.yaml",
      `name: get_current_user_id
language: sql
returns: integer
security: definer
body: SELECT 1
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);
    expect(await functionExists(ctx.connectionString, "get_current_user_id")).toBe(true);

    // Verify it is SECURITY DEFINER
    const res = await execSql(
      ctx.connectionString,
      `SELECT security_type FROM information_schema.routines
       WHERE routine_schema = 'public' AND routine_name = 'get_current_user_id'`,
    );
    expect(res.rows[0].security_type).toBe("DEFINER");
  });

  it("applies mixin with tenant isolation pattern", async () => {
    writeMixin(
      ctx.project.mixinsDir,
      "tenant_isolation.yaml",
      `mixin: tenant_isolation
rls: true
columns:
  - name: tenant_id
    type: uuid
    default: "'00000000-0000-0000-0000-000000000000'"
policies:
  - name: "{table}_tenant_isolation"
    for: ALL
    using: "tenant_id IS NOT NULL"
    check: "tenant_id IS NOT NULL"
`,
    );

    writeSchema(
      ctx.project.schemaDir,
      "items.yaml",
      `table: items
use: [tenant_isolation]
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    expect(await tableExists(ctx.connectionString, "items")).toBe(true);
    expect(await rlsEnabled(ctx.connectionString, "items")).toBe(true);
    expect(await policyExists(ctx.connectionString, "items_tenant_isolation", "items")).toBe(true);
    const cols = await getColumns(ctx.connectionString, "items");
    expect(cols).toContain("tenant_id");
  });

  it("idempotent second run with RLS", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "rls_idem.yaml",
      `table: rls_idem
rls: true
columns:
  - name: id
    type: serial
    primary_key: true
  - name: user_id
    type: integer
policies:
  - name: idem_policy
    for: SELECT
    using: "user_id = 1"
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const first = await runMigrate(config);
    expect(first.success).toBe(true);
    expect(first.operationsExecuted).toBeGreaterThan(0);

    await closePool();

    const second = await runMigrate(config);
    expect(second.success).toBe(true);
    expect(second.operationsExecuted).toBe(0);
  });

  it("idempotent second run with triggers", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "fn_update_timestamp.yaml",
      `name: update_timestamp
language: plpgsql
returns: trigger
body: |
  BEGIN
    NEW.updated_at = now();
    RETURN NEW;
  END;
`,
    );

    writeSchema(
      ctx.project.schemaDir,
      "things.yaml",
      `table: things
columns:
  - name: id
    type: serial
    primary_key: true
  - name: updated_at
    type: timestamptz
    default: now()
triggers:
  - name: set_things_updated
    timing: BEFORE
    events: [UPDATE]
    function: update_timestamp
    for_each: ROW
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const first = await runMigrate(config);
    expect(first.success).toBe(true);
    expect(first.operationsExecuted).toBeGreaterThan(0);

    await closePool();

    const second = await runMigrate(config);
    expect(second.success).toBe(true);
    expect(second.operationsExecuted).toBe(0); // Nothing to do
  });

  it("validate returns valid for a correct schema", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
    unique: true
  - name: created_at
    type: timestamptz
    default: now()
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runValidate(config);
    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
    expect(result.operationsChecked).toBeGreaterThan(0);
  });

  it("validate catches bad SQL in policy using expression", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "orders.yaml",
      `table: orders
rls: true
columns:
  - name: id
    type: serial
    primary_key: true
  - name: user_id
    type: integer
policies:
  - name: bad_policy
    for: SELECT
    using: "user_id = nonexistent_function()"
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runValidate(config);
    expect(result.valid).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.errors[0]).toMatch(/nonexistent_function/i);
  });

  it("validate catches bad SQL in function body", async () => {
    // Use a SQL-language function — Postgres validates SQL bodies at creation time
    // (unlike plpgsql which defers validation to first call)
    writeSchema(
      ctx.project.schemaDir,
      "fn_broken.yaml",
      `name: broken_function
language: sql
returns: integer
body: SELECT id FROM nonexistent_table
`,
    );

    writeSchema(
      ctx.project.schemaDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runValidate(config);
    expect(result.valid).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.errors[0]).toMatch(/nonexistent_table/i);
  });

  it("validate does not modify the database", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runValidate(config);
    expect(result.valid).toBe(true);
    expect(result.operationsChecked).toBeGreaterThan(0);

    // Table should NOT exist after validate (rolled back)
    expect(await tableExists(ctx.connectionString, "users")).toBe(false);
  });
});
