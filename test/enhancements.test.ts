// test/enhancements.test.ts
// TDD tests for schema-flow enhancements:
//   1. Role qualifiers: bypassrls, replication
//   2. Function qualifiers: volatility, parallel, strict, leakproof, cost, set
//   3. Schema grants: GRANT USAGE ON SCHEMA
//   4. Seed !sql expressions: raw SQL in seed values
//   5. Schema creation: CREATE SCHEMA IF NOT EXISTS

import { describe, it, expect } from "vitest";
import { writeSchema, useTestProject, execSql } from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { runMigrate } from "../src/executor/index.js";
import { closePool } from "../src/core/db.js";
import { logger, LogLevel } from "../src/core/logger.js";
import { parseFunctionFile, parseRoleFile, parseExtensionsFile, parseTableFile } from "../src/schema/parser.js";
import { writeFileSync } from "node:fs";
import path from "node:path";

logger.setLevel(LogLevel.SILENT);

// ─── 1. Role qualifiers: bypassrls and replication ──────────────────────────

describe("roles: bypassrls and replication qualifiers", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("parses bypassrls and replication from YAML", () => {
    const filePath = path.join(ctx.project.rolesDir, "test_role.yaml");
    writeFileSync(
      filePath,
      `role: parse_test_role
login: false
bypassrls: false
replication: false
`,
    );
    const role = parseRoleFile(filePath);
    expect(role.bypassrls).toBe(false);
    expect(role.replication).toBe(false);
  });

  it("creates a role with bypassrls: false", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "no_bypass.yaml",
      `role: no_bypass_role
login: false
bypassrls: false
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "dummy.yaml",
      `table: dummy\ncolumns:\n  - name: id\n    type: serial\n    primary_key: true\n`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(
      ctx.connectionString,
      `SELECT rolbypassrls, rolreplication FROM pg_roles WHERE rolname = 'no_bypass_role'`,
    );
    expect(res.rows).toHaveLength(1);
    expect(res.rows[0].rolbypassrls).toBe(false);
  });

  it("creates a role with replication: false", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "no_repl.yaml",
      `role: no_repl_role
login: false
replication: false
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "dummy.yaml",
      `table: dummy\ncolumns:\n  - name: id\n    type: serial\n    primary_key: true\n`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(
      ctx.connectionString,
      `SELECT rolreplication FROM pg_roles WHERE rolname = 'no_repl_role'`,
    );
    expect(res.rows).toHaveLength(1);
    expect(res.rows[0].rolreplication).toBe(false);
  });

  it("detects and alters bypassrls/replication changes", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "alter_role.yaml",
      `role: alter_attrs_role
login: false
bypassrls: false
replication: false
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "dummy.yaml",
      `table: dummy\ncolumns:\n  - name: id\n    type: serial\n    primary_key: true\n`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    await runMigrate(config);
    await closePool();

    // Manually grant BYPASSRLS via SQL to create drift
    await execSql(ctx.connectionString, `ALTER ROLE alter_attrs_role BYPASSRLS`);

    // Touch the role file so the tracker sees it as changed (add a comment)
    writeSchema(
      ctx.project.rolesDir,
      "alter_role.yaml",
      `role: alter_attrs_role
login: false
bypassrls: false
replication: false
# force re-check
`,
    );

    // Re-run should detect and fix the drift
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(
      ctx.connectionString,
      `SELECT rolbypassrls FROM pg_roles WHERE rolname = 'alter_attrs_role'`,
    );
    expect(res.rows[0].rolbypassrls).toBe(false);
  });
});

// ─── 2. Function qualifiers ─────────────────────────────────────────────────

describe("functions: extended qualifiers", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("parses volatility, parallel, strict, leakproof, cost, set from YAML", () => {
    const filePath = path.join(ctx.project.functionsDir, "qualified_fn.yaml");
    writeFileSync(
      filePath,
      `name: qualified_fn
language: sql
returns: text
volatility: stable
parallel: safe
strict: true
leakproof: true
cost: 50
set:
  search_path: public, pg_catalog
body: "SELECT 'ok'::text;"
`,
    );
    const fn = parseFunctionFile(filePath);
    expect(fn.volatility).toBe("stable");
    expect(fn.parallel).toBe("safe");
    expect(fn.strict).toBe(true);
    expect(fn.leakproof).toBe(true);
    expect(fn.cost).toBe(50);
    expect(fn.set).toEqual({ search_path: "public, pg_catalog" });
  });

  it("creates a STABLE PARALLEL SAFE function", async () => {
    writeSchema(
      ctx.project.functionsDir,
      "stable_fn.yaml",
      `name: stable_fn
language: sql
returns: text
volatility: stable
parallel: safe
body: "SELECT 'stable'::text;"
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "dummy.yaml",
      `table: dummy\ncolumns:\n  - name: id\n    type: serial\n    primary_key: true\n`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(
      ctx.connectionString,
      `SELECT provolatile, proparallel FROM pg_proc WHERE proname = 'stable_fn'`,
    );
    expect(res.rows).toHaveLength(1);
    expect(res.rows[0].provolatile).toBe("s"); // s = stable
    expect(res.rows[0].proparallel).toBe("s"); // s = safe
  });

  it("creates a STRICT LEAKPROOF function", async () => {
    writeSchema(
      ctx.project.functionsDir,
      "strict_fn.yaml",
      `name: strict_fn
language: sql
returns: int
args:
  - name: x
    type: int
strict: true
leakproof: true
cost: 10
body: "SELECT x * 2;"
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "dummy.yaml",
      `table: dummy\ncolumns:\n  - name: id\n    type: serial\n    primary_key: true\n`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(
      ctx.connectionString,
      `SELECT proisstrict, proleakproof, procost FROM pg_proc WHERE proname = 'strict_fn'`,
    );
    expect(res.rows).toHaveLength(1);
    expect(res.rows[0].proisstrict).toBe(true);
    expect(res.rows[0].proleakproof).toBe(true);
    expect(Number(res.rows[0].procost)).toBe(10);
  });

  it("creates a function with SET search_path", async () => {
    writeSchema(
      ctx.project.functionsDir,
      "set_path_fn.yaml",
      `name: set_path_fn
language: sql
returns: text
volatility: stable
security: definer
set:
  search_path: public, pg_catalog
body: "SELECT current_setting('search_path');"
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "dummy.yaml",
      `table: dummy\ncolumns:\n  - name: id\n    type: serial\n    primary_key: true\n`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    // Verify the function has SET search_path by calling it
    const res = await execSql(ctx.connectionString, `SELECT set_path_fn() AS sp`);
    // The search_path inside the function should include public and pg_catalog
    expect(res.rows[0].sp).toContain("public");
  });

  it("creates an IMMUTABLE PARALLEL SAFE function", async () => {
    writeSchema(
      ctx.project.functionsDir,
      "immutable_fn.yaml",
      `name: immutable_fn
language: sql
returns: int
args:
  - name: a
    type: int
  - name: b
    type: int
volatility: immutable
parallel: safe
body: "SELECT a + b;"
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "dummy.yaml",
      `table: dummy\ncolumns:\n  - name: id\n    type: serial\n    primary_key: true\n`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(
      ctx.connectionString,
      `SELECT provolatile, proparallel FROM pg_proc WHERE proname = 'immutable_fn'`,
    );
    expect(res.rows[0].provolatile).toBe("i"); // i = immutable
    expect(res.rows[0].proparallel).toBe("s"); // s = safe
  });
});

// ─── 3. Schema grants ──────────────────────────────────────────────────────

describe("extensions: schema grants", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("parses schema_grants from extensions YAML", () => {
    const filePath = path.join(ctx.project.sfDir, "extensions.yaml");
    writeFileSync(
      filePath,
      `extensions:
  - pgcrypto
schema_grants:
  - schemas: [public]
    privilege: USAGE
    roles: [test_role]
`,
    );
    const ext = parseExtensionsFile(filePath);
    expect(ext.schema_grants).toBeDefined();
    expect(ext.schema_grants).toHaveLength(1);
    expect(ext.schema_grants![0].schemas).toEqual(["public"]);
    expect(ext.schema_grants![0].roles).toEqual(["test_role"]);
  });

  it("generates GRANT USAGE ON SCHEMA", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "schema_user.yaml",
      `role: schema_user
login: false
`,
    );
    writeFileSync(
      path.join(ctx.project.sfDir, "extensions.yaml"),
      `extensions: []
schema_grants:
  - schemas: [public]
    privilege: USAGE
    roles: [schema_user]
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "dummy.yaml",
      `table: dummy\ncolumns:\n  - name: id\n    type: serial\n    primary_key: true\n`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    // Verify schema_user can access objects in public schema
    const res = await execSql(
      ctx.connectionString,
      `SELECT has_schema_privilege('schema_user', 'public', 'USAGE') AS has_usage`,
    );
    expect(res.rows[0].has_usage).toBe(true);
  });
});

// ─── 4. Seed !sql expressions ──────────────────────────────────────────────

describe("seeds: !sql expression support", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("treats !sql tagged values as raw SQL expressions", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "events.yaml",
      `table: events
columns:
  - name: id
    type: uuid
    primary_key: true
  - name: name
    type: text
  - name: created_at
    type: timestamptz
    default: now()
seeds:
  - id: "a0000001-0000-4000-8000-000000000001"
    name: "Test Event"
    created_at: !sql "now() - interval '1 day'"
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(
      ctx.connectionString,
      `SELECT name, created_at < now() AS is_past FROM events WHERE id = 'a0000001-0000-4000-8000-000000000001'`,
    );
    expect(res.rows).toHaveLength(1);
    expect(res.rows[0].name).toBe("Test Event");
    expect(res.rows[0].is_past).toBe(true);
  });

  it("handles !sql expressions for computed values", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "prices.yaml",
      `table: prices
columns:
  - name: id
    type: serial
    primary_key: true
  - name: label
    type: text
  - name: amount
    type: numeric(10,2)
seeds:
  - id: 1
    label: "calculated"
    amount: !sql "100.00 * 1.1"
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(
      ctx.connectionString,
      `SELECT amount FROM prices WHERE id = 1`,
    );
    expect(Number(res.rows[0].amount)).toBeCloseTo(110.0);
  });

  it("mixes !sql and literal values in the same row", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "mixed.yaml",
      `table: mixed
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
  - name: ts
    type: timestamptz
    default: now()
seeds:
  - id: 1
    name: "literal string"
    ts: !sql "now()"
  - id: 2
    name: "another literal"
    ts: "2024-01-01T00:00:00Z"
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(ctx.connectionString, `SELECT id, name FROM mixed ORDER BY id`);
    expect(res.rows).toHaveLength(2);
    expect(res.rows[0].name).toBe("literal string");
    expect(res.rows[1].name).toBe("another literal");
  });
});

// ─── 5. Schema creation ────────────────────────────────────────────────────

describe("schema creation: CREATE SCHEMA IF NOT EXISTS", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("creates a non-public schema when pgSchema is set", async () => {
    const schema = "test_custom_schema";

    writeSchema(
      ctx.project.tablesDir,
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
      pgSchema: schema,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    // Verify schema exists
    const res = await execSql(
      ctx.connectionString,
      `SELECT 1 FROM information_schema.schemata WHERE schema_name = $1`,
      [schema],
    );
    expect(res.rowCount).toBe(1);

    // Verify table was created in the custom schema
    const tableRes = await execSql(
      ctx.connectionString,
      `SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'items'`,
      [schema],
    );
    expect(tableRes.rowCount).toBe(1);
  });

  it("is idempotent — creating schema twice succeeds", async () => {
    const schema = "idem_schema";

    writeSchema(
      ctx.project.tablesDir,
      "things.yaml",
      `table: things
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      pgSchema: schema,
      dryRun: false,
    });

    const result1 = await runMigrate(config);
    expect(result1.success).toBe(true);

    await closePool();

    const result2 = await runMigrate(config);
    expect(result2.success).toBe(true);
  });
});

// ─── 6. Seed with composite unique constraints ─────────────────────────────

describe("seeds: composite unique constraint key resolution", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("uses composite unique_constraints columns as seed key when no PK in seed data", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "enrollments.yaml",
      `table: enrollments
columns:
  - name: id
    type: serial
    primary_key: true
  - name: student_id
    type: int
  - name: course_id
    type: int
  - name: grade
    type: text
unique_constraints:
  - columns: [student_id, course_id]
    name: uq_enrollment
seeds:
  - student_id: 1
    course_id: 101
    grade: "A"
  - student_id: 1
    course_id: 102
    grade: "B"
  - student_id: 2
    course_id: 101
    grade: "C"
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(
      ctx.connectionString,
      `SELECT student_id, course_id, grade FROM enrollments ORDER BY student_id, course_id`,
    );
    expect(res.rows).toHaveLength(3);
    expect(res.rows[0]).toMatchObject({ student_id: 1, course_id: 101, grade: "A" });
    expect(res.rows[1]).toMatchObject({ student_id: 1, course_id: 102, grade: "B" });
    expect(res.rows[2]).toMatchObject({ student_id: 2, course_id: 101, grade: "C" });
  });

  it("updates rows matched by composite unique key", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "enrollments.yaml",
      `table: enrollments
columns:
  - name: id
    type: serial
    primary_key: true
  - name: student_id
    type: int
  - name: course_id
    type: int
  - name: grade
    type: text
unique_constraints:
  - columns: [student_id, course_id]
    name: uq_enrollment
seeds:
  - student_id: 1
    course_id: 101
    grade: "A"
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    await runMigrate(config);
    await closePool();

    // Change grade from A to A+
    writeSchema(
      ctx.project.tablesDir,
      "enrollments.yaml",
      `table: enrollments
columns:
  - name: id
    type: serial
    primary_key: true
  - name: student_id
    type: int
  - name: course_id
    type: int
  - name: grade
    type: text
unique_constraints:
  - columns: [student_id, course_id]
    name: uq_enrollment
seeds:
  - student_id: 1
    course_id: 101
    grade: "A+"
`,
    );

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(
      ctx.connectionString,
      `SELECT grade FROM enrollments WHERE student_id = 1 AND course_id = 101`,
    );
    expect(res.rows).toHaveLength(1);
    expect(res.rows[0].grade).toBe("A+");
  });
});

// ─── 7. description: as alias for comment: ──────────────────────────────────

describe("description: alias for comment:", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("parses description: as comment: on tables", () => {
    const filePath = path.join(ctx.project.tablesDir, "desc_table.yaml");
    writeFileSync(
      filePath,
      `table: desc_table
description: This is a table description
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );
    const schema = parseTableFile(filePath);
    expect(schema.comment).toBe("This is a table description");
  });

  it("parses description: on columns as comment:", () => {
    const filePath = path.join(ctx.project.tablesDir, "desc_cols.yaml");
    writeFileSync(
      filePath,
      `table: desc_cols
columns:
  - name: id
    type: serial
    primary_key: true
    description: The primary key
  - name: name
    type: text
    description: The display name
`,
    );
    const schema = parseTableFile(filePath);
    expect(schema.columns[0].comment).toBe("The primary key");
    expect(schema.columns[1].comment).toBe("The display name");
  });

  it("comment: takes precedence over description:", () => {
    const filePath = path.join(ctx.project.tablesDir, "both.yaml");
    writeFileSync(
      filePath,
      `table: both
comment: The real comment
description: The description
columns:
  - name: id
    type: serial
    primary_key: true
    comment: col comment
    description: col description
`,
    );
    const schema = parseTableFile(filePath);
    expect(schema.comment).toBe("The real comment");
    expect(schema.columns[0].comment).toBe("col comment");
  });

  it("applies COMMENT ON TABLE from description:", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "commented.yaml",
      `table: commented_table
description: A commented table
columns:
  - name: id
    type: serial
    primary_key: true
    description: Primary key column
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(
      ctx.connectionString,
      `SELECT obj_description('commented_table'::regclass) AS tbl_comment`,
    );
    expect(res.rows[0].tbl_comment).toBe("A commented table");

    const colRes = await execSql(
      ctx.connectionString,
      `SELECT col_description('commented_table'::regclass, 1) AS col_comment`,
    );
    expect(colRes.rows[0].col_comment).toBe("Primary key column");
  });

  it("parses description: on functions", () => {
    const filePath = path.join(ctx.project.functionsDir, "desc_fn.yaml");
    writeFileSync(
      filePath,
      `name: desc_fn
language: sql
returns: int
description: A described function
body: "SELECT 1;"
`,
    );
    const fn = parseFunctionFile(filePath);
    expect(fn.comment).toBe("A described function");
  });

  it("parses description: on roles", () => {
    const filePath = path.join(ctx.project.rolesDir, "desc_role.yaml");
    writeFileSync(
      filePath,
      `role: desc_role
login: false
description: A described role
`,
    );
    const role = parseRoleFile(filePath);
    expect(role.comment).toBe("A described role");
  });
});

// ─── 8. Seed on_conflict: and object format ─────────────────────────────────

describe("seeds: on_conflict and object format", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("parses seeds as object with on_conflict and rows", () => {
    const filePath = path.join(ctx.project.tablesDir, "obj_seeds.yaml");
    writeFileSync(
      filePath,
      `table: obj_seeds
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
seeds:
  on_conflict: DO NOTHING
  rows:
    - id: 1
      name: first
    - id: 2
      name: second
`,
    );
    const schema = parseTableFile(filePath);
    expect(schema.seeds).toHaveLength(2);
    expect(schema.seeds![0]).toEqual({ id: 1, name: "first" });
    expect(schema.seeds_on_conflict).toBe("DO NOTHING");
  });

  it("on_conflict: DO NOTHING skips updates for existing rows", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "skip_update.yaml",
      `table: skip_update
columns:
  - name: id
    type: serial
    primary_key: true
  - name: val
    type: text
seeds:
  on_conflict: DO NOTHING
  rows:
    - id: 1
      val: original
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    // Manually change the value
    await execSql(ctx.connectionString, `UPDATE skip_update SET val = 'changed' WHERE id = 1`);
    await closePool();

    // Re-run with same seed — should NOT overwrite because on_conflict: DO NOTHING
    writeSchema(
      ctx.project.tablesDir,
      "skip_update.yaml",
      `table: skip_update
columns:
  - name: id
    type: serial
    primary_key: true
  - name: val
    type: text
seeds:
  on_conflict: DO NOTHING
  rows:
    - id: 1
      val: original
    - id: 2
      val: new_row
# force re-run
`,
    );
    const result2 = await runMigrate(config);
    expect(result2.success).toBe(true);

    const res = await execSql(ctx.connectionString, `SELECT id, val FROM skip_update ORDER BY id`);
    expect(res.rows).toHaveLength(2);
    expect(res.rows[0].val).toBe("changed"); // NOT overwritten
    expect(res.rows[1].val).toBe("new_row"); // inserted
  });
});

// ─── 9. Column-level check: sugar ───────────────────────────────────────────

describe("column-level check: sugar", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("parses column-level check: into table checks array", () => {
    const filePath = path.join(ctx.project.tablesDir, "col_check.yaml");
    writeFileSync(
      filePath,
      `table: col_check
columns:
  - name: id
    type: serial
    primary_key: true
  - name: probability
    type: integer
    default: "0"
    check: "probability BETWEEN 0 AND 100"
  - name: amount
    type: numeric(10,2)
    check: "amount > 0"
`,
    );
    const schema = parseTableFile(filePath);
    expect(schema.checks).toBeDefined();
    expect(schema.checks).toHaveLength(2);
    expect(schema.checks![0].expression).toBe("probability BETWEEN 0 AND 100");
    expect(schema.checks![1].expression).toBe("amount > 0");
  });

  it("applies column-level CHECK constraints to the database", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "with_checks.yaml",
      `table: with_checks
columns:
  - name: id
    type: serial
    primary_key: true
  - name: score
    type: integer
    check: "score >= 0 AND score <= 100"
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    // Valid insert should succeed
    await execSql(ctx.connectionString, `INSERT INTO with_checks (score) VALUES (50)`);

    // Invalid insert should fail
    let failed = false;
    try {
      await execSql(ctx.connectionString, `INSERT INTO with_checks (score) VALUES (200)`);
    } catch {
      failed = true;
    }
    expect(failed).toBe(true);
  });

  it("merges column-level checks with explicit checks array", () => {
    const filePath = path.join(ctx.project.tablesDir, "merged_checks.yaml");
    writeFileSync(
      filePath,
      `table: merged_checks
columns:
  - name: id
    type: serial
    primary_key: true
  - name: age
    type: integer
    check: "age >= 0"
checks:
  - name: chk_explicit
    expression: "id > 0"
`,
    );
    const schema = parseTableFile(filePath);
    expect(schema.checks).toHaveLength(2);
    // Explicit check first (from checks: array), then column-level
    expect(schema.checks![0].name).toBe("chk_explicit");
    expect(schema.checks![1].expression).toBe("age >= 0");
  });
});
