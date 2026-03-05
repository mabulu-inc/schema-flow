// test/alter-lifecycle.test.ts
// Alter-path & destructive-path lifecycle tests: YAML v1 → migrate → YAML v2 → migrate → verify
// Proves schema-flow can evolve an existing database via declarative schema changes.

import { describe, it, expect } from "vitest";
import {
  useTestProject,
  writeSchema,
  execSql,
  getColumns,
  constraintExists,
  indexExists,
  triggerExists,
  rlsEnabled,
  policyExists,
  getEnumValues,
  viewExists,
  materializedViewExists,
  columnIsNotNull,
  getComment,
} from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { closePool } from "../src/core/db.js";
import { runMigrate, type ExecutionResult } from "../src/executor/index.js";
import { logger, LogLevel } from "../src/core/logger.js";
logger.setLevel(LogLevel.SILENT);

// ─── Shared helpers ──────────────────────────────────────────────────────────

type Ctx = { connectionString: string; project: { baseDir: string; schemaDir: string } };

async function migrateWith(ctx: Ctx, opts?: { allowDestructive?: boolean }): Promise<ExecutionResult> {
  const cfg = resolveConfig({
    connectionString: ctx.connectionString,
    baseDir: ctx.project.baseDir,
    allowDestructive: opts?.allowDestructive,
  });
  return runMigrate(cfg);
}

async function migrateOk(ctx: Ctx, opts?: { allowDestructive?: boolean }) {
  const result = await migrateWith(ctx, opts);
  expect(result.success).toBe(true);
  return result;
}

// ─── Column Alterations ──────────────────────────────────────────────────────

describe("alter — column alterations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("add column to existing table", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_add_col.yaml",
      `
table: alt_add_col
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    await migrateOk(ctx);
    expect(await getColumns(ctx.connectionString, "alt_add_col")).toEqual(["id", "name"]);

    // v2: add email column
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_add_col.yaml",
      `
table: alt_add_col
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
  - name: email
    type: text
    nullable: true
`,
    );
    await migrateOk(ctx);
    expect(await getColumns(ctx.connectionString, "alt_add_col")).toEqual(["id", "name", "email"]);
  });

  it("widen column type varchar(50) → varchar(255)", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_widen.yaml",
      `
table: alt_widen
columns:
  - name: id
    type: serial
    primary_key: true
  - name: label
    type: varchar(50)
    nullable: true
`,
    );
    await migrateOk(ctx);

    // v2: widen
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_widen.yaml",
      `
table: alt_widen
columns:
  - name: id
    type: serial
    primary_key: true
  - name: label
    type: varchar(255)
    nullable: true
`,
    );
    await migrateOk(ctx);

    // Verify type changed — insert a 100-char string (would fail with varchar(50))
    const longStr = "x".repeat(100);
    await execSql(ctx.connectionString, `INSERT INTO alt_widen (label) VALUES ($1)`, [longStr]);
    const res = await execSql(ctx.connectionString, `SELECT label FROM alt_widen`);
    expect(res.rows[0].label).toBe(longStr);
  });

  it("change default value", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_default.yaml",
      `
table: alt_default
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: text
    default: "'active'"
`,
    );
    await migrateOk(ctx);
    let res = await execSql(ctx.connectionString, `INSERT INTO alt_default DEFAULT VALUES RETURNING status`);
    expect(res.rows[0].status).toBe("active");

    // v2: change default
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_default.yaml",
      `
table: alt_default
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: text
    default: "'pending'"
`,
    );
    await migrateOk(ctx);
    res = await execSql(ctx.connectionString, `INSERT INTO alt_default DEFAULT VALUES RETURNING status`);
    expect(res.rows[0].status).toBe("pending");
  });

  it("drop NOT NULL (widen)", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_drop_nn.yaml",
      `
table: alt_drop_nn
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    await migrateOk(ctx);
    expect(await columnIsNotNull(ctx.connectionString, "alt_drop_nn", "name")).toBe(true);

    // v2: make nullable
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_drop_nn.yaml",
      `
table: alt_drop_nn
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
`,
    );
    await migrateOk(ctx);
    expect(await columnIsNotNull(ctx.connectionString, "alt_drop_nn", "name")).toBe(false);
  });

  it("set NOT NULL (safe pattern)", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_set_nn.yaml",
      `
table: alt_set_nn
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
`,
    );
    await migrateOk(ctx);
    expect(await columnIsNotNull(ctx.connectionString, "alt_set_nn", "name")).toBe(false);

    // v2: set NOT NULL
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_set_nn.yaml",
      `
table: alt_set_nn
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    await migrateOk(ctx);
    expect(await columnIsNotNull(ctx.connectionString, "alt_set_nn", "name")).toBe(true);
  });

  it("add generated column", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_gen.yaml",
      `
table: alt_gen
columns:
  - name: id
    type: serial
    primary_key: true
  - name: a
    type: integer
    default: "0"
  - name: b
    type: integer
    default: "0"
`,
    );
    await migrateOk(ctx);

    // v2: add generated column
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_gen.yaml",
      `
table: alt_gen
columns:
  - name: id
    type: serial
    primary_key: true
  - name: a
    type: integer
    default: "0"
  - name: b
    type: integer
    default: "0"
  - name: total
    type: integer
    generated: "(a + b)"
    nullable: true
`,
    );
    await migrateOk(ctx);
    await execSql(ctx.connectionString, `INSERT INTO alt_gen (a, b) VALUES (3, 7)`);
    const res = await execSql(ctx.connectionString, `SELECT total FROM alt_gen`);
    expect(res.rows[0].total).toBe(10);
  });
});

// ─── Destructive Column Operations ──────────────────────────────────────────

describe("alter — destructive column operations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("drop column blocked without allowDestructive", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_drop_col.yaml",
      `
table: alt_drop_col
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
  - name: extra
    type: text
    nullable: true
`,
    );
    await migrateOk(ctx);
    expect(await getColumns(ctx.connectionString, "alt_drop_col")).toEqual(["id", "name", "extra"]);

    // v2: remove extra — should be blocked
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_drop_col.yaml",
      `
table: alt_drop_col
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    const result = await migrateWith(ctx);
    expect(result.blockedDestructive).toBeGreaterThan(0);
    // Column should still exist
    expect(await getColumns(ctx.connectionString, "alt_drop_col")).toEqual(["id", "name", "extra"]);
  });

  it("drop column allowed with allowDestructive", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_drop_col2.yaml",
      `
table: alt_drop_col2
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
  - name: extra
    type: text
    nullable: true
`,
    );
    await migrateOk(ctx);
    expect(await getColumns(ctx.connectionString, "alt_drop_col2")).toEqual(["id", "name", "extra"]);

    // v2: remove extra with allowDestructive
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_drop_col2.yaml",
      `
table: alt_drop_col2
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    await migrateOk(ctx, { allowDestructive: true });
    expect(await getColumns(ctx.connectionString, "alt_drop_col2")).toEqual(["id", "name"]);
  });

  it("narrow type blocked without allowDestructive", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_narrow.yaml",
      `
table: alt_narrow
columns:
  - name: id
    type: serial
    primary_key: true
  - name: label
    type: varchar(255)
    nullable: true
`,
    );
    await migrateOk(ctx);

    // v2: narrow varchar(255) → varchar(50) — should be blocked
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_narrow.yaml",
      `
table: alt_narrow
columns:
  - name: id
    type: serial
    primary_key: true
  - name: label
    type: varchar(50)
    nullable: true
`,
    );
    const result = await migrateWith(ctx);
    expect(result.blockedDestructive).toBeGreaterThan(0);
  });
});

// ─── Index Alterations ──────────────────────────────────────────────────────

describe("alter — index alterations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("add index to existing table", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_idx.yaml",
      `
table: alt_idx
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    await migrateOk(ctx);
    expect(await indexExists(ctx.connectionString, "idx_alt_name")).toBe(false);

    // v2: add index
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_idx.yaml",
      `
table: alt_idx
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
indexes:
  - name: idx_alt_name
    columns: [name]
`,
    );
    await migrateOk(ctx);
    expect(await indexExists(ctx.connectionString, "idx_alt_name")).toBe(true);
  });

  it("add unique index to existing table", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_uidx.yaml",
      `
table: alt_uidx
columns:
  - name: id
    type: serial
    primary_key: true
  - name: code
    type: text
`,
    );
    await migrateOk(ctx);

    // v2: add unique index
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_uidx.yaml",
      `
table: alt_uidx
columns:
  - name: id
    type: serial
    primary_key: true
  - name: code
    type: text
indexes:
  - name: idx_alt_unique_code
    columns: [code]
    unique: true
`,
    );
    await migrateOk(ctx);
    expect(await indexExists(ctx.connectionString, "idx_alt_unique_code")).toBe(true);

    // Verify uniqueness enforced
    await execSql(ctx.connectionString, `INSERT INTO alt_uidx (code) VALUES ('abc')`);
    await expect(execSql(ctx.connectionString, `INSERT INTO alt_uidx (code) VALUES ('abc')`)).rejects.toThrow();
  });

  it("change index columns (drop + recreate)", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_idx_cols.yaml",
      `
table: alt_idx_cols
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
indexes:
  - name: idx_alt_cols
    columns: [name]
`,
    );
    await migrateOk(ctx);
    expect(await indexExists(ctx.connectionString, "idx_alt_cols")).toBe(true);

    // v2: change index columns
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_idx_cols.yaml",
      `
table: alt_idx_cols
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
indexes:
  - name: idx_alt_cols
    columns: [name, id]
`,
    );
    await migrateOk(ctx, { allowDestructive: true });
    expect(await indexExists(ctx.connectionString, "idx_alt_cols")).toBe(true);
  });

  it("drop index blocked without allowDestructive", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_idx_drop.yaml",
      `
table: alt_idx_drop
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
indexes:
  - name: idx_alt_drop_name
    columns: [name]
`,
    );
    await migrateOk(ctx);
    expect(await indexExists(ctx.connectionString, "idx_alt_drop_name")).toBe(true);

    // v2: remove index — should be blocked
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_idx_drop.yaml",
      `
table: alt_idx_drop
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    const blocked = await migrateWith(ctx);
    expect(blocked.blockedDestructive).toBeGreaterThan(0);
    expect(await indexExists(ctx.connectionString, "idx_alt_drop_name")).toBe(true);
  });

  it("drop index allowed with allowDestructive", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_idx_drop2.yaml",
      `
table: alt_idx_drop2
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
indexes:
  - name: idx_alt_drop2_name
    columns: [name]
`,
    );
    await migrateOk(ctx);
    expect(await indexExists(ctx.connectionString, "idx_alt_drop2_name")).toBe(true);

    // v2: remove index with allowDestructive
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_idx_drop2.yaml",
      `
table: alt_idx_drop2
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    await migrateOk(ctx, { allowDestructive: true });
    expect(await indexExists(ctx.connectionString, "idx_alt_drop2_name")).toBe(false);
  });
});

// ─── Constraint Alterations ─────────────────────────────────────────────────

describe("alter — constraint alterations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("add CHECK to existing table", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_check.yaml",
      `
table: alt_check
columns:
  - name: id
    type: serial
    primary_key: true
  - name: age
    type: integer
`,
    );
    await migrateOk(ctx);

    // v2: add CHECK
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_check.yaml",
      `
table: alt_check
columns:
  - name: id
    type: serial
    primary_key: true
  - name: age
    type: integer
checks:
  - name: chk_alt_age
    expression: "age >= 0"
`,
    );
    await migrateOk(ctx);
    expect(await constraintExists(ctx.connectionString, "chk_alt_age")).toBe(true);

    // Verify constraint enforced
    await expect(execSql(ctx.connectionString, `INSERT INTO alt_check (age) VALUES (-1)`)).rejects.toThrow();
  });

  it("add FK to existing table", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_fk_parent.yaml",
      `
table: alt_fk_parent
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "alt_fk_child.yaml",
      `
table: alt_fk_child
columns:
  - name: id
    type: serial
    primary_key: true
  - name: parent_id
    type: integer
    nullable: true
`,
    );
    await migrateOk(ctx);

    // v2: add FK reference
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_fk_child.yaml",
      `
table: alt_fk_child
columns:
  - name: id
    type: serial
    primary_key: true
  - name: parent_id
    type: integer
    nullable: true
    references:
      table: alt_fk_parent
      column: id
      name: fk_alt_child_parent
`,
    );
    await migrateOk(ctx);

    // Verify FK enforced — insert valid parent first
    await execSql(ctx.connectionString, `INSERT INTO alt_fk_parent (id) VALUES (1)`);
    await execSql(ctx.connectionString, `INSERT INTO alt_fk_child (parent_id) VALUES (1)`);
    // Invalid FK should fail
    await expect(execSql(ctx.connectionString, `INSERT INTO alt_fk_child (parent_id) VALUES (999)`)).rejects.toThrow();
  });

  it("add multi-column unique constraint", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_multi_uq.yaml",
      `
table: alt_multi_uq
columns:
  - name: id
    type: serial
    primary_key: true
  - name: tenant_id
    type: integer
  - name: name
    type: text
`,
    );
    await migrateOk(ctx);

    // v2: add multi-col unique
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_multi_uq.yaml",
      `
table: alt_multi_uq
columns:
  - name: id
    type: serial
    primary_key: true
  - name: tenant_id
    type: integer
  - name: name
    type: text
unique_constraints:
  - name: uq_alt_tenant_name
    columns: [tenant_id, name]
`,
    );
    await migrateOk(ctx);
    expect(await constraintExists(ctx.connectionString, "uq_alt_tenant_name")).toBe(true);

    // Verify uniqueness enforced
    await execSql(ctx.connectionString, `INSERT INTO alt_multi_uq (tenant_id, name) VALUES (1, 'foo')`);
    await expect(
      execSql(ctx.connectionString, `INSERT INTO alt_multi_uq (tenant_id, name) VALUES (1, 'foo')`),
    ).rejects.toThrow();
  });

  it("add single-column unique", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_single_uq.yaml",
      `
table: alt_single_uq
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
`,
    );
    await migrateOk(ctx);

    // v2: add unique: true
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_single_uq.yaml",
      `
table: alt_single_uq
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
    unique: true
    unique_name: uq_alt_email
`,
    );
    await migrateOk(ctx);
    expect(await constraintExists(ctx.connectionString, "uq_alt_email")).toBe(true);

    // Verify uniqueness enforced
    await execSql(ctx.connectionString, `INSERT INTO alt_single_uq (email) VALUES ('a@b.com')`);
    await expect(
      execSql(ctx.connectionString, `INSERT INTO alt_single_uq (email) VALUES ('a@b.com')`),
    ).rejects.toThrow();
  });
});

// ─── Trigger Alterations ────────────────────────────────────────────────────

describe("alter — trigger alterations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  function writeNoopFn() {
    writeSchema(
      ctx.project.functionsDir,
      "alt_noop.yaml",
      `
function: alt_noop
language: plpgsql
returns: trigger
replace: true
body: |
  BEGIN RETURN NEW; END;
`,
    );
  }

  it("add trigger to existing table", async () => {
    writeNoopFn();
    writeSchema(
      ctx.project.tablesDir,
      "alt_trg.yaml",
      `
table: alt_trg
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
`,
    );
    await migrateOk(ctx);
    expect(await triggerExists(ctx.connectionString, "trg_alt_before", "alt_trg")).toBe(false);

    // v2: add trigger
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_trg.yaml",
      `
table: alt_trg
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
triggers:
  - name: trg_alt_before
    timing: BEFORE
    events: [INSERT]
    function: alt_noop
    for_each: ROW
`,
    );
    await migrateOk(ctx);
    expect(await triggerExists(ctx.connectionString, "trg_alt_before", "alt_trg")).toBe(true);
  });

  it("change trigger timing BEFORE → AFTER", async () => {
    writeNoopFn();
    writeSchema(
      ctx.project.tablesDir,
      "alt_trg_timing.yaml",
      `
table: alt_trg_timing
columns:
  - name: id
    type: serial
    primary_key: true
triggers:
  - name: trg_alt_timing
    timing: BEFORE
    events: [INSERT]
    function: alt_noop
    for_each: ROW
`,
    );
    await migrateOk(ctx);

    // Verify timing via pg_trigger
    let res = await execSql(
      ctx.connectionString,
      `SELECT t.tgtype FROM pg_trigger t
       JOIN pg_class c ON t.tgrelid = c.oid
       WHERE t.tgname = 'trg_alt_timing' AND c.relname = 'alt_trg_timing'`,
    );
    const beforeType = res.rows[0].tgtype;

    // v2: change to AFTER
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_trg_timing.yaml",
      `
table: alt_trg_timing
columns:
  - name: id
    type: serial
    primary_key: true
triggers:
  - name: trg_alt_timing
    timing: AFTER
    events: [INSERT]
    function: alt_noop
    for_each: ROW
`,
    );
    await migrateOk(ctx);

    res = await execSql(
      ctx.connectionString,
      `SELECT t.tgtype FROM pg_trigger t
       JOIN pg_class c ON t.tgrelid = c.oid
       WHERE t.tgname = 'trg_alt_timing' AND c.relname = 'alt_trg_timing'`,
    );
    const afterType = res.rows[0].tgtype;
    expect(afterType).not.toBe(beforeType);
  });
});

// ─── RLS & Policy Alterations ───────────────────────────────────────────────

describe("alter — RLS & policy alterations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("enable RLS on existing table", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_rls.yaml",
      `
table: alt_rls
columns:
  - name: id
    type: serial
    primary_key: true
  - name: owner
    type: text
`,
    );
    await migrateOk(ctx);
    expect(await rlsEnabled(ctx.connectionString, "alt_rls")).toBe(false);

    // v2: enable RLS
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_rls.yaml",
      `
table: alt_rls
columns:
  - name: id
    type: serial
    primary_key: true
  - name: owner
    type: text
rls: true
`,
    );
    await migrateOk(ctx);
    expect(await rlsEnabled(ctx.connectionString, "alt_rls")).toBe(true);
  });

  it("add policy to existing table", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_pol.yaml",
      `
table: alt_pol
columns:
  - name: id
    type: serial
    primary_key: true
  - name: owner
    type: text
rls: true
`,
    );
    await migrateOk(ctx);
    expect(await rlsEnabled(ctx.connectionString, "alt_pol")).toBe(true);

    // v2: add policy
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_pol.yaml",
      `
table: alt_pol
columns:
  - name: id
    type: serial
    primary_key: true
  - name: owner
    type: text
rls: true
policies:
  - name: pol_alt_owner
    for: ALL
    using: "(owner = current_user)"
    check: "(owner = current_user)"
`,
    );
    await migrateOk(ctx);
    expect(await policyExists(ctx.connectionString, "pol_alt_owner", "alt_pol")).toBe(true);
  });

  it("disable RLS blocked without allowDestructive", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_rls_off.yaml",
      `
table: alt_rls_off
columns:
  - name: id
    type: serial
    primary_key: true
rls: true
`,
    );
    await migrateOk(ctx);
    expect(await rlsEnabled(ctx.connectionString, "alt_rls_off")).toBe(true);

    // v2: remove rls — should be blocked
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_rls_off.yaml",
      `
table: alt_rls_off
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );
    const result = await migrateWith(ctx);
    expect(result.blockedDestructive).toBeGreaterThan(0);
    expect(await rlsEnabled(ctx.connectionString, "alt_rls_off")).toBe(true);
  });
});

// ─── Enum Alterations ───────────────────────────────────────────────────────

describe("alter — enum alterations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("add enum value", async () => {
    writeSchema(
      ctx.project.enumsDir,
      "alt_status.yaml",
      `
enum: alt_status
values: [a, b, c]
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "alt_enum_tbl.yaml",
      `
table: alt_enum_tbl
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: alt_status
    nullable: true
`,
    );
    await migrateOk(ctx);
    expect(await getEnumValues(ctx.connectionString, "alt_status")).toEqual(["a", "b", "c"]);

    // v2: add value 'd'
    await closePool();
    writeSchema(
      ctx.project.enumsDir,
      "alt_status.yaml",
      `
enum: alt_status
values: [a, b, c, d]
`,
    );
    await migrateOk(ctx);
    expect(await getEnumValues(ctx.connectionString, "alt_status")).toEqual(["a", "b", "c", "d"]);

    // Verify new value usable
    await execSql(ctx.connectionString, `INSERT INTO alt_enum_tbl (status) VALUES ('d')`);
    const res = await execSql(ctx.connectionString, `SELECT status FROM alt_enum_tbl`);
    expect(res.rows[0].status).toBe("d");
  });

  it("create enum + use in table column", async () => {
    writeSchema(
      ctx.project.enumsDir,
      "alt_role.yaml",
      `
enum: alt_role
values: [admin, user, guest]
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "alt_role_tbl.yaml",
      `
table: alt_role_tbl
columns:
  - name: id
    type: serial
    primary_key: true
  - name: role
    type: alt_role
    nullable: true
`,
    );
    await migrateOk(ctx);

    // Insert enum value
    await execSql(ctx.connectionString, `INSERT INTO alt_role_tbl (role) VALUES ('admin')`);
    const res = await execSql(ctx.connectionString, `SELECT role FROM alt_role_tbl`);
    expect(res.rows[0].role).toBe("admin");

    // Invalid enum value should fail
    await expect(
      execSql(ctx.connectionString, `INSERT INTO alt_role_tbl (role) VALUES ('superadmin')`),
    ).rejects.toThrow();
  });
});

// ─── View Alterations ───────────────────────────────────────────────────────

describe("alter — view alterations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("change view query", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_view_src.yaml",
      `
table: alt_view_src
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
  - name: active
    type: boolean
    default: "true"
    nullable: true
`,
    );
    writeSchema(
      ctx.project.viewsDir,
      "alt_names.yaml",
      `
view: alt_names
query: "SELECT id, name FROM alt_view_src"
`,
    );
    await migrateOk(ctx);
    expect(await viewExists(ctx.connectionString, "alt_names")).toBe(true);

    // Insert test data
    await execSql(ctx.connectionString, `INSERT INTO alt_view_src (name) VALUES ('Alice')`);
    let res = await execSql(ctx.connectionString, `SELECT * FROM alt_names`);
    expect(res.rows[0].name).toBe("Alice");

    // v2: change view query (same columns, different filter) — CREATE OR REPLACE VIEW
    // supports changing the query as long as column names/types stay the same
    await closePool();
    writeSchema(
      ctx.project.viewsDir,
      "alt_names.yaml",
      `
view: alt_names
query: "SELECT id, name FROM alt_view_src WHERE active = true"
`,
    );
    await migrateOk(ctx);

    // Insert an inactive row
    await execSql(ctx.connectionString, `INSERT INTO alt_view_src (name, active) VALUES ('Bob', false)`);
    res = await execSql(ctx.connectionString, `SELECT * FROM alt_names`);
    // Only Alice (active=true) should be visible
    expect(res.rows).toHaveLength(1);
    expect(res.rows[0].name).toBe("Alice");
  });

  it("change materialized view query", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_mv_src.yaml",
      `
table: alt_mv_src
columns:
  - name: id
    type: serial
    primary_key: true
  - name: val
    type: integer
    nullable: true
`,
    );
    writeSchema(
      ctx.project.viewsDir,
      "mv_alt_stats.yaml",
      `
materialized_view: alt_stats
query: "SELECT count(*) AS cnt FROM alt_mv_src"
`,
    );
    await migrateOk(ctx);
    expect(await materializedViewExists(ctx.connectionString, "alt_stats")).toBe(true);

    // Insert data
    await execSql(ctx.connectionString, `INSERT INTO alt_mv_src (val) VALUES (10), (20)`);

    // v2: change MV query
    await closePool();
    writeSchema(
      ctx.project.viewsDir,
      "mv_alt_stats.yaml",
      `
materialized_view: alt_stats
query: "SELECT sum(val) AS total FROM alt_mv_src"
`,
    );
    await migrateOk(ctx);

    // Refresh MV and verify
    await execSql(ctx.connectionString, `REFRESH MATERIALIZED VIEW alt_stats`);
    const res = await execSql(ctx.connectionString, `SELECT total FROM alt_stats`);
    expect(Number(res.rows[0].total)).toBe(30);
  });
});

// ─── Function Alterations ───────────────────────────────────────────────────

describe("alter — function alterations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("change function body", async () => {
    writeSchema(
      ctx.project.functionsDir,
      "alt_double.yaml",
      `
function: alt_double
language: sql
returns: integer
args: "n integer"
body: "SELECT n * 2"
`,
    );
    await migrateOk(ctx);
    let res = await execSql(ctx.connectionString, `SELECT alt_double(5) AS result`);
    expect(res.rows[0].result).toBe(10);

    // v2: change body
    await closePool();
    writeSchema(
      ctx.project.functionsDir,
      "alt_double.yaml",
      `
function: alt_double
language: sql
returns: integer
args: "n integer"
body: "SELECT n * 3"
`,
    );
    await migrateOk(ctx);
    res = await execSql(ctx.connectionString, `SELECT alt_double(5) AS result`);
    expect(res.rows[0].result).toBe(15);
  });

  it("change function security invoker → definer", async () => {
    writeSchema(
      ctx.project.functionsDir,
      "alt_sec.yaml",
      `
function: alt_sec
language: plpgsql
returns: void
body: |
  BEGIN NULL; END;
`,
    );
    await migrateOk(ctx);

    // Verify invoker by default
    let res = await execSql(
      ctx.connectionString,
      `SELECT p.prosecdef FROM pg_proc p
       JOIN pg_namespace n ON p.pronamespace = n.oid
       WHERE n.nspname = 'public' AND p.proname = 'alt_sec'`,
    );
    expect(res.rows[0].prosecdef).toBe(false);

    // v2: change to definer
    await closePool();
    writeSchema(
      ctx.project.functionsDir,
      "alt_sec.yaml",
      `
function: alt_sec
language: plpgsql
returns: void
security: definer
body: |
  BEGIN NULL; END;
`,
    );
    await migrateOk(ctx);

    res = await execSql(
      ctx.connectionString,
      `SELECT p.prosecdef FROM pg_proc p
       JOIN pg_namespace n ON p.pronamespace = n.oid
       WHERE n.nspname = 'public' AND p.proname = 'alt_sec'`,
    );
    expect(res.rows[0].prosecdef).toBe(true);
  });
});

// ─── Comment Operations ─────────────────────────────────────────────────────

describe("alter — comment operations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("add table + column comments", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_comments.yaml",
      `
table: alt_comments
comment: "Table for testing comments"
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    comment: "The user name"
`,
    );
    await migrateOk(ctx);

    expect(await getComment(ctx.connectionString, "alt_comments")).toBe("Table for testing comments");
    expect(await getComment(ctx.connectionString, "alt_comments", "name")).toBe("The user name");
  });

  it("change comment", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "alt_comment_chg.yaml",
      `
table: alt_comment_chg
comment: "v1 comment"
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );
    await migrateOk(ctx);
    expect(await getComment(ctx.connectionString, "alt_comment_chg")).toBe("v1 comment");

    // v2: change comment
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "alt_comment_chg.yaml",
      `
table: alt_comment_chg
comment: "v2 comment"
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );
    await migrateOk(ctx);
    expect(await getComment(ctx.connectionString, "alt_comment_chg")).toBe("v2 comment");
  });
});
