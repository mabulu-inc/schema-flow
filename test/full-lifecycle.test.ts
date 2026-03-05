// test/full-lifecycle.test.ts
// Full lifecycle tests: YAML → migrate → verify DB → introspect → zero-ops → modify YAML → detect drift
// Each test runs three phases sequentially on the same database.

import { describe, it, expect } from "vitest";
import pg from "pg";
import {
  useTestProject,
  writeSchema,
  execSql,
  tableExists,
  getColumns,
  constraintExists,
  fkExists,
  indexExists,
  getIndexMethod,
  triggerExists,
  rlsEnabled,
  policyExists,
  enumExists,
  getEnumValues,
  viewExists,
  materializedViewExists,
  columnIsNotNull,
  isConstraintDeferrable,
  isConstraintInitiallyDeferred,
} from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { closePool } from "../src/core/db.js";
import { runMigrate } from "../src/executor/index.js";
import {
  introspectTable,
  getExistingFunctions,
  getExistingEnums,
  getExistingViews,
  getExistingMaterializedViews,
} from "../src/introspect/index.js";
import { buildPlan } from "../src/planner/index.js";
import { detectDrift, type DriftItem } from "../src/drift/index.js";
import { logger, LogLevel } from "../src/core/logger.js";

const { Pool } = pg;

logger.setLevel(LogLevel.SILENT);

// ─── Shared helpers ──────────────────────────────────────────────────────────

function config(ctx: { connectionString: string; project: { baseDir: string } }) {
  return resolveConfig({
    connectionString: ctx.connectionString,
    baseDir: ctx.project.baseDir,
  });
}

async function migrate(ctx: { connectionString: string; project: { baseDir: string } }) {
  const result = await runMigrate(config(ctx));
  expect(result.success).toBe(true);
  return result;
}

async function assertZeroOps(
  connectionString: string,
  tables: string[],
  opts?: { enums?: boolean; views?: boolean; mvs?: boolean },
) {
  const pool = new Pool({ connectionString, max: 2 });
  const client = await pool.connect();
  try {
    const schemas = [];
    for (const t of tables) {
      schemas.push(await introspectTable(client, t, "public"));
    }
    const planOpts: Record<string, unknown> = {};
    if (opts?.enums) planOpts.enums = await getExistingEnums(client, "public");
    if (opts?.views) planOpts.views = await getExistingViews(client, "public");
    if (opts?.mvs) planOpts.materializedViews = await getExistingMaterializedViews(client, "public");
    const plan = await buildPlan(client, schemas, "public", planOpts);
    expect(plan.operations).toHaveLength(0);
    expect(plan.blocked).toHaveLength(0);
  } finally {
    client.release();
    await pool.end();
  }
}

async function drift(ctx: { connectionString: string; project: { baseDir: string } }) {
  return detectDrift(config(ctx));
}

function find(items: DriftItem[], opts: Partial<DriftItem>) {
  return items.filter((i) => Object.entries(opts).every(([k, v]) => i[k as keyof DriftItem] === v));
}

// ─── Tables & Columns ────────────────────────────────────────────────────────

describe("lifecycle — tables & columns", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("column types — serial, text, varchar, integer, numeric, boolean, timestamptz, jsonb, uuid", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_col_types.yaml",
      `
table: lc_col_types
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
  - name: label
    type: varchar(100)
    nullable: true
  - name: count
    type: integer
    nullable: true
  - name: price
    type: numeric(10,2)
    nullable: true
  - name: active
    type: boolean
    default: "true"
    nullable: true
  - name: created_at
    type: timestamptz
    default: "now()"
    nullable: true
  - name: data
    type: jsonb
    nullable: true
  - name: ref
    type: uuid
    nullable: true
`,
    );
    // Phase 1: Migrate
    await migrate(ctx);
    expect(await tableExists(ctx.connectionString, "lc_col_types")).toBe(true);
    const cols = await getColumns(ctx.connectionString, "lc_col_types");
    expect(cols).toEqual(["id", "name", "label", "count", "price", "active", "created_at", "data", "ref"]);

    // Phase 2: Zero-ops
    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_col_types"]);

    // Phase 3: Drift — change varchar(100) → varchar(255)
    writeSchema(
      ctx.project.tablesDir,
      "lc_col_types.yaml",
      `
table: lc_col_types
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
  - name: label
    type: varchar(255)
    nullable: true
  - name: count
    type: integer
    nullable: true
  - name: price
    type: numeric(10,2)
    nullable: true
  - name: active
    type: boolean
    default: "true"
    nullable: true
  - name: created_at
    type: timestamptz
    default: "now()"
    nullable: true
  - name: data
    type: jsonb
    nullable: true
  - name: ref
    type: uuid
    nullable: true
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "label" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "type")).toBe(true);
  });

  it("nullable vs NOT NULL", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_nullable.yaml",
      `
table: lc_nullable
columns:
  - name: id
    type: serial
    primary_key: true
  - name: required_col
    type: text
  - name: optional_col
    type: text
    nullable: true
`,
    );
    await migrate(ctx);
    expect(await columnIsNotNull(ctx.connectionString, "lc_nullable", "required_col")).toBe(true);
    expect(await columnIsNotNull(ctx.connectionString, "lc_nullable", "optional_col")).toBe(false);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_nullable"]);

    // Drift: remove nullable from optional_col
    writeSchema(
      ctx.project.tablesDir,
      "lc_nullable.yaml",
      `
table: lc_nullable
columns:
  - name: id
    type: serial
    primary_key: true
  - name: required_col
    type: text
  - name: optional_col
    type: text
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "optional_col" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "nullable")).toBe(true);
  });

  it("literal and expression defaults", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_defaults.yaml",
      `
table: lc_defaults
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: text
    default: "'active'"
  - name: created_at
    type: timestamptz
    default: "now()"
    nullable: true
`,
    );
    await migrate(ctx);
    // Verify defaults by inserting a row
    const res = await execSql(
      ctx.connectionString,
      `INSERT INTO lc_defaults DEFAULT VALUES RETURNING status, created_at`,
    );
    expect(res.rows[0].status).toBe("active");
    expect(res.rows[0].created_at).toBeTruthy();

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_defaults"]);

    // Drift: change default 'active' → 'pending'
    writeSchema(
      ctx.project.tablesDir,
      "lc_defaults.yaml",
      `
table: lc_defaults
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: text
    default: "'pending'"
  - name: created_at
    type: timestamptz
    default: "now()"
    nullable: true
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "status" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "default")).toBe(true);
  });

  it("generated column (a + b) STORED", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_generated.yaml",
      `
table: lc_generated
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
    await migrate(ctx);
    await execSql(ctx.connectionString, `INSERT INTO lc_generated (a, b) VALUES (3, 5)`);
    const res = await execSql(ctx.connectionString, `SELECT total FROM lc_generated`);
    expect(res.rows[0].total).toBe(8);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_generated"]);

    // Drift: change generated expression
    writeSchema(
      ctx.project.tablesDir,
      "lc_generated.yaml",
      `
table: lc_generated
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
    generated: "(a * b)"
    nullable: true
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "total" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "generated")).toBe(true);
  });
});

// ─── Constraints ─────────────────────────────────────────────────────────────

describe("lifecycle — constraints", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("single PK with custom name", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_pk_named.yaml",
      `
table: lc_pk_named
columns:
  - name: id
    type: integer
    primary_key: true
primary_key_name: pk_lc_custom
`,
    );
    await migrate(ctx);
    expect(await constraintExists(ctx.connectionString, "pk_lc_custom")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_pk_named"]);

    // Drift: rename PK
    writeSchema(
      ctx.project.tablesDir,
      "lc_pk_named.yaml",
      `
table: lc_pk_named
columns:
  - name: id
    type: integer
    primary_key: true
primary_key_name: pk_lc_renamed
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "constraint", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("composite PK", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_composite_pk.yaml",
      `
table: lc_composite_pk
columns:
  - name: a
    type: integer
  - name: b
    type: integer
primary_key:
  - a
  - b
`,
    );
    await migrate(ctx);
    expect(await tableExists(ctx.connectionString, "lc_composite_pk")).toBe(true);
    expect(await columnIsNotNull(ctx.connectionString, "lc_composite_pk", "a")).toBe(true);
    expect(await columnIsNotNull(ctx.connectionString, "lc_composite_pk", "b")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_composite_pk"]);

    // Drift: remove column b from YAML entirely (extra_in_db)
    writeSchema(
      ctx.project.tablesDir,
      "lc_composite_pk.yaml",
      `
table: lc_composite_pk
columns:
  - name: a
    type: integer
    primary_key: true
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "extra_in_db", name: "b" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("single UNIQUE with custom name", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_uq_named.yaml",
      `
table: lc_uq_named
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
    unique: true
    unique_name: uq_lc_email
`,
    );
    await migrate(ctx);
    expect(await constraintExists(ctx.connectionString, "uq_lc_email")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_uq_named"]);

    // Drift: remove unique
    writeSchema(
      ctx.project.tablesDir,
      "lc_uq_named.yaml",
      `
table: lc_uq_named
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
`,
    );
    await closePool();
    const report = await drift(ctx);
    // Should detect the unique mismatch
    const m = find(report.items, { name: "email", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("multi-column UNIQUE", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_multi_uq.yaml",
      `
table: lc_multi_uq
columns:
  - name: id
    type: serial
    primary_key: true
  - name: tenant_id
    type: integer
  - name: name
    type: text
unique_constraints:
  - name: uq_lc_tenant_name
    columns: [tenant_id, name]
`,
    );
    await migrate(ctx);
    expect(await constraintExists(ctx.connectionString, "uq_lc_tenant_name")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_multi_uq"]);

    // Drift: change columns
    writeSchema(
      ctx.project.tablesDir,
      "lc_multi_uq.yaml",
      `
table: lc_multi_uq
columns:
  - name: id
    type: serial
    primary_key: true
  - name: tenant_id
    type: integer
  - name: name
    type: text
unique_constraints:
  - name: uq_lc_tenant_name
    columns: [tenant_id]
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "constraint", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("CHECK constraint", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_check.yaml",
      `
table: lc_check
columns:
  - name: id
    type: serial
    primary_key: true
  - name: age
    type: integer
checks:
  - name: check_lc_age
    expression: "age >= 0"
`,
    );
    await migrate(ctx);
    expect(await constraintExists(ctx.connectionString, "check_lc_age")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_check"]);

    // Drift: change expression
    writeSchema(
      ctx.project.tablesDir,
      "lc_check.yaml",
      `
table: lc_check
columns:
  - name: id
    type: serial
    primary_key: true
  - name: age
    type: integer
checks:
  - name: check_lc_age
    expression: "age >= 18"
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "constraint", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("FK with CASCADE/SET NULL/DEFERRABLE", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_fk_parent.yaml",
      `
table: lc_fk_parent
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "lc_fk_child.yaml",
      `
table: lc_fk_child
columns:
  - name: id
    type: serial
    primary_key: true
  - name: parent_id
    type: integer
    references:
      table: lc_fk_parent
      column: id
      name: fk_lc_child_parent
      on_delete: CASCADE
      on_update: SET NULL
      deferrable: true
      initially_deferred: true
`,
    );
    await migrate(ctx);
    expect(await fkExists(ctx.connectionString, "fk_lc_child_parent")).toBe(true);
    expect(await isConstraintDeferrable(ctx.connectionString, "fk_lc_child_parent")).toBe(true);
    expect(await isConstraintInitiallyDeferred(ctx.connectionString, "fk_lc_child_parent")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_fk_parent", "lc_fk_child"]);

    // Drift: change on_delete
    writeSchema(
      ctx.project.tablesDir,
      "lc_fk_child.yaml",
      `
table: lc_fk_child
columns:
  - name: id
    type: serial
    primary_key: true
  - name: parent_id
    type: integer
    references:
      table: lc_fk_parent
      column: id
      name: fk_lc_child_parent
      on_delete: SET NULL
      on_update: SET NULL
      deferrable: true
      initially_deferred: true
`,
    );
    await closePool();
    const report = await drift(ctx);
    // FK on_delete drift appears as category "column" with on_delete detail
    const m = find(report.items, { category: "column", direction: "mismatch", name: "parent_id" });
    expect(m.length).toBeGreaterThanOrEqual(1);
    expect(m[0].details!.some((d) => d.field === "on_delete")).toBe(true);
  });
});

// ─── Indexes ─────────────────────────────────────────────────────────────────

describe("lifecycle — indexes", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("btree index", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_btree.yaml",
      `
table: lc_idx_btree
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
indexes:
  - name: idx_lc_btree_name
    columns: [name]
`,
    );
    await migrate(ctx);
    expect(await indexExists(ctx.connectionString, "idx_lc_btree_name")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_idx_btree"]);

    // Drift: change columns
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_btree.yaml",
      `
table: lc_idx_btree
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
indexes:
  - name: idx_lc_btree_name
    columns: [id, name]
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("GIN with opclass", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_gin.yaml",
      `
table: lc_idx_gin
columns:
  - name: id
    type: serial
    primary_key: true
  - name: data
    type: jsonb
    nullable: true
indexes:
  - name: idx_lc_gin_data
    columns: [data]
    method: gin
    opclass: jsonb_path_ops
`,
    );
    await migrate(ctx);
    expect(await indexExists(ctx.connectionString, "idx_lc_gin_data")).toBe(true);
    expect(await getIndexMethod(ctx.connectionString, "idx_lc_gin_data")).toBe("gin");

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_idx_gin"]);

    // Drift: remove opclass
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_gin.yaml",
      `
table: lc_idx_gin
columns:
  - name: id
    type: serial
    primary_key: true
  - name: data
    type: jsonb
    nullable: true
indexes:
  - name: idx_lc_gin_data
    columns: [data]
    method: gin
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("partial index (WHERE)", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_partial.yaml",
      `
table: lc_idx_partial
columns:
  - name: id
    type: serial
    primary_key: true
  - name: active
    type: boolean
    default: "true"
    nullable: true
  - name: name
    type: text
    nullable: true
indexes:
  - name: idx_lc_partial_active
    columns: [name]
    where: "(active = true)"
`,
    );
    await migrate(ctx);
    expect(await indexExists(ctx.connectionString, "idx_lc_partial_active")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_idx_partial"]);

    // Drift: change WHERE
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_partial.yaml",
      `
table: lc_idx_partial
columns:
  - name: id
    type: serial
    primary_key: true
  - name: active
    type: boolean
    default: "true"
    nullable: true
  - name: name
    type: text
    nullable: true
indexes:
  - name: idx_lc_partial_active
    columns: [name]
    where: "(active = false)"
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("covering index (INCLUDE)", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_include.yaml",
      `
table: lc_idx_include
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
  - name: name
    type: text
    nullable: true
indexes:
  - name: idx_lc_include_email
    columns: [email]
    include: [name]
`,
    );
    await migrate(ctx);
    expect(await indexExists(ctx.connectionString, "idx_lc_include_email")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_idx_include"]);

    // Drift: change INCLUDE cols
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_include.yaml",
      `
table: lc_idx_include
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
  - name: name
    type: text
    nullable: true
indexes:
  - name: idx_lc_include_email
    columns: [email]
    include: [id]
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("unique index", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_unique.yaml",
      `
table: lc_idx_unique
columns:
  - name: id
    type: serial
    primary_key: true
  - name: code
    type: text
indexes:
  - name: idx_lc_unique_code
    columns: [code]
    unique: true
`,
    );
    await migrate(ctx);
    expect(await indexExists(ctx.connectionString, "idx_lc_unique_code")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_idx_unique"]);

    // Drift: remove unique
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_unique.yaml",
      `
table: lc_idx_unique
columns:
  - name: id
    type: serial
    primary_key: true
  - name: code
    type: text
indexes:
  - name: idx_lc_unique_code
    columns: [code]
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("expression index lower(email)", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_expr.yaml",
      `
table: lc_idx_expr
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
indexes:
  - name: idx_lc_expr_lower
    columns: ["lower(email)"]
`,
    );
    await migrate(ctx);
    expect(await indexExists(ctx.connectionString, "idx_lc_expr_lower")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_idx_expr"]);

    // Drift: change to upper(email)
    writeSchema(
      ctx.project.tablesDir,
      "lc_idx_expr.yaml",
      `
table: lc_idx_expr
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
indexes:
  - name: idx_lc_expr_lower
    columns: ["upper(email)"]
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });
});

// ─── Triggers ────────────────────────────────────────────────────────────────

describe("lifecycle — triggers", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  function writeNoopFn(functionsDir: string) {
    writeSchema(
      functionsDir,
      "noop_lc.yaml",
      `
function: noop_lc
language: plpgsql
returns: trigger
replace: true
body: |
  BEGIN RETURN NEW; END;
`,
    );
  }

  it("BEFORE INSERT FOR EACH ROW", async () => {
    writeNoopFn(ctx.project.functionsDir);
    writeSchema(
      ctx.project.tablesDir,
      "lc_trg_before.yaml",
      `
table: lc_trg_before
columns:
  - name: id
    type: serial
    primary_key: true
  - name: updated_at
    type: timestamptz
    nullable: true
triggers:
  - name: trg_lc_before_insert
    timing: BEFORE
    events: [INSERT]
    function: noop_lc
    for_each: ROW
`,
    );
    await migrate(ctx);
    expect(await triggerExists(ctx.connectionString, "trg_lc_before_insert", "lc_trg_before")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_trg_before"]);

    // Drift: change timing BEFORE → AFTER
    writeSchema(
      ctx.project.tablesDir,
      "lc_trg_before.yaml",
      `
table: lc_trg_before
columns:
  - name: id
    type: serial
    primary_key: true
  - name: updated_at
    type: timestamptz
    nullable: true
triggers:
  - name: trg_lc_before_insert
    timing: AFTER
    events: [INSERT]
    function: noop_lc
    for_each: ROW
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "trigger", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("AFTER UPDATE with WHEN clause", async () => {
    writeNoopFn(ctx.project.functionsDir);
    writeSchema(
      ctx.project.tablesDir,
      "lc_trg_when.yaml",
      `
table: lc_trg_when
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: text
    default: "'active'"
triggers:
  - name: trg_lc_after_update
    timing: AFTER
    events: [UPDATE]
    function: noop_lc
    for_each: ROW
    when: "(OLD.status IS DISTINCT FROM NEW.status)"
`,
    );
    await migrate(ctx);
    expect(await triggerExists(ctx.connectionString, "trg_lc_after_update", "lc_trg_when")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_trg_when"]);

    // Drift: change WHEN clause
    writeSchema(
      ctx.project.tablesDir,
      "lc_trg_when.yaml",
      `
table: lc_trg_when
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: text
    default: "'active'"
triggers:
  - name: trg_lc_after_update
    timing: AFTER
    events: [UPDATE]
    function: noop_lc
    for_each: ROW
    when: "(OLD.id IS DISTINCT FROM NEW.id)"
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "trigger", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("FOR EACH STATEMENT", async () => {
    writeNoopFn(ctx.project.functionsDir);
    writeSchema(
      ctx.project.tablesDir,
      "lc_trg_stmt.yaml",
      `
table: lc_trg_stmt
columns:
  - name: id
    type: serial
    primary_key: true
triggers:
  - name: trg_lc_statement
    timing: AFTER
    events: [INSERT]
    function: noop_lc
    for_each: STATEMENT
`,
    );
    await migrate(ctx);
    expect(await triggerExists(ctx.connectionString, "trg_lc_statement", "lc_trg_stmt")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_trg_stmt"]);

    // Drift: change to FOR EACH ROW
    writeSchema(
      ctx.project.tablesDir,
      "lc_trg_stmt.yaml",
      `
table: lc_trg_stmt
columns:
  - name: id
    type: serial
    primary_key: true
triggers:
  - name: trg_lc_statement
    timing: AFTER
    events: [INSERT]
    function: noop_lc
    for_each: ROW
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "trigger", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });
});

// ─── RLS & Policies ──────────────────────────────────────────────────────────

describe("lifecycle — RLS & policies", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("RLS enabled + force_rls", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_rls.yaml",
      `
table: lc_rls
columns:
  - name: id
    type: serial
    primary_key: true
  - name: owner
    type: text
rls: true
force_rls: true
`,
    );
    await migrate(ctx);
    expect(await rlsEnabled(ctx.connectionString, "lc_rls")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_rls"]);

    // Drift: remove force_rls
    writeSchema(
      ctx.project.tablesDir,
      "lc_rls.yaml",
      `
table: lc_rls
columns:
  - name: id
    type: serial
    primary_key: true
  - name: owner
    type: text
rls: true
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "rls", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("PERMISSIVE policy USING + CHECK", async () => {
    // Create role first
    await execSql(
      ctx.connectionString,
      `DO $$ BEGIN CREATE ROLE test_user_lc; EXCEPTION WHEN duplicate_object THEN NULL; END $$`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "lc_pol_perm.yaml",
      `
table: lc_pol_perm
columns:
  - name: id
    type: serial
    primary_key: true
  - name: owner
    type: text
rls: true
policies:
  - name: pol_lc_owner
    for: ALL
    to: [test_user_lc]
    using: "(owner = current_user)"
    check: "(owner = current_user)"
    permissive: true
`,
    );
    await migrate(ctx);
    expect(await policyExists(ctx.connectionString, "pol_lc_owner", "lc_pol_perm")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_pol_perm"]);

    // Drift: change USING expression
    writeSchema(
      ctx.project.tablesDir,
      "lc_pol_perm.yaml",
      `
table: lc_pol_perm
columns:
  - name: id
    type: serial
    primary_key: true
  - name: owner
    type: text
rls: true
policies:
  - name: pol_lc_owner
    for: ALL
    to: [test_user_lc]
    using: "(owner = 'admin')"
    check: "(owner = current_user)"
    permissive: true
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "policy", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("RESTRICTIVE policy", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_pol_restr.yaml",
      `
table: lc_pol_restr
columns:
  - name: id
    type: serial
    primary_key: true
  - name: tenant_id
    type: integer
rls: true
policies:
  - name: pol_lc_tenant
    for: SELECT
    using: "(tenant_id = 1)"
    permissive: false
`,
    );
    await migrate(ctx);
    expect(await policyExists(ctx.connectionString, "pol_lc_tenant", "lc_pol_restr")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_pol_restr"]);

    // Drift: change for SELECT → ALL
    writeSchema(
      ctx.project.tablesDir,
      "lc_pol_restr.yaml",
      `
table: lc_pol_restr
columns:
  - name: id
    type: serial
    primary_key: true
  - name: tenant_id
    type: integer
rls: true
policies:
  - name: pol_lc_tenant
    for: ALL
    using: "(tenant_id = 1)"
    permissive: false
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "policy", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });
});

// ─── Functions ───────────────────────────────────────────────────────────────

describe("lifecycle — functions", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("SQL function with args", async () => {
    writeSchema(
      ctx.project.functionsDir,
      "add_lc.yaml",
      `
function: add_lc
language: sql
returns: integer
args: "x integer, y integer"
body: "SELECT x + y"
`,
    );
    await migrate(ctx);

    // Phase 2: verify properties via introspection
    await closePool();
    const pool = new Pool({ connectionString: ctx.connectionString, max: 2 });
    const client = await pool.connect();
    try {
      const fns = await getExistingFunctions(client, "public");
      const fn = fns.find((f) => f.routine_name === "add_lc");
      expect(fn).toBeDefined();
      expect(fn!.external_language).toBe("sql");
      expect(fn!.data_type).toBe("integer");
      expect(fn!.parameter_list).toBe("x integer, y integer");
    } finally {
      client.release();
      await pool.end();
    }

    // Phase 3: Drift — change returns
    writeSchema(
      ctx.project.functionsDir,
      "add_lc.yaml",
      `
function: add_lc
language: sql
returns: bigint
args: "x integer, y integer"
body: "SELECT (x + y)::bigint"
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "function", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("plpgsql SECURITY DEFINER", async () => {
    writeSchema(
      ctx.project.functionsDir,
      "secure_lc.yaml",
      `
function: secure_lc
language: plpgsql
returns: void
security: definer
body: |
  BEGIN NULL; END;
`,
    );
    await migrate(ctx);

    await closePool();
    const pool = new Pool({ connectionString: ctx.connectionString, max: 2 });
    const client = await pool.connect();
    try {
      const fns = await getExistingFunctions(client, "public");
      const fn = fns.find((f) => f.routine_name === "secure_lc");
      expect(fn).toBeDefined();
      expect(fn!.security_type).toBe("DEFINER");
    } finally {
      client.release();
      await pool.end();
    }

    // Drift: remove security
    writeSchema(
      ctx.project.functionsDir,
      "secure_lc.yaml",
      `
function: secure_lc
language: plpgsql
returns: void
body: |
  BEGIN NULL; END;
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "function", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("function body fidelity", async () => {
    writeSchema(
      ctx.project.functionsDir,
      "body_lc.yaml",
      `
function: body_lc
language: sql
returns: integer
args: "n integer"
body: "SELECT n * 2"
`,
    );
    await migrate(ctx);

    await closePool();
    const pool = new Pool({ connectionString: ctx.connectionString, max: 2 });
    const client = await pool.connect();
    try {
      const fns = await getExistingFunctions(client, "public");
      const fn = fns.find((f) => f.routine_name === "body_lc");
      expect(fn).toBeDefined();
      expect(fn!.routine_definition.trim()).toBe("SELECT n * 2");
    } finally {
      client.release();
      await pool.end();
    }

    // Drift: change body
    writeSchema(
      ctx.project.functionsDir,
      "body_lc.yaml",
      `
function: body_lc
language: sql
returns: integer
args: "n integer"
body: "SELECT n * 3"
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "function", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });
});

// ─── Enums ───────────────────────────────────────────────────────────────────

describe("lifecycle — enums", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("enum values", async () => {
    writeSchema(
      ctx.project.enumsDir,
      "status_lc.yaml",
      `
enum: status_lc
values: [active, inactive, pending]
`,
    );
    await migrate(ctx);
    expect(await enumExists(ctx.connectionString, "status_lc")).toBe(true);
    expect(await getEnumValues(ctx.connectionString, "status_lc")).toEqual(["active", "inactive", "pending"]);

    await closePool();
    await assertZeroOps(ctx.connectionString, [], { enums: true });

    // Drift: remove a value from YAML
    writeSchema(
      ctx.project.enumsDir,
      "status_lc.yaml",
      `
enum: status_lc
values: [active, pending]
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "enum" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });
});

// ─── Views ───────────────────────────────────────────────────────────────────

describe("lifecycle — views", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("view query", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_view_src.yaml",
      `
table: lc_view_src
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
`,
    );
    writeSchema(
      ctx.project.viewsDir,
      "lc_names.yaml",
      `
view: lc_names
query: "SELECT id, name FROM lc_view_src"
`,
    );
    await migrate(ctx);
    expect(await viewExists(ctx.connectionString, "lc_names")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_view_src"], { views: true });

    // Drift: change query
    writeSchema(
      ctx.project.viewsDir,
      "lc_names.yaml",
      `
view: lc_names
query: "SELECT id FROM lc_view_src"
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "view", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });
});

// ─── Materialized Views ──────────────────────────────────────────────────────

describe("lifecycle — materialized views", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("MV query", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_mv_src.yaml",
      `
table: lc_mv_src
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
      "mv_lc_totals.yaml",
      `
materialized_view: lc_totals
query: "SELECT count(*) AS cnt FROM lc_mv_src"
`,
    );
    await migrate(ctx);
    expect(await materializedViewExists(ctx.connectionString, "lc_totals")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_mv_src"], { mvs: true });

    // Drift: change query
    writeSchema(
      ctx.project.viewsDir,
      "mv_lc_totals.yaml",
      `
materialized_view: lc_totals
query: "SELECT sum(val) AS total FROM lc_mv_src"
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "materialized_view", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });

  it("MV with indexes", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "lc_mv_idx_src.yaml",
      `
table: lc_mv_idx_src
columns:
  - name: id
    type: serial
    primary_key: true
  - name: category
    type: text
    nullable: true
`,
    );
    writeSchema(
      ctx.project.viewsDir,
      "mv_lc_cats.yaml",
      `
materialized_view: lc_cats
query: "SELECT DISTINCT category FROM lc_mv_idx_src"
indexes:
  - name: idx_mv_lc_cats_category
    columns: [category]
    unique: true
`,
    );
    await migrate(ctx);
    expect(await materializedViewExists(ctx.connectionString, "lc_cats")).toBe(true);
    expect(await indexExists(ctx.connectionString, "idx_mv_lc_cats_category")).toBe(true);

    await closePool();
    await assertZeroOps(ctx.connectionString, ["lc_mv_idx_src"], { mvs: true });

    // Drift: remove index from YAML
    writeSchema(
      ctx.project.viewsDir,
      "mv_lc_cats.yaml",
      `
materialized_view: lc_cats
query: "SELECT DISTINCT category FROM lc_mv_idx_src"
`,
    );
    await closePool();
    const report = await drift(ctx);
    const m = find(report.items, { category: "index" });
    expect(m.length).toBeGreaterThanOrEqual(1);
  });
});
