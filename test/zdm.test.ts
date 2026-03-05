// test/zdm.test.ts
// Zero-Downtime Migration tests: prove that migrations don't block concurrent reads/writes.
// Each test seeds a table, runs an "application workload" concurrently with a migration,
// and asserts the workload completes without timeout errors.

import { describe, it, expect } from "vitest";
import pg from "pg";
import {
  useTestProject,
  writeSchema,
  execSql,
  indexExists,
  constraintExists,
  columnIsNotNull,
  getColumns,
  fkExists,
} from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { closePool } from "../src/core/db.js";
import { runMigrate, type ExecutionResult } from "../src/executor/index.js";
import { logger, LogLevel } from "../src/core/logger.js";

const { Pool } = pg;

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

/**
 * Seed `count` rows into a table using a batch INSERT.
 */
async function seedRows(connStr: string, table: string, count: number, valueFn: (i: number) => string) {
  const batchSize = 500;
  for (let offset = 0; offset < count; offset += batchSize) {
    const end = Math.min(offset + batchSize, count);
    const values = Array.from({ length: end - offset }, (_, j) => valueFn(offset + j)).join(",");
    await execSql(connStr, `INSERT INTO ${table} VALUES ${values}`);
  }
}

/**
 * Run an "application workload" concurrently with a migration.
 * The workload does INSERT + SELECT in a loop until the migration finishes.
 * Uses a short statement_timeout so it fails fast if blocked by a lock.
 */
async function runWorkloadDuringMigration(
  ctx: Ctx,
  opts: {
    tableName: string;
    insertSql: string;
    selectSql: string;
    allowDestructive?: boolean;
  },
): Promise<{ migrationResult: ExecutionResult; workloadErrors: Error[] }> {
  const workloadErrors: Error[] = [];
  const workloadPool = new Pool({
    connectionString: ctx.connectionString,
    max: 2,
    statement_timeout: 2000,
  });

  let migrationDone = false;
  const workload = (async () => {
    while (!migrationDone) {
      try {
        await workloadPool.query(opts.insertSql);
        await workloadPool.query(opts.selectSql);
      } catch (err) {
        workloadErrors.push(err as Error);
      }
      await new Promise((r) => setTimeout(r, 10));
    }
  })();

  const migrationResult = await migrateWith(ctx, {
    allowDestructive: opts.allowDestructive,
  });
  migrationDone = true;
  await workload;
  await workloadPool.end();

  return { migrationResult, workloadErrors };
}

// ─── ZDM Tests ───────────────────────────────────────────────────────────────

describe("zdm — zero-downtime migrations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("add FK uses NOT VALID pattern — no lock on concurrent writes", async () => {
    // v1: parent + child tables, no FK
    writeSchema(
      ctx.project.tablesDir,
      "zdm_fk_parent.yaml",
      `
table: zdm_fk_parent
columns:
  - name: id
    type: serial
    primary_key: true
  - name: label
    type: text
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "zdm_fk_child.yaml",
      `
table: zdm_fk_child
columns:
  - name: id
    type: serial
    primary_key: true
  - name: parent_id
    type: integer
  - name: value
    type: text
`,
    );
    await migrateOk(ctx);

    // Seed parent rows, then child rows referencing valid parents
    await seedRows(ctx.connectionString, "zdm_fk_parent", 100, (i) => `(DEFAULT, 'parent_${i}')`);
    await seedRows(ctx.connectionString, "zdm_fk_child", 1000, (i) => `(DEFAULT, ${(i % 100) + 1}, 'val_${i}')`);

    // v2: add FK on child.parent_id → parent.id
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "zdm_fk_child.yaml",
      `
table: zdm_fk_child
columns:
  - name: id
    type: serial
    primary_key: true
  - name: parent_id
    type: integer
    references:
      table: zdm_fk_parent
      column: id
      name: fk_zdm_child_parent
  - name: value
    type: text
`,
    );

    const { migrationResult, workloadErrors } = await runWorkloadDuringMigration(ctx, {
      tableName: "zdm_fk_child",
      insertSql: "INSERT INTO zdm_fk_child VALUES (DEFAULT, 1, 'wl')",
      selectSql: "SELECT * FROM zdm_fk_child LIMIT 1",
    });

    expect(workloadErrors).toHaveLength(0);
    expect(migrationResult.success).toBe(true);
    expect(await fkExists(ctx.connectionString, "fk_zdm_child_parent")).toBe(true);
  });

  it("add CHECK constraint uses NOT VALID pattern — no lock", async () => {
    // v1: table with age column, no check
    writeSchema(
      ctx.project.tablesDir,
      "zdm_check.yaml",
      `
table: zdm_check
columns:
  - name: id
    type: serial
    primary_key: true
  - name: age
    type: integer
`,
    );
    await migrateOk(ctx);
    await seedRows(ctx.connectionString, "zdm_check", 1000, (i) => `(DEFAULT, ${i + 1})`);

    // v2: add CHECK constraint
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "zdm_check.yaml",
      `
table: zdm_check
columns:
  - name: id
    type: serial
    primary_key: true
  - name: age
    type: integer
checks:
  - name: chk_zdm_age
    expression: "age >= 0"
`,
    );

    const { migrationResult, workloadErrors } = await runWorkloadDuringMigration(ctx, {
      tableName: "zdm_check",
      insertSql: "INSERT INTO zdm_check VALUES (DEFAULT, 1)",
      selectSql: "SELECT * FROM zdm_check LIMIT 1",
    });

    expect(workloadErrors).toHaveLength(0);
    expect(migrationResult.success).toBe(true);
    expect(await constraintExists(ctx.connectionString, "chk_zdm_age")).toBe(true);
  });

  it("set NOT NULL uses safe pattern — no lock", async () => {
    // v1: nullable name column
    writeSchema(
      ctx.project.tablesDir,
      "zdm_notnull.yaml",
      `
table: zdm_notnull
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
    await seedRows(ctx.connectionString, "zdm_notnull", 1000, (i) => `(DEFAULT, 'name_${i}')`);

    // v2: make NOT NULL
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "zdm_notnull.yaml",
      `
table: zdm_notnull
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    const { migrationResult, workloadErrors } = await runWorkloadDuringMigration(ctx, {
      tableName: "zdm_notnull",
      insertSql: "INSERT INTO zdm_notnull VALUES (DEFAULT, 'x')",
      selectSql: "SELECT * FROM zdm_notnull LIMIT 1",
    });

    expect(workloadErrors).toHaveLength(0);
    expect(migrationResult.success).toBe(true);
    expect(await columnIsNotNull(ctx.connectionString, "zdm_notnull", "name")).toBe(true);
  });

  it("create index uses CONCURRENTLY — no lock", async () => {
    // v1: table without index
    writeSchema(
      ctx.project.tablesDir,
      "zdm_idx.yaml",
      `
table: zdm_idx
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    await migrateOk(ctx);
    await seedRows(ctx.connectionString, "zdm_idx", 1000, (i) => `(DEFAULT, 'name_${i}')`);

    // v2: add index
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "zdm_idx.yaml",
      `
table: zdm_idx
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
indexes:
  - name: idx_zdm_name
    columns: [name]
`,
    );

    const { migrationResult, workloadErrors } = await runWorkloadDuringMigration(ctx, {
      tableName: "zdm_idx",
      insertSql: "INSERT INTO zdm_idx VALUES (DEFAULT, 'wl')",
      selectSql: "SELECT * FROM zdm_idx LIMIT 1",
    });

    expect(workloadErrors).toHaveLength(0);
    expect(migrationResult.success).toBe(true);
    expect(await indexExists(ctx.connectionString, "idx_zdm_name")).toBe(true);
  });

  it("create unique index uses CONCURRENTLY — no lock", async () => {
    // v1: table without unique index
    writeSchema(
      ctx.project.tablesDir,
      "zdm_uidx.yaml",
      `
table: zdm_uidx
columns:
  - name: id
    type: serial
    primary_key: true
  - name: code
    type: text
`,
    );
    await migrateOk(ctx);
    await seedRows(ctx.connectionString, "zdm_uidx", 1000, (i) => `(DEFAULT, 'code_${i}')`);

    // v2: add unique index
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "zdm_uidx.yaml",
      `
table: zdm_uidx
columns:
  - name: id
    type: serial
    primary_key: true
  - name: code
    type: text
indexes:
  - name: idx_zdm_code_unique
    columns: [code]
    unique: true
`,
    );

    const { migrationResult, workloadErrors } = await runWorkloadDuringMigration(ctx, {
      tableName: "zdm_uidx",
      insertSql: "INSERT INTO zdm_uidx VALUES (DEFAULT, 'wl_' || gen_random_uuid()::text)",
      selectSql: "SELECT * FROM zdm_uidx LIMIT 1",
    });

    expect(workloadErrors).toHaveLength(0);
    expect(migrationResult.success).toBe(true);
    expect(await indexExists(ctx.connectionString, "idx_zdm_code_unique")).toBe(true);
  });

  it("drop index uses CONCURRENTLY — no lock", async () => {
    // v1: table with index
    writeSchema(
      ctx.project.tablesDir,
      "zdm_idx_drop.yaml",
      `
table: zdm_idx_drop
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
indexes:
  - name: idx_zdm_drop_name
    columns: [name]
`,
    );
    await migrateOk(ctx);
    await seedRows(ctx.connectionString, "zdm_idx_drop", 1000, (i) => `(DEFAULT, 'name_${i}')`);
    expect(await indexExists(ctx.connectionString, "idx_zdm_drop_name")).toBe(true);

    // v2: remove index
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "zdm_idx_drop.yaml",
      `
table: zdm_idx_drop
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    const { migrationResult, workloadErrors } = await runWorkloadDuringMigration(ctx, {
      tableName: "zdm_idx_drop",
      insertSql: "INSERT INTO zdm_idx_drop VALUES (DEFAULT, 'wl')",
      selectSql: "SELECT * FROM zdm_idx_drop LIMIT 1",
      allowDestructive: true,
    });

    expect(workloadErrors).toHaveLength(0);
    expect(migrationResult.success).toBe(true);
    expect(await indexExists(ctx.connectionString, "idx_zdm_drop_name")).toBe(false);
  });

  it("add nullable column — no lock on concurrent reads/writes", async () => {
    // v1: simple table
    writeSchema(
      ctx.project.tablesDir,
      "zdm_addcol.yaml",
      `
table: zdm_addcol
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    await migrateOk(ctx);
    await seedRows(ctx.connectionString, "zdm_addcol", 1000, (i) => `(DEFAULT, 'name_${i}')`);

    // v2: add nullable column
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "zdm_addcol.yaml",
      `
table: zdm_addcol
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
  - name: bio
    type: text
    nullable: true
`,
    );

    const { migrationResult, workloadErrors } = await runWorkloadDuringMigration(ctx, {
      tableName: "zdm_addcol",
      insertSql: "INSERT INTO zdm_addcol (id, name) VALUES (DEFAULT, 'wl')",
      selectSql: "SELECT * FROM zdm_addcol LIMIT 1",
    });

    expect(workloadErrors).toHaveLength(0);
    expect(migrationResult.success).toBe(true);
    expect(await getColumns(ctx.connectionString, "zdm_addcol")).toEqual(["id", "name", "bio"]);
  });

  it("add column-level unique via concurrent index — no lock", async () => {
    // v1: table with email, no unique
    writeSchema(
      ctx.project.tablesDir,
      "zdm_col_uq.yaml",
      `
table: zdm_col_uq
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
`,
    );
    await migrateOk(ctx);
    await seedRows(ctx.connectionString, "zdm_col_uq", 1000, (i) => `(DEFAULT, 'user_${i}@test.com')`);

    // v2: add unique on email
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "zdm_col_uq.yaml",
      `
table: zdm_col_uq
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
    unique: true
`,
    );

    const { migrationResult, workloadErrors } = await runWorkloadDuringMigration(ctx, {
      tableName: "zdm_col_uq",
      insertSql: "INSERT INTO zdm_col_uq VALUES (DEFAULT, gen_random_uuid()::text || '@test.com')",
      selectSql: "SELECT * FROM zdm_col_uq LIMIT 1",
    });

    expect(workloadErrors).toHaveLength(0);
    expect(migrationResult.success).toBe(true);
    // Verify unique constraint exists (either as index or constraint)
    const res = await execSql(
      ctx.connectionString,
      `SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'zdm_col_uq' AND indexdef LIKE '%UNIQUE%'`,
    );
    expect(res.rowCount).toBeGreaterThan(0);
  });
});
