// test/retry.test.ts
// Retry & recovery tests: prove that schema-flow handles transient errors
// (lock contention, advisory lock conflicts) and recovers from invalid state
// (e.g. INVALID indexes left by failed CREATE INDEX CONCURRENTLY).

import { describe, it, expect } from "vitest";
import pg from "pg";
import { useTestProject, writeSchema, execSql } from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { closePool } from "../src/core/db.js";
import { runMigrate, tryAdvisoryLock, releaseAdvisoryLock, type ExecutionResult } from "../src/executor/index.js";
import { logger, LogLevel } from "../src/core/logger.js";

const { Pool } = pg;

logger.setLevel(LogLevel.SILENT);

// ─── Shared helpers ──────────────────────────────────────────────────────────

type Ctx = { connectionString: string; project: { baseDir: string; schemaDir: string } };

async function migrateWith(
  ctx: Ctx,
  opts?: { allowDestructive?: boolean; maxRetries?: number; lockTimeout?: string; statementTimeout?: string },
): Promise<ExecutionResult> {
  const cfg = resolveConfig({
    connectionString: ctx.connectionString,
    baseDir: ctx.project.baseDir,
    allowDestructive: opts?.allowDestructive,
    maxRetries: opts?.maxRetries,
    lockTimeout: opts?.lockTimeout,
    statementTimeout: opts?.statementTimeout,
  });
  return runMigrate(cfg);
}

async function migrateOk(ctx: Ctx, opts?: { allowDestructive?: boolean }) {
  const result = await migrateWith(ctx, opts);
  expect(result.success).toBe(true);
  return result;
}

// ─── Advisory Lock Tests ─────────────────────────────────────────────────────

describe("retry — advisory lock contention", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("migration fails after exhausting retries when advisory lock is held", async () => {
    // Set up a schema so migration has something to do
    writeSchema(
      ctx.project.tablesDir,
      "retry_lock.yaml",
      `
table: retry_lock
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    // Hold the advisory lock from a separate connection
    const lockPool = new Pool({ connectionString: ctx.connectionString, max: 1 });
    const lockClient = await lockPool.connect();
    const acquired = await tryAdvisoryLock(lockClient, "public");
    expect(acquired).toBe(true);

    try {
      // Run migration with 0 retries so it fails immediately
      const result = await migrateWith(ctx, { maxRetries: 0 });
      expect(result.success).toBe(false);
      expect(result.errors[0]).toContain("Advisory lock not acquired");
    } finally {
      await releaseAdvisoryLock(lockClient, "public");
      lockClient.release();
      await lockPool.end();
    }
  });

  it("migration succeeds when advisory lock is released during retry window", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "retry_lock_ok.yaml",
      `
table: retry_lock_ok
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    // Hold the advisory lock from a separate connection
    const lockPool = new Pool({ connectionString: ctx.connectionString, max: 1 });
    const lockClient = await lockPool.connect();
    const acquired = await tryAdvisoryLock(lockClient, "public");
    expect(acquired).toBe(true);

    // Release the lock after 500ms — migration retry should pick it up
    setTimeout(async () => {
      await releaseAdvisoryLock(lockClient, "public");
      lockClient.release();
      await lockPool.end();
    }, 500);

    // Run migration with retries (default baseDelay is 1s, first retry at 1s)
    // Use maxRetries: 3 to give plenty of room
    const result = await migrateWith(ctx, { maxRetries: 3 });
    expect(result.success).toBe(true);
  });
});

// ─── INVALID Index Recovery ──────────────────────────────────────────────────

describe("retry — INVALID index recovery", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("re-run migration succeeds when table has an INVALID index from a prior failed attempt", async () => {
    // v1: create the table
    writeSchema(
      ctx.project.tablesDir,
      "retry_invalid_idx.yaml",
      `
table: retry_invalid_idx
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    await migrateOk(ctx);

    // Simulate a failed CREATE INDEX CONCURRENTLY by:
    // 1. Insert duplicate data
    // 2. Try to create a UNIQUE index CONCURRENTLY — it will fail, leaving an INVALID index
    await execSql(ctx.connectionString, `INSERT INTO retry_invalid_idx (name) VALUES ('dup'), ('dup')`);

    // This will fail due to duplicates but leave an INVALID index
    try {
      await execSql(
        ctx.connectionString,
        `CREATE UNIQUE INDEX CONCURRENTLY idx_retry_invalid ON retry_invalid_idx (name)`,
      );
    } catch {
      // Expected: duplicate key violation
    }

    // Verify the INVALID index exists
    const invalidCheck = await execSql(
      ctx.connectionString,
      `SELECT i.indisvalid
       FROM pg_index i
       JOIN pg_class c ON i.indexrelid = c.oid
       WHERE c.relname = 'idx_retry_invalid'`,
    );
    expect(invalidCheck.rows).toHaveLength(1);
    expect(invalidCheck.rows[0].indisvalid).toBe(false);

    // Fix the data so the unique index can be created
    await execSql(
      ctx.connectionString,
      `DELETE FROM retry_invalid_idx WHERE id IN (
         SELECT id FROM retry_invalid_idx WHERE name = 'dup' LIMIT 1
       )`,
    );

    // v2: declare the index in schema
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "retry_invalid_idx.yaml",
      `
table: retry_invalid_idx
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
indexes:
  - name: idx_retry_invalid
    columns: [name]
    unique: true
`,
    );

    const result = await migrateWith(ctx);
    expect(result.success).toBe(true);

    // Verify the index is now VALID
    const validCheck = await execSql(
      ctx.connectionString,
      `SELECT i.indisvalid
       FROM pg_index i
       JOIN pg_class c ON i.indexrelid = c.oid
       WHERE c.relname = 'idx_retry_invalid'`,
    );
    expect(validCheck.rows).toHaveLength(1);
    expect(validCheck.rows[0].indisvalid).toBe(true);
  });
});

// ─── DDL Retry on Lock Timeout ───────────────────────────────────────────────

describe("retry — DDL lock contention", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("migration retries and succeeds when a competing lock is released", async () => {
    // v1: create table with a row so we can lock it
    writeSchema(
      ctx.project.tablesDir,
      "retry_ddl.yaml",
      `
table: retry_ddl
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );
    await migrateOk(ctx);
    await execSql(ctx.connectionString, "INSERT INTO retry_ddl (name) VALUES ('lock_me')");

    // Hold a row-level lock by starting a transaction that touches the table.
    // SELECT FOR UPDATE acquires RowExclusive which conflicts with AccessExclusive
    // needed by ALTER TABLE ADD COLUMN.
    const lockPool = new Pool({ connectionString: ctx.connectionString, max: 1 });
    const lockClient = await lockPool.connect();
    await lockClient.query("BEGIN");
    await lockClient.query("SELECT * FROM retry_ddl FOR UPDATE");

    // Release the lock after 500ms so a retry attempt can succeed
    const lockReleased = new Promise<void>((resolve) => {
      setTimeout(async () => {
        try {
          await lockClient.query("COMMIT");
          lockClient.release();
          await lockPool.end();
        } catch {
          // Connection may have been terminated by test cleanup
        }
        resolve();
      }, 500);
    });

    // v2: add a column — ALTER TABLE ADD COLUMN needs AccessExclusive
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "retry_ddl.yaml",
      `
table: retry_ddl
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

    // Use short lock_timeout so it hits the retry path quickly
    const result = await migrateWith(ctx, {
      lockTimeout: "200ms",
      maxRetries: 3,
    });

    await lockReleased;
    expect(result.success).toBe(true);
  });
});
