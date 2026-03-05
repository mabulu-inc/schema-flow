// test/helpers.ts
// Shared test infrastructure: database lifecycle, temp directories, fixtures
//
// Core DB functions (createTestDb, execSql, withConnection) are re-exported
// from the public testing module so internal and external tests use the
// same infrastructure.

import pg from "pg";
import { mkdirSync, writeFileSync, rmSync, existsSync } from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";
import { beforeEach, afterEach } from "vitest";

// Import and re-export core functions from the public testing module
import { createTestDb, execSql, withConnection, ensureTestDb, stopTestDb } from "../src/testing/index.js";
export { createTestDb, execSql, withConnection, ensureTestDb, stopTestDb };

const { Pool } = pg;

/**
 * Create a temporary directory structure that mimics a schema-flow project.
 * Returns the base dir path and a cleanup function.
 */
export function createTempProject(): {
  baseDir: string;
  tablesDir: string;
  enumsDir: string;
  functionsDir: string;
  viewsDir: string;
  rolesDir: string;
  preDir: string;
  postDir: string;
  mixinsDir: string;
  /** The schema root dir (baseDir/schema/) */
  sfDir: string;
  cleanup: () => void;
} {
  const suffix = randomBytes(4).toString("hex");
  const baseDir = path.join("/tmp", `sf_test_project_${suffix}`);
  const sfDir = path.join(baseDir, "schema");
  const tablesDir = path.join(sfDir, "tables");
  const enumsDir = path.join(sfDir, "enums");
  const functionsDir = path.join(sfDir, "functions");
  const viewsDir = path.join(sfDir, "views");
  const rolesDir = path.join(sfDir, "roles");
  const preDir = path.join(sfDir, "pre");
  const postDir = path.join(sfDir, "post");
  const mixinsDir = path.join(sfDir, "mixins");

  for (const dir of [tablesDir, enumsDir, functionsDir, viewsDir, rolesDir, preDir, postDir, mixinsDir]) {
    mkdirSync(dir, { recursive: true });
  }

  const cleanup = () => {
    if (existsSync(baseDir)) {
      rmSync(baseDir, { recursive: true, force: true });
    }
  };

  return { baseDir, tablesDir, enumsDir, functionsDir, viewsDir, rolesDir, preDir, postDir, mixinsDir, sfDir, cleanup };
}

/**
 * Write a YAML schema file into the schema directory.
 */
export function writeSchema(schemaDir: string, filename: string, content: string): string {
  const filePath = path.join(schemaDir, filename);
  writeFileSync(filePath, content, "utf-8");
  return filePath;
}

/**
 * Write a SQL script into a directory.
 */
export function writeScript(dir: string, filename: string, content: string): string {
  const filePath = path.join(dir, filename);
  writeFileSync(filePath, content, "utf-8");
  return filePath;
}

/**
 * Check if a table exists in the public schema.
 */
export async function tableExists(connectionString: string, tableName: string): Promise<boolean> {
  const res = await querySql(
    connectionString,
    `SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1`,
    [tableName],
  );
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * Get column names for a table.
 */
export async function getColumns(connectionString: string, tableName: string): Promise<string[]> {
  const res = await querySql(
    connectionString,
    `SELECT column_name FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = $1
     ORDER BY ordinal_position`,
    [tableName],
  );
  return res.rows.map((r: { column_name: string }) => r.column_name);
}

/**
 * Check if a foreign key constraint exists.
 */
export async function fkExists(connectionString: string, constraintName: string): Promise<boolean> {
  const res = await querySql(
    connectionString,
    `SELECT 1 FROM information_schema.table_constraints
     WHERE constraint_type = 'FOREIGN KEY' AND constraint_name = $1`,
    [constraintName],
  );
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * Lifecycle helper for tests needing a raw PG client (introspect, planner, tracker).
 * Registers vitest beforeEach/afterEach hooks internally.
 * Returns a mutable context object populated before each test.
 */
export function useTestClient(): {
  connectionString: string;
  pool: pg.Pool;
  client: pg.PoolClient;
} {
  const ctx = {} as {
    connectionString: string;
    pool: pg.Pool;
    client: pg.PoolClient;
    _cleanup: () => Promise<void>;
  };

  beforeEach(async () => {
    const db = await createTestDb();
    ctx.connectionString = db.connectionString;
    ctx._cleanup = db.cleanup;
    ctx.pool = new Pool({ connectionString: db.connectionString, max: 2 });
    ctx.client = await ctx.pool.connect();
  });

  afterEach(async () => {
    ctx.client?.release();
    await ctx.pool?.end();
    await ctx._cleanup?.();
  });

  return ctx;
}

/**
 * Check if a trigger exists on a table.
 */
export async function triggerExists(
  connectionString: string,
  triggerName: string,
  tableName: string,
): Promise<boolean> {
  const res = await querySql(
    connectionString,
    `SELECT 1 FROM information_schema.triggers
     WHERE trigger_schema = 'public' AND trigger_name = $1 AND event_object_table = $2`,
    [triggerName, tableName],
  );
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * Check if a function exists in the public schema.
 */
export async function functionExists(connectionString: string, functionName: string): Promise<boolean> {
  const res = await querySql(
    connectionString,
    `SELECT 1 FROM information_schema.routines
     WHERE routine_schema = 'public' AND routine_name = $1`,
    [functionName],
  );
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * Write a YAML mixin file into the mixins directory.
 */
export function writeMixin(mixinsDir: string, filename: string, content: string): string {
  const filePath = path.join(mixinsDir, filename);
  writeFileSync(filePath, content, "utf-8");
  return filePath;
}

/**
 * Check if an RLS policy exists on a table.
 */
export async function policyExists(connectionString: string, policyName: string, tableName: string): Promise<boolean> {
  const res = await querySql(
    connectionString,
    `SELECT 1 FROM pg_policies
     WHERE schemaname = 'public' AND policyname = $1 AND tablename = $2`,
    [policyName, tableName],
  );
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * Check if row-level security is enabled on a table.
 */
export async function rlsEnabled(connectionString: string, tableName: string): Promise<boolean> {
  const res = await querySql(
    connectionString,
    `SELECT c.relrowsecurity
     FROM pg_class c
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = 'public' AND c.relname = $1`,
    [tableName],
  );
  return res.rows.length > 0 && res.rows[0].relrowsecurity === true;
}

/**
 * Check if an enum type exists in the public schema.
 */
export async function enumExists(connectionString: string, enumName: string): Promise<boolean> {
  const res = await querySql(
    connectionString,
    `SELECT 1 FROM pg_type t
     JOIN pg_namespace n ON t.typnamespace = n.oid
     WHERE n.nspname = 'public' AND t.typname = $1 AND t.typtype = 'e'`,
    [enumName],
  );
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * Get values of an enum type.
 */
export async function getEnumValues(connectionString: string, enumName: string): Promise<string[]> {
  const res = await querySql(
    connectionString,
    `SELECT e.enumlabel
     FROM pg_enum e
     JOIN pg_type t ON e.enumtypid = t.oid
     JOIN pg_namespace n ON t.typnamespace = n.oid
     WHERE n.nspname = 'public' AND t.typname = $1
     ORDER BY e.enumsortorder`,
    [enumName],
  );
  return res.rows.map((r: { enumlabel: string }) => r.enumlabel);
}

/**
 * Check if a PostgreSQL extension is installed.
 */
export async function extensionExists(connectionString: string, extName: string): Promise<boolean> {
  const res = await querySql(connectionString, `SELECT 1 FROM pg_extension WHERE extname = $1`, [extName]);
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * Check if a view exists in the public schema.
 */
export async function viewExists(connectionString: string, viewName: string): Promise<boolean> {
  const res = await querySql(
    connectionString,
    `SELECT 1 FROM information_schema.views
     WHERE table_schema = 'public' AND table_name = $1`,
    [viewName],
  );
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * Check if a materialized view exists in the public schema.
 */
export async function materializedViewExists(connectionString: string, mvName: string): Promise<boolean> {
  const res = await querySql(
    connectionString,
    `SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = $1`,
    [mvName],
  );
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * Check if an index exists.
 */
export async function indexExists(connectionString: string, indexName: string): Promise<boolean> {
  const res = await querySql(
    connectionString,
    `SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1`,
    [indexName],
  );
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * Get the access method of an index (btree, gin, gist, etc.).
 */
export async function getIndexMethod(connectionString: string, indexName: string): Promise<string> {
  const res = await querySql(
    connectionString,
    `SELECT am.amname
     FROM pg_index i
     JOIN pg_class c ON i.indexrelid = c.oid
     JOIN pg_am am ON c.relam = am.oid
     WHERE c.relname = $1`,
    [indexName],
  );
  return res.rows.length > 0 ? res.rows[0].amname : "";
}

/**
 * Get a comment on a table or column. If columnName is omitted, returns the table comment.
 */
export async function getComment(
  connectionString: string,
  tableName: string,
  columnName?: string,
): Promise<string | null> {
  if (columnName) {
    const res = await querySql(
      connectionString,
      `SELECT col_description(c.oid, a.attnum) AS comment
       FROM pg_class c
       JOIN pg_namespace n ON c.relnamespace = n.oid
       JOIN pg_attribute a ON a.attrelid = c.oid
       WHERE n.nspname = 'public' AND c.relname = $1 AND a.attname = $2`,
      [tableName, columnName],
    );
    return res.rows.length > 0 ? res.rows[0].comment : null;
  }

  const res = await querySql(
    connectionString,
    `SELECT obj_description(c.oid, 'pg_class') AS comment
     FROM pg_class c
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = 'public' AND c.relname = $1`,
    [tableName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/**
 * Check if a constraint is deferrable.
 */
export async function isConstraintDeferrable(connectionString: string, constraintName: string): Promise<boolean> {
  const res = await querySql(connectionString, `SELECT condeferrable FROM pg_constraint WHERE conname = $1`, [
    constraintName,
  ]);
  return res.rows.length > 0 && res.rows[0].condeferrable === true;
}

/**
 * Check if a constraint is initially deferred.
 */
export async function isConstraintInitiallyDeferred(
  connectionString: string,
  constraintName: string,
): Promise<boolean> {
  const res = await querySql(connectionString, `SELECT condeferred FROM pg_constraint WHERE conname = $1`, [
    constraintName,
  ]);
  return res.rows.length > 0 && res.rows[0].condeferred === true;
}

/**
 * Check if a constraint is validated (convalidated=true in pg_constraint).
 */
export async function constraintValidated(connectionString: string, constraintName: string): Promise<boolean> {
  const res = await querySql(connectionString, `SELECT convalidated FROM pg_constraint WHERE conname = $1`, [
    constraintName,
  ]);
  return res.rows.length > 0 && res.rows[0].convalidated === true;
}

/**
 * Check if a constraint exists.
 */
export async function constraintExists(connectionString: string, constraintName: string): Promise<boolean> {
  const res = await querySql(connectionString, `SELECT 1 FROM pg_constraint WHERE conname = $1`, [constraintName]);
  return res.rowCount !== null && res.rowCount > 0;
}

/**
 * Check if a column is NOT NULL.
 */
export async function columnIsNotNull(
  connectionString: string,
  tableName: string,
  columnName: string,
): Promise<boolean> {
  const res = await querySql(
    connectionString,
    `SELECT is_nullable FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2`,
    [tableName, columnName],
  );
  return res.rows.length > 0 && res.rows[0].is_nullable === "NO";
}

/**
 * Get a comment on a view.
 */
export async function getViewComment(connectionString: string, viewName: string): Promise<string | null> {
  const res = await querySql(
    connectionString,
    `SELECT obj_description(c.oid, 'pg_class') AS comment
     FROM pg_class c
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = 'public' AND c.relname = $1 AND c.relkind = 'v'`,
    [viewName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/**
 * Get a comment on an enum type.
 */
export async function getEnumComment(connectionString: string, enumName: string): Promise<string | null> {
  const res = await querySql(
    connectionString,
    `SELECT obj_description(t.oid, 'pg_type') AS comment
     FROM pg_type t
     JOIN pg_namespace n ON t.typnamespace = n.oid
     WHERE n.nspname = 'public' AND t.typname = $1`,
    [enumName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/**
 * Get a comment on a function.
 */
export async function getFunctionComment(connectionString: string, funcName: string): Promise<string | null> {
  const res = await querySql(
    connectionString,
    `SELECT obj_description(p.oid, 'pg_proc') AS comment
     FROM pg_proc p
     JOIN pg_namespace n ON p.pronamespace = n.oid
     WHERE n.nspname = 'public' AND p.proname = $1
     LIMIT 1`,
    [funcName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/**
 * Get a comment on an index.
 */
export async function getIndexComment(connectionString: string, indexName: string): Promise<string | null> {
  const res = await querySql(
    connectionString,
    `SELECT obj_description(c.oid, 'pg_class') AS comment
     FROM pg_class c
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = 'public' AND c.relname = $1`,
    [indexName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/**
 * Get a comment on a trigger.
 */
export async function getTriggerComment(
  connectionString: string,
  triggerName: string,
  tableName: string,
): Promise<string | null> {
  const res = await querySql(
    connectionString,
    `SELECT obj_description(t.oid, 'pg_trigger') AS comment
     FROM pg_trigger t
     JOIN pg_class c ON t.tgrelid = c.oid
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = 'public' AND c.relname = $1 AND t.tgname = $2`,
    [tableName, triggerName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/**
 * Get a comment on a constraint.
 */
export async function getConstraintComment(connectionString: string, constraintName: string): Promise<string | null> {
  const res = await querySql(
    connectionString,
    `SELECT obj_description(c.oid, 'pg_constraint') AS comment
     FROM pg_constraint c
     WHERE c.conname = $1`,
    [constraintName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/**
 * Get a comment on a materialized view.
 */
export async function getMaterializedViewComment(connectionString: string, mvName: string): Promise<string | null> {
  const res = await querySql(
    connectionString,
    `SELECT obj_description(c.oid, 'pg_class') AS comment
     FROM pg_class c
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = 'public' AND c.relname = $1 AND c.relkind = 'm'`,
    [mvName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/**
 * Get a comment on a policy.
 */
export async function getPolicyComment(
  connectionString: string,
  policyName: string,
  tableName: string,
): Promise<string | null> {
  const res = await querySql(
    connectionString,
    `SELECT obj_description(p.oid, 'pg_policy') AS comment
     FROM pg_policy p
     JOIN pg_class c ON p.polrelid = c.oid
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = 'public' AND c.relname = $1 AND p.polname = $2`,
    [tableName, policyName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/**
 * Lifecycle helper for tests using the app's module-level pool (executor, scaffold, cli).
 * Registers vitest beforeEach/afterEach hooks internally.
 * Returns a mutable context object populated before each test.
 */
export function useTestProject(opts?: { closeAppPool?: () => Promise<void> }): {
  connectionString: string;
  project: ReturnType<typeof createTempProject>;
} {
  const ctx = {} as {
    connectionString: string;
    project: ReturnType<typeof createTempProject>;
    _cleanup: () => Promise<void>;
  };

  beforeEach(async () => {
    const db = await createTestDb();
    ctx.connectionString = db.connectionString;
    ctx._cleanup = db.cleanup;
    ctx.project = createTempProject();
  });

  afterEach(async () => {
    if (opts?.closeAppPool) {
      await opts.closeAppPool();
    }
    await ctx._cleanup?.();
    ctx.project?.cleanup();
  });

  return ctx;
}

// ---------------------------------------------------------------------------
// Internal helper — all assertion helpers above use this instead of the
// re-exported execSql so we avoid the top-level re-export creating a
// runtime dependency loop.
// ---------------------------------------------------------------------------

async function querySql(connectionString: string, sql: string, params?: unknown[]): Promise<pg.QueryResult> {
  const pool = new Pool({ connectionString, max: 1 });
  try {
    return await pool.query(sql, params);
  } finally {
    await pool.end();
  }
}
