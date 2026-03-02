// test/helpers.ts
// Shared test infrastructure: database lifecycle, temp directories, fixtures

import pg from "pg";
import { mkdirSync, writeFileSync, rmSync, existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";
import { beforeEach, afterEach } from "vitest";

const { Pool } = pg;

function resolveRootUrl(): string {
  if (process.env.TEST_DATABASE_URL) return process.env.TEST_DATABASE_URL;
  if (process.env.DATABASE_URL) return process.env.DATABASE_URL;

  // Read port from .test-db-port written by scripts/test-db-start.sh
  const portFile = path.resolve(__dirname, "..", ".test-db-port");
  let port = "5432";
  try {
    port = readFileSync(portFile, "utf-8").trim();
  } catch {
    // file missing — fall back to default port
  }
  return `postgresql://postgres:postgres@localhost:${port}/postgres`;
}

const ROOT_URL = resolveRootUrl();

/**
 * Create an isolated test database. Returns a connection string and cleanup function.
 * Each test suite gets its own database to avoid cross-contamination.
 */
export async function createTestDb(): Promise<{
  connectionString: string;
  dbName: string;
  cleanup: () => Promise<void>;
}> {
  const suffix = randomBytes(4).toString("hex");
  const dbName = `sf_test_${suffix}`;

  const rootPool = new Pool({ connectionString: ROOT_URL, max: 2 });

  try {
    await rootPool.query(`CREATE DATABASE "${dbName}"`);
  } finally {
    await rootPool.end();
  }

  // Build connection string for the new database
  const url = new URL(ROOT_URL);
  url.pathname = `/${dbName}`;
  const connectionString = url.toString();

  const cleanup = async () => {
    // Terminate all connections to the test database first
    const pool = new Pool({ connectionString: ROOT_URL, max: 2 });
    try {
      await pool.query(
        `SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE datname = $1 AND pid <> pg_backend_pid()`,
        [dbName],
      );
      // DROP DATABASE doesn't support parameterized identifiers; dbName is hex-only from randomBytes
      await pool.query(`DROP DATABASE IF EXISTS "${dbName}"`);
    } finally {
      await pool.end();
    }
  };

  return { connectionString, dbName, cleanup };
}

/**
 * Create a temporary directory structure that mimics a schema-flow project.
 * Returns the base dir path and a cleanup function.
 */
export function createTempProject(): {
  baseDir: string;
  schemaDir: string;
  preDir: string;
  postDir: string;
  mixinsDir: string;
  cleanup: () => void;
} {
  const suffix = randomBytes(4).toString("hex");
  const baseDir = path.join("/tmp", `sf_test_project_${suffix}`);
  const sfDir = path.join(baseDir, "schema-flow");
  const schemaDir = path.join(sfDir, "schema");
  const preDir = path.join(sfDir, "pre");
  const postDir = path.join(sfDir, "post");
  const mixinsDir = path.join(sfDir, "mixins");

  mkdirSync(schemaDir, { recursive: true });
  mkdirSync(preDir, { recursive: true });
  mkdirSync(postDir, { recursive: true });
  mkdirSync(mixinsDir, { recursive: true });

  const cleanup = () => {
    if (existsSync(baseDir)) {
      rmSync(baseDir, { recursive: true, force: true });
    }
  };

  return { baseDir, schemaDir, preDir, postDir, mixinsDir, cleanup };
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
 * Execute raw SQL against a connection string.
 */
export async function execSql(connectionString: string, sql: string, params?: unknown[]): Promise<pg.QueryResult> {
  const pool = new Pool({ connectionString, max: 1 });
  try {
    return await pool.query(sql, params);
  } finally {
    await pool.end();
  }
}

/**
 * Check if a table exists in the public schema.
 */
export async function tableExists(connectionString: string, tableName: string): Promise<boolean> {
  const res = await execSql(
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
  const res = await execSql(
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
  const res = await execSql(
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
  const res = await execSql(
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
  const res = await execSql(
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
