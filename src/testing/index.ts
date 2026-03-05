// src/testing/index.ts
// Public testing utilities for schema-flow examples and user projects.
//
// Usage:
//   import { ensureTestDb, createTestDb, execSql } from "@mabulu-inc/schema-flow/testing";

import pg from "pg";
import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";

const { Pool } = pg;

const CONTAINER_NAME = "sf-test-postgres";

// ---------------------------------------------------------------------------
// Container runtime detection
// ---------------------------------------------------------------------------

function detectRuntime(): string {
  for (const cmd of ["podman", "docker"]) {
    const r = spawnSync(cmd, ["--version"], { stdio: "ignore" });
    if (r.status === 0) return cmd;
  }

  throw new Error(
    [
      "No container runtime found. schema-flow tests need Docker or Podman to run PostgreSQL.",
      "",
      "Install one of:",
      "  Docker  — https://docs.docker.com/get-started/get-docker/",
      "  Podman  — https://podman.io/docs/installation",
      "",
      "Or set TEST_DATABASE_URL to point at an existing PostgreSQL instance.",
    ].join("\n"),
  );
}

// ---------------------------------------------------------------------------
// Container lifecycle
// ---------------------------------------------------------------------------

function isContainerReady(runtime: string): boolean {
  const r = spawnSync(runtime, ["exec", CONTAINER_NAME, "pg_isready", "-U", "postgres"], {
    stdio: "ignore",
  });
  return r.status === 0;
}

function getContainerPort(runtime: string): number {
  const r = spawnSync(runtime, ["port", CONTAINER_NAME, "5432"], { encoding: "utf-8" });
  if (r.status !== 0) throw new Error("Could not determine container port");
  // Output: "0.0.0.0:12345" or ":::12345"
  const match = r.stdout.trim().match(/:(\d+)$/);
  return match ? parseInt(match[1], 10) : 5432;
}

function findFreePort(): number {
  // Try 5432 first
  const check = spawnSync(
    "node",
    [
      "-e",
      `const s=require("net").createServer();s.once("error",()=>process.exit(1));s.listen(5432,()=>{s.close();process.exit(0)})`,
    ],
    {
      stdio: "ignore",
    },
  );
  if (check.status === 0) return 5432;

  // Fall back to OS-assigned port
  const r = spawnSync(
    "node",
    ["-e", `const s=require("net").createServer();s.listen(0,()=>{console.log(s.address().port);s.close()})`],
    {
      encoding: "utf-8",
    },
  );
  return parseInt(r.stdout.trim(), 10);
}

function startContainer(runtime: string): number {
  // Remove stale container
  spawnSync(runtime, ["rm", "-f", CONTAINER_NAME], { stdio: "ignore" });

  const port = findFreePort();

  const r = spawnSync(
    runtime,
    [
      "run",
      "-d",
      "--name",
      CONTAINER_NAME,
      "--tmpfs",
      "/var/lib/postgresql/data",
      "-e",
      "POSTGRES_USER=postgres",
      "-e",
      "POSTGRES_PASSWORD=postgres",
      "-e",
      "POSTGRES_DB=postgres",
      "-p",
      `${port}:5432`,
      "postgres:latest",
      "-c",
      "fsync=off",
      "-c",
      "full_page_writes=off",
      "-c",
      "synchronous_commit=off",
    ],
    { stdio: "inherit" },
  );

  if (r.status !== 0) {
    throw new Error(`Failed to start PostgreSQL container (${runtime})`);
  }

  // Wait for ready (up to 30 seconds)
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (isContainerReady(runtime)) return port;
    spawnSync("sleep", ["0.5"], { stdio: "ignore" });
  }

  throw new Error("PostgreSQL container did not become ready within 30 seconds");
}

/**
 * Stop and remove the test PostgreSQL container.
 * Safe to call even if no container is running.
 */
export function stopTestDb(): void {
  try {
    const runtime = detectRuntime();
    spawnSync(runtime, ["rm", "-f", CONTAINER_NAME], { stdio: "ignore" });
  } catch {
    // No runtime — nothing to stop
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Ensure a PostgreSQL instance is available for testing.
 *
 * Resolution order:
 * 1. `TEST_DATABASE_URL` env var — uses your existing database as-is
 * 2. Running `sf-test-postgres` container — reuses it
 * 3. Starts a new container via Docker or Podman (in-memory, fast)
 *
 * Returns a root connection string (to the `postgres` database).
 */
export function ensureTestDb(): string {
  // 1. User-provided URL
  if (process.env.TEST_DATABASE_URL) return process.env.TEST_DATABASE_URL;

  // 2. Detect runtime (throws with install links if missing)
  const runtime = detectRuntime();

  // 3. Reuse running container
  if (isContainerReady(runtime)) {
    const port = getContainerPort(runtime);
    return `postgresql://postgres:postgres@localhost:${port}/postgres`;
  }

  // 4. Start fresh container
  const port = startContainer(runtime);
  return `postgresql://postgres:postgres@localhost:${port}/postgres`;
}

/**
 * Create an isolated test database. Each call creates a new database
 * with a random name so tests never interfere with each other.
 *
 * @param rootUrl - Root PostgreSQL connection string (from `ensureTestDb()`).
 *                  If omitted, calls `ensureTestDb()` automatically.
 * @returns Connection string for the new database and a cleanup function.
 */
export async function createTestDb(rootUrl?: string): Promise<{
  connectionString: string;
  dbName: string;
  cleanup: () => Promise<void>;
}> {
  const root = rootUrl ?? ensureTestDb();
  const suffix = randomBytes(4).toString("hex");
  const dbName = `sf_test_${suffix}`;

  const rootPool = new Pool({ connectionString: root, max: 2 });
  try {
    await rootPool.query(`CREATE DATABASE "${dbName}"`);
  } finally {
    await rootPool.end();
  }

  const url = new URL(root);
  url.pathname = `/${dbName}`;
  const connectionString = url.toString();

  const cleanup = async () => {
    const pool = new Pool({ connectionString: root, max: 2 });
    try {
      await pool.query(
        `SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE datname = $1 AND pid <> pg_backend_pid()`,
        [dbName],
      );
      await pool.query(`DROP DATABASE IF EXISTS "${dbName}"`);
    } finally {
      await pool.end();
    }
  };

  return { connectionString, dbName, cleanup };
}

/**
 * Execute a SQL statement against a connection string.
 * Opens a short-lived connection, runs the query, and closes it.
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
 * Run multiple statements on a single persistent connection.
 * Use this when you need session state (SET ROLE, set_config) to persist across queries.
 *
 * The callback receives a `query` function bound to the connection.
 * The connection is released (and RESET) when the callback completes.
 */
export async function withConnection<T>(
  connectionString: string,
  fn: (query: (sql: string, params?: unknown[]) => Promise<pg.QueryResult>) => Promise<T>,
): Promise<T> {
  const pool = new Pool({ connectionString, max: 1 });
  const client = await pool.connect();
  try {
    return await fn((sql, params) => client.query(sql, params));
  } finally {
    try {
      await client.query("RESET ROLE");
    } catch {
      // best effort
    }
    client.release();
    await pool.end();
  }
}
