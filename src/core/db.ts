// src/core/db.ts
// PostgreSQL connection management with proper lifecycle

import pg from "pg";
import { logger } from "./logger.js";

const { Pool } = pg;

const RETRYABLE_CODES = new Set(["55P03", "57014", "40001", "40P01"]);

export function isRetryable(err: unknown): boolean {
  return err instanceof Error && RETRYABLE_CODES.has((err as any).code);
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export async function retryOnTimeout<T>(
  fn: () => Promise<T>,
  opts?: { maxRetries?: number; baseDelayMs?: number; label?: string },
): Promise<T> {
  const maxRetries = opts?.maxRetries ?? 3;
  const baseDelay = opts?.baseDelayMs ?? 1000;
  const label = opts?.label ?? "operation";

  for (let attempt = 0; ; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt >= maxRetries || !isRetryable(err)) throw err;
      const delay = baseDelay * 2 ** attempt;
      logger.warn(
        `${label} timed out (${(err as any).code}), retrying in ${delay}ms (attempt ${attempt + 1}/${maxRetries})`,
      );
      await sleep(delay);
    }
  }
}

export interface ClientOptions {
  lockTimeout?: string;
  statementTimeout?: string;
}

let pool: pg.Pool | null = null;

export function getPool(connectionString: string): pg.Pool {
  if (!pool) {
    pool = new Pool({
      connectionString,
      max: 5,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    });
  }
  return pool;
}

export async function withClient<T>(
  connectionString: string,
  fn: (client: pg.PoolClient) => Promise<T>,
  options?: ClientOptions,
): Promise<T> {
  const p = getPool(connectionString);
  const client = await p.connect();
  let paramsSet = false;
  try {
    if (options?.lockTimeout && options.lockTimeout !== "0") {
      await client.query(`SET lock_timeout = '${options.lockTimeout}'`);
      paramsSet = true;
    }
    if (options?.statementTimeout && options.statementTimeout !== "0") {
      await client.query(`SET statement_timeout = '${options.statementTimeout}'`);
      paramsSet = true;
    }
    return await fn(client);
  } finally {
    if (paramsSet) {
      await client.query("RESET lock_timeout; RESET statement_timeout;").catch(() => {});
    }
    client.release();
  }
}

export async function withTransaction<T>(
  connectionString: string,
  fn: (client: pg.PoolClient) => Promise<T>,
  options?: ClientOptions,
): Promise<T> {
  const p = getPool(connectionString);
  const client = await p.connect();
  let paramsSet = false;
  try {
    if (options?.lockTimeout && options.lockTimeout !== "0") {
      await client.query(`SET lock_timeout = '${options.lockTimeout}'`);
      paramsSet = true;
    }
    if (options?.statementTimeout && options.statementTimeout !== "0") {
      await client.query(`SET statement_timeout = '${options.statementTimeout}'`);
      paramsSet = true;
    }
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    if (paramsSet) {
      await client.query("RESET lock_timeout; RESET statement_timeout;").catch(() => {});
    }
    client.release();
  }
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}

export async function testConnection(connectionString: string): Promise<void> {
  await withClient(connectionString, async (client) => {
    const res = await client.query("SELECT 1 AS ok");
    if (res.rows[0]?.ok !== 1) {
      throw new Error("Database connection test failed");
    }
  });
}
