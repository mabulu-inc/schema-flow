// src/core/db.ts
// PostgreSQL connection management with proper lifecycle

import pg from "pg";

const { Pool } = pg;

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
