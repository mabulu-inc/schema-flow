// src/core/db.ts
// PostgreSQL connection management with proper lifecycle

import pg from "pg";

const { Pool } = pg;

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
  fn: (client: pg.PoolClient) => Promise<T>
): Promise<T> {
  const p = getPool(connectionString);
  const client = await p.connect();
  try {
    return await fn(client);
  } finally {
    client.release();
  }
}

export async function withTransaction<T>(
  connectionString: string,
  fn: (client: pg.PoolClient) => Promise<T>
): Promise<T> {
  const p = getPool(connectionString);
  const client = await p.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
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
