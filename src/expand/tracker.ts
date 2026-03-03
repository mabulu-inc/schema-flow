// src/expand/tracker.ts
// Track expand/contract state in a database table

import pg from "pg";
import { logger } from "../core/logger.js";

export interface ExpandRecord {
  id: number;
  table_name: string;
  old_column: string;
  new_column: string;
  transform: string;
  reverse_expr: string | null;
  trigger_name: string;
  function_name: string;
  status: "expanding" | "backfilling" | "expanded" | "contracting" | "contracted";
  started_at: string;
  batch_size: number;
}

const EXPAND_TABLE = "_schema_flow_expand";

export class ExpandTracker {
  /** Ensure the expand state table exists */
  async ensureTable(client: pg.PoolClient): Promise<void> {
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${EXPAND_TABLE} (
        id            SERIAL PRIMARY KEY,
        table_name    TEXT NOT NULL,
        old_column    TEXT NOT NULL,
        new_column    TEXT NOT NULL,
        transform     TEXT NOT NULL,
        reverse_expr  TEXT,
        trigger_name  TEXT NOT NULL,
        function_name TEXT NOT NULL,
        status        TEXT NOT NULL CHECK (status IN ('expanding','backfilling','expanded','contracting','contracted')),
        started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        batch_size    INTEGER NOT NULL DEFAULT 1000,
        UNIQUE (table_name, old_column, new_column)
      );
    `);
    logger.debug(`Ensured expand table: ${EXPAND_TABLE}`);
  }

  /** Register a new expand operation */
  async register(
    client: pg.PoolClient,
    opts: {
      tableName: string;
      oldColumn: string;
      newColumn: string;
      transform: string;
      reverseExpr?: string;
      triggerName: string;
      functionName: string;
      batchSize: number;
    },
  ): Promise<number> {
    const res = await client.query(
      `INSERT INTO ${EXPAND_TABLE}
        (table_name, old_column, new_column, transform, reverse_expr, trigger_name, function_name, status, batch_size)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'expanding', $8)
       ON CONFLICT (table_name, old_column, new_column) DO UPDATE
         SET status = 'expanding', transform = EXCLUDED.transform, reverse_expr = EXCLUDED.reverse_expr
       RETURNING id`,
      [
        opts.tableName,
        opts.oldColumn,
        opts.newColumn,
        opts.transform,
        opts.reverseExpr || null,
        opts.triggerName,
        opts.functionName,
        opts.batchSize,
      ],
    );
    return res.rows[0].id;
  }

  /** Update status */
  async updateStatus(client: pg.PoolClient, id: number, status: ExpandRecord["status"]): Promise<void> {
    await client.query(`UPDATE ${EXPAND_TABLE} SET status = $2 WHERE id = $1`, [id, status]);
  }

  /** Get active expand records (not contracted) */
  async getActive(client: pg.PoolClient): Promise<ExpandRecord[]> {
    const res = await client.query<ExpandRecord>(
      `SELECT * FROM ${EXPAND_TABLE} WHERE status != 'contracted' ORDER BY id`,
    );
    return res.rows;
  }

  /** Get expand record for a specific column */
  async getForColumn(
    client: pg.PoolClient,
    tableName: string,
    oldColumn: string,
    newColumn: string,
  ): Promise<ExpandRecord | null> {
    const res = await client.query<ExpandRecord>(
      `SELECT * FROM ${EXPAND_TABLE}
       WHERE table_name = $1 AND old_column = $2 AND new_column = $3
       AND status != 'contracted'`,
      [tableName, oldColumn, newColumn],
    );
    return res.rows[0] || null;
  }

  /** Get all records (for status display) */
  async getAll(client: pg.PoolClient): Promise<ExpandRecord[]> {
    const res = await client.query<ExpandRecord>(`SELECT * FROM ${EXPAND_TABLE} ORDER BY started_at DESC`);
    return res.rows;
  }
}
