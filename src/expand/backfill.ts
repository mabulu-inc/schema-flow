// src/expand/backfill.ts
// Batched backfill executor for expand/contract

import pg from "pg";
import { logger } from "../core/logger.js";
import { isRetryable } from "../core/db.js";

export interface BackfillOptions {
  tableName: string;
  newColumn: string;
  transform: string;
  batchSize: number;
  pgSchema: string;
  maxRetries?: number;
}

export interface BackfillResult {
  totalRows: number;
  batches: number;
}

/** Run a batched backfill — each batch in its own transaction */
export async function runBackfill(pool: pg.Pool, options: BackfillOptions): Promise<BackfillResult> {
  const { tableName, newColumn, transform, batchSize, pgSchema } = options;
  const qualifiedTable = `"${pgSchema}"."${tableName}"`;
  let totalRows = 0;
  let batches = 0;

  logger.info(`Starting backfill of ${tableName}.${newColumn} (batch_size=${batchSize})`);

  const maxBatchRetries = options.maxRetries ?? 3;
  const baseDelay = 1000;

  // Loop until no more NULLs
  let batchRetries = 0;
  while (true) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      // Use FOR UPDATE SKIP LOCKED to avoid blocking concurrent operations
      const res = await client.query(
        `WITH batch AS (
           SELECT ctid
           FROM ${qualifiedTable}
           WHERE "${newColumn}" IS NULL
           LIMIT ${batchSize}
           FOR UPDATE SKIP LOCKED
         )
         UPDATE ${qualifiedTable} t
         SET "${newColumn}" = ${transform}
         FROM batch
         WHERE t.ctid = batch.ctid
         RETURNING 1`,
      );
      await client.query("COMMIT");

      const updated = res.rowCount || 0;
      totalRows += updated;
      batches++;
      batchRetries = 0; // Reset on success

      if (updated > 0) {
        logger.debug(`  Batch ${batches}: updated ${updated} rows`);
      }

      if (updated < batchSize) {
        break; // No more rows to update
      }
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      if (isRetryable(err) && batchRetries < maxBatchRetries) {
        batchRetries++;
        const delay = baseDelay * 2 ** (batchRetries - 1);
        logger.warn(
          `Backfill batch timed out (${(err as unknown as Record<string, unknown>).code}), retrying in ${delay}ms (attempt ${batchRetries}/${maxBatchRetries})`,
        );
        client.release();
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }
      throw err;
    } finally {
      client.release();
    }
  }

  logger.info(`Backfill complete: ${totalRows} rows updated in ${batches} batch(es)`);
  return { totalRows, batches };
}
