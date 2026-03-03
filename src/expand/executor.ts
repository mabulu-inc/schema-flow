// src/expand/executor.ts
// Contract command and expand status display

import type { SchemaFlowConfig } from "../core/config.js";
import { withClient, getPool } from "../core/db.js";
import { logger } from "../core/logger.js";
import { ExpandTracker } from "./tracker.js";
import { planContractColumn } from "./planner.js";

export interface ContractResult {
  success: boolean;
  operationsExecuted: number;
  errors: string[];
}

/** Run the contract phase — drop triggers and old columns */
export async function runContract(
  config: SchemaFlowConfig,
  options: { allowDestructive?: boolean } = {},
): Promise<ContractResult> {
  const tracker = new ExpandTracker();
  const errors: string[] = [];
  let opsExecuted = 0;

  return await withClient(config.connectionString, async (client) => {
    await tracker.ensureTable(client);

    const active = await tracker.getActive(client);
    const expandedRecords = active.filter(
      (r) => r.status === "expanded" || r.status === "backfilling",
    );

    if (expandedRecords.length === 0) {
      logger.info("No expanded columns ready for contraction.");
      return { success: true, operationsExecuted: 0, errors: [] };
    }

    for (const record of expandedRecords) {
      const qualifiedTable = `"${config.pgSchema}"."${record.table_name}"`;

      // Verify backfill is complete (no NULLs in new column)
      const nullCheck = await client.query(
        `SELECT count(*) as cnt FROM ${qualifiedTable} WHERE "${record.new_column}" IS NULL`,
      );
      const nullCount = parseInt(nullCheck.rows[0].cnt, 10);

      if (nullCount > 0) {
        logger.warn(
          `Cannot contract ${record.table_name}.${record.old_column} → ${record.new_column}: ` +
          `${nullCount} NULL values remain in ${record.new_column}. Run backfill first.`,
        );
        continue;
      }

      if (!options.allowDestructive) {
        logger.warn(
          `Contraction of ${record.table_name}.${record.old_column} requires --allow-destructive ` +
          `(will drop the old column).`,
        );
        continue;
      }

      logger.info(`Contracting ${record.table_name}: ${record.old_column} → ${record.new_column}`);

      const contractOps = planContractColumn(
        record.table_name,
        record.old_column,
        record.new_column,
        record.trigger_name,
        record.function_name,
        config.pgSchema,
      );

      try {
        await client.query("BEGIN");
        for (const op of contractOps) {
          logger.debug(`Executing: ${op.description}`);
          await client.query(op.sql);
          opsExecuted++;
        }
        await client.query("COMMIT");

        await tracker.updateStatus(client, record.id, "contracted");
        logger.success(`Contracted ${record.table_name}.${record.old_column} → ${record.new_column}`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(msg);
        logger.error(`Contract failed for ${record.table_name}: ${msg}`);
        try { await client.query("ROLLBACK"); } catch { /* ignore */ }
      }
    }

    return { success: errors.length === 0, operationsExecuted: opsExecuted, errors };
  });
}

/** Show the current expand/contract status */
export async function showExpandStatus(config: SchemaFlowConfig): Promise<void> {
  const tracker = new ExpandTracker();

  await withClient(config.connectionString, async (client) => {
    await tracker.ensureTable(client);
    const records = await tracker.getAll(client);

    if (records.length === 0) {
      logger.info("No expand/contract operations recorded.");
      return;
    }

    console.log("");
    console.log("  Expand/Contract Status:");
    console.log("  " + "─".repeat(50));

    for (const r of records) {
      const status = r.status.toUpperCase();
      const arrow = `${r.old_column} → ${r.new_column}`;
      console.log(`  ${r.table_name}: ${arrow} [${status}]`);
      console.log(`    Transform: ${r.transform}`);
      if (r.reverse_expr) console.log(`    Reverse:   ${r.reverse_expr}`);
      console.log(`    Trigger:   ${r.trigger_name}`);
      console.log(`    Started:   ${r.started_at}`);
      console.log("");
    }
  });
}
