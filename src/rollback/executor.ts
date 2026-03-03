// src/rollback/executor.ts
// Execute down migrations (rollback)

import type { SchemaFlowConfig } from "../core/config.js";
import { FileTracker } from "../core/tracker.js";
import { withClient, type ClientOptions } from "../core/db.js";
import { logger } from "../core/logger.js";
import { computeRollback, type RollbackResult, type ReverseOperation } from "./index.js";
import type { MigrationSnapshot } from "./snapshot.js";
import type { Operation } from "../planner/index.js";

export interface DownResult {
  success: boolean;
  operationsExecuted: number;
  errors: string[];
  plan: RollbackResult;
}

/** Show the down plan (and optionally execute it) */
export async function runDown(
  config: SchemaFlowConfig,
  options: { apply?: boolean; allowDestructive?: boolean } = {},
): Promise<DownResult> {
  const tracker = new FileTracker(config.historyTable);
  const clientOpts: ClientOptions = {
    lockTimeout: config.lockTimeout,
    statementTimeout: config.statementTimeout,
  };

  return await withClient(config.connectionString, async (client) => {
    await tracker.ensureRunsTable(client);

    const lastRun = await tracker.getLastRun(client);
    if (!lastRun) {
      logger.warn("No completed migration runs found. Nothing to roll back.");
      return {
        success: true,
        operationsExecuted: 0,
        errors: [],
        plan: { operations: [], hasDestructive: false, hasIrreversible: false },
      };
    }

    logger.info(`Last run: ${lastRun.run_id} (${lastRun.status}, ${lastRun.ops_executed} ops, ${lastRun.started_at})`);

    const forwardOps = lastRun.operations as Operation[];
    const snapshot = (lastRun.snapshot || { tables: {}, capturedAt: "" }) as MigrationSnapshot;
    const rollbackPlan = computeRollback(forwardOps, snapshot, config.pgSchema);

    // Display the plan
    logger.divider();
    logger.info(`Rollback plan: ${rollbackPlan.operations.length} reverse operations`);
    for (const op of rollbackPlan.operations) {
      const marker = op.irreversible ? "[IRREVERSIBLE] " : op.destructive ? "[DESTRUCTIVE] " : "";
      logger.info(`  ${marker}${op.description}`);
      logger.debug(`  SQL: ${op.sql}`);
    }
    logger.divider();

    if (rollbackPlan.hasIrreversible) {
      logger.warn("Some operations are irreversible — original data cannot be recovered.");
    }

    if (!options.apply) {
      logger.info("Dry run — use --apply to execute the rollback.");
      return { success: true, operationsExecuted: 0, errors: [], plan: rollbackPlan };
    }

    if (rollbackPlan.hasDestructive && !options.allowDestructive) {
      logger.warn("Rollback contains destructive operations. Use --allow-destructive to proceed.");
      return { success: false, operationsExecuted: 0, errors: ["Destructive operations blocked"], plan: rollbackPlan };
    }

    // Execute
    const errors: string[] = [];
    let opsExecuted = 0;

    try {
      // Separate index drops (CONCURRENTLY) from others
      const indexOps = rollbackPlan.operations.filter((o) => o.sql.includes("DROP INDEX CONCURRENTLY"));
      const otherOps = rollbackPlan.operations.filter(
        (o) => !o.sql.includes("DROP INDEX CONCURRENTLY") && !o.irreversible,
      );

      if (otherOps.length > 0) {
        await client.query("BEGIN");
        for (const op of otherOps) {
          logger.debug(`Executing: ${op.description}`);
          await client.query(op.sql);
          opsExecuted++;
        }
        await client.query("COMMIT");
      }

      for (const op of indexOps) {
        logger.debug(`Executing: ${op.description}`);
        await client.query(op.sql);
        opsExecuted++;
      }

      // Mark the run as rolled back
      await tracker.rollbackRun(client, lastRun.run_id);
      logger.success(`Rollback complete — ${opsExecuted} operations executed`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(msg);
      logger.error(`Rollback failed: ${msg}`);
      try { await client.query("ROLLBACK"); } catch { /* ignore */ }
    }

    return {
      success: errors.length === 0,
      operationsExecuted: opsExecuted,
      errors,
      plan: rollbackPlan,
    };
  }, clientOpts);
}
