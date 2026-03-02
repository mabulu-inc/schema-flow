// src/executor/index.ts
// Execute migration operations: pre-scripts → schema migration → post-scripts
// Supports plan (dry-run) and apply modes, and running phases independently

import { readFileSync } from "node:fs";
import path from "node:path";
import { glob } from "glob";
import type { SchemaFlowConfig } from "../core/config.js";
import { buildPlan } from "../planner/index.js";
import { parseTableFile, parseFunctionFile } from "../schema/parser.js";
import { loadMixins, expandMixins } from "../schema/mixins.js";
import type { FunctionSchema } from "../schema/types.js";
import { FileTracker } from "../core/tracker.js";
import { withClient } from "../core/db.js";
import { logger } from "../core/logger.js";

export type Phase = "pre" | "migrate" | "post";

export interface ExecutionResult {
  phase: Phase;
  success: boolean;
  operationsExecuted: number;
  errors: string[];
  dryRun: boolean;
  /** Number of destructive operations that were blocked by safe mode */
  blockedDestructive: number;
}

/** Discover and sort SQL scripts in a directory */
async function discoverScripts(dir: string): Promise<string[]> {
  const patterns = ["*.sql"];
  const files = await glob(patterns.map((p) => path.join(dir, p)));
  return files.sort(); // Alphabetical sort → timestamps in filenames ensure order
}

/** Discover YAML schema files */
async function discoverSchemaFiles(dir: string): Promise<string[]> {
  const patterns = ["*.yaml", "*.yml"];
  const files = await glob(patterns.map((p) => path.join(dir, p)));
  return files.sort();
}

/** Run pre-migration SQL scripts */
export async function runPre(config: SchemaFlowConfig): Promise<ExecutionResult> {
  logger.step("PRE", "Running pre-migration scripts");
  return runSqlScripts(config, config.preDir, "pre");
}

/** Run post-migration SQL scripts */
export async function runPost(config: SchemaFlowConfig): Promise<ExecutionResult> {
  logger.step("POST", "Running post-migration scripts");
  return runSqlScripts(config, config.postDir, "post");
}

/** Run schema migration (declarative YAML files) */
export async function runMigrate(config: SchemaFlowConfig): Promise<ExecutionResult> {
  logger.step("MIGRATE", "Running declarative schema migration");
  const errors: string[] = [];
  let opsExecuted = 0;

  const schemaFiles = await discoverSchemaFiles(config.schemaDir);
  if (schemaFiles.length === 0) {
    logger.info("No schema files found — skipping migration phase");
    return {
      phase: "migrate",
      success: true,
      operationsExecuted: 0,
      errors: [],
      dryRun: config.dryRun,
      blockedDestructive: 0,
    };
  }

  const tracker = new FileTracker(config.historyTable);

  return await withClient(config.connectionString, async (client) => {
    await tracker.ensureTable(client);
    const tracked = await tracker.getTracked(client);

    // Classify schema files
    const { newFiles, changedFiles, unchangedFiles } = tracker.classifyFiles(schemaFiles, tracked, "schema");

    logger.info(
      `Schema files: ${newFiles.length} new, ${changedFiles.length} changed, ${unchangedFiles.length} unchanged`,
    );

    if (newFiles.length === 0 && changedFiles.length === 0) {
      logger.success("Schema is up to date — nothing to do");
      return {
        phase: "migrate",
        success: true,
        operationsExecuted: 0,
        errors: [],
        dryRun: config.dryRun,
        blockedDestructive: 0,
      };
    }

    // Separate function files from table files
    const functionFiles = schemaFiles.filter((f) => {
      const base = path.basename(f);
      return base.startsWith("fn_");
    });
    const tableFiles = schemaFiles.filter((f) => {
      const base = path.basename(f);
      return !base.startsWith("fn_");
    });

    // Parse and apply function files first
    const parsedFunctions: FunctionSchema[] = [];
    for (const f of functionFiles) {
      try {
        parsedFunctions.push(parseFunctionFile(f));
      } catch (err) {
        const msg = `Failed to parse function ${f}: ${err instanceof Error ? err.message : String(err)}`;
        errors.push(msg);
        logger.error(msg);
      }
    }

    if (errors.length > 0) {
      return {
        phase: "migrate",
        success: false,
        operationsExecuted: 0,
        errors,
        dryRun: config.dryRun,
        blockedDestructive: 0,
      };
    }

    // Apply functions before building plan
    if (parsedFunctions.length > 0) {
      if (config.dryRun) {
        for (const fn of parsedFunctions) {
          logger.info(`  [DRY RUN] Would create function: ${fn.name}`);
        }
      } else {
        await client.query("BEGIN");
        for (const fn of parsedFunctions) {
          const replaceClause = fn.replace ? "OR REPLACE " : "";
          const argsClause = fn.args ? `(${fn.args})` : "()";
          const sql = `CREATE ${replaceClause}FUNCTION ${fn.name}${argsClause} RETURNS ${fn.returns} LANGUAGE ${fn.language} AS $fn_body$\n${fn.body}\n$fn_body$;`;
          logger.debug(`Creating function: ${fn.name}`);
          await client.query(sql);
        }
        await client.query("COMMIT");
        logger.info(`Applied ${parsedFunctions.length} function(s)`);
      }
    }

    // Parse ALL table schema files (not just changed ones) to build complete picture for FK resolution
    const allSchemas = tableFiles
      .map((f) => {
        try {
          return parseTableFile(f);
        } catch (err) {
          const msg = `Failed to parse ${f}: ${err instanceof Error ? err.message : String(err)}`;
          errors.push(msg);
          logger.error(msg);
          return null;
        }
      })
      .filter(Boolean) as Awaited<ReturnType<typeof parseTableFile>>[];

    if (errors.length > 0) {
      return {
        phase: "migrate",
        success: false,
        operationsExecuted: 0,
        errors,
        dryRun: config.dryRun,
        blockedDestructive: 0,
      };
    }

    // Expand mixins before building plan
    const mixinMap = await loadMixins(config.mixinsDir);
    if (mixinMap.size > 0) {
      logger.info(`Loaded ${mixinMap.size} mixin(s)`);
    }
    const expandedSchemas = expandMixins(allSchemas, mixinMap);

    // Build migration plan — pass safety flag
    const plan = await buildPlan(client, expandedSchemas, config.pgSchema, {
      allowDestructive: config.allowDestructive,
    });

    if (plan.operations.length === 0 && plan.blocked.length === 0) {
      logger.success("Database matches desired schema — nothing to do");
      // Update tracking for changed files (content changed but schema matches)
      if (!config.dryRun) {
        for (const f of [...newFiles, ...changedFiles]) {
          await tracker.recordFile(client, f, "schema");
        }
      }
      return {
        phase: "migrate",
        success: true,
        operationsExecuted: 0,
        errors: [],
        dryRun: config.dryRun,
        blockedDestructive: 0,
      };
    }

    // Log the plan
    logger.divider();
    logger.info(`Migration plan: ${plan.summary.totalOperations} operations`);
    if (plan.summary.tablesToCreate.length > 0) {
      logger.info(`  Tables to create: ${plan.summary.tablesToCreate.join(", ")}`);
    }
    if (plan.summary.tablesToAlter.length > 0) {
      logger.info(`  Tables to alter: ${plan.summary.tablesToAlter.join(", ")}`);
    }
    if (plan.summary.foreignKeysToAdd > 0) {
      logger.info(`  Foreign keys to add: ${plan.summary.foreignKeysToAdd}`);
    }
    if (plan.summary.destructiveCount > 0) {
      logger.warn(`  Destructive operations: ${plan.summary.destructiveCount} (--allow-destructive is ON)`);
    }
    if (plan.summary.blockedCount > 0) {
      logger.warn(`  Blocked (destructive): ${plan.summary.blockedCount} — use --allow-destructive to include`);
    }
    logger.divider();

    for (const op of plan.operations) {
      const marker = op.destructive ? "⚠ DESTRUCTIVE " : "";
      logger.info(`  ${config.dryRun ? "[DRY RUN] " : ""}${marker}${op.description}`);
      if (config.dryRun) {
        logger.debug(`  SQL: ${op.sql}`);
      }
    }

    if (config.dryRun) {
      const suffix = plan.blocked.length > 0 ? ` (${plan.blocked.length} destructive operations blocked)` : "";
      logger.info(`Dry run complete — ${plan.operations.length} operations would be executed${suffix}`);
      return {
        phase: "migrate",
        success: true,
        operationsExecuted: plan.operations.length,
        errors: [],
        dryRun: true,
        blockedDestructive: plan.blocked.length,
      };
    }

    // Execute: structure ops first, then FK ops
    // Use a transaction for structure operations
    try {
      // Structure ops in a transaction
      if (plan.structureOps.length > 0) {
        logger.step("MIGRATE", `Executing ${plan.structureOps.length} structure operations`);
        // Note: CREATE INDEX CONCURRENTLY cannot run inside a transaction,
        // so we separate index ops from other structure ops
        const indexOps = plan.structureOps.filter((o) => o.type === "add_index");
        const nonIndexOps = plan.structureOps.filter((o) => o.type !== "add_index");

        if (nonIndexOps.length > 0) {
          await client.query("BEGIN");
          for (const op of nonIndexOps) {
            const marker = op.destructive ? "[DESTRUCTIVE] " : "";
            logger.debug(`Executing: ${marker}${op.description}`);
            await client.query(op.sql);
            opsExecuted++;
          }
          await client.query("COMMIT");
        }

        // Index ops outside transaction (CONCURRENTLY)
        for (const op of indexOps) {
          logger.debug(`Executing: ${op.description}`);
          await client.query(op.sql);
          opsExecuted++;
        }
      }

      // FK ops in a separate transaction
      if (plan.foreignKeyOps.length > 0) {
        logger.step("MIGRATE", `Executing ${plan.foreignKeyOps.length} foreign key operations`);
        await client.query("BEGIN");
        for (const op of plan.foreignKeyOps) {
          logger.debug(`Executing: ${op.description}`);
          await client.query(op.sql);
          opsExecuted++;
        }
        await client.query("COMMIT");
      }

      // Record all processed files
      for (const f of [...newFiles, ...changedFiles]) {
        await tracker.recordFile(client, f, "schema");
      }

      logger.success(`Migration complete — ${opsExecuted} operations executed`);
      if (plan.blocked.length > 0) {
        logger.warn(
          `${plan.blocked.length} destructive operation(s) were skipped. Use --allow-destructive to apply them.`,
        );
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(msg);
      logger.error(`Migration failed: ${msg}`);
      try {
        await client.query("ROLLBACK");
      } catch {
        // Rollback may fail if no transaction is active
      }
    }

    return {
      phase: "migrate",
      success: errors.length === 0,
      operationsExecuted: opsExecuted,
      errors,
      dryRun: false,
      blockedDestructive: plan.blocked.length,
    };
  });
}

/** Run SQL scripts from a directory, tracking changes */
async function runSqlScripts(config: SchemaFlowConfig, dir: string, phase: "pre" | "post"): Promise<ExecutionResult> {
  const errors: string[] = [];
  let opsExecuted = 0;

  const scripts = await discoverScripts(dir);
  if (scripts.length === 0) {
    logger.info(`No ${phase}-migration scripts found — skipping`);
    return { phase, success: true, operationsExecuted: 0, errors: [], dryRun: config.dryRun, blockedDestructive: 0 };
  }

  const tracker = new FileTracker(config.historyTable);

  return await withClient(config.connectionString, async (client) => {
    await tracker.ensureTable(client);
    const tracked = await tracker.getTracked(client);

    const { newFiles, changedFiles, unchangedFiles } = tracker.classifyFiles(scripts, tracked, phase);

    logger.info(
      `${phase} scripts: ${newFiles.length} new, ${changedFiles.length} changed, ${unchangedFiles.length} unchanged`,
    );

    const toRun = [...newFiles, ...changedFiles].sort();

    if (toRun.length === 0) {
      logger.success(`All ${phase}-migration scripts are up to date`);
      return { phase, success: true, operationsExecuted: 0, errors: [], dryRun: config.dryRun, blockedDestructive: 0 };
    }

    for (const scriptPath of toRun) {
      const scriptName = path.relative(config.baseDir, scriptPath);
      const sql = readFileSync(scriptPath, "utf-8").trim();

      if (!sql) {
        logger.warn(`Skipping empty script: ${scriptName}`);
        continue;
      }

      logger.info(`  ${config.dryRun ? "[DRY RUN] " : ""}${scriptName}`);

      if (config.dryRun) {
        logger.debug(`  SQL preview: ${sql.substring(0, 200)}${sql.length > 200 ? "..." : ""}`);
        opsExecuted++;
        continue;
      }

      try {
        await client.query("BEGIN");
        await client.query(sql);
        await tracker.recordFile(client, scriptPath, phase);
        await client.query("COMMIT");
        opsExecuted++;
        logger.success(`  Applied: ${scriptName}`);
      } catch (err) {
        await client.query("ROLLBACK");
        const msg = `Script ${scriptName} failed: ${err instanceof Error ? err.message : String(err)}`;
        errors.push(msg);
        logger.error(msg);
        // Stop on first error for safety
        break;
      }
    }

    return {
      phase,
      success: errors.length === 0,
      operationsExecuted: opsExecuted,
      errors,
      dryRun: config.dryRun,
      blockedDestructive: 0,
    };
  });
}

/** Run all phases: pre → migrate → post */
export async function runAll(config: SchemaFlowConfig): Promise<ExecutionResult[]> {
  const results: ExecutionResult[] = [];

  const preResult = await runPre(config);
  results.push(preResult);
  if (!preResult.success) {
    logger.error("Pre-migration failed — aborting");
    return results;
  }

  const migrateResult = await runMigrate(config);
  results.push(migrateResult);
  if (!migrateResult.success) {
    logger.error("Migration failed — aborting (post-migration scripts will not run)");
    return results;
  }

  const postResult = await runPost(config);
  results.push(postResult);

  return results;
}
