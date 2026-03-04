// src/executor/index.ts
// Execute migration operations: pre-scripts → schema migration → post-scripts
// Supports plan (dry-run) and apply modes, and running phases independently

import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import path from "node:path";
import pg from "pg";
import type { SchemaFlowConfig } from "../core/config.js";
import { buildPlan, type PlanOptions } from "../planner/index.js";
import {
  parseTableFile,
  parseFunctionFile,
  parseEnumFile,
  parseExtensionsFile,
  parseViewFile,
  parseMaterializedViewFile,
} from "../schema/parser.js";
import { loadMixins, expandMixins } from "../schema/mixins.js";
import type {
  FunctionSchema,
  TableSchema,
  EnumSchema,
  ExtensionsSchema,
  ViewSchema,
  MaterializedViewSchema,
} from "../schema/types.js";
import { FileTracker } from "../core/tracker.js";
import { withClient, retryOnTimeout, type ClientOptions } from "../core/db.js";
import { logger } from "../core/logger.js";
import { discoverSchemaFiles, discoverScripts } from "../core/files.js";
import { getFunctionComment } from "../introspect/index.js";

/** Derive a stable advisory lock key from the pgSchema name */
function advisoryLockKey(pgSchema: string): string {
  const hash = createHash("sha256").update(`schema-flow:${pgSchema}`).digest();
  // Use first 8 bytes as a BigInt for pg_try_advisory_lock
  const bigint = hash.readBigInt64BE(0);
  // Ensure positive by taking absolute value
  return (bigint < 0n ? -bigint : bigint).toString();
}

/** Try to acquire an advisory lock. Returns true if acquired. */
export async function tryAdvisoryLock(client: pg.PoolClient, pgSchema: string): Promise<boolean> {
  const key = advisoryLockKey(pgSchema);
  const res = await client.query(`SELECT pg_try_advisory_lock(${key}) AS acquired`);
  return res.rows[0].acquired === true;
}

/** Release the advisory lock */
export async function releaseAdvisoryLock(client: pg.PoolClient, pgSchema: string): Promise<void> {
  const key = advisoryLockKey(pgSchema);
  await client.query(`SELECT pg_advisory_unlock(${key})`).catch(() => {});
}

/** Build ClientOptions from config */
function clientOptionsFromConfig(config: SchemaFlowConfig): ClientOptions {
  return {
    lockTimeout: config.lockTimeout,
    statementTimeout: config.statementTimeout,
  };
}

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

export interface ValidationResult {
  valid: boolean;
  errors: string[];
  operationsChecked: number;
}

export interface PrecheckResult {
  passed: boolean;
  failures: { name: string; table: string; message: string }[];
}

/** Run all prechecks from schemas in a read-only transaction */
export async function runPrechecks(client: pg.PoolClient, schemas: TableSchema[]): Promise<PrecheckResult> {
  const failures: PrecheckResult["failures"] = [];
  const allChecks = schemas.flatMap((s) => (s.prechecks || []).map((pc) => ({ ...pc, table: s.table })));

  if (allChecks.length === 0) {
    return { passed: true, failures: [] };
  }

  logger.step("PRECHECKS", `Running ${allChecks.length} pre-migration check(s)`);

  try {
    await client.query("BEGIN READ ONLY");

    for (const check of allChecks) {
      try {
        const res = await client.query(check.query);
        const value = res.rows[0] ? Object.values(res.rows[0])[0] : null;
        const passed = Boolean(value) && value !== 0 && value !== "0" && value !== "f" && value !== "false";
        if (!passed) {
          const msg = check.message || `Precheck "${check.name}" on table ${check.table} returned falsy value`;
          failures.push({ name: check.name, table: check.table, message: msg });
          logger.error(`  FAIL: ${check.name} — ${msg}`);
        } else {
          logger.info(`  PASS: ${check.name}`);
        }
      } catch (err) {
        const msg = `Precheck "${check.name}" on table ${check.table} failed: ${err instanceof Error ? err.message : String(err)}`;
        failures.push({ name: check.name, table: check.table, message: msg });
        logger.error(`  FAIL: ${check.name} — ${msg}`);
      }
    }

    await client.query("ROLLBACK");
  } catch {
    try {
      await client.query("ROLLBACK");
    } catch {
      /* ignore */
    }
  }

  return { passed: failures.length === 0, failures };
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
  const clientOpts = clientOptionsFromConfig(config);

  return await withClient(
    config.connectionString,
    async (client) => {
      // Acquire advisory lock to prevent concurrent migrations (retry with backoff)
      if (!config.dryRun) {
        const maxLockAttempts = config.maxRetries + 1;
        let acquired = false;
        for (let attempt = 0; attempt < maxLockAttempts; attempt++) {
          acquired = await tryAdvisoryLock(client, config.pgSchema);
          if (acquired) break;
          if (attempt < maxLockAttempts - 1) {
            const delay = 1000 * 2 ** attempt;
            logger.warn(
              `Advisory lock not available, retrying in ${delay}ms (attempt ${attempt + 1}/${config.maxRetries})`,
            );
            await new Promise((r) => setTimeout(r, delay));
          }
        }
        if (!acquired) {
          return {
            phase: "migrate" as const,
            success: false,
            operationsExecuted: 0,
            errors: [
              `Another schema-flow migration is already running on this schema. Advisory lock not acquired after ${maxLockAttempts} attempts.`,
            ],
            dryRun: false,
            blockedDestructive: 0,
          };
        }
      }

      try {
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

        // Classify schema files by type
        const functionFiles: string[] = [];
        const tableFiles: string[] = [];
        const enumFiles: string[] = [];
        const extensionFiles: string[] = [];
        const viewFiles: string[] = [];
        const mvFiles: string[] = [];

        for (const f of schemaFiles) {
          const base = path.basename(f);
          if (base.startsWith("fn_")) functionFiles.push(f);
          else if (base.startsWith("enum_")) enumFiles.push(f);
          else if (base === "extensions.yaml" || base === "extensions.yml") extensionFiles.push(f);
          else if (base.startsWith("view_")) viewFiles.push(f);
          else if (base.startsWith("mv_")) mvFiles.push(f);
          else tableFiles.push(f);
        }

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
              const securityClause = fn.security === "definer" ? " SECURITY DEFINER" : "";
              const sql = `CREATE ${replaceClause}FUNCTION ${fn.name}${argsClause} RETURNS ${fn.returns} LANGUAGE ${fn.language}${securityClause} AS $fn_body$\n${fn.body}\n$fn_body$;`;
              logger.debug(`Creating function: ${fn.name}`);
              try {
                await client.query(sql);
              } catch (fnErr) {
                const pgMsg = fnErr instanceof Error ? fnErr.message : String(fnErr);
                throw new Error(`Create function ${fn.name}\n  SQL: ${sql}\n  Error: ${pgMsg}`, { cause: fnErr });
              }
            }
            // Apply function comments
            for (const fn of parsedFunctions) {
              if (fn.comment) {
                const argsClause = fn.args ? `(${fn.args})` : "()";
                const currentComment = await getFunctionComment(client, fn.name, config.pgSchema);
                if (fn.comment !== currentComment) {
                  const escapedComment = fn.comment.replace(/'/g, "''");
                  await client.query(`COMMENT ON FUNCTION ${fn.name}${argsClause} IS '${escapedComment}';`);
                  logger.debug(`Set comment on function: ${fn.name}`);
                }
              }
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

        // Parse enum files
        const parsedEnums: EnumSchema[] = [];
        for (const f of enumFiles) {
          try {
            parsedEnums.push(parseEnumFile(f));
          } catch (err) {
            const msg = `Failed to parse enum ${f}: ${err instanceof Error ? err.message : String(err)}`;
            errors.push(msg);
            logger.error(msg);
          }
        }

        // Parse extensions files
        let parsedExtensions: ExtensionsSchema | undefined;
        for (const f of extensionFiles) {
          try {
            const ext = parseExtensionsFile(f);
            if (parsedExtensions) {
              parsedExtensions.extensions.push(...ext.extensions);
            } else {
              parsedExtensions = ext;
            }
          } catch (err) {
            const msg = `Failed to parse extensions ${f}: ${err instanceof Error ? err.message : String(err)}`;
            errors.push(msg);
            logger.error(msg);
          }
        }

        // Parse view files
        const parsedViews: ViewSchema[] = [];
        for (const f of viewFiles) {
          try {
            parsedViews.push(parseViewFile(f));
          } catch (err) {
            const msg = `Failed to parse view ${f}: ${err instanceof Error ? err.message : String(err)}`;
            errors.push(msg);
            logger.error(msg);
          }
        }

        // Parse materialized view files
        const parsedMvs: MaterializedViewSchema[] = [];
        for (const f of mvFiles) {
          try {
            parsedMvs.push(parseMaterializedViewFile(f));
          } catch (err) {
            const msg = `Failed to parse materialized view ${f}: ${err instanceof Error ? err.message : String(err)}`;
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

        // Expand mixins before building plan
        const mixinMap = await loadMixins(config.mixinsDir);
        if (mixinMap.size > 0) {
          logger.info(`Loaded ${mixinMap.size} mixin(s)`);
        }
        const expandedSchemas = expandMixins(allSchemas, mixinMap);

        // Run prechecks (unless skipped)
        if (!config.skipChecks && !config.dryRun) {
          const precheckResult = await runPrechecks(client, expandedSchemas);
          if (!precheckResult.passed) {
            return {
              phase: "migrate",
              success: false,
              operationsExecuted: 0,
              errors: precheckResult.failures.map((f) => f.message),
              dryRun: false,
              blockedDestructive: 0,
            };
          }
        }

        // Build migration plan — pass safety flag and new schema types
        const planOptions: PlanOptions = {
          allowDestructive: config.allowDestructive,
        };
        if (parsedEnums.length > 0) planOptions.enums = parsedEnums;
        if (parsedExtensions) planOptions.extensions = parsedExtensions;
        if (parsedViews.length > 0) planOptions.views = parsedViews;
        if (parsedMvs.length > 0) planOptions.materializedViews = parsedMvs;

        const plan = await buildPlan(client, expandedSchemas, config.pgSchema, planOptions);

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
        if (plan.summary.validateOpsCount > 0) {
          logger.info(`  Validate operations: ${plan.summary.validateOpsCount}`);
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

        // Capture snapshot and start run tracking
        const { captureSnapshot } = await import("../rollback/snapshot.js");
        const affectedTables = [...new Set(plan.operations.filter((o) => o.table).map((o) => o.table!))];
        const snapshot = await captureSnapshot(client, affectedTables, config.pgSchema);
        await tracker.ensureRunsTable(client);
        const runId = await tracker.startRun(client, plan.operations, snapshot);

        // Execute: structure ops → FK ops → validate ops
        try {
          // Structure ops in a transaction
          if (plan.structureOps.length > 0) {
            logger.step("MIGRATE", `Executing ${plan.structureOps.length} structure operations`);
            // Note: CREATE INDEX CONCURRENTLY and ALTER TYPE ADD VALUE cannot run inside a transaction
            const indexOps = plan.structureOps.filter((o) => o.type === "add_index" || o.type === "add_unique_index");
            const backfillOps = plan.structureOps.filter((o) => o.type === "backfill_column");
            const enumValueOps = plan.structureOps.filter((o) => o.type === "add_enum_value");
            const dropIndexOps = plan.structureOps.filter((o) => o.type === "drop_index");
            const nonIndexOps = plan.structureOps.filter(
              (o) =>
                o.type !== "add_index" &&
                o.type !== "add_unique_index" &&
                o.type !== "backfill_column" &&
                o.type !== "add_enum_value" &&
                o.type !== "drop_index",
            );

            if (nonIndexOps.length > 0) {
              await retryOnTimeout(
                async () => {
                  await client.query("BEGIN");
                  for (const op of nonIndexOps) {
                    const marker = op.destructive ? "[DESTRUCTIVE] " : "";
                    logger.debug(`Executing: ${marker}${op.description}`);
                    try {
                      await client.query(op.sql);
                    } catch (opErr) {
                      const pgMsg = opErr instanceof Error ? opErr.message : String(opErr);
                      throw new Error(`${op.description}\n  SQL: ${op.sql}\n  Error: ${pgMsg}`, { cause: opErr });
                    }
                  }
                  await client.query("COMMIT");
                },
                { label: "structure ops", maxRetries: config.maxRetries },
              );
              opsExecuted += nonIndexOps.length;
            }

            // Index ops outside transaction (CONCURRENTLY)
            for (const op of indexOps) {
              await retryOnTimeout(
                async () => {
                  logger.debug(`Executing: ${op.description}`);
                  try {
                    await client.query(op.sql);
                  } catch (opErr) {
                    const pgMsg = opErr instanceof Error ? opErr.message : String(opErr);
                    throw new Error(`${op.description}\n  SQL: ${op.sql}\n  Error: ${pgMsg}`, { cause: opErr });
                  }
                },
                { label: `index: ${op.description}`, maxRetries: config.maxRetries },
              );
              opsExecuted++;
            }

            // Drop index ops outside transaction (CONCURRENTLY)
            for (const op of dropIndexOps) {
              await retryOnTimeout(
                async () => {
                  logger.debug(`Executing: ${op.description}`);
                  try {
                    await client.query(op.sql);
                  } catch (opErr) {
                    const pgMsg = opErr instanceof Error ? opErr.message : String(opErr);
                    throw new Error(`${op.description}\n  SQL: ${op.sql}\n  Error: ${pgMsg}`, { cause: opErr });
                  }
                },
                { label: `drop index: ${op.description}`, maxRetries: config.maxRetries },
              );
              opsExecuted++;
            }

            // Enum ADD VALUE ops outside transaction (PG requirement)
            for (const op of enumValueOps) {
              logger.debug(`Executing: ${op.description}`);
              try {
                await client.query(op.sql);
              } catch (opErr) {
                const pgMsg = opErr instanceof Error ? opErr.message : String(opErr);
                throw new Error(`${op.description}\n  SQL: ${op.sql}\n  Error: ${pgMsg}`, { cause: opErr });
              }
              opsExecuted++;
            }

            // Backfill ops — each runs in batched transactions
            if (backfillOps.length > 0) {
              const { runBackfill } = await import("../expand/backfill.js");
              const { ExpandTracker } = await import("../expand/tracker.js");
              const expandTracker = new ExpandTracker();
              await expandTracker.ensureTable(client);
              const pool = (await import("../core/db.js")).getPool(config.connectionString);

              for (const op of backfillOps) {
                const meta = op.meta as Record<string, unknown>;
                const tableName = op.table!;
                const newCol = meta.to as string;
                const oldCol = meta.from as string;
                const transform = meta.transform as string;
                const batchSize = (meta.batchSize as number) || 1000;
                const triggerName = meta.triggerName as string;
                const functionName = meta.functionName as string;

                // Register expand
                const expandId = await expandTracker.register(client, {
                  tableName,
                  oldColumn: oldCol,
                  newColumn: newCol,
                  transform,
                  triggerName,
                  functionName,
                  batchSize,
                });

                await expandTracker.updateStatus(client, expandId, "backfilling");

                // Run batched backfill
                await runBackfill(pool, {
                  tableName,
                  newColumn: newCol,
                  transform,
                  batchSize,
                  pgSchema: config.pgSchema,
                  maxRetries: config.maxRetries,
                });

                await expandTracker.updateStatus(client, expandId, "expanded");
                opsExecuted++;
              }
            }
          }

          // FK ops in a separate transaction
          if (plan.foreignKeyOps.length > 0) {
            logger.step("MIGRATE", `Executing ${plan.foreignKeyOps.length} foreign key operations`);
            await retryOnTimeout(
              async () => {
                await client.query("BEGIN");
                for (const op of plan.foreignKeyOps) {
                  logger.debug(`Executing: ${op.description}`);
                  try {
                    await client.query(op.sql);
                  } catch (opErr) {
                    const pgMsg = opErr instanceof Error ? opErr.message : String(opErr);
                    throw new Error(`${op.description}\n  SQL: ${op.sql}\n  Error: ${pgMsg}`, { cause: opErr });
                  }
                }
                await client.query("COMMIT");
              },
              { label: "foreign key ops", maxRetries: config.maxRetries },
            );
            opsExecuted += plan.foreignKeyOps.length;
          }

          // Validate phase — runs outside any transaction, one statement at a time
          if (plan.validateOps.length > 0) {
            logger.step("MIGRATE", `Validating ${plan.validateOps.length} constraint(s)`);
            for (const op of plan.validateOps) {
              await retryOnTimeout(
                async () => {
                  logger.debug(`Executing: ${op.description}`);
                  try {
                    await client.query(op.sql);
                  } catch (opErr) {
                    const pgMsg = opErr instanceof Error ? opErr.message : String(opErr);
                    throw new Error(`${op.description}\n  SQL: ${op.sql}\n  Error: ${pgMsg}`, { cause: opErr });
                  }
                },
                { label: `validate: ${op.description}`, maxRetries: config.maxRetries },
              );
              opsExecuted++;
            }
          }

          // Record all processed files
          for (const f of [...newFiles, ...changedFiles]) {
            await tracker.recordFile(client, f, "schema");
          }

          // Mark run as completed
          await tracker.completeRun(client, runId, opsExecuted);

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
          // Mark run as failed
          try {
            await tracker.failRun(client, runId, opsExecuted);
          } catch {
            // Best effort
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
      } finally {
        // Release advisory lock
        if (!config.dryRun) {
          await releaseAdvisoryLock(client, config.pgSchema);
        }
      }
    },
    clientOpts,
  );
}

/** Validate schema against a live database (executes in a transaction, always rolls back) */
export async function runValidate(config: SchemaFlowConfig): Promise<ValidationResult> {
  logger.step("VALIDATE", "Validating schema against database");
  const errors: string[] = [];
  let opsChecked = 0;

  const schemaFiles = await discoverSchemaFiles(config.schemaDir);
  if (schemaFiles.length === 0) {
    logger.info("No schema files found — nothing to validate");
    return { valid: true, errors: [], operationsChecked: 0 };
  }

  // Separate function files from table files
  const functionFiles = schemaFiles.filter((f) => path.basename(f).startsWith("fn_"));
  const tableFiles = schemaFiles.filter((f) => !path.basename(f).startsWith("fn_"));

  // Parse function files
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
    return { valid: false, errors, operationsChecked: 0 };
  }

  // Parse ALL table schema files
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
    return { valid: false, errors, operationsChecked: 0 };
  }

  // Expand mixins
  const mixinMap = await loadMixins(config.mixinsDir);
  const expandedSchemas = expandMixins(allSchemas, mixinMap);

  return await withClient(config.connectionString, async (client) => {
    // Build plan with allowDestructive: true so all operations are validated
    const plan = await buildPlan(client, expandedSchemas, config.pgSchema, {
      allowDestructive: true,
    });

    const allOps = [...plan.structureOps, ...plan.foreignKeyOps];

    if (parsedFunctions.length === 0 && allOps.length === 0) {
      logger.success("Schema is valid — no operations to check");
      return { valid: true, errors: [], operationsChecked: 0 };
    }

    // Skip validate ops (they require committed data)
    if (plan.validateOps.length > 0) {
      logger.info(`Skipping ${plan.validateOps.length} validate operations (requires committed data)`);
    }

    // Execute everything inside a single transaction that always rolls back
    try {
      await client.query("BEGIN");

      // Functions first
      for (const fn of parsedFunctions) {
        const replaceClause = fn.replace ? "OR REPLACE " : "";
        const argsClause = fn.args ? `(${fn.args})` : "()";
        const securityClause = fn.security === "definer" ? " SECURITY DEFINER" : "";
        const sql = `CREATE ${replaceClause}FUNCTION ${fn.name}${argsClause} RETURNS ${fn.returns} LANGUAGE ${fn.language}${securityClause} AS $fn_body$\n${fn.body}\n$fn_body$;`;
        logger.debug(`Validating function: ${fn.name}`);
        await client.query(sql);
        opsChecked++;
      }

      // Structure ops — strip CONCURRENTLY and NOT VALID from SQL (can't run inside a transaction)
      for (const op of plan.structureOps) {
        let sql = op.sql.replace(/\bCONCURRENTLY\s+/gi, "");
        sql = sql.replace(/\s+NOT VALID/gi, "");
        logger.debug(`Validating: ${op.description}`);
        await client.query(sql);
        opsChecked++;
      }

      // FK ops — strip NOT VALID for validation
      for (const op of plan.foreignKeyOps) {
        const sql = op.sql.replace(/\s+NOT VALID/gi, "");
        logger.debug(`Validating: ${op.description}`);
        await client.query(sql);
        opsChecked++;
      }

      // Always roll back — validation only
      await client.query("ROLLBACK");
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      errors.push(msg);
      logger.error(`Validation failed: ${msg}`);
      try {
        await client.query("ROLLBACK");
      } catch {
        // Rollback may fail if connection is in a bad state
      }
      return { valid: false, errors, operationsChecked: opsChecked };
    }

    return { valid: true, errors: [], operationsChecked: opsChecked };
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
  const clientOpts = clientOptionsFromConfig(config);

  return await withClient(
    config.connectionString,
    async (client) => {
      await tracker.ensureTable(client);
      const tracked = await tracker.getTracked(client);

      const { newFiles, changedFiles, unchangedFiles } = tracker.classifyFiles(scripts, tracked, phase);

      logger.info(
        `${phase} scripts: ${newFiles.length} new, ${changedFiles.length} changed, ${unchangedFiles.length} unchanged`,
      );

      const toRun = [...newFiles, ...changedFiles].sort();

      if (toRun.length === 0) {
        logger.success(`All ${phase}-migration scripts are up to date`);
        return {
          phase,
          success: true,
          operationsExecuted: 0,
          errors: [],
          dryRun: config.dryRun,
          blockedDestructive: 0,
        };
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
          await retryOnTimeout(
            async () => {
              await client.query("BEGIN");
              await client.query(sql);
              await tracker.recordFile(client, scriptPath, phase);
              await client.query("COMMIT");
            },
            { label: `script: ${scriptName}`, maxRetries: config.maxRetries },
          );
          opsExecuted++;
          logger.success(`  Applied: ${scriptName}`);
        } catch (err) {
          try {
            await client.query("ROLLBACK");
          } catch {
            /* may not be in txn */
          }
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
    },
    clientOpts,
  );
}

export interface BaselineResult {
  success: boolean;
  filesRecorded: number;
  errors: string[];
}

/** Mark all current schema files as applied without running any SQL */
export async function runBaseline(config: SchemaFlowConfig): Promise<BaselineResult> {
  logger.step("BASELINE", "Recording current schema state without executing migrations");

  const schemaFiles = await discoverSchemaFiles(config.schemaDir);
  if (schemaFiles.length === 0) {
    logger.info("No schema files found — nothing to baseline");
    return { success: true, filesRecorded: 0, errors: [] };
  }

  const tracker = new FileTracker(config.historyTable);
  const clientOpts = clientOptionsFromConfig(config);

  return await withClient(
    config.connectionString,
    async (client) => {
      await tracker.ensureTable(client);
      await tracker.ensureRunsTable(client);

      // Record all schema files as applied
      let recorded = 0;
      for (const f of schemaFiles) {
        await tracker.recordFile(client, f, "schema");
        recorded++;
      }

      // Record pre/post scripts too
      const preScripts = await discoverScripts(config.preDir);
      for (const f of preScripts) {
        await tracker.recordFile(client, f, "pre");
        recorded++;
      }
      const postScripts = await discoverScripts(config.postDir);
      for (const f of postScripts) {
        await tracker.recordFile(client, f, "post");
        recorded++;
      }

      // Create a baseline run record
      const runId = await tracker.startRun(client, [], null);
      await tracker.completeRun(client, runId, 0);

      logger.success(`Baseline complete — ${recorded} files recorded`);
      return { success: true, filesRecorded: recorded, errors: [] };
    },
    clientOpts,
  );
}

export interface RepeatableResult {
  success: boolean;
  filesExecuted: number;
  errors: string[];
}

/** Execute repeatable SQL files that have changed since last run */
export async function runRepeatables(config: SchemaFlowConfig): Promise<RepeatableResult> {
  const { existsSync } = await import("node:fs");
  if (!existsSync(config.repeatableDir)) {
    return { success: true, filesExecuted: 0, errors: [] };
  }

  const scripts = await discoverScripts(config.repeatableDir);
  if (scripts.length === 0) {
    return { success: true, filesExecuted: 0, errors: [] };
  }

  const errors: string[] = [];
  let executed = 0;

  const tracker = new FileTracker(config.historyTable);
  const clientOpts = clientOptionsFromConfig(config);

  return await withClient(
    config.connectionString,
    async (client) => {
      await tracker.ensureTable(client);
      const tracked = await tracker.getTracked(client);

      const { newFiles, changedFiles } = tracker.classifyFiles(scripts, tracked, "repeatable");

      const toRun = [...newFiles, ...changedFiles].sort();
      if (toRun.length === 0) {
        return { success: true, filesExecuted: 0, errors: [] };
      }

      logger.step("REPEATABLE", `Executing ${toRun.length} repeatable script(s)`);

      for (const scriptPath of toRun) {
        const scriptName = path.relative(config.baseDir, scriptPath);
        const sql = readFileSync(scriptPath, "utf-8").trim();

        if (!sql) continue;

        if (config.dryRun) {
          logger.info(`  [DRY RUN] ${scriptName}`);
          executed++;
          continue;
        }

        try {
          logger.info(`  Executing: ${scriptName}`);
          await retryOnTimeout(
            async () => {
              await client.query(sql);
              await tracker.recordFile(client, scriptPath, "repeatable");
            },
            { label: `repeatable: ${scriptName}`, maxRetries: config.maxRetries },
          );
          executed++;
          logger.success(`  Applied: ${scriptName}`);
        } catch (err) {
          const msg = `Repeatable script ${scriptName} failed: ${err instanceof Error ? err.message : String(err)}`;
          errors.push(msg);
          logger.error(msg);
          break;
        }
      }

      return { success: errors.length === 0, filesExecuted: executed, errors };
    },
    clientOpts,
  );
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

  // Run repeatable scripts (after migrate, before post)
  await runRepeatables(config);

  const postResult = await runPost(config);
  results.push(postResult);

  return results;
}
