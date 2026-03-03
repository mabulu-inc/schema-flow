#!/usr/bin/env node
// src/cli/index.ts
// CLI entry point for schema-flow — declarative zero-downtime PostgreSQL migrations

import { createRequire } from "node:module";
import { resolveConfig, validateDirectories, type SchemaFlowConfig } from "../core/config.js";
import { loadConfigFile, resolveEnvironmentConfig } from "../core/config-file.js";
import { logger, LogLevel } from "../core/logger.js";
import { testConnection, closePool } from "../core/db.js";
import { runAll, runPre, runMigrate, runPost, runValidate, runBaseline } from "../executor/index.js";
import { scaffoldPre, scaffoldPost, generateFromDb, scaffoldInit, scaffoldMixin } from "../scaffold/index.js";
import { printDocsReference } from "../docs/index.js";

const require = createRequire(import.meta.url);
const { version } = require("../../package.json") as { version: string };

// ─── Argument Parsing ───────────────────────────────────────────────────────

interface CliArgs {
  command: string;
  subcommand?: string;
  name?: string;
  flags: {
    dryRun: boolean;
    verbose: boolean;
    quiet: boolean;
    allowDestructive: boolean;
    connectionString?: string;
    baseDir?: string;
    schema?: string;
    lockTimeout?: string;
    statementTimeout?: string;
    help: boolean;
    version: boolean;
    json: boolean;
    output?: string;
    outputDir?: string;
    outputName?: string;
    skipChecks: boolean;
    apply: boolean;
    env?: string;
  };
}

function parseArgs(argv: string[]): CliArgs {
  const args = argv.slice(2);
  const command = args[0] || "help";
  const subcommand = args[1] && !args[1].startsWith("-") ? args[1] : undefined;
  const name = args.find((a, i) => i > 0 && !a.startsWith("-") && a !== subcommand) || subcommand;

  const flags = {
    dryRun: args.includes("--dry-run") || args.includes("--plan") || args.includes("-n"),
    verbose: args.includes("--verbose") || args.includes("-v"),
    quiet: args.includes("--quiet") || args.includes("-q"),
    allowDestructive: args.includes("--allow-destructive"),
    connectionString: getFlag(args, "--connection-string") || getFlag(args, "--db"),
    baseDir: getFlag(args, "--dir"),
    schema: getFlag(args, "--schema"),
    lockTimeout: getFlag(args, "--lock-timeout"),
    statementTimeout: getFlag(args, "--statement-timeout"),
    help: args.includes("--help") || args.includes("-h"),
    version: args.includes("--version") || args.includes("-V"),
    json: args.includes("--json"),
    output: getFlag(args, "--output") || getFlag(args, "-o"),
    outputDir: getFlag(args, "--output-dir"),
    outputName: getFlag(args, "--name"),
    skipChecks: args.includes("--skip-checks"),
    apply: args.includes("--apply"),
    env: getFlag(args, "--env"),
  };

  return { command, subcommand, name, flags };
}

function getFlag(args: string[], flag: string): string | undefined {
  const idx = args.indexOf(flag);
  if (idx !== -1 && idx + 1 < args.length) {
    return args[idx + 1];
  }
  // Also check for --flag=value
  const eqFlag = args.find((a) => a.startsWith(`${flag}=`));
  if (eqFlag) return eqFlag.split("=").slice(1).join("=");
  return undefined;
}

// ─── Help ───────────────────────────────────────────────────────────────────

function printHelp(): void {
  console.log(`
  ${"\x1b[1m\x1b[36m"}◆ schema-flow${"\x1b[0m"} — Declarative zero-downtime PostgreSQL migrations

  ${"\x1b[1m"}Usage:${"\x1b[0m"}
    npx @mabulu-inc/schema-flow <command> [options]

  ${"\x1b[1m"}Commands:${"\x1b[0m"}
    ${"\x1b[36m"}run${"\x1b[0m"}                Run all phases: pre → migrate → post
    ${"\x1b[36m"}run pre${"\x1b[0m"}             Run only pre-migration scripts
    ${"\x1b[36m"}run migrate${"\x1b[0m"}         Run only declarative schema migration
    ${"\x1b[36m"}run post${"\x1b[0m"}            Run only post-migration scripts
    ${"\x1b[36m"}plan${"\x1b[0m"}               Show what would be done without applying (alias for run --dry-run)
    ${"\x1b[36m"}validate${"\x1b[0m"}           Validate schema against a live database (dry run with rollback)
    ${"\x1b[36m"}drift${"\x1b[0m"}              Compare live database vs. YAML and report differences
    ${"\x1b[36m"}lint${"\x1b[0m"}               Static analysis of migration plan for dangerous patterns
    ${"\x1b[36m"}down${"\x1b[0m"}               Show or apply reverse migration (rollback last run)
    ${"\x1b[36m"}sql${"\x1b[0m"}                Generate migration SQL file from plan
    ${"\x1b[36m"}erd${"\x1b[0m"}                Generate Mermaid ER diagram from schema YAML
    ${"\x1b[36m"}contract${"\x1b[0m"}            Finalize expand/contract: drop old columns and triggers
    ${"\x1b[36m"}expand-status${"\x1b[0m"}       Show current expand/contract operation status
    ${"\x1b[36m"}generate${"\x1b[0m"}            Generate schema files from existing database
    ${"\x1b[36m"}baseline${"\x1b[0m"}            Mark existing database as managed without running migrations
    ${"\x1b[36m"}new pre <name>${"\x1b[0m"}      Scaffold a new pre-migration script
    ${"\x1b[36m"}new post <name>${"\x1b[0m"}     Scaffold a new post-migration script
    ${"\x1b[36m"}new mixin <name>${"\x1b[0m"}    Scaffold a new mixin YAML file
    ${"\x1b[36m"}init${"\x1b[0m"}               Initialize directory structure (schema/, pre/, post/, repeatable/)
    ${"\x1b[36m"}docs${"\x1b[0m"}               Print YAML format reference
    ${"\x1b[36m"}status${"\x1b[0m"}              Show migration status and pending changes
    ${"\x1b[36m"}help${"\x1b[0m"}               Show this help message

  ${"\x1b[1m"}Options:${"\x1b[0m"}
    --dry-run, --plan, -n      Show what would be done without applying
    --allow-destructive        Allow destructive operations (column drops, type narrowing, etc.)
    --lock-timeout <duration>  Lock timeout for DDL statements (default: 5s, 0 to disable)
    --statement-timeout <dur>  Statement timeout for DDL statements (default: 30s, 0 to disable)
    --verbose, -v              Enable debug logging
    --quiet, -q                Suppress non-essential output
    --json                     Output in JSON format (drift, lint)
    --output, -o <file>        Write output to a file (erd, sql)
    --output-dir <dir>         Output directory (sql)
    --name <name>              Output file name suffix (sql)
    --skip-checks              Skip pre-migration checks
    --apply                    Execute the operation (down)
    --env <name>               Select environment from schema-flow.config.yaml
    --connection-string, --db  PostgreSQL connection string (or set DATABASE_URL)
    --dir                      Base directory (default: current directory)
    --schema                   PostgreSQL schema (default: public)
    -h, --help                 Show help
    -V, --version              Show version

  ${"\x1b[1m"}Safety:${"\x1b[0m"}
    By default, schema-flow only performs safe, additive operations:
      ✓ Create tables, add columns, add indexes, add constraints
      ✓ Widen column types (integer → bigint), make columns nullable
      ✗ Drop columns, drop tables, narrow types — ${"\x1b[33m"}blocked unless --allow-destructive${"\x1b[0m"}

    Use ${"\x1b[36m"}schema-flow plan${"\x1b[0m"} to preview what would change, including blocked operations.

  ${"\x1b[1m"}Convention:${"\x1b[0m"}
    schema-flow expects the following directory structure:
      schema-flow/
        schema/    Declarative table YAML files (one per table) and function files (fn_*.yaml)
        pre/       Pre-migration SQL scripts (run before schema changes)
        post/      Post-migration SQL scripts (run after schema changes)
        mixins/    Reusable schema mixins (e.g., timestamps, soft_delete)

  ${"\x1b[1m"}Environment:${"\x1b[0m"}
    SCHEMA_FLOW_DATABASE_URL           PostgreSQL connection string (takes precedence)
    DATABASE_URL                       Fallback connection string
    SCHEMA_FLOW_LOG_LEVEL              Log level: debug, info, warn, error, silent
    SCHEMA_FLOW_ALLOW_DESTRUCTIVE      Set to "true" to allow destructive operations
    SCHEMA_FLOW_LOCK_TIMEOUT           Lock timeout for DDL (default: "5s")
    SCHEMA_FLOW_STATEMENT_TIMEOUT      Statement timeout for DDL (default: "30s")
`);
}

// ─── Status Command ─────────────────────────────────────────────────────────

async function showStatus(config: SchemaFlowConfig): Promise<void> {
  logger.banner("Migration Status");

  const { withClient } = await import("../core/db.js");
  const { FileTracker } = await import("../core/tracker.js");
  const { glob } = await import("glob");
  const path = await import("node:path");

  const tracker = new FileTracker(config.historyTable);

  await withClient(config.connectionString, async (client) => {
    await tracker.ensureTable(client);
    const tracked = await tracker.getTracked(client);

    // Schema files
    const schemaFiles = await glob([path.join(config.schemaDir, "*.yaml"), path.join(config.schemaDir, "*.yml")]);
    const schemaStatus = tracker.classifyFiles(schemaFiles, tracked, "schema");

    // Pre scripts
    const preFiles = await glob([path.join(config.preDir, "*.sql")]);
    const preStatus = tracker.classifyFiles(preFiles, tracked, "pre");

    // Post scripts
    const postFiles = await glob([path.join(config.postDir, "*.sql")]);
    const postStatus = tracker.classifyFiles(postFiles, tracked, "post");

    console.log("");
    console.log("  Schema files:");
    console.log(`    New:       ${schemaStatus.newFiles.length}`);
    console.log(`    Changed:   ${schemaStatus.changedFiles.length}`);
    console.log(`    Unchanged: ${schemaStatus.unchangedFiles.length}`);

    console.log("");
    console.log("  Pre-migration scripts:");
    console.log(`    New:       ${preStatus.newFiles.length}`);
    console.log(`    Changed:   ${preStatus.changedFiles.length}`);
    console.log(`    Unchanged: ${preStatus.unchangedFiles.length}`);

    // Mixin files
    const mixinFiles = await glob([
      path.join(config.mixinsDir, "*.yaml"),
      path.join(config.mixinsDir, "*.yml"),
    ]);

    console.log("");
    console.log("  Post-migration scripts:");
    console.log(`    New:       ${postStatus.newFiles.length}`);
    console.log(`    Changed:   ${postStatus.changedFiles.length}`);
    console.log(`    Unchanged: ${postStatus.unchangedFiles.length}`);

    if (mixinFiles.length > 0) {
      console.log("");
      console.log(`  Mixins: ${mixinFiles.length} file(s)`);
    }

    const totalPending =
      schemaStatus.newFiles.length +
      schemaStatus.changedFiles.length +
      preStatus.newFiles.length +
      preStatus.changedFiles.length +
      postStatus.newFiles.length +
      postStatus.changedFiles.length;

    console.log("");

    if (!config.allowDestructive) {
      console.log("  Mode: \x1b[32mSafe\x1b[0m (destructive operations are blocked)");
    } else {
      console.log("  Mode: \x1b[33mDestructive allowed\x1b[0m (--allow-destructive is ON)");
    }

    console.log("");
    if (totalPending > 0) {
      logger.warn(`${totalPending} pending changes detected`);
    } else {
      logger.success("Everything is up to date");
    }
    console.log("");
  });
}

// ─── Main ───────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const args = parseArgs(process.argv);

  // Configure log level
  if (args.flags.verbose) {
    logger.setLevel(LogLevel.DEBUG);
  } else if (args.flags.quiet) {
    logger.setLevel(LogLevel.WARN);
  } else if (process.env.SCHEMA_FLOW_LOG_LEVEL) {
    const envLevel = process.env.SCHEMA_FLOW_LOG_LEVEL.toUpperCase();
    const levelMap: Record<string, LogLevel> = {
      DEBUG: LogLevel.DEBUG,
      INFO: LogLevel.INFO,
      WARN: LogLevel.WARN,
      ERROR: LogLevel.ERROR,
      SILENT: LogLevel.SILENT,
    };
    if (levelMap[envLevel] !== undefined) {
      logger.setLevel(levelMap[envLevel]);
    }
  }

  // Version
  if (args.flags.version) {
    console.log(version);
    process.exit(0);
  }

  // Help
  if (args.flags.help || args.command === "help") {
    printHelp();
    process.exit(0);
  }

  // Init command doesn't need a database connection
  if (args.command === "init") {
    const baseDir = args.flags.baseDir || process.cwd();
    scaffoldInit(baseDir);
    process.exit(0);
  }

  // ERD command doesn't need a database connection
  if (args.command === "erd") {
    const baseDir = args.flags.baseDir || process.cwd();
    const minimalConfig = resolveConfig({
      baseDir,
      connectionString: "not-needed",
    });

    const { generateErdFromFiles } = await import("../erd/index.js");
    const erd = await generateErdFromFiles(minimalConfig.schemaDir, minimalConfig.mixinsDir);

    if (args.flags.output) {
      const { writeFileSync, mkdirSync } = await import("node:fs");
      const path = await import("node:path");
      mkdirSync(path.dirname(args.flags.output), { recursive: true });
      writeFileSync(args.flags.output, erd, "utf-8");
      logger.success(`ERD written to ${args.flags.output}`);
    } else {
      process.stdout.write(erd);
    }
    process.exit(0);
  }

  // Scaffold commands don't need a database connection
  if (args.command === "new") {
    const baseDir = args.flags.baseDir || process.cwd();
    const minimalConfig = resolveConfig({
      baseDir,
      connectionString: "not-needed",
    });

    if (args.subcommand === "pre") {
      const name = args.name || "migration";
      scaffoldPre(minimalConfig, name);
    } else if (args.subcommand === "post") {
      const name = args.name || "migration";
      scaffoldPost(minimalConfig, name);
    } else if (args.subcommand === "mixin") {
      if (!args.name || args.name === "mixin") {
        logger.error("Usage: schema-flow new mixin <name>");
        process.exit(1);
      }
      scaffoldMixin(minimalConfig, args.name);
    } else {
      logger.error("Usage: schema-flow new <pre|post|mixin> <name>");
      process.exit(1);
    }
    process.exit(0);
  }

  // Docs command doesn't need a database connection
  if (args.command === "docs") {
    printDocsReference();
    process.exit(0);
  }

  // All other commands need a database connection
  let config: SchemaFlowConfig;
  try {
    // Load config file if present
    const configBaseDir = args.flags.baseDir || process.cwd();
    const configFile = loadConfigFile(configBaseDir);
    let envConfig: Partial<SchemaFlowConfig> = {};

    if (configFile && args.flags.env) {
      const resolved = resolveEnvironmentConfig(configFile, args.flags.env);
      if (!resolved) {
        logger.error(`Environment "${args.flags.env}" not found in schema-flow.config.yaml`);
        process.exit(1);
      }
      envConfig = {
        connectionString: resolved.connectionString,
        pgSchema: resolved.pgSchema,
        lockTimeout: resolved.lockTimeout,
        statementTimeout: resolved.statementTimeout,
      };
    }

    // CLI flags take precedence over config file
    config = resolveConfig({
      connectionString: args.flags.connectionString || envConfig.connectionString,
      baseDir: args.flags.baseDir,
      pgSchema: args.flags.schema || envConfig.pgSchema,
      dryRun: args.flags.dryRun || args.command === "plan",
      allowDestructive: args.flags.allowDestructive,
      lockTimeout: args.flags.lockTimeout || envConfig.lockTimeout,
      statementTimeout: args.flags.statementTimeout || envConfig.statementTimeout,
      skipChecks: args.flags.skipChecks,
    });
  } catch (err) {
    logger.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  }

  // Validate directories (skip for generate since it creates them)
  if (args.command !== "generate") {
    const warnings = validateDirectories(config);
    for (const w of warnings) {
      logger.warn(w);
    }
  }

  // Test connection
  try {
    await testConnection(config.connectionString);
    logger.debug("Database connection verified");
  } catch (err) {
    logger.error(`Cannot connect to database: ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  }

  let exitCode = 0;

  try {
    switch (args.command) {
      case "run": {
        if (!args.subcommand) {
          // Run all phases
          logger.banner(config.dryRun ? "Dry Run — All Phases" : "Running All Phases");
          if (!config.allowDestructive) {
            logger.info("Safe mode — destructive operations will be blocked");
          }
          const results = await runAll(config);
          const failed = results.some((r) => !r.success);
          if (failed) exitCode = 1;

          // Summary
          logger.divider();
          for (const r of results) {
            const status = r.success ? "✓" : "✗";
            const dryLabel = r.dryRun ? " (dry run)" : "";
            const blockedLabel = r.blockedDestructive > 0 ? ` (${r.blockedDestructive} destructive blocked)` : "";
            logger.info(`${status} ${r.phase}: ${r.operationsExecuted} operations${dryLabel}${blockedLabel}`);
          }
        } else if (args.subcommand === "pre") {
          logger.banner(config.dryRun ? "Dry Run — Pre-Migration" : "Pre-Migration");
          const result = await runPre(config);
          if (!result.success) exitCode = 1;
        } else if (args.subcommand === "migrate") {
          logger.banner(config.dryRun ? "Dry Run — Schema Migration" : "Schema Migration");
          if (!config.allowDestructive) {
            logger.info("Safe mode — destructive operations will be blocked");
          }
          const result = await runMigrate(config);
          if (!result.success) exitCode = 1;
        } else if (args.subcommand === "post") {
          logger.banner(config.dryRun ? "Dry Run — Post-Migration" : "Post-Migration");
          const result = await runPost(config);
          if (!result.success) exitCode = 1;
        } else {
          logger.error(`Unknown subcommand: ${args.subcommand}`);
          exitCode = 1;
        }
        break;
      }

      case "plan": {
        logger.banner("Migration Plan (Dry Run)");
        config.dryRun = true;
        if (!config.allowDestructive) {
          logger.info("Safe mode — destructive operations will be shown as blocked");
        }
        const results = await runAll(config);
        const totalOps = results.reduce((sum, r) => sum + r.operationsExecuted, 0);
        const totalBlocked = results.reduce((sum, r) => sum + r.blockedDestructive, 0);
        logger.divider();
        if (totalOps === 0 && totalBlocked === 0) {
          logger.success("No changes detected");
        } else {
          if (totalOps > 0) {
            logger.info(`${totalOps} total operations would be executed`);
          }
          if (totalBlocked > 0) {
            logger.warn(`${totalBlocked} destructive operations blocked — use --allow-destructive to include`);
          }
        }
        break;
      }

      case "validate": {
        logger.banner("Validating Schema");
        const result = await runValidate(config);
        if (result.valid) {
          logger.success(`Validation passed — ${result.operationsChecked} operation(s) checked`);
        } else {
          for (const err of result.errors) {
            logger.error(err);
          }
          exitCode = 1;
        }
        break;
      }

      case "lint": {
        logger.banner("Migration Lint");
        const pathMod = await import("node:path");
        const { discoverSchemaFiles: discoverFiles } = await import("../core/files.js");
        const schemaFilesForLint = await discoverFiles(config.schemaDir);
        const tableFilesForLint = schemaFilesForLint.filter(
          (f) => !pathMod.default.basename(f).startsWith("fn_"),
        );

        // Parse and expand
        const { parseTableFile: ptf } = await import("../schema/parser.js");
        const { loadMixins: lm, expandMixins: em } = await import("../schema/mixins.js");
        const parsedForLint = tableFilesForLint.map((f) => ptf(f));
        const mixinsForLint = await lm(config.mixinsDir);
        const expandedForLint = em(parsedForLint, mixinsForLint);

        // Build plan with allowDestructive to see all ops
        const { buildPlan: bp } = await import("../planner/index.js");
        const { withClient: wc } = await import("../core/db.js");
        const lintPlan = await wc(config.connectionString, async (client) => {
          return bp(client, expandedForLint, config.pgSchema, { allowDestructive: true });
        });

        const { lintPlan: lint, formatLintFindings, formatLintFindingsJson } = await import("../lint/index.js");
        const findings = lint({ plan: lintPlan, schemas: expandedForLint, pgSchema: config.pgSchema });

        if (args.flags.json) {
          console.log(formatLintFindingsJson(findings));
        } else {
          console.log(formatLintFindings(findings));
        }

        const hasErrors = findings.some((f) => f.severity === "error");
        if (hasErrors) exitCode = 1;
        break;
      }

      case "drift": {
        logger.banner("Drift Detection");
        const { detectDrift } = await import("../drift/index.js");
        const { formatDriftReport, formatDriftReportJson } = await import("../drift/format.js");
        const report = await detectDrift(config);
        if (args.flags.json) {
          console.log(formatDriftReportJson(report));
        } else if (!args.flags.quiet) {
          console.log(formatDriftReport(report));
        }
        if (report.hasDrift) exitCode = 1;
        break;
      }

      case "sql": {
        logger.banner("SQL File Generation");
        const pathModSql = await import("node:path");
        const { discoverSchemaFiles: discoverSql } = await import("../core/files.js");
        const sqlSchemaFiles = await discoverSql(config.schemaDir);
        const sqlFunctionFiles = sqlSchemaFiles.filter((f) => pathModSql.default.basename(f).startsWith("fn_"));
        const sqlTableFiles = sqlSchemaFiles.filter((f) => !pathModSql.default.basename(f).startsWith("fn_"));

        const { parseTableFile: ptfSql, parseFunctionFile: pffSql } = await import("../schema/parser.js");
        const { loadMixins: lmSql, expandMixins: emSql } = await import("../schema/mixins.js");

        const sqlParsedFunctions = sqlFunctionFiles.map((f) => pffSql(f));
        const sqlParsed = sqlTableFiles.map((f) => ptfSql(f));
        const sqlMixins = await lmSql(config.mixinsDir);
        const sqlExpanded = emSql(sqlParsed, sqlMixins);

        const { buildPlan: bpSql } = await import("../planner/index.js");
        const { withClient: wcSql } = await import("../core/db.js");
        const sqlPlan = await wcSql(config.connectionString, async (client) => {
          return bpSql(client, sqlExpanded, config.pgSchema, {
            allowDestructive: config.allowDestructive,
          });
        });

        const { generateSqlFile, formatMigrationSql } = await import("../sql/index.js");

        if (args.flags.output) {
          // Write to specific file
          const { writeFileSync: wfs } = await import("node:fs");
          const sql = formatMigrationSql(sqlPlan, sqlParsedFunctions, {
            pgSchema: config.pgSchema,
            version: version,
          });
          wfs(args.flags.output, sql, "utf-8");
          logger.success(`SQL written to ${args.flags.output}`);
        } else {
          const outputDir = args.flags.outputDir || pathModSql.default.join(config.baseDir, "migrations");
          const result = generateSqlFile(sqlPlan, sqlParsedFunctions, {
            outputDir,
            name: args.flags.outputName,
            pgSchema: config.pgSchema,
            version: version,
          });
          logger.success(`Generated ${result.filePath} (${result.operationCount} operations)`);
        }
        break;
      }

      case "generate": {
        await generateFromDb(config);
        break;
      }

      case "contract": {
        logger.banner("Contract (Expand/Contract)");
        const { runContract } = await import("../expand/executor.js");
        const contractResult = await runContract(config, {
          allowDestructive: args.flags.allowDestructive,
        });
        if (!contractResult.success) exitCode = 1;
        break;
      }

      case "expand-status": {
        const { showExpandStatus } = await import("../expand/executor.js");
        await showExpandStatus(config);
        break;
      }

      case "down": {
        logger.banner("Down Migration (Rollback)");
        const { runDown } = await import("../rollback/executor.js");
        const downResult = await runDown(config, {
          apply: args.flags.apply,
          allowDestructive: args.flags.allowDestructive,
        });
        if (!downResult.success) exitCode = 1;
        break;
      }

      case "baseline": {
        logger.banner("Baseline");
        const baselineResult = await runBaseline(config);
        if (!baselineResult.success) exitCode = 1;
        break;
      }

      case "status": {
        await showStatus(config);
        break;
      }

      default: {
        logger.error(`Unknown command: ${args.command}`);
        printHelp();
        exitCode = 1;
      }
    }
  } catch (err) {
    logger.error(`Unexpected error: ${err instanceof Error ? err.message : String(err)}`);
    if (args.flags.verbose && err instanceof Error) {
      console.error(err.stack);
    }
    exitCode = 1;
  } finally {
    await closePool();
  }

  process.exit(exitCode);
}

main();
