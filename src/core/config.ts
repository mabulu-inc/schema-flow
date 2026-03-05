// src/core/config.ts
// Convention-over-configuration: resolve paths, connection string, defaults

import path from "node:path";
import { existsSync } from "node:fs";

export interface SchemaFlowConfig {
  /** PostgreSQL connection string */
  connectionString: string;
  /** Base directory containing tables/, enums/, functions/, views/, pre/, post/ folders */
  baseDir: string;
  /** Table YAML files directory */
  tablesDir: string;
  /** Enum YAML files directory */
  enumsDir: string;
  /** Function YAML files directory */
  functionsDir: string;
  /** View and materialized view YAML files directory */
  viewsDir: string;
  /** Role YAML files directory */
  rolesDir: string;
  /** Pre-migration scripts directory */
  preDir: string;
  /** Post-migration scripts directory */
  postDir: string;
  /** Mixin definitions directory */
  mixinsDir: string;
  /** Repeatable SQL scripts directory */
  repeatableDir: string;
  /** PostgreSQL schema name (default: public) */
  pgSchema: string;
  /** Migration history table name */
  historyTable: string;
  /** Dry run mode */
  dryRun: boolean;
  /**
   * Allow destructive operations (column drops, table drops, narrowing type changes).
   * By default, schema-flow only performs safe, additive operations.
   * Set to true via --allow-destructive flag or SCHEMA_FLOW_ALLOW_DESTRUCTIVE=true.
   */
  allowDestructive: boolean;
  /** Lock timeout for DDL statements (default: "5s"). Set to "0" to disable. */
  lockTimeout: string;
  /** Statement timeout for DDL statements (default: "30s"). Set to "0" to disable. */
  statementTimeout: string;
  /** Skip pre-migration checks (prechecks) */
  skipChecks: boolean;
  /** Maximum number of retries for transient database errors (default: 3) */
  maxRetries: number;
}

const CONVENTION_DIR = "schema";

const DEFAULTS = {
  tablesDir: "tables",
  enumsDir: "enums",
  functionsDir: "functions",
  viewsDir: "views",
  rolesDir: "roles",
  preDir: "pre",
  postDir: "post",
  pgSchema: "public",
  historyTable: "_schema_flow_history",
};

export function resolveConfig(overrides: Partial<SchemaFlowConfig> = {}): SchemaFlowConfig {
  const connectionString =
    overrides.connectionString || process.env.SCHEMA_FLOW_DATABASE_URL || process.env.DATABASE_URL || "";

  if (!connectionString) {
    throw new Error(
      "No database connection string provided. Set SCHEMA_FLOW_DATABASE_URL or DATABASE_URL, or pass --connection-string.",
    );
  }

  const cwd = overrides.baseDir || process.cwd();
  // Always use the schema/ subdirectory under the project root
  const baseDir = path.join(cwd, CONVENTION_DIR);
  const tablesDir = path.resolve(baseDir, overrides.tablesDir || DEFAULTS.tablesDir);
  const enumsDir = path.resolve(baseDir, overrides.enumsDir || DEFAULTS.enumsDir);
  const functionsDir = path.resolve(baseDir, overrides.functionsDir || DEFAULTS.functionsDir);
  const viewsDir = path.resolve(baseDir, overrides.viewsDir || DEFAULTS.viewsDir);
  const rolesDir = path.resolve(baseDir, overrides.rolesDir || DEFAULTS.rolesDir);
  const preDir = path.resolve(baseDir, overrides.preDir || DEFAULTS.preDir);
  const postDir = path.resolve(baseDir, overrides.postDir || DEFAULTS.postDir);
  const mixinsDir = path.resolve(baseDir, "mixins");
  const repeatableDir = path.resolve(baseDir, "repeatable");

  const allowDestructive =
    overrides.allowDestructive ?? (process.env.SCHEMA_FLOW_ALLOW_DESTRUCTIVE === "true" || false);

  return {
    connectionString,
    baseDir,
    tablesDir,
    enumsDir,
    functionsDir,
    viewsDir,
    rolesDir,
    preDir,
    postDir,
    mixinsDir,
    repeatableDir,
    pgSchema: overrides.pgSchema || DEFAULTS.pgSchema,
    historyTable: overrides.historyTable || DEFAULTS.historyTable,
    dryRun: overrides.dryRun ?? false,
    allowDestructive,
    lockTimeout: overrides.lockTimeout || process.env.SCHEMA_FLOW_LOCK_TIMEOUT || "5s",
    statementTimeout: overrides.statementTimeout || process.env.SCHEMA_FLOW_STATEMENT_TIMEOUT || "30s",
    skipChecks: overrides.skipChecks ?? false,
    maxRetries: overrides.maxRetries ?? parseInt(process.env.SCHEMA_FLOW_MAX_RETRIES || "3", 10),
  };
}

/** Validate that the conventional directories exist (warn if missing) */
export function validateDirectories(config: SchemaFlowConfig): string[] {
  const warnings: string[] = [];
  if (!existsSync(config.tablesDir)) {
    warnings.push(`Tables directory not found: ${config.tablesDir}`);
  }
  if (!existsSync(config.preDir)) {
    warnings.push(`Pre-migration directory not found: ${config.preDir}`);
  }
  if (!existsSync(config.postDir)) {
    warnings.push(`Post-migration directory not found: ${config.postDir}`);
  }
  return warnings;
}
