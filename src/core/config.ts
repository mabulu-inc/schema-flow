// src/core/config.ts
// Convention-over-configuration: resolve paths, connection string, defaults

import path from "node:path";
import { existsSync } from "node:fs";

export interface SchemaFlowConfig {
  /** PostgreSQL connection string */
  connectionString: string;
  /** Base directory containing schema/, pre/, post/ folders */
  baseDir: string;
  /** Schema files directory */
  schemaDir: string;
  /** Pre-migration scripts directory */
  preDir: string;
  /** Post-migration scripts directory */
  postDir: string;
  /** PostgreSQL schema name (default: public) */
  pgSchema: string;
  /** Migration history table name */
  historyTable: string;
  /** Dry run mode */
  dryRun: boolean;
}

const DEFAULTS = {
  schemaDir: "schema",
  preDir: "pre",
  postDir: "post",
  pgSchema: "public",
  historyTable: "_schema_flow_history",
};

export function resolveConfig(overrides: Partial<SchemaFlowConfig> = {}): SchemaFlowConfig {
  const connectionString =
    overrides.connectionString ||
    process.env.DATABASE_URL ||
    process.env.SCHEMA_FLOW_DATABASE_URL ||
    "";

  if (!connectionString) {
    throw new Error(
      "No database connection string provided. Set DATABASE_URL or SCHEMA_FLOW_DATABASE_URL, or pass --connection-string."
    );
  }

  const baseDir = overrides.baseDir || process.cwd();
  const schemaDir = path.resolve(baseDir, overrides.schemaDir || DEFAULTS.schemaDir);
  const preDir = path.resolve(baseDir, overrides.preDir || DEFAULTS.preDir);
  const postDir = path.resolve(baseDir, overrides.postDir || DEFAULTS.postDir);

  return {
    connectionString,
    baseDir,
    schemaDir,
    preDir,
    postDir,
    pgSchema: overrides.pgSchema || DEFAULTS.pgSchema,
    historyTable: overrides.historyTable || DEFAULTS.historyTable,
    dryRun: overrides.dryRun ?? false,
  };
}

/** Validate that the conventional directories exist (warn if missing) */
export function validateDirectories(config: SchemaFlowConfig): string[] {
  const warnings: string[] = [];
  if (!existsSync(config.schemaDir)) {
    warnings.push(`Schema directory not found: ${config.schemaDir}`);
  }
  if (!existsSync(config.preDir)) {
    warnings.push(`Pre-migration directory not found: ${config.preDir}`);
  }
  if (!existsSync(config.postDir)) {
    warnings.push(`Post-migration directory not found: ${config.postDir}`);
  }
  return warnings;
}
