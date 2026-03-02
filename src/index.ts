// src/index.ts
// Public API — for programmatic usage

export { resolveConfig, type SchemaFlowConfig } from "./core/config.js";
export { logger, LogLevel } from "./core/logger.js";
export { FileTracker } from "./core/tracker.js";
export { testConnection, closePool, withClient, withTransaction } from "./core/db.js";
export { parseTableFile, parseFunctionFile } from "./schema/parser.js";
export type { TableSchema, ColumnDef, IndexDef, CheckDef, FunctionSchema, ForeignKeyAction } from "./schema/types.js";
export { buildPlan, type MigrationPlan, type Operation } from "./planner/index.js";
export { runAll, runPre, runMigrate, runPost, type ExecutionResult, type Phase } from "./executor/index.js";
export { scaffoldPre, scaffoldPost, generateFromDb, scaffoldInit } from "./scaffold/index.js";
export {
  introspectTable,
  getExistingTables,
  getExistingFunctions,
  getTableColumns,
  getTableConstraints,
  getTableIndexes,
} from "./introspect/index.js";
