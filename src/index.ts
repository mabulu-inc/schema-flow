// src/index.ts
// Public API — for programmatic usage

export { resolveConfig, type SchemaFlowConfig } from "./core/config.js";
export {
  loadConfigFile,
  resolveEnvironmentConfig,
  type ConfigFile,
  type EnvironmentConfig,
} from "./core/config-file.js";
export { logger, LogLevel } from "./core/logger.js";
export { FileTracker } from "./core/tracker.js";
export { testConnection, closePool, withClient, withTransaction } from "./core/db.js";
export { discoverSchemaFiles, discoverScripts, utcTimestamp } from "./core/files.js";
export {
  parseTableFile,
  parseFunctionFile,
  parseMixinFile,
  parseEnumFile,
  parseExtensionsFile,
  parseViewFile,
  parseMaterializedViewFile,
} from "./schema/parser.js";
export type {
  TableSchema,
  ColumnDef,
  IndexDef,
  CheckDef,
  TriggerDef,
  MixinSchema,
  FunctionSchema,
  ForeignKeyAction,
  PrecheckDef,
  ExpandDef,
  EnumSchema,
  ExtensionsSchema,
  ViewSchema,
  MaterializedViewSchema,
} from "./schema/types.js";
export { loadMixins, expandMixins } from "./schema/mixins.js";
export { buildPlan, normalizeType, type MigrationPlan, type Operation, type PlanOptions } from "./planner/index.js";
export {
  runAll,
  runPre,
  runMigrate,
  runPost,
  runValidate,
  runBaseline,
  runRepeatables,
  type ExecutionResult,
  type Phase,
  type BaselineResult,
  type RepeatableResult,
} from "./executor/index.js";
export { scaffoldPre, scaffoldPost, generateFromDb, scaffoldInit, scaffoldMixin } from "./scaffold/index.js";
export {
  introspectTable,
  getExistingTables,
  getExistingFunctions,
  getExistingEnums,
  getExistingExtensions,
  getExistingViews,
  getExistingMaterializedViews,
  getTableColumns,
  getTableConstraints,
  getTableIndexes,
  getTableTriggers,
  getTableComment,
  getColumnComments,
  getGeneratedColumns,
  parseIndexDef,
  parseIndexDefFull,
  type ParsedIndex,
} from "./introspect/index.js";

// Drift detection
export { detectDrift, type DriftReport, type DriftItem } from "./drift/index.js";
export { formatDriftReport, formatDriftReportJson } from "./drift/format.js";

// Migration linting
export {
  lintPlan,
  formatLintFindings,
  formatLintFindingsJson,
  type LintFinding,
  type LintContext,
  type LintRule,
  type LintSeverity,
} from "./lint/index.js";
export { builtinRules } from "./lint/rules.js";

// Rollback / down migrations
export { computeRollback, type ReverseOperation, type RollbackResult } from "./rollback/index.js";
export { captureSnapshot, type MigrationSnapshot } from "./rollback/snapshot.js";
export { runDown, type DownResult } from "./rollback/executor.js";

// SQL file generation
export { formatMigrationSql, generateSqlFile, type SqlGenerateResult } from "./sql/index.js";

// ERD / Mermaid output
export { generateMermaidErd, generateErdFromFiles } from "./erd/index.js";

// Expand/contract
export { ExpandTracker, type ExpandRecord } from "./expand/tracker.js";
export { planExpandColumn, planContractColumn } from "./expand/planner.js";
export { runBackfill, type BackfillOptions, type BackfillResult } from "./expand/backfill.js";
export { runContract, showExpandStatus, type ContractResult } from "./expand/executor.js";
