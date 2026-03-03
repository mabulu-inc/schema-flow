// src/schema/parser.ts
// Parse YAML schema files into typed definitions

import { readFileSync } from "node:fs";
import path from "node:path";
import { parse as parseYaml } from "yaml";
import type { TableSchema, FunctionSchema, ColumnDef, TriggerDef, PolicyDef, MixinSchema, PrecheckDef, ExpandDef } from "./types.js";
import { logger } from "../core/logger.js";

/** Parse a single column definition from raw YAML */
export function parseColumnDef(col: Record<string, unknown>, filePath: string): ColumnDef {
  if (!col.name || !col.type) {
    throw new Error(`Each column in ${filePath} must have "name" and "type"`);
  }
  return {
    name: col.name as string,
    type: col.type as string,
    nullable: col.nullable !== undefined ? Boolean(col.nullable) : undefined,
    default: col.default !== undefined ? String(col.default) : undefined,
    primary_key: col.primary_key ? Boolean(col.primary_key) : undefined,
    unique: col.unique ? Boolean(col.unique) : undefined,
    references: col.references
      ? {
          table: (col.references as Record<string, string>).table,
          column: (col.references as Record<string, string>).column,
          on_delete: (col.references as Record<string, string>).on_delete as ColumnDef["references"] extends {
            on_delete?: infer T;
          }
            ? T
            : never,
          on_update: (col.references as Record<string, string>).on_update as ColumnDef["references"] extends {
            on_update?: infer T;
          }
            ? T
            : never,
        }
      : undefined,
    expand: col.expand
      ? {
          from: (col.expand as Record<string, unknown>).from as string,
          transform: (col.expand as Record<string, unknown>).transform as string,
          reverse: (col.expand as Record<string, unknown>).reverse as string | undefined,
          batch_size: (col.expand as Record<string, unknown>).batch_size as number | undefined,
        }
      : undefined,
  };
}

/** Parse a single trigger definition from raw YAML */
export function parseTriggerDef(trigger: Record<string, unknown>, filePath: string): TriggerDef {
  if (!trigger.name || !trigger.function) {
    throw new Error(`Each trigger in ${filePath} must have "name" and "function"`);
  }
  if (!trigger.timing) {
    throw new Error(`Trigger "${trigger.name}" in ${filePath} must have "timing" (BEFORE, AFTER, or INSTEAD OF)`);
  }
  if (!trigger.events || !Array.isArray(trigger.events) || trigger.events.length === 0) {
    throw new Error(`Trigger "${trigger.name}" in ${filePath} must have "events" as a non-empty array`);
  }

  return {
    name: trigger.name as string,
    timing: trigger.timing as TriggerDef["timing"],
    events: trigger.events as TriggerDef["events"],
    function: trigger.function as string,
    for_each: (trigger.for_each as TriggerDef["for_each"]) || "ROW",
    when: trigger.when !== undefined ? String(trigger.when) : undefined,
  };
}

const VALID_POLICY_COMMANDS = ["SELECT", "INSERT", "UPDATE", "DELETE", "ALL"];

/** Parse a single RLS policy definition from raw YAML */
export function parsePolicyDef(policy: Record<string, unknown>, filePath: string): PolicyDef {
  if (!policy.name) {
    throw new Error(`Each policy in ${filePath} must have "name"`);
  }
  if (!policy.for) {
    throw new Error(`Policy "${policy.name}" in ${filePath} must have "for" (SELECT, INSERT, UPDATE, DELETE, or ALL)`);
  }
  const forValue = String(policy.for).toUpperCase();
  if (!VALID_POLICY_COMMANDS.includes(forValue)) {
    throw new Error(
      `Policy "${policy.name}" in ${filePath} has invalid "for" value "${policy.for}". Must be one of: ${VALID_POLICY_COMMANDS.join(", ")}`,
    );
  }

  // Coerce `to` to string array
  let to: string[] | undefined;
  if (policy.to !== undefined) {
    if (Array.isArray(policy.to)) {
      to = policy.to.map(String);
    } else {
      to = [String(policy.to)];
    }
  }

  return {
    name: String(policy.name),
    for: forValue as PolicyDef["for"],
    to,
    using: policy.using !== undefined ? String(policy.using) : undefined,
    check: policy.check !== undefined ? String(policy.check) : undefined,
    permissive: policy.permissive !== undefined ? Boolean(policy.permissive) : true,
  };
}

export function parseTableFile(filePath: string): TableSchema {
  const content = readFileSync(filePath, "utf-8");
  const raw = parseYaml(content);

  if (!raw || typeof raw !== "object") {
    throw new Error(`Invalid schema file: ${filePath} — expected a YAML object`);
  }

  // Derive table name from filename if not specified
  const tableName = raw.table || path.basename(filePath, path.extname(filePath));

  if (!raw.columns || !Array.isArray(raw.columns)) {
    throw new Error(`Schema file ${filePath} must define "columns" as an array`);
  }

  const columns: ColumnDef[] = raw.columns.map((col: Record<string, unknown>) => parseColumnDef(col, filePath));

  const schema: TableSchema = {
    table: tableName,
    columns,
  };

  if (raw.primary_key && Array.isArray(raw.primary_key)) {
    schema.primary_key = raw.primary_key as string[];
  }

  if (raw.indexes && Array.isArray(raw.indexes)) {
    schema.indexes = raw.indexes;
  }

  if (raw.checks && Array.isArray(raw.checks)) {
    schema.checks = raw.checks;
  }

  if (raw.triggers && Array.isArray(raw.triggers)) {
    schema.triggers = raw.triggers.map((t: Record<string, unknown>) => parseTriggerDef(t, filePath));
  }

  if (raw.use && Array.isArray(raw.use)) {
    schema.use = raw.use as string[];
  }

  if (raw.rls !== undefined) {
    schema.rls = Boolean(raw.rls);
  }

  if (raw.force_rls !== undefined) {
    schema.force_rls = Boolean(raw.force_rls);
  }

  if (raw.policies && Array.isArray(raw.policies)) {
    schema.policies = raw.policies.map((p: Record<string, unknown>) => parsePolicyDef(p, filePath));
  }

  if (raw.prechecks && Array.isArray(raw.prechecks)) {
    schema.prechecks = raw.prechecks.map((pc: Record<string, unknown>) => {
      if (!pc.name || !pc.query) {
        throw new Error(`Each precheck in ${filePath} must have "name" and "query"`);
      }
      return {
        name: String(pc.name),
        query: String(pc.query),
        message: pc.message !== undefined ? String(pc.message) : undefined,
      } as PrecheckDef;
    });
  }

  logger.debug(`Parsed schema for table: ${schema.table}`, { columns: schema.columns.length });
  return schema;
}

export function parseMixinFile(filePath: string): MixinSchema {
  const content = readFileSync(filePath, "utf-8");
  const raw = parseYaml(content);

  if (!raw || typeof raw !== "object") {
    throw new Error(`Invalid mixin file: ${filePath} — expected a YAML object`);
  }

  // Derive mixin name from filename if not specified
  const mixinName = raw.mixin || path.basename(filePath, path.extname(filePath));

  const schema: MixinSchema = {
    mixin: mixinName,
  };

  if (raw.columns && Array.isArray(raw.columns)) {
    schema.columns = raw.columns.map((col: Record<string, unknown>) => parseColumnDef(col, filePath));
  }

  if (raw.indexes && Array.isArray(raw.indexes)) {
    schema.indexes = raw.indexes;
  }

  if (raw.checks && Array.isArray(raw.checks)) {
    schema.checks = raw.checks;
  }

  if (raw.triggers && Array.isArray(raw.triggers)) {
    schema.triggers = raw.triggers.map((t: Record<string, unknown>) => parseTriggerDef(t, filePath));
  }

  if (raw.rls !== undefined) {
    schema.rls = Boolean(raw.rls);
  }

  if (raw.force_rls !== undefined) {
    schema.force_rls = Boolean(raw.force_rls);
  }

  if (raw.policies && Array.isArray(raw.policies)) {
    schema.policies = raw.policies.map((p: Record<string, unknown>) => parsePolicyDef(p, filePath));
  }

  logger.debug(`Parsed mixin: ${schema.mixin}`);
  return schema;
}

export function parseFunctionFile(filePath: string): FunctionSchema {
  const content = readFileSync(filePath, "utf-8");
  const raw = parseYaml(content);

  // Accept `function` as alias for `name`
  const name = raw?.name || raw?.function;

  if (!name || !raw?.body) {
    throw new Error(`Function schema file ${filePath} must define "name" (or "function") and "body"`);
  }

  const fn: FunctionSchema = {
    name,
    language: raw.language || "plpgsql",
    returns: raw.returns || "void",
    args: raw.args || "",
    body: raw.body,
    replace: raw.replace !== false,
  };

  if (raw.security !== undefined) {
    const sec = String(raw.security).toLowerCase();
    if (sec === "definer" || sec === "invoker") {
      fn.security = sec;
    }
  }

  return fn;
}
