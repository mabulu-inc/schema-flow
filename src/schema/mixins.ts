// src/schema/mixins.ts
// Mixin loading, expansion, and interpolation engine

import { existsSync } from "node:fs";
import { glob } from "glob";
import path from "node:path";
import type { TableSchema, MixinSchema, ColumnDef, IndexDef, CheckDef, TriggerDef, PolicyDef } from "./types.js";
import { parseMixinFile } from "./parser.js";
import { logger } from "../core/logger.js";

/**
 * Load all mixin definitions from a directory.
 * Returns an empty map if the directory doesn't exist.
 */
export async function loadMixins(mixinsDir: string): Promise<Map<string, MixinSchema>> {
  const map = new Map<string, MixinSchema>();

  if (!existsSync(mixinsDir)) {
    return map;
  }

  const patterns = ["*.yaml", "*.yml"];
  const files = await glob(patterns.map((p) => path.join(mixinsDir, p)));

  for (const filePath of files.sort()) {
    const mixin = parseMixinFile(filePath);
    if (map.has(mixin.mixin)) {
      throw new Error(`Duplicate mixin name "${mixin.mixin}" — found in multiple files`);
    }
    map.set(mixin.mixin, mixin);
    logger.debug(`Loaded mixin: ${mixin.mixin}`);
  }

  return map;
}

/**
 * Replace `{table}` placeholder with the consuming table's name.
 */
function interpolateNames(str: string, tableName: string): string {
  return str.replace(/\{table\}/g, tableName);
}

/**
 * Expand mixin references in table schemas.
 * For each table with `use`, resolves mixin references and merges:
 * - Columns: mixin columns first (in use order), then table columns. Table overrides mixin on name clash.
 * - Indexes, checks, triggers: mixin entries appended before table entries.
 * - `{table}` interpolation in name and expression fields of triggers, indexes, checks, and policies.
 * Strips the `use` property from output.
 */
export function expandMixins(schemas: TableSchema[], mixinMap: Map<string, MixinSchema>): TableSchema[] {
  return schemas.map((schema) => {
    if (!schema.use || schema.use.length === 0) {
      // Strip use even if empty
      const { use: _, ...rest } = schema;
      return rest;
    }

    // Collect mixin contributions
    const mixinColumns: ColumnDef[] = [];
    const mixinIndexes: IndexDef[] = [];
    const mixinChecks: CheckDef[] = [];
    const mixinTriggers: TriggerDef[] = [];
    const mixinPolicies: PolicyDef[] = [];
    let mixinRls = false;
    let mixinForceRls = false;

    for (const mixinName of schema.use) {
      const mixin = mixinMap.get(mixinName);
      if (!mixin) {
        throw new Error(`Table "${schema.table}" references unknown mixin "${mixinName}"`);
      }

      if (mixin.columns) {
        mixinColumns.push(...mixin.columns);
      }
      if (mixin.indexes) {
        for (const idx of mixin.indexes) {
          mixinIndexes.push({
            ...idx,
            name: idx.name ? interpolateNames(idx.name, schema.table) : idx.name,
            where: idx.where ? interpolateNames(idx.where, schema.table) : idx.where,
          });
        }
      }
      if (mixin.checks) {
        for (const chk of mixin.checks) {
          mixinChecks.push({
            ...chk,
            name: chk.name ? interpolateNames(chk.name, schema.table) : chk.name,
            expression: interpolateNames(chk.expression, schema.table),
          });
        }
      }
      if (mixin.triggers) {
        for (const trg of mixin.triggers) {
          mixinTriggers.push({
            ...trg,
            name: interpolateNames(trg.name, schema.table),
          });
        }
      }
      if (mixin.policies) {
        for (const pol of mixin.policies) {
          mixinPolicies.push({
            ...pol,
            name: interpolateNames(pol.name, schema.table),
            using: pol.using ? interpolateNames(pol.using, schema.table) : pol.using,
            check: pol.check ? interpolateNames(pol.check, schema.table) : pol.check,
          });
        }
      }
      if (mixin.rls) {
        mixinRls = true;
      }
      if (mixin.force_rls) {
        mixinForceRls = true;
      }
    }

    // Merge columns: mixin first, table overrides on name clash
    const tableColNames = new Set(schema.columns.map((c) => c.name));
    const mergedColumns: ColumnDef[] = [...mixinColumns.filter((c) => !tableColNames.has(c.name)), ...schema.columns];

    // Merge indexes, checks, triggers, policies: mixin entries before table entries
    const mergedIndexes =
      mixinIndexes.length > 0 || schema.indexes ? [...mixinIndexes, ...(schema.indexes || [])] : undefined;

    const mergedChecks =
      mixinChecks.length > 0 || schema.checks ? [...mixinChecks, ...(schema.checks || [])] : undefined;

    const mergedTriggers =
      mixinTriggers.length > 0 || schema.triggers ? [...mixinTriggers, ...(schema.triggers || [])] : undefined;

    const mergedPolicies =
      mixinPolicies.length > 0 || schema.policies ? [...mixinPolicies, ...(schema.policies || [])] : undefined;

    const { use: _, ...rest } = schema;

    // RLS flags: table can override mixin, but mixin sets the default
    const mergedRls = schema.rls !== undefined ? schema.rls : mixinRls || undefined;
    const mergedForceRls = schema.force_rls !== undefined ? schema.force_rls : mixinForceRls || undefined;

    return {
      ...rest,
      columns: mergedColumns,
      indexes: mergedIndexes,
      checks: mergedChecks,
      triggers: mergedTriggers,
      ...(mergedRls !== undefined ? { rls: mergedRls } : {}),
      ...(mergedForceRls !== undefined ? { force_rls: mergedForceRls } : {}),
      policies: mergedPolicies,
    };
  });
}
