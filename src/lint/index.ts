// src/lint/index.ts
// Migration linting: static analysis of a plan for dangerous patterns

import type { MigrationPlan } from "../planner/index.js";
import type { TableSchema } from "../schema/types.js";
import { builtinRules } from "./rules.js";

export type LintSeverity = "error" | "warning" | "info";

export interface LintFinding {
  severity: LintSeverity;
  rule: string;
  table?: string;
  column?: string;
  message: string;
}

export interface LintContext {
  plan: MigrationPlan;
  schemas: TableSchema[];
  pgSchema: string;
}

export interface LintRule {
  id: string;
  name: string;
  severity: LintSeverity;
  check(ctx: LintContext): LintFinding[];
}

/** Run all lint rules against a plan and return findings */
export function lintPlan(ctx: LintContext, rules?: LintRule[]): LintFinding[] {
  const activeRules = rules || builtinRules;
  const findings: LintFinding[] = [];

  for (const rule of activeRules) {
    findings.push(...rule.check(ctx));
  }

  // Sort by severity: error first, then warning, then info
  const severityOrder: Record<LintSeverity, number> = { error: 0, warning: 1, info: 2 };
  findings.sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity]);

  return findings;
}

/** Format lint findings as text */
export function formatLintFindings(findings: LintFinding[]): string {
  if (findings.length === 0) {
    return "No lint issues found.";
  }

  const lines: string[] = [`${findings.length} lint issue(s) found:`, ""];

  const symbolMap: Record<LintSeverity, string> = {
    error: "E",
    warning: "W",
    info: "I",
  };

  for (const f of findings) {
    const sym = symbolMap[f.severity];
    const scope = f.table ? (f.column ? `${f.table}.${f.column}` : f.table) : "";
    const scopeStr = scope ? ` [${scope}]` : "";
    lines.push(`  ${sym} ${f.rule}${scopeStr}: ${f.message}`);
  }

  const errors = findings.filter((f) => f.severity === "error").length;
  const warnings = findings.filter((f) => f.severity === "warning").length;
  const infos = findings.filter((f) => f.severity === "info").length;
  lines.push("");
  lines.push(`  ${errors} error(s), ${warnings} warning(s), ${infos} info(s)`);

  return lines.join("\n");
}

/** Format lint findings as JSON */
export function formatLintFindingsJson(findings: LintFinding[]): string {
  return JSON.stringify({ findings, count: findings.length }, null, 2);
}
