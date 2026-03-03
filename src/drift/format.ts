// src/drift/format.ts
// Formatters for drift detection reports

import type { DriftReport, DriftItem } from "./index.js";

const DIRECTION_LABELS: Record<DriftItem["direction"], string> = {
  extra_in_db: "EXTRA IN DB",
  missing_from_db: "MISSING FROM DB",
  mismatch: "MISMATCH",
};

const DIRECTION_SYMBOLS: Record<DriftItem["direction"], string> = {
  extra_in_db: "+",
  missing_from_db: "-",
  mismatch: "~",
};

export function formatDriftReport(report: DriftReport): string {
  const lines: string[] = [];

  if (!report.hasDrift) {
    lines.push("No drift detected — database matches YAML schema.");
    lines.push("");
    lines.push(`  Tables checked:    ${report.summary.tablesChecked}`);
    lines.push(`  Functions checked: ${report.summary.functionsChecked}`);
    return lines.join("\n");
  }

  lines.push(`Drift detected: ${report.items.length} difference(s) found`);
  lines.push("");

  // Group by category
  const grouped = new Map<string, DriftItem[]>();
  for (const item of report.items) {
    const key = item.category;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key)!.push(item);
  }

  for (const [category, items] of grouped) {
    lines.push(`  ${category.toUpperCase()} (${items.length}):`);
    for (const item of items) {
      const sym = DIRECTION_SYMBOLS[item.direction];
      const label = DIRECTION_LABELS[item.direction];
      const scope = item.table ? `${item.table}.${item.name}` : item.name;
      lines.push(`    ${sym} [${label}] ${scope}: ${item.description}`);
      if (item.details) {
        for (const d of item.details) {
          lines.push(`        ${d.field}: expected "${d.expected}", got "${d.actual}"`);
        }
      }
    }
    lines.push("");
  }

  lines.push("  Summary:");
  lines.push(`    Extra in DB:      ${report.summary.extraInDb}`);
  lines.push(`    Missing from DB:  ${report.summary.missingFromDb}`);
  lines.push(`    Mismatches:       ${report.summary.mismatches}`);
  lines.push(`    Tables checked:   ${report.summary.tablesChecked}`);
  lines.push(`    Functions checked: ${report.summary.functionsChecked}`);

  return lines.join("\n");
}

export function formatDriftReportJson(report: DriftReport): string {
  return JSON.stringify(report, null, 2);
}
