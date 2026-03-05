// src/rollback/index.ts
// Compute reverse SQL operations from forward operations

import type { Operation } from "../planner/index.js";
import type { MigrationSnapshot } from "./snapshot.js";

export interface ReverseOperation {
  sql: string;
  description: string;
  destructive: boolean;
  /** Whether the reverse is safe to execute */
  safe: boolean;
  /** Whether data loss is irreversible */
  irreversible: boolean;
}

export interface RollbackResult {
  operations: ReverseOperation[];
  hasDestructive: boolean;
  hasIrreversible: boolean;
}

/** Map forward operations to reverse operations */
export function computeRollback(
  forwardOps: Operation[],
  snapshot: MigrationSnapshot,
  pgSchema: string,
): RollbackResult {
  const reverseOps: ReverseOperation[] = [];

  // Reverse in reverse order
  const ops = [...forwardOps].reverse();

  for (const op of ops) {
    const reversed = reverseOperation(op, snapshot, pgSchema);
    if (reversed) {
      reverseOps.push(reversed);
    }
  }

  return {
    operations: reverseOps,
    hasDestructive: reverseOps.some((o) => o.destructive),
    hasIrreversible: reverseOps.some((o) => o.irreversible),
  };
}

function reverseOperation(op: Operation, snapshot: MigrationSnapshot, pgSchema: string): ReverseOperation | null {
  const qualifiedTable = op.table ? `"${pgSchema}"."${op.table}"` : "";

  switch (op.type) {
    case "create_table":
      return {
        sql: `DROP TABLE IF EXISTS ${qualifiedTable} CASCADE;`,
        description: `Drop table ${op.table} (reverse of create)`,
        destructive: true,
        safe: false,
        irreversible: false,
      };

    case "add_column": {
      const colMatch = op.sql.match(/ADD COLUMN "([^"]+)"/);
      const colName = colMatch?.[1] || "unknown";
      return {
        sql: `ALTER TABLE ${qualifiedTable} DROP COLUMN IF EXISTS "${colName}";`,
        description: `Drop column ${op.table}.${colName} (reverse of add)`,
        destructive: true,
        safe: false,
        irreversible: false,
      };
    }

    case "alter_column": {
      if (op.sql.includes("SET NOT NULL")) {
        const colMatch = op.sql.match(/ALTER COLUMN "([^"]+)" SET NOT NULL/);
        const colName = colMatch?.[1] || "unknown";
        return {
          sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${colName}" DROP NOT NULL;`,
          description: `Make ${op.table}.${colName} nullable (reverse of SET NOT NULL)`,
          destructive: false,
          safe: true,
          irreversible: false,
        };
      }

      if (op.sql.includes("DROP NOT NULL")) {
        const colMatch = op.sql.match(/ALTER COLUMN "([^"]+)" DROP NOT NULL/);
        const colName = colMatch?.[1] || "unknown";
        return {
          sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${colName}" SET NOT NULL;`,
          description: `Make ${op.table}.${colName} NOT NULL (reverse of DROP NOT NULL) — may fail if NULLs exist`,
          destructive: false,
          safe: false,
          irreversible: false,
        };
      }

      if (op.sql.includes("TYPE")) {
        const colMatch = op.sql.match(/ALTER COLUMN "([^"]+)" TYPE/);
        const colName = colMatch?.[1] || "unknown";
        const tableSnap = snapshot.tables[op.table || ""];
        const colSnap = tableSnap?.columns[colName];
        if (colSnap) {
          return {
            sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${colName}" TYPE ${colSnap.type} USING "${colName}"::${colSnap.type};`,
            description: `Revert type of ${op.table}.${colName} to ${colSnap.type}`,
            destructive: true,
            safe: false,
            irreversible: false,
          };
        }
        return null;
      }

      if (op.sql.includes("SET DEFAULT")) {
        const colMatch = op.sql.match(/ALTER COLUMN "([^"]+)" SET DEFAULT/);
        const colName = colMatch?.[1] || "unknown";
        const tableSnap = snapshot.tables[op.table || ""];
        const colSnap = tableSnap?.columns[colName];
        if (colSnap?.default) {
          return {
            sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${colName}" SET DEFAULT ${colSnap.default};`,
            description: `Revert default of ${op.table}.${colName}`,
            destructive: false,
            safe: true,
            irreversible: false,
          };
        }
        return {
          sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${colName}" DROP DEFAULT;`,
          description: `Drop default of ${op.table}.${colName} (reverse of set)`,
          destructive: false,
          safe: true,
          irreversible: false,
        };
      }

      if (op.sql.includes("DROP DEFAULT")) {
        const colMatch = op.sql.match(/ALTER COLUMN "([^"]+)" DROP DEFAULT/);
        const colName = colMatch?.[1] || "unknown";
        const tableSnap = snapshot.tables[op.table || ""];
        const colSnap = tableSnap?.columns[colName];
        if (colSnap?.default) {
          return {
            sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${colName}" SET DEFAULT ${colSnap.default};`,
            description: `Restore default of ${op.table}.${colName}`,
            destructive: false,
            safe: true,
            irreversible: false,
          };
        }
        return null;
      }

      // Other alter_column ops (e.g., UNIQUE USING INDEX, DROP CONSTRAINT helper)
      return null;
    }

    case "add_index":
    case "add_unique_index": {
      const idxMatch = op.sql.match(/INDEX(?:\s+CONCURRENTLY)?\s+(?:IF NOT EXISTS\s+)?"([^"]+)"/);
      const idxName = idxMatch?.[1] || "unknown";
      return {
        sql: `DROP INDEX CONCURRENTLY IF EXISTS "${pgSchema}"."${idxName}";`,
        description: `Drop index ${idxName} (reverse of create)`,
        destructive: false,
        safe: true,
        irreversible: false,
      };
    }

    case "add_check":
    case "add_check_not_valid": {
      const nameMatch = op.sql.match(/CONSTRAINT "([^"]+)"/);
      const constraintName = nameMatch?.[1] || "unknown";
      return {
        sql: `ALTER TABLE ${qualifiedTable} DROP CONSTRAINT IF EXISTS "${constraintName}";`,
        description: `Drop check constraint ${constraintName} (reverse of add)`,
        destructive: false,
        safe: true,
        irreversible: false,
      };
    }

    case "add_foreign_key":
    case "add_foreign_key_not_valid": {
      const nameMatch = op.sql.match(/CONSTRAINT "([^"]+)"/);
      const constraintName = nameMatch?.[1] || "unknown";
      return {
        sql: `ALTER TABLE ${qualifiedTable} DROP CONSTRAINT IF EXISTS "${constraintName}";`,
        description: `Drop FK ${constraintName} (reverse of add)`,
        destructive: false,
        safe: true,
        irreversible: false,
      };
    }

    case "validate_constraint":
      // Can't un-validate
      return null;

    case "create_trigger": {
      const trigMatch = op.sql.match(/CREATE TRIGGER "([^"]+)"/);
      const trigName = trigMatch?.[1] || "unknown";
      return {
        sql: `DROP TRIGGER IF EXISTS "${trigName}" ON ${qualifiedTable};`,
        description: `Drop trigger ${trigName} (reverse of create)`,
        destructive: false,
        safe: true,
        irreversible: false,
      };
    }

    case "enable_rls":
      if (op.sql.includes("FORCE")) {
        return {
          sql: `ALTER TABLE ${qualifiedTable} NO FORCE ROW LEVEL SECURITY;`,
          description: `Remove force RLS on ${op.table} (reverse of enable)`,
          destructive: false,
          safe: true,
          irreversible: false,
        };
      }
      return {
        sql: `ALTER TABLE ${qualifiedTable} DISABLE ROW LEVEL SECURITY;`,
        description: `Disable RLS on ${op.table} (reverse of enable)`,
        destructive: false,
        safe: true,
        irreversible: false,
      };

    case "create_policy": {
      const polMatch = op.sql.match(/CREATE POLICY "([^"]+)"/);
      const polName = polMatch?.[1] || "unknown";
      return {
        sql: `DROP POLICY IF EXISTS "${polName}" ON ${qualifiedTable};`,
        description: `Drop policy ${polName} (reverse of create)`,
        destructive: false,
        safe: true,
        irreversible: false,
      };
    }

    case "create_role": {
      const roleMatch = op.sql.match(/CREATE ROLE "([^"]+)"/);
      const roleName = roleMatch?.[1] || "unknown";
      return {
        sql: `DROP ROLE IF EXISTS "${roleName}";`,
        description: `Drop role ${roleName} (reverse of create)`,
        destructive: true,
        safe: false,
        irreversible: false,
      };
    }

    case "alter_role":
      return null; // Cannot generically reverse without snapshot

    case "grant_membership": {
      const grantMatch = op.sql.match(/GRANT "([^"]+)" TO "([^"]+)"/);
      if (grantMatch) {
        return {
          sql: `REVOKE "${grantMatch[1]}" FROM "${grantMatch[2]}";`,
          description: `Revoke role ${grantMatch[1]} from ${grantMatch[2]} (reverse of grant)`,
          destructive: false,
          safe: true,
          irreversible: false,
        };
      }
      return null;
    }

    case "grant_table":
    case "grant_column": {
      const grantSql = op.sql.replace(/^GRANT\b/, "REVOKE").replace(/\bTO\b/, "FROM");
      return {
        sql: grantSql,
        description: `Revoke (reverse of ${op.description})`,
        destructive: false,
        safe: true,
        irreversible: false,
      };
    }

    case "revoke_table":
    case "revoke_column":
      return {
        sql: `-- IRREVERSIBLE: ${op.description} — privilege was revoked`,
        description: `${op.description} — IRREVERSIBLE`,
        destructive: false,
        safe: false,
        irreversible: true,
      };

    // Destructive ops are irreversible (data gone)
    case "drop_column":
    case "drop_table":
    case "drop_trigger":
    case "drop_policy":
    case "drop_foreign_key":
    case "disable_rls":
    case "drop_index":
      return {
        sql: `-- IRREVERSIBLE: ${op.description} — data/object was destroyed`,
        description: `${op.description} — IRREVERSIBLE (original data not recoverable)`,
        destructive: false,
        safe: false,
        irreversible: true,
      };

    default:
      return null;
  }
}
