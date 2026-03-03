// test/expand.test.ts
// Tests for expand/contract pattern: planner, tracker, backfill, contract

import { describe, it, expect } from "vitest";
import { planExpandColumn, planContractColumn } from "../src/expand/planner.js";
import type { ExpandDef } from "../src/schema/types.js";

describe("Expand/Contract", () => {
  describe("planExpandColumn", () => {
    it("produces 4 operations: add column, create function, create trigger, backfill", () => {
      const expand: ExpandDef = {
        from: "name",
        transform: "name",
      };
      const ops = planExpandColumn("users", "full_name", "text", expand, "public");
      expect(ops).toHaveLength(4);
      expect(ops[0].type).toBe("expand_column");
      expect(ops[1].type).toBe("create_dual_write_trigger");
      expect(ops[2].type).toBe("create_dual_write_trigger");
      expect(ops[3].type).toBe("backfill_column");
    });

    it("generates correct ADD COLUMN SQL", () => {
      const expand: ExpandDef = { from: "name", transform: "name" };
      const ops = planExpandColumn("users", "full_name", "text", expand, "public");
      expect(ops[0].sql).toContain('ADD COLUMN IF NOT EXISTS "full_name" text');
      expect(ops[0].sql).toContain('"public"."users"');
      expect(ops[0].destructive).toBe(false);
    });

    it("generates dual-write function with transform expression", () => {
      const expand: ExpandDef = {
        from: "first_name",
        transform: "first_name || ' ' || last_name",
      };
      const ops = planExpandColumn("users", "full_name", "text", expand, "public");
      const fnSql = ops[1].sql;
      expect(fnSql).toContain("CREATE OR REPLACE FUNCTION");
      expect(fnSql).toContain("RETURNS trigger");
      expect(fnSql).toContain("first_name || ' ' || last_name");
      expect(fnSql).toContain('NEW."full_name"');
    });

    it("includes reverse expression when provided", () => {
      const expand: ExpandDef = {
        from: "name",
        transform: "name",
        reverse: "full_name",
      };
      const ops = planExpandColumn("users", "full_name", "text", expand, "public");
      const fnSql = ops[1].sql;
      expect(fnSql).toContain('NEW."name" = full_name');
    });

    it("omits reverse expression when not provided", () => {
      const expand: ExpandDef = { from: "name", transform: "name" };
      const ops = planExpandColumn("users", "full_name", "text", expand, "public");
      const fnSql = ops[1].sql;
      expect(fnSql).not.toContain('NEW."name"');
    });

    it("generates trigger on correct table", () => {
      const expand: ExpandDef = { from: "name", transform: "name" };
      const ops = planExpandColumn("users", "full_name", "text", expand, "public");
      const trigSql = ops[2].sql;
      expect(trigSql).toContain("CREATE TRIGGER");
      expect(trigSql).toContain("BEFORE INSERT OR UPDATE");
      expect(trigSql).toContain('"public"."users"');
      expect(trigSql).toContain("FOR EACH ROW");
    });

    it("backfill op carries meta with batch_size and transform", () => {
      const expand: ExpandDef = {
        from: "name",
        transform: "name",
        batch_size: 5000,
      };
      const ops = planExpandColumn("users", "full_name", "text", expand, "public");
      const backfillOp = ops[3];
      expect(backfillOp.type).toBe("backfill_column");
      expect(backfillOp.meta?.backfill).toBe(true);
      expect(backfillOp.meta?.batchSize).toBe(5000);
      expect(backfillOp.meta?.transform).toBe("name");
      expect(backfillOp.meta?.from).toBe("name");
      expect(backfillOp.meta?.to).toBe("full_name");
    });

    it("uses default batch_size of 1000", () => {
      const expand: ExpandDef = { from: "name", transform: "name" };
      const ops = planExpandColumn("users", "full_name", "text", expand, "public");
      expect(ops[3].meta?.batchSize).toBe(1000);
    });

    it("uses correct naming convention for triggers and functions", () => {
      const expand: ExpandDef = { from: "name", transform: "name" };
      const ops = planExpandColumn("users", "full_name", "text", expand, "public");
      expect(ops[1].sql).toContain("_sf_dw_users_name_full_name");
      expect(ops[2].sql).toContain("_sf_dw_users_name_full_name");
      expect(ops[3].meta?.triggerName).toBe("_sf_dw_users_name_full_name");
      expect(ops[3].meta?.functionName).toBe("_sf_dw_users_name_full_name");
    });

    it("respects custom pgSchema", () => {
      const expand: ExpandDef = { from: "name", transform: "name" };
      const ops = planExpandColumn("users", "full_name", "text", expand, "myschema");
      expect(ops[0].sql).toContain('"myschema"."users"');
      expect(ops[2].sql).toContain('"myschema"."users"');
    });
  });

  describe("planContractColumn", () => {
    it("produces 3 operations: drop trigger, drop function, drop column", () => {
      const ops = planContractColumn(
        "users",
        "name",
        "full_name",
        "_sf_dw_users_name_full_name",
        "_sf_dw_users_name_full_name",
        "public",
      );
      expect(ops).toHaveLength(3);
      expect(ops[0].type).toBe("drop_dual_write_trigger");
      expect(ops[1].type).toBe("drop_dual_write_trigger");
      expect(ops[2].type).toBe("contract_column");
    });

    it("drops trigger on correct table", () => {
      const ops = planContractColumn(
        "users",
        "name",
        "full_name",
        "_sf_dw_users_name_full_name",
        "_sf_dw_users_name_full_name",
        "public",
      );
      expect(ops[0].sql).toContain('DROP TRIGGER IF EXISTS "_sf_dw_users_name_full_name"');
      expect(ops[0].sql).toContain('"public"."users"');
    });

    it("drops function", () => {
      const ops = planContractColumn(
        "users",
        "name",
        "full_name",
        "_sf_dw_users_name_full_name",
        "_sf_dw_users_name_full_name",
        "public",
      );
      expect(ops[1].sql).toContain('DROP FUNCTION IF EXISTS "_sf_dw_users_name_full_name"()');
    });

    it("drops old column and marks as destructive", () => {
      const ops = planContractColumn(
        "users",
        "name",
        "full_name",
        "_sf_dw_users_name_full_name",
        "_sf_dw_users_name_full_name",
        "public",
      );
      expect(ops[2].sql).toContain('DROP COLUMN IF EXISTS "name"');
      expect(ops[2].destructive).toBe(true);
    });

    it("carries contract meta", () => {
      const ops = planContractColumn(
        "users",
        "name",
        "full_name",
        "_sf_dw_users_name_full_name",
        "_sf_dw_users_name_full_name",
        "public",
      );
      expect(ops[0].meta?.contract).toBe(true);
      expect(ops[2].meta?.oldColumn).toBe("name");
      expect(ops[2].meta?.newColumn).toBe("full_name");
    });
  });
});
