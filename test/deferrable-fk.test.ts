import { describe, it, expect } from "vitest";
import { useTestClient, createTempProject, writeSchema } from "./helpers.js";
import { parseTableFile } from "../src/schema/parser.js";
import { buildPlan } from "../src/planner/index.js";
import type { TableSchema } from "../src/schema/types.js";

describe("deferrable foreign keys", () => {
  const ctx = useTestClient();

  describe("parser", () => {
    it("parses deferrable and initially_deferred on references", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.tablesDir,
          "nodes.yaml",
          `
table: nodes
columns:
  - name: id
    type: serial
    primary_key: true
  - name: parent_id
    type: integer
    nullable: true
    references:
      table: nodes
      column: id
      deferrable: true
      initially_deferred: true
`,
        );
        const schema = parseTableFile(filePath);
        const parentId = schema.columns.find((c) => c.name === "parent_id");
        expect(parentId!.references!.deferrable).toBe(true);
        expect(parentId!.references!.initially_deferred).toBe(true);
      } finally {
        project.cleanup();
      }
    });
  });

  describe("planner", () => {
    it("adds DEFERRABLE INITIALLY DEFERRED to FK SQL", async () => {
      const desired: TableSchema[] = [
        {
          table: "nodes",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            {
              name: "parent_id",
              type: "integer",
              nullable: true,
              references: {
                table: "nodes",
                column: "id",
                deferrable: true,
                initially_deferred: true,
              },
            },
          ],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const fkOp = plan.foreignKeyOps.find((o) => o.type === "add_foreign_key_not_valid");
      expect(fkOp).toBeDefined();
      expect(fkOp!.sql).toContain("DEFERRABLE");
      expect(fkOp!.sql).toContain("INITIALLY DEFERRED");
    });

    it("adds DEFERRABLE only when initially_deferred is false", async () => {
      const desired: TableSchema[] = [
        {
          table: "nodes",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            {
              name: "parent_id",
              type: "integer",
              nullable: true,
              references: {
                table: "nodes",
                column: "id",
                deferrable: true,
              },
            },
          ],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const fkOp = plan.foreignKeyOps.find((o) => o.type === "add_foreign_key_not_valid");
      expect(fkOp).toBeDefined();
      expect(fkOp!.sql).toContain("DEFERRABLE");
      expect(fkOp!.sql).not.toContain("INITIALLY DEFERRED");
    });

    it("omits deferrable clauses when not specified", async () => {
      const desired: TableSchema[] = [
        {
          table: "parents",
          columns: [{ name: "id", type: "serial", primary_key: true }],
        },
        {
          table: "children",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            {
              name: "parent_id",
              type: "integer",
              references: { table: "parents", column: "id" },
            },
          ],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const fkOp = plan.foreignKeyOps.find((o) => o.type === "add_foreign_key_not_valid");
      expect(fkOp).toBeDefined();
      expect(fkOp!.sql).not.toContain("DEFERRABLE");
    });
  });
});
