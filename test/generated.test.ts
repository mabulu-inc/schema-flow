import { describe, it, expect } from "vitest";
import { useTestClient, createTempProject, writeSchema } from "./helpers.js";
import { parseTableFile } from "../src/schema/parser.js";
import { buildPlan } from "../src/planner/index.js";
import type { TableSchema } from "../src/schema/types.js";

describe("generated columns", () => {
  const ctx = useTestClient();

  describe("parser", () => {
    it("parses a column with generated expression", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(project.schemaDir, "users.yaml", `
table: users
columns:
  - name: first_name
    type: text
  - name: last_name
    type: text
  - name: full_name
    type: text
    generated: "first_name || ' ' || last_name"
`);
        const schema = parseTableFile(filePath);
        const fullName = schema.columns.find((c) => c.name === "full_name");
        expect(fullName).toBeDefined();
        expect(fullName!.generated).toBe("first_name || ' ' || last_name");
      } finally {
        project.cleanup();
      }
    });
  });

  describe("planner", () => {
    it("plans CREATE TABLE with GENERATED ALWAYS AS for generated column", async () => {
      const desired: TableSchema[] = [{
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "first_name", type: "text" },
          { name: "last_name", type: "text" },
          { name: "full_name", type: "text", generated: "first_name || ' ' || last_name" },
        ],
      }];

      const plan = await buildPlan(ctx.client, desired, "public");
      const createOp = plan.structureOps.find((o) => o.type === "create_table");
      expect(createOp).toBeDefined();
      expect(createOp!.sql).toContain("GENERATED ALWAYS AS");
      expect(createOp!.sql).toContain("STORED");
    });

    it("plans ADD COLUMN with GENERATED for new generated column", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, first_name text NOT NULL, last_name text NOT NULL)`);

      const desired: TableSchema[] = [{
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "first_name", type: "text" },
          { name: "last_name", type: "text" },
          { name: "full_name", type: "text", generated: "first_name || ' ' || last_name" },
        ],
      }];

      const plan = await buildPlan(ctx.client, desired, "public");
      const addOp = plan.structureOps.find((o) => o.type === "add_column" && o.sql.includes("full_name"));
      expect(addOp).toBeDefined();
      expect(addOp!.sql).toContain("GENERATED ALWAYS AS");
      expect(addOp!.sql).toContain("STORED");
    });

    it("generated column does not get NOT NULL or DEFAULT", async () => {
      const desired: TableSchema[] = [{
        table: "items",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "price", type: "integer" },
          { name: "qty", type: "integer" },
          { name: "total", type: "integer", generated: "price * qty" },
        ],
      }];

      const plan = await buildPlan(ctx.client, desired, "public");
      const createOp = plan.structureOps.find((o) => o.type === "create_table");
      expect(createOp).toBeDefined();
      // The generated column should not have NOT NULL
      const totalLine = createOp!.sql.split("\n").find((l) => l.includes("total"));
      expect(totalLine).toContain("GENERATED ALWAYS AS");
      expect(totalLine).not.toContain("NOT NULL");
    });
  });
});
