import { describe, it, expect } from "vitest";
import { useTestClient, createTempProject, writeSchema } from "./helpers.js";
import { parseTableFile } from "../src/schema/parser.js";
import { buildPlan } from "../src/planner/index.js";
import type { TableSchema } from "../src/schema/types.js";

describe("cross-schema foreign keys", () => {
  const ctx = useTestClient();

  describe("parser", () => {
    it("parses schema field on references", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.tablesDir,
          "orders.yaml",
          `
table: orders
columns:
  - name: id
    type: serial
    primary_key: true
  - name: user_id
    type: integer
    references:
      schema: core
      table: users
      column: id
`,
        );
        const schema = parseTableFile(filePath);
        const userId = schema.columns.find((c) => c.name === "user_id");
        expect(userId!.references!.schema).toBe("core");
        expect(userId!.references!.table).toBe("users");
        expect(userId!.references!.column).toBe("id");
      } finally {
        project.cleanup();
      }
    });

    it("leaves schema undefined when not specified", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.tablesDir,
          "posts.yaml",
          `
table: posts
columns:
  - name: id
    type: serial
    primary_key: true
  - name: author_id
    type: integer
    references:
      table: users
      column: id
`,
        );
        const schema = parseTableFile(filePath);
        const authorId = schema.columns.find((c) => c.name === "author_id");
        expect(authorId!.references!.schema).toBeUndefined();
      } finally {
        project.cleanup();
      }
    });
  });

  describe("planner", () => {
    it("uses cross-schema reference in FK SQL when schema is specified", async () => {
      // Create the referenced table in the "core" schema first
      await ctx.client.query(`CREATE SCHEMA IF NOT EXISTS core`);
      await ctx.client.query(`CREATE TABLE core.users (id serial PRIMARY KEY)`);

      const desired: TableSchema[] = [
        {
          table: "orders",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            {
              name: "user_id",
              type: "integer",
              references: {
                schema: "core",
                table: "users",
                column: "id",
              },
            },
          ],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const fkOp = plan.foreignKeyOps.find((o) => o.type === "add_foreign_key_not_valid");
      expect(fkOp).toBeDefined();
      expect(fkOp!.sql).toContain('REFERENCES "core"."users"');
      expect(fkOp!.sql).not.toContain('REFERENCES "public"."users"');
    });

    it("uses current pgSchema when references.schema is not specified", async () => {
      const desired: TableSchema[] = [
        {
          table: "categories",
          columns: [{ name: "id", type: "serial", primary_key: true }],
        },
        {
          table: "products",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            {
              name: "category_id",
              type: "integer",
              references: { table: "categories", column: "id" },
            },
          ],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const fkOp = plan.foreignKeyOps.find((o) => o.type === "add_foreign_key_not_valid");
      expect(fkOp).toBeDefined();
      expect(fkOp!.sql).toContain('REFERENCES "public"."categories"');
    });
  });

  describe("introspection round-trip", () => {
    it("introspects cross-schema FK and populates references.schema", async () => {
      // Set up cross-schema FK in the database
      await ctx.client.query(`CREATE SCHEMA IF NOT EXISTS core`);
      await ctx.client.query(`CREATE TABLE core.users (id serial PRIMARY KEY)`);
      await ctx.client.query(`CREATE TABLE public.orders (
        id serial PRIMARY KEY,
        user_id integer REFERENCES core.users(id)
      )`);

      const { introspectTable } = await import("../src/introspect/index.js");
      const schema = await introspectTable(ctx.client, "orders", "public");
      const userId = schema.columns.find((c) => c.name === "user_id");
      expect(userId!.references).toBeDefined();
      expect(userId!.references!.schema).toBe("core");
      expect(userId!.references!.table).toBe("users");
    });

    it("omits schema on same-schema FK", async () => {
      await ctx.client.query(`CREATE TABLE public.categories (id serial PRIMARY KEY)`);
      await ctx.client.query(`CREATE TABLE public.items (
        id serial PRIMARY KEY,
        category_id integer REFERENCES public.categories(id)
      )`);

      const { introspectTable } = await import("../src/introspect/index.js");
      const schema = await introspectTable(ctx.client, "items", "public");
      const catId = schema.columns.find((c) => c.name === "category_id");
      expect(catId!.references).toBeDefined();
      expect(catId!.references!.schema).toBeUndefined();
      expect(catId!.references!.table).toBe("categories");
    });
  });
});
