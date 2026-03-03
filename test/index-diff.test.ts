import { describe, it, expect } from "vitest";
import { useTestClient } from "./helpers.js";
import { buildPlan } from "../src/planner/index.js";
import { parseIndexDefFull } from "../src/introspect/index.js";
import type { TableSchema } from "../src/schema/types.js";

describe("index diffing", () => {
  const ctx = useTestClient();

  describe("parseIndexDefFull", () => {
    it("parses a basic btree index", () => {
      const result = parseIndexDefFull(`CREATE INDEX idx_users_email ON public.users USING btree (email)`);
      expect(result.name).toBe("idx_users_email");
      expect(result.method).toBe("btree");
      expect(result.unique).toBe(false);
    });

    it("parses a unique index", () => {
      const result = parseIndexDefFull(`CREATE UNIQUE INDEX idx_users_email ON public.users USING btree (email)`);
      expect(result.unique).toBe(true);
    });

    it("parses a GIN index", () => {
      const result = parseIndexDefFull(`CREATE INDEX idx_data ON public.items USING gin (data)`);
      expect(result.method).toBe("gin");
    });

    it("parses a WHERE clause", () => {
      const result = parseIndexDefFull(
        `CREATE INDEX idx_active ON public.users USING btree (email) WHERE (active = true)`,
      );
      expect(result.where).toContain("active");
    });
  });

  describe("enhanced planCreateIndex", () => {
    it("creates index with GIN method", async () => {
      const desired: TableSchema[] = [
        {
          table: "items",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            { name: "data", type: "jsonb", nullable: true },
          ],
          indexes: [{ columns: ["data"], method: "gin", name: "idx_items_data" }],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const idxOp = plan.structureOps.find((o) => o.type === "add_index" && o.sql.includes("gin"));
      expect(idxOp).toBeDefined();
      expect(idxOp!.sql).toContain("USING gin");
    });

    it("creates index with expression columns (not quoted)", async () => {
      const desired: TableSchema[] = [
        {
          table: "users",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            { name: "email", type: "text" },
          ],
          indexes: [{ columns: ["lower(email)"], unique: true, name: "idx_users_email_lower" }],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const idxOp = plan.structureOps.find((o) => o.type === "add_index" && o.sql.includes("lower(email)"));
      expect(idxOp).toBeDefined();
      // Expression columns should NOT be quoted
      expect(idxOp!.sql).not.toContain('"lower(email)"');
    });

    it("creates index with INCLUDE columns", async () => {
      const desired: TableSchema[] = [
        {
          table: "users",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            { name: "email", type: "text" },
            { name: "name", type: "text" },
          ],
          indexes: [{ columns: ["email"], include: ["name"], name: "idx_users_email_covering" }],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const idxOp = plan.structureOps.find((o) => o.type === "add_index" && o.sql.includes("INCLUDE"));
      expect(idxOp).toBeDefined();
      expect(idxOp!.sql).toContain('INCLUDE ("name")');
    });

    it("creates index with opclass", async () => {
      const desired: TableSchema[] = [
        {
          table: "items",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            { name: "data", type: "jsonb", nullable: true },
          ],
          indexes: [{ columns: ["data"], method: "gin", opclass: "jsonb_path_ops", name: "idx_items_data_pathops" }],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const idxOp = plan.structureOps.find((o) => o.type === "add_index" && o.sql.includes("jsonb_path_ops"));
      expect(idxOp).toBeDefined();
    });
  });

  describe("index diff on existing table", () => {
    it("detects new index needed on existing table", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text NOT NULL)`);

      const desired: TableSchema[] = [
        {
          table: "users",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            { name: "email", type: "text" },
          ],
          indexes: [{ columns: ["email"], name: "idx_users_email" }],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const idxOp = plan.structureOps.find((o) => o.type === "add_index" && o.sql.includes("idx_users_email"));
      expect(idxOp).toBeDefined();
    });

    it("detects removed index as destructive", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text NOT NULL)`);
      await ctx.client.query(`CREATE INDEX idx_users_email ON users (email)`);

      const desired: TableSchema[] = [
        {
          table: "users",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            { name: "email", type: "text" },
          ],
          // No indexes
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const blocked = plan.blocked.find((o) => o.type === "drop_index" && o.sql.includes("idx_users_email"));
      expect(blocked).toBeDefined();
      expect(blocked!.destructive).toBe(true);
    });

    it("produces no ops when index matches", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text NOT NULL)`);
      await ctx.client.query(`CREATE INDEX idx_users_email ON users (email)`);

      const desired: TableSchema[] = [
        {
          table: "users",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            { name: "email", type: "text" },
          ],
          indexes: [{ columns: ["email"], name: "idx_users_email" }],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const indexOps = plan.operations.filter((o) => o.type === "add_index" || o.type === "drop_index");
      expect(indexOps).toHaveLength(0);
    });
  });
});
