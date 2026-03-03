import { describe, it, expect } from "vitest";
import { useTestClient, createTempProject, writeSchema } from "./helpers.js";
import { parseViewFile, parseMaterializedViewFile } from "../src/schema/parser.js";
import { buildPlan } from "../src/planner/index.js";
import { getExistingViews, getExistingMaterializedViews } from "../src/introspect/index.js";

describe("views", () => {
  const ctx = useTestClient();

  describe("parser", () => {
    it("parses a valid view file", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(project.schemaDir, "view_active_users.yaml", `
view: active_users
query: "SELECT id, name FROM users WHERE active = true"
`);
        const result = parseViewFile(filePath);
        expect(result.name).toBe("active_users");
        expect(result.query).toContain("SELECT");
      } finally {
        project.cleanup();
      }
    });

    it("throws on missing view key", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(project.schemaDir, "bad.yaml", `query: "SELECT 1"`);
        expect(() => parseViewFile(filePath)).toThrow("expected \"view\" key");
      } finally {
        project.cleanup();
      }
    });

    it("parses a valid materialized view file", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(project.schemaDir, "mv_stats.yaml", `
materialized_view: daily_stats
query: "SELECT count(*) as total FROM events"
indexes:
  - columns: [total]
    unique: true
`);
        const result = parseMaterializedViewFile(filePath);
        expect(result.name).toBe("daily_stats");
        expect(result.query).toContain("SELECT");
        expect(result.indexes).toHaveLength(1);
      } finally {
        project.cleanup();
      }
    });

    it("throws on missing materialized_view key", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(project.schemaDir, "bad.yaml", `query: "SELECT 1"`);
        expect(() => parseMaterializedViewFile(filePath)).toThrow("expected \"materialized_view\" key");
      } finally {
        project.cleanup();
      }
    });
  });

  describe("planner", () => {
    it("plans create_view for a new view", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, name text, active boolean)`);

      const plan = await buildPlan(ctx.client, [
        { table: "users", columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "name", type: "text" },
          { name: "active", type: "boolean" },
        ]},
      ], "public", {
        views: [{ name: "active_users", query: "SELECT id, name FROM users WHERE active = true" }],
      });
      const createOp = plan.structureOps.find((o) => o.type === "create_view");
      expect(createOp).toBeDefined();
      expect(createOp!.sql).toContain("CREATE OR REPLACE VIEW");
      expect(createOp!.sql).toContain("active_users");
      expect(createOp!.destructive).toBe(false);
    });

    it("plans create_materialized_view for a new mat view", async () => {
      await ctx.client.query(`CREATE TABLE events (id serial PRIMARY KEY, created_at timestamptz)`);

      const plan = await buildPlan(ctx.client, [
        { table: "events", columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "created_at", type: "timestamptz" },
        ]},
      ], "public", {
        materializedViews: [{ name: "event_counts", query: "SELECT count(*) FROM events" }],
      });
      const createOp = plan.structureOps.find((o) => o.type === "create_materialized_view");
      expect(createOp).toBeDefined();
      expect(createOp!.sql).toContain("CREATE MATERIALIZED VIEW");
      expect(createOp!.sql).toContain("event_counts");
    });
  });

  describe("introspection", () => {
    it("introspects existing views", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial, name text)`);
      await ctx.client.query(`CREATE VIEW active_users AS SELECT id, name FROM users`);

      const views = await getExistingViews(ctx.client, "public");
      const found = views.find((v) => v.name === "active_users");
      expect(found).toBeDefined();
      expect(found!.query).toContain("SELECT");
    });

    it("introspects existing materialized views", async () => {
      await ctx.client.query(`CREATE TABLE events (id serial, val int)`);
      await ctx.client.query(`CREATE MATERIALIZED VIEW event_stats AS SELECT count(*) FROM events`);

      const mvs = await getExistingMaterializedViews(ctx.client, "public");
      const found = mvs.find((v) => v.name === "event_stats");
      expect(found).toBeDefined();
      expect(found!.query).toContain("SELECT");
    });
  });
});
