import { describe, it, expect } from "vitest";
import { useTestClient, createTempProject, writeSchema } from "./helpers.js";
import { parseEnumFile } from "../src/schema/parser.js";
import { buildPlan } from "../src/planner/index.js";
import { getExistingEnums } from "../src/introspect/index.js";

describe("enums", () => {
  const ctx = useTestClient();

  describe("parser", () => {
    it("parses a valid enum file", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(project.schemaDir, "enum_status.yaml", `
enum: status
values: [active, inactive, suspended]
`);
        const result = parseEnumFile(filePath);
        expect(result.name).toBe("status");
        expect(result.values).toEqual(["active", "inactive", "suspended"]);
      } finally {
        project.cleanup();
      }
    });

    it("throws on missing enum key", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(project.schemaDir, "bad.yaml", `values: [a, b]`);
        expect(() => parseEnumFile(filePath)).toThrow("expected \"enum\" key");
      } finally {
        project.cleanup();
      }
    });

    it("throws on missing values", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(project.schemaDir, "bad.yaml", `enum: status`);
        expect(() => parseEnumFile(filePath)).toThrow("non-empty array");
      } finally {
        project.cleanup();
      }
    });
  });

  describe("planner", () => {
    it("plans create_enum for a new enum type", async () => {
      const plan = await buildPlan(ctx.client, [], "public", {
        enums: [{ name: "status", values: ["active", "inactive"] }],
      });
      const createOp = plan.structureOps.find((o) => o.type === "create_enum");
      expect(createOp).toBeDefined();
      expect(createOp!.sql).toContain("CREATE TYPE");
      expect(createOp!.sql).toContain("'active'");
      expect(createOp!.sql).toContain("'inactive'");
      expect(createOp!.destructive).toBe(false);
    });

    it("plans add_enum_value for new values on existing enum", async () => {
      await ctx.client.query(`CREATE TYPE "public"."status" AS ENUM ('active', 'inactive')`);

      const plan = await buildPlan(ctx.client, [], "public", {
        enums: [{ name: "status", values: ["active", "inactive", "suspended"] }],
      });
      const addOp = plan.structureOps.find((o) => o.type === "add_enum_value");
      expect(addOp).toBeDefined();
      expect(addOp!.sql).toContain("ADD VALUE");
      expect(addOp!.sql).toContain("'suspended'");
      expect(addOp!.destructive).toBe(false);
    });

    it("produces no ops when enum matches", async () => {
      await ctx.client.query(`CREATE TYPE "public"."status" AS ENUM ('active', 'inactive')`);

      const plan = await buildPlan(ctx.client, [], "public", {
        enums: [{ name: "status", values: ["active", "inactive"] }],
      });
      const enumOps = plan.structureOps.filter((o) => o.type === "create_enum" || o.type === "add_enum_value");
      expect(enumOps).toHaveLength(0);
    });
  });

  describe("introspection", () => {
    it("introspects existing enums", async () => {
      await ctx.client.query(`CREATE TYPE "public"."priority" AS ENUM ('low', 'medium', 'high')`);

      const enums = await getExistingEnums(ctx.client, "public");
      const priority = enums.find((e) => e.name === "priority");
      expect(priority).toBeDefined();
      expect(priority!.values).toEqual(["low", "medium", "high"]);
    });
  });
});
