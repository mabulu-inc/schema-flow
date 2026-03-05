import { describe, it, expect } from "vitest";
import { useTestClient, createTempProject, writeSchema } from "./helpers.js";
import { parseExtensionsFile } from "../src/schema/parser.js";
import { buildPlan } from "../src/planner/index.js";
import { getExistingExtensions } from "../src/introspect/index.js";

describe("extensions", () => {
  const ctx = useTestClient();

  describe("parser", () => {
    it("parses a valid extensions file", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.tablesDir,
          "extensions.yaml",
          `
extensions:
  - pgcrypto
  - pg_trgm
`,
        );
        const result = parseExtensionsFile(filePath);
        expect(result.extensions).toEqual(["pgcrypto", "pg_trgm"]);
      } finally {
        project.cleanup();
      }
    });

    it("throws on missing extensions key", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(project.tablesDir, "bad.yaml", `other: value`);
        expect(() => parseExtensionsFile(filePath)).toThrow('expected "extensions" key');
      } finally {
        project.cleanup();
      }
    });
  });

  describe("planner", () => {
    it("plans create_extension for missing extensions", async () => {
      const plan = await buildPlan(ctx.client, [], "public", {
        extensions: { extensions: ["pgcrypto"] },
      });
      const createOp = plan.structureOps.find((o) => o.type === "create_extension");
      expect(createOp).toBeDefined();
      expect(createOp!.sql).toContain("CREATE EXTENSION");
      expect(createOp!.sql).toContain("pgcrypto");
      expect(createOp!.destructive).toBe(false);
    });

    it("produces no ops when extension already installed", async () => {
      await ctx.client.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto`);

      const plan = await buildPlan(ctx.client, [], "public", {
        extensions: { extensions: ["pgcrypto"] },
      });
      const extOps = plan.structureOps.filter((o) => o.type === "create_extension");
      expect(extOps).toHaveLength(0);
    });
  });

  describe("introspection", () => {
    it("lists installed extensions", async () => {
      await ctx.client.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto`);
      const exts = await getExistingExtensions(ctx.client);
      expect(exts).toContain("pgcrypto");
      // plpgsql is filtered out
      expect(exts).not.toContain("plpgsql");
    });
  });
});
