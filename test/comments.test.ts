import { describe, it, expect } from "vitest";
import { useTestClient, createTempProject, writeSchema } from "./helpers.js";
import { parseTableFile } from "../src/schema/parser.js";
import { buildPlan } from "../src/planner/index.js";
import { getTableComment, getColumnComments } from "../src/introspect/index.js";
import type { TableSchema } from "../src/schema/types.js";

describe("comments", () => {
  const ctx = useTestClient();

  describe("parser", () => {
    it("parses table and column comments", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(project.schemaDir, "users.yaml", `
table: users
comment: "Core user accounts table"
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
    comment: "User's primary email"
`);
        const schema = parseTableFile(filePath);
        expect(schema.comment).toBe("Core user accounts table");
        const email = schema.columns.find((c) => c.name === "email");
        expect(email!.comment).toBe("User's primary email");
      } finally {
        project.cleanup();
      }
    });
  });

  describe("planner", () => {
    it("plans COMMENT ON TABLE for new table with comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text NOT NULL)`);

      const desired: TableSchema[] = [{
        table: "users",
        comment: "Core user accounts",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "email", type: "text" },
        ],
      }];

      const plan = await buildPlan(ctx.client, desired, "public");
      const commentOp = plan.structureOps.find((o) => o.type === "set_comment" && o.sql.includes("COMMENT ON TABLE"));
      expect(commentOp).toBeDefined();
      expect(commentOp!.sql).toContain("Core user accounts");
      expect(commentOp!.destructive).toBe(false);
    });

    it("plans COMMENT ON COLUMN for column with comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text NOT NULL)`);

      const desired: TableSchema[] = [{
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "email", type: "text", comment: "Primary email address" },
        ],
      }];

      const plan = await buildPlan(ctx.client, desired, "public");
      const commentOp = plan.structureOps.find((o) => o.type === "set_comment" && o.sql.includes("COMMENT ON COLUMN"));
      expect(commentOp).toBeDefined();
      expect(commentOp!.sql).toContain("Primary email address");
    });

    it("produces no comment ops when comments match", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text NOT NULL)`);
      await ctx.client.query(`COMMENT ON TABLE users IS 'Core table'`);
      await ctx.client.query(`COMMENT ON COLUMN users.email IS 'Email address'`);

      const desired: TableSchema[] = [{
        table: "users",
        comment: "Core table",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "email", type: "text", comment: "Email address" },
        ],
      }];

      const plan = await buildPlan(ctx.client, desired, "public");
      const commentOps = plan.structureOps.filter((o) => o.type === "set_comment");
      expect(commentOps).toHaveLength(0);
    });
  });

  describe("introspection", () => {
    it("reads table comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY)`);
      await ctx.client.query(`COMMENT ON TABLE users IS 'The users table'`);

      const comment = await getTableComment(ctx.client, "users", "public");
      expect(comment).toBe("The users table");
    });

    it("reads column comments", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text)`);
      await ctx.client.query(`COMMENT ON COLUMN users.email IS 'Email col'`);

      const comments = await getColumnComments(ctx.client, "users", "public");
      expect(comments.get("email")).toBe("Email col");
    });

    it("returns null for table without comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY)`);

      const comment = await getTableComment(ctx.client, "users", "public");
      expect(comment).toBeNull();
    });
  });
});
