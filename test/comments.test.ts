import { describe, it, expect } from "vitest";
import { useTestClient, createTempProject, writeSchema } from "./helpers.js";
import {
  parseTableFile,
  parseEnumFile,
  parseViewFile,
  parseMaterializedViewFile,
  parseFunctionFile,
} from "../src/schema/parser.js";
import { buildPlan } from "../src/planner/index.js";
import {
  getTableComment,
  getColumnComments,
  getEnumComment,
  getViewComment,
  getMaterializedViewComment,
  getFunctionComment,
  getIndexComments,
  getTriggerComments,
  getConstraintComments,
  getPolicyComments,
} from "../src/introspect/index.js";
import type { TableSchema } from "../src/schema/types.js";

describe("comments", () => {
  const ctx = useTestClient();

  describe("parser", () => {
    it("parses table and column comments", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.schemaDir,
          "users.yaml",
          `
table: users
comment: "Core user accounts table"
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
    comment: "User's primary email"
`,
        );
        const schema = parseTableFile(filePath);
        expect(schema.comment).toBe("Core user accounts table");
        const email = schema.columns.find((c) => c.name === "email");
        expect(email!.comment).toBe("User's primary email");
      } finally {
        project.cleanup();
      }
    });

    it("parses index comment", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.schemaDir,
          "users.yaml",
          `
table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
indexes:
  - columns: [email]
    name: idx_users_email
    comment: "Speed up email lookups"
`,
        );
        const schema = parseTableFile(filePath);
        expect(schema.indexes![0].comment).toBe("Speed up email lookups");
      } finally {
        project.cleanup();
      }
    });

    it("parses trigger comment", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.schemaDir,
          "users.yaml",
          `
table: users
columns:
  - name: id
    type: serial
    primary_key: true
triggers:
  - name: trg_audit
    timing: AFTER
    events: [INSERT]
    function: audit_log
    comment: "Audit trail trigger"
`,
        );
        const schema = parseTableFile(filePath);
        expect(schema.triggers![0].comment).toBe("Audit trail trigger");
      } finally {
        project.cleanup();
      }
    });

    it("parses policy comment", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.schemaDir,
          "users.yaml",
          `
table: users
columns:
  - name: id
    type: serial
    primary_key: true
rls: true
policies:
  - name: users_select
    for: SELECT
    using: "true"
    comment: "Allow all reads"
`,
        );
        const schema = parseTableFile(filePath);
        expect(schema.policies![0].comment).toBe("Allow all reads");
      } finally {
        project.cleanup();
      }
    });

    it("parses check constraint comment", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.schemaDir,
          "users.yaml",
          `
table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: age
    type: integer
checks:
  - name: chk_age_positive
    expression: "age > 0"
    comment: "Age must be positive"
`,
        );
        const schema = parseTableFile(filePath);
        expect(schema.checks![0].comment).toBe("Age must be positive");
      } finally {
        project.cleanup();
      }
    });

    it("parses enum comment", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.schemaDir,
          "enum_status.yaml",
          `
enum: status
values: [active, inactive]
comment: "User account status"
`,
        );
        const schema = parseEnumFile(filePath);
        expect(schema.comment).toBe("User account status");
      } finally {
        project.cleanup();
      }
    });

    it("parses view comment", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.schemaDir,
          "view_active.yaml",
          `
view: active_users
query: "SELECT * FROM users WHERE active = true"
comment: "Only active users"
`,
        );
        const schema = parseViewFile(filePath);
        expect(schema.comment).toBe("Only active users");
      } finally {
        project.cleanup();
      }
    });

    it("parses materialized view comment", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.schemaDir,
          "mv_stats.yaml",
          `
materialized_view: user_stats
query: "SELECT count(*) FROM users"
comment: "User statistics"
`,
        );
        const schema = parseMaterializedViewFile(filePath);
        expect(schema.comment).toBe("User statistics");
      } finally {
        project.cleanup();
      }
    });

    it("parses function comment", () => {
      const project = createTempProject();
      try {
        const filePath = writeSchema(
          project.schemaDir,
          "fn_audit.yaml",
          `
name: audit_log
language: plpgsql
returns: trigger
body: "BEGIN RETURN NEW; END;"
comment: "Audit logging function"
`,
        );
        const schema = parseFunctionFile(filePath);
        expect(schema.comment).toBe("Audit logging function");
      } finally {
        project.cleanup();
      }
    });
  });

  describe("planner", () => {
    it("plans COMMENT ON TABLE for new table with comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text NOT NULL)`);

      const desired: TableSchema[] = [
        {
          table: "users",
          comment: "Core user accounts",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            { name: "email", type: "text" },
          ],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const commentOp = plan.structureOps.find((o) => o.type === "set_comment" && o.sql.includes("COMMENT ON TABLE"));
      expect(commentOp).toBeDefined();
      expect(commentOp!.sql).toContain("Core user accounts");
      expect(commentOp!.destructive).toBe(false);
    });

    it("plans COMMENT ON COLUMN for column with comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text NOT NULL)`);

      const desired: TableSchema[] = [
        {
          table: "users",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            { name: "email", type: "text", comment: "Primary email address" },
          ],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const commentOp = plan.structureOps.find((o) => o.type === "set_comment" && o.sql.includes("COMMENT ON COLUMN"));
      expect(commentOp).toBeDefined();
      expect(commentOp!.sql).toContain("Primary email address");
    });

    it("produces no comment ops when comments match", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text NOT NULL)`);
      await ctx.client.query(`COMMENT ON TABLE users IS 'Core table'`);
      await ctx.client.query(`COMMENT ON COLUMN users.email IS 'Email address'`);

      const desired: TableSchema[] = [
        {
          table: "users",
          comment: "Core table",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            { name: "email", type: "text", comment: "Email address" },
          ],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const commentOps = plan.structureOps.filter((o) => o.type === "set_comment");
      expect(commentOps).toHaveLength(0);
    });

    it("plans COMMENT ON INDEX for index with comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text NOT NULL)`);
      await ctx.client.query(`CREATE INDEX idx_users_email ON users (email)`);

      const desired: TableSchema[] = [
        {
          table: "users",
          columns: [
            { name: "id", type: "serial", primary_key: true },
            { name: "email", type: "text" },
          ],
          indexes: [{ columns: ["email"], name: "idx_users_email", comment: "Email lookup index" }],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const commentOp = plan.validateOps.find((o) => o.type === "set_comment" && o.sql.includes("COMMENT ON INDEX"));
      expect(commentOp).toBeDefined();
      expect(commentOp!.sql).toContain("Email lookup index");
    });

    it("plans COMMENT ON TRIGGER for trigger with comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY)`);
      await ctx.client.query(
        `CREATE FUNCTION noop_trigger() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END;'`,
      );
      await ctx.client.query(
        `CREATE TRIGGER trg_test AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION noop_trigger()`,
      );

      const desired: TableSchema[] = [
        {
          table: "users",
          columns: [{ name: "id", type: "serial", primary_key: true }],
          triggers: [
            {
              name: "trg_test",
              timing: "AFTER",
              events: ["INSERT"],
              function: "noop_trigger",
              for_each: "ROW",
              comment: "Test trigger comment",
            },
          ],
        },
      ];

      const plan = await buildPlan(ctx.client, desired, "public");
      const commentOp = plan.structureOps.find((o) => o.type === "set_comment" && o.sql.includes("COMMENT ON TRIGGER"));
      expect(commentOp).toBeDefined();
      expect(commentOp!.sql).toContain("Test trigger comment");
    });

    it("plans COMMENT ON TYPE for enum with comment", async () => {
      const plan = await buildPlan(ctx.client, [], "public", {
        enums: [{ name: "status", values: ["active", "inactive"], comment: "Account status" }],
      });
      const commentOp = plan.structureOps.find((o) => o.type === "set_comment" && o.sql.includes("COMMENT ON TYPE"));
      expect(commentOp).toBeDefined();
      expect(commentOp!.sql).toContain("Account status");
    });

    it("plans COMMENT ON VIEW for view with comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY)`);
      const plan = await buildPlan(
        ctx.client,
        [{ table: "users", columns: [{ name: "id", type: "serial", primary_key: true }] }],
        "public",
        {
          views: [{ name: "all_users", query: "SELECT * FROM users", comment: "All users view" }],
        },
      );
      const commentOp = plan.structureOps.find((o) => o.type === "set_comment" && o.sql.includes("COMMENT ON VIEW"));
      expect(commentOp).toBeDefined();
      expect(commentOp!.sql).toContain("All users view");
    });

    it("plans COMMENT ON MATERIALIZED VIEW for MV with comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY)`);
      const plan = await buildPlan(
        ctx.client,
        [{ table: "users", columns: [{ name: "id", type: "serial", primary_key: true }] }],
        "public",
        {
          materializedViews: [
            { name: "user_counts", query: "SELECT count(*) FROM users", comment: "User count cache" },
          ],
        },
      );
      const commentOp = plan.structureOps.find(
        (o) => o.type === "set_comment" && o.sql.includes("COMMENT ON MATERIALIZED VIEW"),
      );
      expect(commentOp).toBeDefined();
      expect(commentOp!.sql).toContain("User count cache");
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

    it("reads enum comment", async () => {
      await ctx.client.query(`CREATE TYPE status AS ENUM ('active', 'inactive')`);
      await ctx.client.query(`COMMENT ON TYPE status IS 'Account status'`);

      const comment = await getEnumComment(ctx.client, "status", "public");
      expect(comment).toBe("Account status");
    });

    it("reads view comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY)`);
      await ctx.client.query(`CREATE VIEW all_users AS SELECT * FROM users`);
      await ctx.client.query(`COMMENT ON VIEW all_users IS 'All users'`);

      const comment = await getViewComment(ctx.client, "all_users", "public");
      expect(comment).toBe("All users");
    });

    it("reads materialized view comment", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY)`);
      await ctx.client.query(`CREATE MATERIALIZED VIEW user_counts AS SELECT count(*) FROM users`);
      await ctx.client.query(`COMMENT ON MATERIALIZED VIEW user_counts IS 'User counts'`);

      const comment = await getMaterializedViewComment(ctx.client, "user_counts", "public");
      expect(comment).toBe("User counts");
    });

    it("reads function comment", async () => {
      await ctx.client.query(`CREATE FUNCTION noop() RETURNS void LANGUAGE plpgsql AS 'BEGIN END;'`);
      await ctx.client.query(`COMMENT ON FUNCTION noop() IS 'Does nothing'`);

      const comment = await getFunctionComment(ctx.client, "noop", "public");
      expect(comment).toBe("Does nothing");
    });

    it("reads index comments", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email text)`);
      await ctx.client.query(`CREATE INDEX idx_users_email ON users (email)`);
      await ctx.client.query(`COMMENT ON INDEX idx_users_email IS 'Email lookup'`);

      const comments = await getIndexComments(ctx.client, "users", "public");
      expect(comments.get("idx_users_email")).toBe("Email lookup");
    });

    it("reads trigger comments", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY)`);
      await ctx.client.query(
        `CREATE FUNCTION noop_trigger() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END;'`,
      );
      await ctx.client.query(
        `CREATE TRIGGER trg_test AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION noop_trigger()`,
      );
      await ctx.client.query(`COMMENT ON TRIGGER trg_test ON users IS 'Test trigger'`);

      const comments = await getTriggerComments(ctx.client, "users", "public");
      expect(comments.get("trg_test")).toBe("Test trigger");
    });

    it("reads constraint comments", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, age integer)`);
      await ctx.client.query(`ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age > 0)`);
      await ctx.client.query(`COMMENT ON CONSTRAINT chk_age ON users IS 'Age must be positive'`);

      const comments = await getConstraintComments(ctx.client, "users", "public");
      expect(comments.get("chk_age")).toBe("Age must be positive");
    });

    it("reads policy comments", async () => {
      await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY)`);
      await ctx.client.query(`ALTER TABLE users ENABLE ROW LEVEL SECURITY`);
      await ctx.client.query(`CREATE POLICY users_select ON users FOR SELECT USING (true)`);
      await ctx.client.query(`COMMENT ON POLICY users_select ON users IS 'Allow reads'`);

      const comments = await getPolicyComments(ctx.client, "users", "public");
      expect(comments.get("users_select")).toBe("Allow reads");
    });
  });
});
