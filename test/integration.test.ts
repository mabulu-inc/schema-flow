// test/integration.test.ts
// End-to-end integration tests: run migrations and verify objects exist in the database

import { describe, it, expect } from "vitest";
import {
  writeSchema,
  useTestProject,
  execSql,
  tableExists,
  getColumns,
  enumExists,
  getEnumValues,
  extensionExists,
  viewExists,
  materializedViewExists,
  indexExists,
  getIndexMethod,
  getComment,
  isConstraintDeferrable,
  isConstraintInitiallyDeferred,
  fkExists,
  getEnumComment,
  getViewComment,
  getFunctionComment,
  getIndexComment,
  getTriggerComment,
  getConstraintComment,
  getPolicyComment,
  getMaterializedViewComment,
} from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { runMigrate } from "../src/executor/index.js";
import { closePool } from "../src/core/db.js";
import { logger, LogLevel } from "../src/core/logger.js";

// Suppress logs during tests
logger.setLevel(LogLevel.SILENT);

describe("integration: end-to-end execution", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  // ─── Enums ───────────────────────────────────────────────────────────────

  describe("enums", () => {
    it("creates an enum type via runMigrate", async () => {
      writeSchema(
        ctx.project.enumsDir,
        "status.yaml",
        `enum: status
values: [active, inactive, suspended]
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await enumExists(ctx.connectionString, "status")).toBe(true);
      const values = await getEnumValues(ctx.connectionString, "status");
      expect(values).toEqual(["active", "inactive", "suspended"]);
    });

    it("adds a value to an existing enum", async () => {
      // Create the initial enum
      writeSchema(
        ctx.project.enumsDir,
        "status.yaml",
        `enum: status
values: [active, inactive]
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      await runMigrate(config);
      await closePool();

      // Add a new value
      writeSchema(
        ctx.project.enumsDir,
        "status.yaml",
        `enum: status
values: [active, inactive, suspended]
`,
      );

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      const values = await getEnumValues(ctx.connectionString, "status");
      expect(values).toContain("suspended");
    });

    it("uses enum type in a table column", async () => {
      writeSchema(
        ctx.project.enumsDir,
        "status.yaml",
        `enum: status
values: [active, inactive]
`,
      );

      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: status
    default: "'active'"
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await tableExists(ctx.connectionString, "users")).toBe(true);
      expect(await enumExists(ctx.connectionString, "status")).toBe(true);

      // Insert a row using the enum
      await execSql(ctx.connectionString, `INSERT INTO users (status) VALUES ('active')`);
      const res = await execSql(ctx.connectionString, `SELECT status FROM users`);
      expect(res.rows[0].status).toBe("active");
    });
  });

  // ─── Extensions ──────────────────────────────────────────────────────────

  describe("extensions", () => {
    it("creates an extension via runMigrate", async () => {
      writeSchema(
        ctx.project.sfDir,
        "extensions.yaml",
        `extensions:
  - pgcrypto
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await extensionExists(ctx.connectionString, "pgcrypto")).toBe(true);
    });

    it("is idempotent — second run produces no errors", async () => {
      writeSchema(
        ctx.project.sfDir,
        "extensions.yaml",
        `extensions:
  - pgcrypto
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      await runMigrate(config);
      await closePool();

      const result = await runMigrate(config);
      expect(result.success).toBe(true);
    });
  });

  // ─── Views ───────────────────────────────────────────────────────────────

  describe("views", () => {
    it("creates a view via runMigrate and can query it", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
  - name: is_active
    type: boolean
    default: "true"
`,
      );

      writeSchema(
        ctx.project.viewsDir,
        "active_users.yaml",
        `view: active_users
query: "SELECT id, email FROM users WHERE is_active = true"
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await viewExists(ctx.connectionString, "active_users")).toBe(true);

      // Insert data and query the view
      await execSql(
        ctx.connectionString,
        `INSERT INTO users (email, is_active) VALUES ('a@b.com', true), ('c@d.com', false)`,
      );
      const res = await execSql(ctx.connectionString, `SELECT * FROM active_users`);
      expect(res.rowCount).toBe(1);
      expect(res.rows[0].email).toBe("a@b.com");
    });

    it("updates a view definition on re-run", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
  - name: is_active
    type: boolean
    default: "true"
`,
      );

      writeSchema(
        ctx.project.viewsDir,
        "active_users.yaml",
        `view: active_users
query: "SELECT id, email FROM users WHERE is_active = true"
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      await runMigrate(config);
      await closePool();

      // Update the view query to include all users
      writeSchema(
        ctx.project.viewsDir,
        "active_users.yaml",
        `view: active_users
query: "SELECT id, email FROM users"
`,
      );

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      // Insert data and verify updated view returns all users
      await execSql(
        ctx.connectionString,
        `INSERT INTO users (email, is_active) VALUES ('a@b.com', true), ('c@d.com', false)`,
      );
      const res = await execSql(ctx.connectionString, `SELECT * FROM active_users`);
      expect(res.rowCount).toBe(2);
    });
  });

  // ─── Materialized Views ──────────────────────────────────────────────────

  describe("materialized views", () => {
    it("creates a materialized view via runMigrate", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "events.yaml",
        `table: events
columns:
  - name: id
    type: serial
    primary_key: true
  - name: created_at
    type: timestamptz
    default: now()
`,
      );

      writeSchema(
        ctx.project.viewsDir,
        "mv_event_counts.yaml",
        `materialized_view: event_counts
query: "SELECT count(*) AS total FROM events"
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await materializedViewExists(ctx.connectionString, "event_counts")).toBe(true);

      // Query the mat view
      const res = await execSql(ctx.connectionString, `SELECT total FROM event_counts`);
      expect(Number(res.rows[0].total)).toBe(0);
    });

    it("creates a materialized view with indexes", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "events.yaml",
        `table: events
columns:
  - name: id
    type: serial
    primary_key: true
  - name: category
    type: text
`,
      );

      writeSchema(
        ctx.project.viewsDir,
        "mv_event_summary.yaml",
        `materialized_view: event_summary
query: "SELECT category, count(*) AS total FROM events GROUP BY category"
indexes:
  - columns: [category]
    unique: true
    name: idx_event_summary_category
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await materializedViewExists(ctx.connectionString, "event_summary")).toBe(true);
      expect(await indexExists(ctx.connectionString, "idx_event_summary_category")).toBe(true);
    });
  });

  // ─── Generated Columns ──────────────────────────────────────────────────

  describe("generated columns", () => {
    it("creates a table with generated column and verifies computed value", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: first_name
    type: text
  - name: last_name
    type: text
  - name: full_name
    type: text
    generated: "first_name || ' ' || last_name"
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await tableExists(ctx.connectionString, "users")).toBe(true);

      // Insert data and verify the generated column computes correctly
      await execSql(ctx.connectionString, `INSERT INTO users (first_name, last_name) VALUES ('Jane', 'Doe')`);
      const res = await execSql(ctx.connectionString, `SELECT full_name FROM users`);
      expect(res.rows[0].full_name).toBe("Jane Doe");
    });

    it("adds generated column to existing table", async () => {
      // Create the table without the generated column first
      writeSchema(
        ctx.project.tablesDir,
        "items.yaml",
        `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: price
    type: integer
  - name: qty
    type: integer
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      await runMigrate(config);
      await closePool();

      // Now add a generated column
      writeSchema(
        ctx.project.tablesDir,
        "items.yaml",
        `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: price
    type: integer
  - name: qty
    type: integer
  - name: total
    type: integer
    generated: "price * qty"
`,
      );

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      const cols = await getColumns(ctx.connectionString, "items");
      expect(cols).toContain("total");

      // Insert data and verify
      await execSql(ctx.connectionString, `INSERT INTO items (price, qty) VALUES (10, 3)`);
      const res = await execSql(ctx.connectionString, `SELECT total FROM items`);
      expect(Number(res.rows[0].total)).toBe(30);
    });
  });

  // ─── Comments ────────────────────────────────────────────────────────────

  describe("comments", () => {
    it("applies table and column comments on existing table", async () => {
      // First create the table without comments
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      await runMigrate(config);
      await closePool();

      // Now add comments
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
comment: "Core user accounts"
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
    comment: "Primary email address"
`,
      );

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await getComment(ctx.connectionString, "users")).toBe("Core user accounts");
      expect(await getComment(ctx.connectionString, "users", "email")).toBe("Primary email address");
    });

    it("updates comments on re-run", async () => {
      // Create table with initial comments
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      await runMigrate(config);
      await closePool();

      // Add initial comments
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
comment: "User table v1"
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
    comment: "Email v1"
`,
      );

      await runMigrate(config);
      await closePool();

      // Update comments
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
comment: "User table v2"
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
    comment: "Email v2"
`,
      );

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await getComment(ctx.connectionString, "users")).toBe("User table v2");
      expect(await getComment(ctx.connectionString, "users", "email")).toBe("Email v2");
    });

    it("applies enum comment", async () => {
      writeSchema(
        ctx.project.enumsDir,
        "status.yaml",
        `enum: status
values: [active, inactive]
comment: "Account status type"
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await getEnumComment(ctx.connectionString, "status")).toBe("Account status type");
    });

    it("applies view comment", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
`,
      );

      writeSchema(
        ctx.project.viewsDir,
        "all_users.yaml",
        `view: all_users
query: "SELECT id, email FROM users"
comment: "All users view"
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await getViewComment(ctx.connectionString, "all_users")).toBe("All users view");
    });

    it("applies function comment", async () => {
      writeSchema(
        ctx.project.functionsDir,
        "noop.yaml",
        `name: noop
language: plpgsql
returns: void
body: "BEGIN END;"
comment: "A no-op function"
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await getFunctionComment(ctx.connectionString, "noop")).toBe("A no-op function");
    });

    it("applies index comment", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
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

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await getIndexComment(ctx.connectionString, "idx_users_email")).toBe("Speed up email lookups");
    });

    it("applies trigger comment", async () => {
      writeSchema(
        ctx.project.functionsDir,
        "noop_trigger.yaml",
        `name: noop_trigger
language: plpgsql
returns: trigger
body: "BEGIN RETURN NEW; END;"
`,
      );

      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
triggers:
  - name: trg_audit
    timing: AFTER
    events: [INSERT]
    function: noop_trigger
    comment: "Audit trail trigger"
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await getTriggerComment(ctx.connectionString, "trg_audit", "users")).toBe("Audit trail trigger");
    });

    it("applies materialized view comment", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "events.yaml",
        `table: events
columns:
  - name: id
    type: serial
    primary_key: true
  - name: created_at
    type: timestamptz
    default: now()
`,
      );

      writeSchema(
        ctx.project.viewsDir,
        "mv_event_counts.yaml",
        `materialized_view: event_counts
query: "SELECT count(*) AS total FROM events"
comment: "Cached event totals"
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await getMaterializedViewComment(ctx.connectionString, "event_counts")).toBe("Cached event totals");
    });

    it("applies check constraint comment", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
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

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await getConstraintComment(ctx.connectionString, "chk_age_positive")).toBe("Age must be positive");
    });

    it("applies policy comment", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
rls: true
columns:
  - name: id
    type: serial
    primary_key: true
policies:
  - name: users_read
    for: SELECT
    using: "true"
    comment: "Allow all reads"
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await getPolicyComment(ctx.connectionString, "users_read", "users")).toBe("Allow all reads");
    });
  });

  // ─── Deferrable FKs ─────────────────────────────────────────────────────

  describe("deferrable foreign keys", () => {
    it("creates a deferrable FK constraint", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "nodes.yaml",
        `table: nodes
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

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await fkExists(ctx.connectionString, "fk_nodes_parent_id_nodes")).toBe(true);
      expect(await isConstraintDeferrable(ctx.connectionString, "fk_nodes_parent_id_nodes")).toBe(true);
      expect(await isConstraintInitiallyDeferred(ctx.connectionString, "fk_nodes_parent_id_nodes")).toBe(true);
    });

    it("allows inserts within deferred transaction", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "nodes.yaml",
        `table: nodes
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

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      await runMigrate(config);

      // Insert child before parent within a deferred transaction — would fail without deferrable
      await execSql(
        ctx.connectionString,
        `BEGIN;
         INSERT INTO nodes (id, parent_id) VALUES (2, 1);
         INSERT INTO nodes (id, parent_id) VALUES (1, NULL);
         COMMIT;`,
      );

      const res = await execSql(ctx.connectionString, `SELECT count(*) AS cnt FROM nodes`);
      expect(Number(res.rows[0].cnt)).toBe(2);
    });
  });

  // ─── Enhanced Indexes ────────────────────────────────────────────────────

  describe("enhanced indexes", () => {
    it("creates a GIN index", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "items.yaml",
        `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: data
    type: jsonb
    nullable: true
indexes:
  - columns: [data]
    method: gin
    name: idx_items_data_gin
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await indexExists(ctx.connectionString, "idx_items_data_gin")).toBe(true);
      expect(await getIndexMethod(ctx.connectionString, "idx_items_data_gin")).toBe("gin");
    });

    it("creates an expression index", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
indexes:
  - columns: ["lower(email)"]
    unique: true
    name: idx_users_email_lower
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await indexExists(ctx.connectionString, "idx_users_email_lower")).toBe(true);
    });

    it("creates an index with INCLUDE columns", async () => {
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
  - name: name
    type: text
    nullable: true
indexes:
  - columns: [email]
    include: [name]
    name: idx_users_email_covering
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await indexExists(ctx.connectionString, "idx_users_email_covering")).toBe(true);

      // Verify it's actually a covering index by checking pg_indexes
      const res = await execSql(
        ctx.connectionString,
        `SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_users_email_covering'`,
      );
      expect(res.rows[0].indexdef).toContain("INCLUDE");
    });
  });

  // ─── Index Diffing (end-to-end) ──────────────────────────────────────────

  describe("index diffing end-to-end", () => {
    it("adds an index via YAML and verifies it exists", async () => {
      // First create the table without indexes
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
      });

      await runMigrate(config);
      await closePool();

      // Now add an index
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
indexes:
  - columns: [email]
    name: idx_users_email
`,
      );

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await indexExists(ctx.connectionString, "idx_users_email")).toBe(true);
    });

    it("removes an index when removed from YAML with --allow-destructive", async () => {
      // Create table with index
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
indexes:
  - columns: [email]
    name: idx_users_email
`,
      );

      const config = resolveConfig({
        connectionString: ctx.connectionString,
        baseDir: ctx.project.baseDir,
        dryRun: false,
        allowDestructive: true,
      });

      await runMigrate(config);
      expect(await indexExists(ctx.connectionString, "idx_users_email")).toBe(true);

      await closePool();

      // Remove the index from YAML
      writeSchema(
        ctx.project.tablesDir,
        "users.yaml",
        `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
`,
      );

      const result = await runMigrate(config);
      expect(result.success).toBe(true);

      expect(await indexExists(ctx.connectionString, "idx_users_email")).toBe(false);
    });
  });
});
