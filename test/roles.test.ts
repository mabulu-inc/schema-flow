// test/roles.test.ts
// TDD tests for declarative role creation and column-level grants

import { describe, it, expect } from "vitest";
import { writeSchema, writeMixin, useTestProject, execSql } from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { runMigrate } from "../src/executor/index.js";
import { closePool } from "../src/core/db.js";
import { logger, LogLevel } from "../src/core/logger.js";
import { detectDrift, type DriftItem } from "../src/drift/index.js";

logger.setLevel(LogLevel.SILENT);

// ─── Helper: check if a role exists ──────────────────────────────────────────

async function roleExists(connectionString: string, roleName: string): Promise<boolean> {
  const res = await execSql(connectionString, `SELECT 1 FROM pg_roles WHERE rolname = $1`, [roleName]);
  return res.rowCount !== null && res.rowCount > 0;
}

async function getRoleAttributes(
  connectionString: string,
  roleName: string,
): Promise<{
  login: boolean;
  superuser: boolean;
  createdb: boolean;
  createrole: boolean;
  inherit: boolean;
  connection_limit: number;
} | null> {
  const res = await execSql(
    connectionString,
    `SELECT rolcanlogin AS login, rolsuper AS superuser, rolcreatedb AS createdb,
            rolcreaterole AS createrole, rolinherit AS inherit, rolconnlimit AS connection_limit
     FROM pg_roles WHERE rolname = $1`,
    [roleName],
  );
  if (res.rows.length === 0) return null;
  return res.rows[0];
}

async function getRoleMemberships(connectionString: string, roleName: string): Promise<string[]> {
  const res = await execSql(
    connectionString,
    `SELECT g.rolname AS group_name
     FROM pg_auth_members m
     JOIN pg_roles r ON r.oid = m.member
     JOIN pg_roles g ON g.oid = m.roleid
     WHERE r.rolname = $1
     ORDER BY g.rolname`,
    [roleName],
  );
  return res.rows.map((r: { group_name: string }) => r.group_name);
}

// ─── Helper: check table grants ──────────────────────────────────────────────

async function getTableGrants(connectionString: string, tableName: string, roleName: string): Promise<string[]> {
  const res = await execSql(
    connectionString,
    `SELECT p.privilege_type
     FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
     CROSS JOIN LATERAL aclexplode(c.relacl) AS p(grantor, grantee, privilege_type, is_grantable)
     JOIN pg_roles r ON r.oid = p.grantee
     WHERE n.nspname = 'public' AND c.relname = $1 AND r.rolname = $2
     ORDER BY p.privilege_type`,
    [tableName, roleName],
  );
  return res.rows.map((r: { privilege_type: string }) => r.privilege_type);
}

async function getColumnGrants(
  connectionString: string,
  tableName: string,
  roleName: string,
): Promise<{ column: string; privilege: string }[]> {
  const res = await execSql(
    connectionString,
    `SELECT column_name AS column, privilege_type AS privilege
     FROM information_schema.column_privileges
     WHERE table_schema = 'public' AND table_name = $1 AND grantee = $2
     ORDER BY column_name, privilege_type`,
    [tableName, roleName],
  );
  return res.rows.map((r: { column: string; privilege: string }) => ({
    column: r.column,
    privilege: r.privilege,
  }));
}

// ─── Tests ───────────────────────────────────────────────────────────────────

describe("roles: declarative role creation", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("creates a role via runMigrate", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_app_reader.yaml",
      `role: app_reader
login: false
`,
    );

    // Need at least one table file for the migration to process schema files
    writeSchema(
      ctx.project.tablesDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);
    expect(await roleExists(ctx.connectionString, "app_reader")).toBe(true);

    const attrs = await getRoleAttributes(ctx.connectionString, "app_reader");
    expect(attrs).not.toBeNull();
    expect(attrs!.login).toBe(false);
  });

  it("creates a role with all attributes", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_admin.yaml",
      `role: admin_role
login: true
createdb: true
createrole: true
inherit: true
connection_limit: 10
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
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const attrs = await getRoleAttributes(ctx.connectionString, "admin_role");
    expect(attrs).not.toBeNull();
    expect(attrs!.login).toBe(true);
    expect(attrs!.createdb).toBe(true);
    expect(attrs!.createrole).toBe(true);
    expect(attrs!.inherit).toBe(true);
    expect(attrs!.connection_limit).toBe(10);
  });

  it("creates a role with membership (in)", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_base.yaml",
      `role: base_role
login: false
`,
    );

    writeSchema(
      ctx.project.rolesDir,
      "role_child.yaml",
      `role: child_role
login: false
in:
  - base_role
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
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    expect(await roleExists(ctx.connectionString, "base_role")).toBe(true);
    expect(await roleExists(ctx.connectionString, "child_role")).toBe(true);
    const memberships = await getRoleMemberships(ctx.connectionString, "child_role");
    expect(memberships).toContain("base_role");
  });

  it("is idempotent — running twice with same role produces no errors", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_app_reader.yaml",
      `role: app_reader
login: false
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
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result1 = await runMigrate(config);
    expect(result1.success).toBe(true);

    await closePool();

    const result2 = await runMigrate(config);
    expect(result2.success).toBe(true);
    expect(await roleExists(ctx.connectionString, "app_reader")).toBe(true);
  });

  it("alters a role when attributes change", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_app.yaml",
      `role: app_role
login: false
createdb: false
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
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result1 = await runMigrate(config);
    expect(result1.success).toBe(true);

    let attrs = await getRoleAttributes(ctx.connectionString, "app_role");
    expect(attrs!.login).toBe(false);
    expect(attrs!.createdb).toBe(false);

    await closePool();

    // Change attributes
    writeSchema(
      ctx.project.rolesDir,
      "role_app.yaml",
      `role: app_role
login: true
createdb: true
`,
    );

    const result2 = await runMigrate(config);
    expect(result2.success).toBe(true);

    attrs = await getRoleAttributes(ctx.connectionString, "app_role");
    expect(attrs!.login).toBe(true);
    expect(attrs!.createdb).toBe(true);
  });
});

describe("grants: table-level grants", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("grants table-level privileges to a role", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_reader.yaml",
      `role: reader_role
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "orders.yaml",
      `table: orders
columns:
  - name: id
    type: serial
    primary_key: true
  - name: amount
    type: numeric
grants:
  - to: reader_role
    privileges: [SELECT]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const grants = await getTableGrants(ctx.connectionString, "orders", "reader_role");
    expect(grants).toContain("SELECT");
  });

  it("grants multiple privileges", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_writer.yaml",
      `role: writer_role
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "orders.yaml",
      `table: orders
columns:
  - name: id
    type: serial
    primary_key: true
  - name: amount
    type: numeric
grants:
  - to: writer_role
    privileges: [SELECT, INSERT, UPDATE, DELETE]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const grants = await getTableGrants(ctx.connectionString, "orders", "writer_role");
    expect(grants).toContain("SELECT");
    expect(grants).toContain("INSERT");
    expect(grants).toContain("UPDATE");
    expect(grants).toContain("DELETE");
  });

  it("is idempotent — granting twice produces no errors", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_reader.yaml",
      `role: reader_role
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "orders.yaml",
      `table: orders
columns:
  - name: id
    type: serial
    primary_key: true
grants:
  - to: reader_role
    privileges: [SELECT]
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

    const grants = await getTableGrants(ctx.connectionString, "orders", "reader_role");
    expect(grants).toContain("SELECT");
  });

  it("revokes removed privileges with --allow-destructive", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_writer.yaml",
      `role: writer_role
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "orders.yaml",
      `table: orders
columns:
  - name: id
    type: serial
    primary_key: true
grants:
  - to: writer_role
    privileges: [SELECT, INSERT, UPDATE]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
      allowDestructive: true,
    });

    await runMigrate(config);
    await closePool();

    // Remove INSERT and UPDATE
    writeSchema(
      ctx.project.tablesDir,
      "orders.yaml",
      `table: orders
columns:
  - name: id
    type: serial
    primary_key: true
grants:
  - to: writer_role
    privileges: [SELECT]
`,
    );

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const grants = await getTableGrants(ctx.connectionString, "orders", "writer_role");
    expect(grants).toContain("SELECT");
    expect(grants).not.toContain("INSERT");
    expect(grants).not.toContain("UPDATE");
  });
});

describe("grants: column-level grants", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("grants column-level SELECT to a role", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_auditor.yaml",
      `role: auditor_role
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "orders.yaml",
      `table: orders
columns:
  - name: id
    type: serial
    primary_key: true
  - name: amount
    type: numeric
  - name: payment_ref
    type: text
    nullable: true
  - name: status
    type: text
grants:
  - to: auditor_role
    privileges: [SELECT]
    columns: [id, amount, status]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const colGrants = await getColumnGrants(ctx.connectionString, "orders", "auditor_role");
    const selectCols = colGrants.filter((g) => g.privilege === "SELECT").map((g) => g.column);
    expect(selectCols).toContain("id");
    expect(selectCols).toContain("amount");
    expect(selectCols).toContain("status");
    expect(selectCols).not.toContain("payment_ref");
  });

  it("combines table-level and column-level grants on same table", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_user.yaml",
      `role: app_user
login: false
`,
    );

    writeSchema(
      ctx.project.rolesDir,
      "role_auditor.yaml",
      `role: auditor_role
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "orders.yaml",
      `table: orders
columns:
  - name: id
    type: serial
    primary_key: true
  - name: amount
    type: numeric
  - name: payment_ref
    type: text
    nullable: true
grants:
  - to: app_user
    privileges: [SELECT, INSERT, UPDATE]
  - to: auditor_role
    privileges: [SELECT]
    columns: [id, amount]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    // app_user has table-level grants
    const userGrants = await getTableGrants(ctx.connectionString, "orders", "app_user");
    expect(userGrants).toContain("SELECT");
    expect(userGrants).toContain("INSERT");
    expect(userGrants).toContain("UPDATE");

    // auditor_role has column-level grants only
    const auditorColGrants = await getColumnGrants(ctx.connectionString, "orders", "auditor_role");
    const selectCols = auditorColGrants.filter((g) => g.privilege === "SELECT").map((g) => g.column);
    expect(selectCols).toContain("id");
    expect(selectCols).toContain("amount");
    expect(selectCols).not.toContain("payment_ref");
  });
});

describe("drift: roles and grants", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  function find(items: DriftItem[], opts: Partial<DriftItem>) {
    return items.filter((i) => Object.entries(opts).every(([k, v]) => i[k as keyof DriftItem] === v));
  }

  it("detects missing role in database", async () => {
    // Use a unique name that no other test creates (PG roles are cluster-wide)
    writeSchema(ctx.project.rolesDir, "role_drift_missing.yaml", `role: drift_missing_role\nlogin: false\n`);
    writeSchema(
      ctx.project.tablesDir,
      "users.yaml",
      `table: users\ncolumns:\n  - name: id\n    type: serial\n    primary_key: true\n`,
    );

    const config = resolveConfig({ connectionString: ctx.connectionString, baseDir: ctx.project.baseDir });
    const report = await detectDrift(config);
    const missing = find(report.items, { category: "role", direction: "missing_from_db" });
    expect(missing.length).toBeGreaterThanOrEqual(1);
    expect(missing.some((m) => m.name === "drift_missing_role")).toBe(true);
  });

  it("detects role attribute mismatch", async () => {
    await execSql(ctx.connectionString, `DROP ROLE IF EXISTS drift_role`);
    await execSql(ctx.connectionString, `CREATE ROLE drift_role NOLOGIN`);
    writeSchema(ctx.project.rolesDir, "role_drift.yaml", `role: drift_role\nlogin: true\n`);
    writeSchema(
      ctx.project.tablesDir,
      "users.yaml",
      `table: users\ncolumns:\n  - name: id\n    type: serial\n    primary_key: true\n`,
    );

    const config = resolveConfig({ connectionString: ctx.connectionString, baseDir: ctx.project.baseDir });
    const report = await detectDrift(config);
    const mismatch = find(report.items, { category: "role", direction: "mismatch", name: "drift_role" });
    expect(mismatch).toHaveLength(1);
    expect(mismatch[0].details!.some((d) => d.field === "login")).toBe(true);
  });

  it("detects missing table grant", async () => {
    await execSql(ctx.connectionString, `DROP ROLE IF EXISTS grant_role`);
    await execSql(ctx.connectionString, `CREATE ROLE grant_role NOLOGIN`);
    await execSql(ctx.connectionString, `CREATE TABLE items (id serial PRIMARY KEY)`);
    writeSchema(ctx.project.rolesDir, "role_grant.yaml", `role: grant_role\nlogin: false\n`);
    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
grants:
  - to: grant_role
    privileges: [SELECT]
`,
    );

    const config = resolveConfig({ connectionString: ctx.connectionString, baseDir: ctx.project.baseDir });
    const report = await detectDrift(config);
    const missing = find(report.items, { category: "grant", direction: "missing_from_db" });
    expect(missing.length).toBeGreaterThanOrEqual(1);
  });

  it("no drift after applying roles and grants", async () => {
    writeSchema(ctx.project.rolesDir, "role_ok.yaml", `role: ok_role\nlogin: false\n`);
    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
grants:
  - to: ok_role
    privileges: [SELECT]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    await runMigrate(config);
    await closePool();

    const driftConfig = resolveConfig({ connectionString: ctx.connectionString, baseDir: ctx.project.baseDir });
    const report = await detectDrift(driftConfig);

    const roleItems = report.items.filter((i) => i.category === "role" || i.category === "grant");
    expect(roleItems).toHaveLength(0);
  });
});

describe("grants: via mixins", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("inherits grants from a mixin", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_service.yaml",
      `role: app_service
login: false
`,
    );

    writeMixin(
      ctx.project.mixinsDir,
      "service_access.yaml",
      `mixin: service_access
grants:
  - to: app_service
    privileges: [SELECT, INSERT, UPDATE, DELETE]
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "orders.yaml",
      `table: orders
use:
  - service_access
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const grants = await getTableGrants(ctx.connectionString, "orders", "app_service");
    expect(grants).toContain("SELECT");
    expect(grants).toContain("INSERT");
    expect(grants).toContain("UPDATE");
    expect(grants).toContain("DELETE");
  });
});

// ─── View & Materialized View Grants ─────────────────────────────────────────

describe("grants: view grants", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("grants privileges on a view", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_viewer.yaml",
      `role: view_reader
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    writeSchema(
      ctx.project.viewsDir,
      "active_items.yaml",
      `view: active_items
query: |
  SELECT id, name FROM items
grants:
  - to: view_reader
    privileges: [SELECT]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const grants = await getTableGrants(ctx.connectionString, "active_items", "view_reader");
    expect(grants).toContain("SELECT");
  });

  it("is idempotent — granting view privileges twice succeeds", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_viewer.yaml",
      `role: view_reader
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    writeSchema(
      ctx.project.viewsDir,
      "active_items.yaml",
      `view: active_items
query: |
  SELECT id, name FROM items
grants:
  - to: view_reader
    privileges: [SELECT]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result1 = await runMigrate(config);
    expect(result1.success).toBe(true);

    const result2 = await runMigrate(config);
    expect(result2.success).toBe(true);

    const grants = await getTableGrants(ctx.connectionString, "active_items", "view_reader");
    expect(grants).toContain("SELECT");
  });

  it("revokes removed view privileges with --allow-destructive", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_viewer.yaml",
      `role: view_reader
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    // Initial: grant SELECT and INSERT
    writeSchema(
      ctx.project.viewsDir,
      "active_items.yaml",
      `view: active_items
query: |
  SELECT id, name FROM items
grants:
  - to: view_reader
    privileges: [SELECT, INSERT]
`,
    );

    const config1 = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result1 = await runMigrate(config1);
    expect(result1.success).toBe(true);

    // Updated: remove INSERT
    writeSchema(
      ctx.project.viewsDir,
      "active_items.yaml",
      `view: active_items
query: |
  SELECT id, name FROM items
grants:
  - to: view_reader
    privileges: [SELECT]
`,
    );

    const config2 = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
      allowDestructive: true,
    });
    const result2 = await runMigrate(config2);
    expect(result2.success).toBe(true);

    const grants = await getTableGrants(ctx.connectionString, "active_items", "view_reader");
    expect(grants).toContain("SELECT");
    expect(grants).not.toContain("INSERT");
  });
});

describe("grants: materialized view grants", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("grants privileges on a materialized view", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_mv_reader.yaml",
      `role: mv_reader
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "events.yaml",
      `table: events
columns:
  - name: id
    type: serial
    primary_key: true
  - name: kind
    type: text
`,
    );

    writeSchema(
      ctx.project.viewsDir,
      "mv_event_counts.yaml",
      `materialized_view: event_counts
query: |
  SELECT kind, count(*) AS total FROM events GROUP BY kind
grants:
  - to: mv_reader
    privileges: [SELECT]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const grants = await getTableGrants(ctx.connectionString, "event_counts", "mv_reader");
    expect(grants).toContain("SELECT");
  });
});

describe("grants: view grant drift detection", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects missing view grant", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_viewer.yaml",
      `role: view_reader
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    // Create the view without grants first
    writeSchema(
      ctx.project.viewsDir,
      "active_items.yaml",
      `view: active_items
query: |
  SELECT id, name FROM items
`,
    );

    const config1 = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config1);
    expect(result.success).toBe(true);

    // Now add grants to the YAML (but don't migrate)
    writeSchema(
      ctx.project.viewsDir,
      "active_items.yaml",
      `view: active_items
query: |
  SELECT id, name FROM items
grants:
  - to: view_reader
    privileges: [SELECT]
`,
    );

    const driftConfig = resolveConfig({ connectionString: ctx.connectionString, baseDir: ctx.project.baseDir });
    const report = await detectDrift(driftConfig);

    const grantItems = report.items.filter((i) => i.category === "grant");
    expect(grantItems).toHaveLength(1);
    expect(grantItems[0].name).toBe("SELECT → view_reader");
  });

  it("no drift after applying view grants", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_viewer.yaml",
      `role: view_reader
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    writeSchema(
      ctx.project.viewsDir,
      "active_items.yaml",
      `view: active_items
query: |
  SELECT id, name FROM items
grants:
  - to: view_reader
    privileges: [SELECT]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const driftConfig = resolveConfig({ connectionString: ctx.connectionString, baseDir: ctx.project.baseDir });
    const report = await detectDrift(driftConfig);

    const grantItems = report.items.filter((i) => i.category === "grant");
    expect(grantItems).toHaveLength(0);
  });
});

// ─── Auto Sequence Grants ────────────────────────────────────────────────────

async function getSequenceGrants(connectionString: string, seqName: string, roleName: string): Promise<string[]> {
  const res = await execSql(
    connectionString,
    `SELECT p.privilege_type
     FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
     CROSS JOIN LATERAL aclexplode(c.relacl) AS p(grantor, grantee, privilege_type, is_grantable)
     JOIN pg_roles r ON r.oid = p.grantee
     WHERE n.nspname = 'public' AND c.relname = $1 AND r.rolname = $2
     ORDER BY p.privilege_type`,
    [seqName, roleName],
  );
  return res.rows.map((r: { privilege_type: string }) => r.privilege_type);
}

describe("grants: auto sequence grants", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("auto-grants USAGE and SELECT on owned sequences when INSERT is granted", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_writer.yaml",
      `role: seq_writer
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
grants:
  - to: seq_writer
    privileges: [SELECT, INSERT]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const seqGrants = await getSequenceGrants(ctx.connectionString, "items_id_seq", "seq_writer");
    expect(seqGrants).toContain("USAGE");
    expect(seqGrants).toContain("SELECT");
  });

  it("does not grant sequence privileges when only SELECT is granted (no INSERT)", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_reader.yaml",
      `role: seq_reader
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
grants:
  - to: seq_reader
    privileges: [SELECT]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const seqGrants = await getSequenceGrants(ctx.connectionString, "items_id_seq", "seq_reader");
    expect(seqGrants).not.toContain("USAGE");
  });

  it("auto-grants sequence privileges for ALL privilege", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_admin.yaml",
      `role: seq_admin
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
grants:
  - to: seq_admin
    privileges: [ALL]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const seqGrants = await getSequenceGrants(ctx.connectionString, "items_id_seq", "seq_admin");
    expect(seqGrants).toContain("USAGE");
    expect(seqGrants).toContain("SELECT");
  });

  it("handles tables with multiple serial columns", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_multi.yaml",
      `role: seq_multi
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "multi_seq.yaml",
      `table: multi_seq
columns:
  - name: id
    type: serial
    primary_key: true
  - name: code
    type: serial
  - name: name
    type: text
grants:
  - to: seq_multi
    privileges: [INSERT]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const idGrants = await getSequenceGrants(ctx.connectionString, "multi_seq_id_seq", "seq_multi");
    expect(idGrants).toContain("USAGE");

    const codeGrants = await getSequenceGrants(ctx.connectionString, "multi_seq_code_seq", "seq_multi");
    expect(codeGrants).toContain("USAGE");
  });

  it("is idempotent — running twice succeeds", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_idem.yaml",
      `role: seq_idem
login: false
`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
grants:
  - to: seq_idem
    privileges: [SELECT, INSERT]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    const result1 = await runMigrate(config);
    expect(result1.success).toBe(true);

    const result2 = await runMigrate(config);
    expect(result2.success).toBe(true);

    const seqGrants = await getSequenceGrants(ctx.connectionString, "items_id_seq", "seq_idem");
    expect(seqGrants).toContain("USAGE");
    expect(seqGrants).toContain("SELECT");
  });

  it("revokes sequence privileges when INSERT is removed with --allow-destructive", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "role_revoke.yaml",
      `role: seq_revoke
login: false
`,
    );

    // Initial: grant INSERT (triggers auto sequence grant)
    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
grants:
  - to: seq_revoke
    privileges: [SELECT, INSERT]
`,
    );

    const config1 = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result1 = await runMigrate(config1);
    expect(result1.success).toBe(true);

    const seqBefore = await getSequenceGrants(ctx.connectionString, "items_id_seq", "seq_revoke");
    expect(seqBefore).toContain("USAGE");

    // Updated: remove INSERT, keep only SELECT
    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
grants:
  - to: seq_revoke
    privileges: [SELECT]
`,
    );

    const config2 = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
      allowDestructive: true,
    });
    const result2 = await runMigrate(config2);
    expect(result2.success).toBe(true);

    const seqAfter = await getSequenceGrants(ctx.connectionString, "items_id_seq", "seq_revoke");
    expect(seqAfter).not.toContain("USAGE");
    expect(seqAfter).not.toContain("SELECT");
  });
});

// ─── Helper: check function grants ────────────────────────────────────────────

async function getFunctionGrants(connectionString: string, fnName: string, roleName: string): Promise<string[]> {
  const res = await execSql(
    connectionString,
    `SELECT p.privilege_type
     FROM pg_proc proc
     JOIN pg_namespace n ON n.oid = proc.pronamespace
     CROSS JOIN LATERAL aclexplode(proc.proacl) AS p(grantor, grantee, privilege_type, is_grantable)
     JOIN pg_roles r ON r.oid = p.grantee
     WHERE n.nspname = 'public' AND proc.proname = $1 AND r.rolname = $2
     ORDER BY p.privilege_type`,
    [fnName, roleName],
  );
  return res.rows.map((r: { privilege_type: string }) => r.privilege_type);
}

// ─── Function grant tests ─────────────────────────────────────────────────────

describe("grants: function EXECUTE grants", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("grants EXECUTE on a function to a role", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "fn_reader.yaml",
      `role: fn_reader
login: false
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "hello.yaml",
      `name: hello
language: sql
returns: text
body: "SELECT 'hello'::text;"
replace: true
grants:
  - to: fn_reader
    privileges: [EXECUTE]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const grants = await getFunctionGrants(ctx.connectionString, "hello", "fn_reader");
    expect(grants).toContain("EXECUTE");
  });

  it("revokes EXECUTE when grant is removed from YAML", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "fn_revoker.yaml",
      `role: fn_revoker
login: false
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "greet.yaml",
      `name: greet
language: sql
returns: text
body: "SELECT 'hi'::text;"
replace: true
grants:
  - to: fn_revoker
    privileges: [EXECUTE]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const grantsBefore = await getFunctionGrants(ctx.connectionString, "greet", "fn_revoker");
    expect(grantsBefore).toContain("EXECUTE");

    // Remove grants from YAML
    writeSchema(
      ctx.project.functionsDir,
      "greet.yaml",
      `name: greet
language: sql
returns: text
body: "SELECT 'hi'::text;"
replace: true
`,
    );

    const config2 = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
      allowDestructive: true,
    });
    const result2 = await runMigrate(config2);
    expect(result2.success).toBe(true);

    const grantsAfter = await getFunctionGrants(ctx.connectionString, "greet", "fn_revoker");
    expect(grantsAfter).not.toContain("EXECUTE");
  });

  it("is idempotent — re-running does not produce errors", async () => {
    writeSchema(
      ctx.project.rolesDir,
      "fn_idem.yaml",
      `role: fn_idem
login: false
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "idem_fn.yaml",
      `name: idem_fn
language: sql
returns: text
body: "SELECT 'ok'::text;"
replace: true
grants:
  - to: fn_idem
    privileges: [EXECUTE]
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result1 = await runMigrate(config);
    expect(result1.success).toBe(true);

    // Run again — should be a no-op
    const result2 = await runMigrate(config);
    expect(result2.success).toBe(true);
  });
});
