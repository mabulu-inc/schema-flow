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
    `SELECT privilege_type
     FROM information_schema.role_table_grants
     WHERE table_schema = 'public' AND table_name = $1 AND grantee = $2
     ORDER BY privilege_type`,
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
      ctx.project.schemaDir,
      "role_app_reader.yaml",
      `role: app_reader
login: false
`,
    );

    // Need at least one table file for the migration to process schema files
    writeSchema(
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
      "role_base.yaml",
      `role: base_role
login: false
`,
    );

    writeSchema(
      ctx.project.schemaDir,
      "role_child.yaml",
      `role: child_role
login: false
in:
  - base_role
`,
    );

    writeSchema(
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
      "role_app_reader.yaml",
      `role: app_reader
login: false
`,
    );

    writeSchema(
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
      "role_app.yaml",
      `role: app_role
login: false
createdb: false
`,
    );

    writeSchema(
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
      "role_reader.yaml",
      `role: reader_role
login: false
`,
    );

    writeSchema(
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
      "role_writer.yaml",
      `role: writer_role
login: false
`,
    );

    writeSchema(
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
      "role_reader.yaml",
      `role: reader_role
login: false
`,
    );

    writeSchema(
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
      "role_writer.yaml",
      `role: writer_role
login: false
`,
    );

    writeSchema(
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
      "role_auditor.yaml",
      `role: auditor_role
login: false
`,
    );

    writeSchema(
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
      "role_user.yaml",
      `role: app_user
login: false
`,
    );

    writeSchema(
      ctx.project.schemaDir,
      "role_auditor.yaml",
      `role: auditor_role
login: false
`,
    );

    writeSchema(
      ctx.project.schemaDir,
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
    writeSchema(ctx.project.schemaDir, "role_drift_missing.yaml", `role: drift_missing_role\nlogin: false\n`);
    writeSchema(
      ctx.project.schemaDir,
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
    writeSchema(ctx.project.schemaDir, "role_drift.yaml", `role: drift_role\nlogin: true\n`);
    writeSchema(
      ctx.project.schemaDir,
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
    writeSchema(ctx.project.schemaDir, "role_grant.yaml", `role: grant_role\nlogin: false\n`);
    writeSchema(
      ctx.project.schemaDir,
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
    writeSchema(ctx.project.schemaDir, "role_ok.yaml", `role: ok_role\nlogin: false\n`);
    writeSchema(
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
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
      ctx.project.schemaDir,
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
