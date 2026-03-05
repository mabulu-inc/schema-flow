// Integration test: multi-tenant-orders
// Verifies application behavior — tenant isolation, membership flow, security
// functions, generated columns, column-level grants, and cross-tenant admin.
//
// Standalone usage (outside this repo):
//   import { resolveConfig, runAll, closePool, ... } from "@mabulu-inc/schema-flow";
//   import { createTestDb, execSql, withConnection } from "@mabulu-inc/schema-flow/testing";

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import path from "node:path";
import { resolveConfig } from "../../src/core/config.js";
import { runAll } from "../../src/executor/index.js";
import { closePool } from "../../src/core/db.js";
import { logger, LogLevel } from "../../src/core/logger.js";
import { createTestDb, execSql, withConnection } from "../../src/testing/index.js";

logger.setLevel(LogLevel.SILENT);

describe("example: multi-tenant-orders", () => {
  let db: string;
  let cleanup: () => Promise<void>;

  let tenantAcme: string;
  let tenantGlobex: string;
  let userSam: number;
  let userJordan: number;

  beforeAll(async () => {
    const testDb = await createTestDb();
    db = testDb.connectionString;
    cleanup = testDb.cleanup;

    const config = resolveConfig({
      connectionString: db,
      baseDir: path.resolve(__dirname),
      dryRun: false,
    });
    const results = await runAll(config);
    for (const r of results) expect(r.success).toBe(true);

    // Seed tenants
    const acme = await execSql(db, `INSERT INTO tenants (name, slug) VALUES ('Acme Corp', 'acme') RETURNING id`);
    tenantAcme = acme.rows[0].id;

    const globex = await execSql(db, `INSERT INTO tenants (name, slug) VALUES ('Globex Inc', 'globex') RETURNING id`);
    tenantGlobex = globex.rows[0].id;

    // Register users via security function
    const sam = await execSql(db, `SELECT register_account('sam@example.com', 'Sam')`);
    userSam = sam.rows[0].register_account;

    const jordan = await execSql(db, `SELECT register_account('jordan@example.com', 'Jordan')`);
    userJordan = jordan.rows[0].register_account;

    // Create memberships via security function
    await execSql(db, `SELECT create_membership($1, $2, ARRAY['user', 'manager', 'admin'])`, [userSam, tenantAcme]);
    await execSql(db, `SELECT create_membership($1, $2, ARRAY['user'])`, [userJordan, tenantAcme]);
    await execSql(db, `SELECT create_membership($1, $2, ARRAY['user', 'auditor'])`, [userSam, tenantGlobex]);
  });

  afterAll(async () => {
    await closePool();
    await cleanup();
  });

  // --- Registration & membership ---

  describe("registration and membership", () => {
    it("registers a user account", async () => {
      const res = await execSql(db, `SELECT email, display_name FROM users WHERE id = $1`, [userSam]);
      expect(res.rows[0].email).toBe("sam@example.com");
      expect(res.rows[0].display_name).toBe("Sam");
    });

    it("creates membership with modular roles", async () => {
      const res = await execSql(
        db,
        `SELECT mr.role::text FROM membership_roles mr
         JOIN memberships m ON m.id = mr.membership_id
         WHERE m.user_id = $1 AND m.tenant_id = $2
         ORDER BY mr.role`,
        [userSam, tenantAcme],
      );
      const roles = res.rows.map((r: { role: string }) => r.role);
      expect(roles).toEqual(["user", "manager", "admin"]);
    });

    it("supports membership in multiple tenants", async () => {
      const res = await execSql(db, `SELECT count(*)::int AS cnt FROM memberships WHERE user_id = $1`, [userSam]);
      expect(res.rows[0].cnt).toBe(2);
    });
  });

  // --- Session setup ---

  describe("begin_session", () => {
    it("returns roles and recommended PG role", async () => {
      const res = await execSql(db, `SELECT * FROM begin_session($1, $2)`, [userSam, tenantAcme]);
      expect(res.rows[0].roles).toContain("manager");
      expect(res.rows[0].roles).toContain("admin");
      expect(res.rows[0].pg_role).toBe("app_manager");
    });

    it("picks auditor when user has [user, auditor]", async () => {
      const res = await execSql(db, `SELECT * FROM begin_session($1, $2)`, [userSam, tenantGlobex]);
      expect(res.rows[0].pg_role).toBe("app_auditor");
    });

    it("rejects non-members", async () => {
      await expect(execSql(db, `SELECT * FROM begin_session($1, $2)`, [userJordan, tenantGlobex])).rejects.toThrow(
        /No membership/,
      );
    });
  });

  // --- Tenant admin manages roles ---

  describe("tenant admin", () => {
    it("can grant a role to a member", async () => {
      await withConnection(db, async (query) => {
        await query(`SELECT * FROM begin_session($1, $2)`, [userSam, tenantAcme]);
        await query(`SELECT grant_role($1, $2, 'auditor')`, [userJordan, tenantAcme]);
      });

      const res = await execSql(
        db,
        `SELECT mr.role::text FROM membership_roles mr
         JOIN memberships m ON m.id = mr.membership_id
         WHERE m.user_id = $1 AND m.tenant_id = $2 ORDER BY mr.role`,
        [userJordan, tenantAcme],
      );
      expect(res.rows.map((r: { role: string }) => r.role)).toContain("auditor");
    });

    it("grant is idempotent", async () => {
      await withConnection(db, async (query) => {
        await query(`SELECT * FROM begin_session($1, $2)`, [userSam, tenantAcme]);
        await query(`SELECT grant_role($1, $2, 'auditor')`, [userJordan, tenantAcme]);
      });
    });

    it("can revoke a role", async () => {
      await withConnection(db, async (query) => {
        await query(`SELECT * FROM begin_session($1, $2)`, [userSam, tenantAcme]);
        await query(`SELECT revoke_role($1, $2, 'auditor')`, [userJordan, tenantAcme]);
      });

      const res = await execSql(
        db,
        `SELECT mr.role::text FROM membership_roles mr
         JOIN memberships m ON m.id = mr.membership_id
         WHERE m.user_id = $1 AND m.tenant_id = $2`,
        [userJordan, tenantAcme],
      );
      expect(res.rows.map((r: { role: string }) => r.role)).not.toContain("auditor");
    });

    it("cannot revoke the last role", async () => {
      await withConnection(db, async (query) => {
        await query(`SELECT * FROM begin_session($1, $2)`, [userSam, tenantAcme]);
        await expect(query(`SELECT revoke_role($1, $2, 'user')`, [userJordan, tenantAcme])).rejects.toThrow(
          /last role/,
        );
      });
    });

    it("non-admin cannot manage roles", async () => {
      await withConnection(db, async (query) => {
        await query(`SELECT * FROM begin_session($1, $2)`, [userJordan, tenantAcme]);
        await expect(query(`SELECT grant_role($1, $2, 'manager')`, [userJordan, tenantAcme])).rejects.toThrow(
          /tenant admin/,
        );
      });
    });
  });

  // --- Orders ---

  describe("orders", () => {
    beforeAll(async () => {
      await execSql(
        db,
        `INSERT INTO orders (user_id, tenant_id, amount, currency, status, payment_method)
         VALUES ($1, $2, 4999, 'USD', 'draft', '4242424242424242')`,
        [userSam, tenantAcme],
      );
      await execSql(
        db,
        `INSERT INTO orders (user_id, tenant_id, amount, currency, status, payment_method)
         VALUES ($1, $2, 2500, 'EUR', 'paid', '5555555555554444')`,
        [userJordan, tenantAcme],
      );
    });

    it("generates masked payment column automatically", async () => {
      const res = await execSql(db, `SELECT payment_method, payment_masked FROM orders WHERE user_id = $1`, [userSam]);
      expect(res.rows[0].payment_method).toBe("4242424242424242");
      expect(res.rows[0].payment_masked).toBe("****4242");
    });

    it("rejects negative amounts", async () => {
      await expect(
        execSql(db, `INSERT INTO orders (user_id, tenant_id, amount, currency) VALUES ($1, $2, -100, 'USD')`, [
          userSam,
          tenantAcme,
        ]),
      ).rejects.toThrow();
    });

    it("rejects invalid currency codes", async () => {
      await expect(
        execSql(db, `INSERT INTO orders (user_id, tenant_id, amount, currency) VALUES ($1, $2, 100, 'dollars')`, [
          userSam,
          tenantAcme,
        ]),
      ).rejects.toThrow();
    });
  });

  // --- Order items ---

  describe("order items", () => {
    it("computes line_total as quantity * unit_price", async () => {
      const order = await execSql(db, `SELECT id FROM orders WHERE user_id = $1 LIMIT 1`, [userSam]);

      const res = await execSql(
        db,
        `INSERT INTO order_items (order_id, tenant_id, product_name, quantity, unit_price)
         VALUES ($1, $2, 'Widget', 3, 1500) RETURNING line_total`,
        [order.rows[0].id, tenantAcme],
      );
      expect(res.rows[0].line_total).toBe(4500);
    });

    it("rejects zero quantity", async () => {
      const order = await execSql(db, `SELECT id FROM orders WHERE user_id = $1 LIMIT 1`, [userSam]);
      await expect(
        execSql(
          db,
          `INSERT INTO order_items (order_id, tenant_id, product_name, quantity, unit_price)
           VALUES ($1, $2, 'Bad', 0, 100)`,
          [order.rows[0].id, tenantAcme],
        ),
      ).rejects.toThrow();
    });
  });

  // --- Tenant isolation ---

  describe("tenant isolation", () => {
    beforeAll(async () => {
      await execSql(
        db,
        `INSERT INTO orders (user_id, tenant_id, amount, currency, status)
         VALUES ($1, $2, 7777, 'USD', 'draft')`,
        [userSam, tenantGlobex],
      );
    });

    it("manager only sees orders in their tenant", async () => {
      await withConnection(db, async (query) => {
        await query(`SELECT * FROM begin_session($1, $2)`, [userSam, tenantAcme]);
        await query(`SET ROLE app_manager`);

        const res = await query(`SELECT tenant_id FROM orders`);
        for (const row of res.rows) {
          expect(row.tenant_id).toBe(tenantAcme);
        }
      });
    });

    it("user only sees their own orders", async () => {
      await withConnection(db, async (query) => {
        await query(`SELECT * FROM begin_session($1, $2)`, [userJordan, tenantAcme]);
        await query(`SET ROLE app_user`);

        const res = await query(`SELECT user_id FROM orders`);
        for (const row of res.rows) {
          expect(row.user_id).toBe(userJordan);
        }
      });
    });

    it("admin sees orders across all tenants", async () => {
      await withConnection(db, async (query) => {
        await query(`SET ROLE app_admin`);

        const res = await query(`SELECT DISTINCT tenant_id FROM orders`);
        const tenantIds = res.rows.map((r: { tenant_id: string }) => r.tenant_id);
        expect(tenantIds).toContain(tenantAcme);
        expect(tenantIds).toContain(tenantGlobex);
      });
    });
  });

  // --- Column-level grants ---

  describe("column-level security", () => {
    it("auditor cannot read payment_method", async () => {
      await withConnection(db, async (query) => {
        await query(`SELECT * FROM begin_session($1, $2)`, [userSam, tenantGlobex]);
        await query(`SET ROLE app_auditor`);

        await expect(query(`SELECT payment_method FROM orders`)).rejects.toThrow(/permission denied/);
      });
    });

    it("auditor can read payment_masked", async () => {
      await withConnection(db, async (query) => {
        await query(`SELECT * FROM begin_session($1, $2)`, [userSam, tenantGlobex]);
        await query(`SET ROLE app_auditor`);

        const res = await query(`SELECT payment_masked FROM orders`);
        expect(res.rows).toBeDefined();
      });
    });
  });

  // --- Audit trail ---

  describe("audit trail", () => {
    it("tracks created_by from session", async () => {
      await withConnection(db, async (query) => {
        await query(`SELECT set_config('app.user_id', $1::text, false)`, [userSam]);

        const res = await query(
          `INSERT INTO orders (user_id, tenant_id, amount, currency)
           VALUES ($1, $2, 100, 'USD') RETURNING created_by, updated_by`,
          [userSam, tenantAcme],
        );
        expect(res.rows[0].created_by).toBe(userSam);
        expect(res.rows[0].updated_by).toBe(userSam);
      });
    });
  });

  // --- Idempotent ---

  it("second run is a no-op", async () => {
    await closePool();
    const config = resolveConfig({
      connectionString: db,
      baseDir: path.resolve(__dirname),
      dryRun: false,
    });
    const results = await runAll(config);
    for (const r of results) expect(r.success).toBe(true);
    const totalOps = results.reduce((sum, r) => sum + r.operationsExecuted, 0);
    expect(totalOps).toBe(0);
  });
});
