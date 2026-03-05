// Integration test: cms
// Verifies application behavior — content lifecycle, role-based access, soft delete.
//
// Standalone usage (outside this repo):
//   import { resolveConfig, runAll, closePool, ... } from "@mabulu-inc/schema-flow";
//   import { createTestDb, execSql } from "@mabulu-inc/schema-flow/testing";

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import path from "node:path";
import { resolveConfig } from "../../src/core/config.js";
import { runAll } from "../../src/executor/index.js";
import { closePool } from "../../src/core/db.js";
import { logger, LogLevel } from "../../src/core/logger.js";
import { createTestDb, execSql, withConnection } from "../../src/testing/index.js";

logger.setLevel(LogLevel.SILENT);

describe("example: cms", () => {
  let db: string;
  let cleanup: () => Promise<void>;
  let authorId: number;
  let editorId: number;
  let categoryId: number;

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

    // Seed test users
    const author = await execSql(
      db,
      `INSERT INTO users (email, display_name, role) VALUES ('alice@example.com', 'Alice', 'author') RETURNING id`,
    );
    authorId = author.rows[0].id;

    const editor = await execSql(
      db,
      `INSERT INTO users (email, display_name, role) VALUES ('bob@example.com', 'Bob', 'editor') RETURNING id`,
    );
    editorId = editor.rows[0].id;

    // Use a seeded category
    const cat = await execSql(db, `SELECT id FROM categories WHERE slug = 'tutorials'`);
    categoryId = cat.rows[0].id;
  });

  afterAll(async () => {
    await closePool();
    await cleanup();
  });

  // --- Content lifecycle ---

  it("creates a draft article", async () => {
    const res = await execSql(
      db,
      `INSERT INTO articles (title, slug, author_id, category_id)
       VALUES ('Getting Started', 'getting-started', $1, $2)
       RETURNING status`,
      [authorId, categoryId],
    );
    expect(res.rows[0].status).toBe("draft");
  });

  it("publishes an article with a published_at date", async () => {
    await execSql(
      db,
      `UPDATE articles SET status = 'published', published_at = now()
       WHERE slug = 'getting-started'`,
    );
    const res = await execSql(db, `SELECT status, published_at FROM articles WHERE slug = 'getting-started'`);
    expect(res.rows[0].status).toBe("published");
    expect(res.rows[0].published_at).not.toBeNull();
  });

  it("rejects publishing without a published_at date", async () => {
    await expect(
      execSql(
        db,
        `INSERT INTO articles (title, slug, author_id, status, published_at)
         VALUES ('Bad', 'bad-publish', $1, 'published', NULL)`,
        [authorId],
      ),
    ).rejects.toThrow();
  });

  it("rejects invalid slugs", async () => {
    await expect(
      execSql(db, `INSERT INTO articles (title, slug, author_id) VALUES ('Bad', 'INVALID SLUG!', $1)`, [authorId]),
    ).rejects.toThrow();
  });

  // --- Published articles view ---

  it("published articles appear in the published_articles view", async () => {
    const res = await execSql(
      db,
      `SELECT title, author_name, category_name FROM published_articles WHERE slug = 'getting-started'`,
    );
    expect(res.rows[0].title).toBe("Getting Started");
    expect(res.rows[0].author_name).toBe("Alice");
    expect(res.rows[0].category_name).toBe("Tutorials");
  });

  it("draft articles do not appear in the published_articles view", async () => {
    await execSql(db, `INSERT INTO articles (title, slug, author_id) VALUES ('Unpublished', 'unpublished', $1)`, [
      authorId,
    ]);
    const res = await execSql(db, `SELECT 1 FROM published_articles WHERE slug = 'unpublished'`);
    expect(res.rowCount).toBe(0);
  });

  // --- Tags ---

  it("tags an article", async () => {
    const tag = await execSql(db, `INSERT INTO tags (name, slug) VALUES ('postgres', 'postgres') RETURNING id`);
    const article = await execSql(db, `SELECT id FROM articles WHERE slug = 'getting-started'`);

    await execSql(db, `INSERT INTO article_tags (article_id, tag_id) VALUES ($1, $2)`, [
      article.rows[0].id,
      tag.rows[0].id,
    ]);

    const res = await execSql(
      db,
      `SELECT t.name FROM tags t
       JOIN article_tags at ON at.tag_id = t.id
       JOIN articles a ON a.id = at.article_id
       WHERE a.slug = 'getting-started'`,
    );
    expect(res.rows[0].name).toBe("postgres");
  });

  // --- Comments ---

  it("adds a comment to an article", async () => {
    const article = await execSql(db, `SELECT id FROM articles WHERE slug = 'getting-started'`);

    await execSql(
      db,
      `INSERT INTO comments (article_id, author_name, author_email, body, is_approved)
       VALUES ($1, 'Reader', 'reader@example.com', 'Great article!', true)`,
      [article.rows[0].id],
    );

    const res = await execSql(db, `SELECT body FROM comments WHERE article_id = $1`, [article.rows[0].id]);
    expect(res.rows[0].body).toBe("Great article!");
  });

  // --- Soft delete ---

  it("soft-deleted articles are hidden from the published view", async () => {
    await execSql(
      db,
      `INSERT INTO articles (title, slug, author_id, status, published_at)
       VALUES ('To Delete', 'to-delete', $1, 'published', now())`,
      [authorId],
    );

    // Visible before soft delete
    let view = await execSql(db, `SELECT 1 FROM published_articles WHERE slug = 'to-delete'`);
    expect(view.rowCount).toBe(1);

    // Soft delete
    await execSql(db, `UPDATE articles SET deleted_at = now() WHERE slug = 'to-delete'`);

    // Hidden after soft delete, but row still exists
    view = await execSql(db, `SELECT 1 FROM published_articles WHERE slug = 'to-delete'`);
    expect(view.rowCount).toBe(0);

    const raw = await execSql(db, `SELECT deleted_at FROM articles WHERE slug = 'to-delete'`);
    expect(raw.rows[0].deleted_at).not.toBeNull();
  });

  // --- Audit columns ---

  it("tracks who created and last modified a record", async () => {
    await withConnection(db, async (query) => {
      await query(`SELECT set_config('app.user_id', $1::text, false)`, [authorId]);

      await query(`INSERT INTO articles (title, slug, author_id) VALUES ('Audited', 'audited', $1)`, [authorId]);

      const res = await query(`SELECT created_by, updated_by FROM articles WHERE slug = 'audited'`);
      expect(res.rows[0].created_by).toBe(authorId);
      expect(res.rows[0].updated_by).toBe(authorId);
    });

    // Different user updates the record
    await withConnection(db, async (query) => {
      await query(`SELECT set_config('app.user_id', $1::text, false)`, [editorId]);
      await new Promise((r) => setTimeout(r, 50));
      await query(`UPDATE articles SET title = 'Audited (edited)' WHERE slug = 'audited'`);
    });

    const after = await execSql(db, `SELECT created_by, updated_by FROM articles WHERE slug = 'audited'`);
    expect(after.rows[0].created_by).toBe(authorId);
    expect(after.rows[0].updated_by).toBe(editorId);
  });

  // --- Materialized view ---

  it("article_stats reflects counts after refresh", async () => {
    await execSql(db, `REFRESH MATERIALIZED VIEW article_stats`);

    const res = await execSql(db, `SELECT title, comment_count FROM article_stats WHERE title = 'Getting Started'`);
    expect(res.rows.length).toBe(1);
    expect(Number(res.rows[0].comment_count)).toBeGreaterThanOrEqual(1);
  });

  // --- RLS ---

  it("authors can only see their own articles", async () => {
    // Create an article by the editor
    await execSql(db, `INSERT INTO articles (title, slug, author_id) VALUES ('By Editor', 'by-editor', $1)`, [
      editorId,
    ]);

    // Switch to author role
    await withConnection(db, async (query) => {
      await query(`SELECT set_config('app.user_id', $1::text, false)`, [authorId]);
      await query(`SET ROLE cms_author`);

      const res = await query(`SELECT title FROM articles ORDER BY title`);
      const titles = res.rows.map((r: { title: string }) => r.title);

      // Author should not see editor's article
      expect(titles).not.toContain("By Editor");
    });
  });

  // --- Seed data ---

  it("post-migration seeds default categories", async () => {
    const res = await execSql(db, `SELECT name FROM categories ORDER BY position`);
    const names = res.rows.map((r: { name: string }) => r.name);
    expect(names).toEqual(["General", "News", "Tutorials", "Engineering"]);
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
