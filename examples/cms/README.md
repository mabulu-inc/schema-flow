# Content Management System

A multi-role CMS that demonstrates every schema-flow feature.

## Schema overview

```
users
  id, email, display_name, bio, avatar_url, is_active
  + timestamps mixin

categories  (hierarchical via parent_id)
  id, name, slug, description, parent_id FK(self), position
  + timestamps, soft_delete mixins

articles
  id, title, slug, excerpt, body, author_id FK, category_id FK,
  status (enum), published_at, featured_image_url, metadata (jsonb), word_count
  + timestamps, soft_delete mixins

article_revisions  (append-only history)
  id, article_id FK, editor_id FK, title, body, revision_note, created_at

tags
  id, name, slug

article_tags  (join table, composite PK)
  article_id FK, tag_id FK

media
  id, uploader_id FK, file_name, file_path, mime_type, file_size,
  media_type (enum), alt_text, access_token (uuid)
  + timestamps, soft_delete mixins

comments  (threaded via parent_id)
  id, article_id FK, author_name, author_email, body, parent_id FK(self),
  is_approved
  + timestamps, soft_delete mixins
```

## Features demonstrated

| Feature                  | Where                                         |
| ------------------------ | --------------------------------------------- |
| Extensions               | `extensions.yaml` — pgcrypto, pg_trgm         |
| Enums                    | `enums/article_status.yaml`, `media_type.yaml` |
| Roles                    | `roles/cms_author.yaml`, `cms_editor`, `cms_admin` |
| Mixins                   | `mixins/timestamps.yaml`, `soft_delete.yaml`  |
| Trigger function         | `functions/update_timestamp.yaml`             |
| SECURITY DEFINER fn      | `functions/get_current_user_id.yaml`          |
| Foreign keys             | CASCADE, SET NULL across all tables           |
| Self-referencing FK      | `categories.parent_id`, `comments.parent_id`  |
| Composite primary key    | `article_tags`                                |
| GIN index (jsonb)        | `articles.metadata`                           |
| GIN index (trigram)      | `articles.title`, `users.display_name`        |
| Partial indexes          | Active records, pending comments, etc.        |
| Check constraints        | Slug format, email format, file size limit    |
| RLS (multi-role)         | Per-table policies for author/editor/admin    |
| Permissive policies      | Role-specific SELECT/INSERT/UPDATE/DELETE      |
| Table grants             | Inline in each table YAML                     |
| View grants              | Inline in `views/published_articles.yaml`     |
| Materialized view grants | Inline in `views/article_stats.yaml`          |
| Auto sequence grants     | Automatic when INSERT is granted on a table   |
| View                     | `views/published_articles.yaml`               |
| Materialized view        | `views/article_stats.yaml` with indexes       |
| Post-migration script    | `post/` — seed default categories             |

## Roles

| Role         | Can do                                                  |
| ------------ | ------------------------------------------------------- |
| `cms_author` | Create/edit own articles, upload media, view own data   |
| `cms_editor` | Read/edit all articles, manage categories/tags/comments |
| `cms_admin`  | Full access to everything                               |

## Usage

```bash
# Preview the migration plan
npx schema-flow plan --dir examples/cms/schema

# Apply to a database
npx schema-flow run --dir examples/cms/schema --db postgresql://localhost/cms
```
