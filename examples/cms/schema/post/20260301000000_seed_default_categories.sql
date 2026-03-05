-- Post-migration script: seed_default_categories
-- Inserts starter categories so the CMS is usable immediately.

BEGIN;

INSERT INTO "public"."categories" (name, slug, position) VALUES
  ('General',     'general',     0),
  ('News',        'news',        1),
  ('Tutorials',   'tutorials',   2),
  ('Engineering', 'engineering', 3)
ON CONFLICT (slug) DO NOTHING;

COMMIT;
