# Todo App

A single-user todo list — the simplest starting point for schema-flow.

## What this example covers

| Feature              | Where                              |
| -------------------- | ---------------------------------- |
| Tables               | `schema/tables/todos.yaml`, `categories.yaml` |
| Enum type            | `schema/enums/priority.yaml`       |
| Foreign key          | `todos.category_id -> categories`  |
| Indexes              | Partial indexes on active todos    |
| Check constraints    | Non-empty title, valid hex color   |
| Mixin (timestamps)   | `schema/mixins/timestamps.yaml`    |
| Trigger function     | `schema/functions/update_timestamp.yaml` |
| Defaults & nullable  | Throughout                         |

## Schema at a glance

```
categories
  id          serial PK
  name        varchar(100) UNIQUE
  color       varchar(7) default '#6B7280'
  position    integer
  created_at  timestamptz  ← timestamps mixin
  updated_at  timestamptz  ← timestamps mixin

todos
  id            serial PK
  title         varchar(255)
  description   text (nullable)
  category_id   integer FK → categories (nullable, ON DELETE SET NULL)
  priority      priority enum (low | medium | high | urgent)
  due_date      date (nullable)
  completed_at  timestamptz (nullable)
  position      integer
  created_at    timestamptz  ← timestamps mixin
  updated_at    timestamptz  ← timestamps mixin
```

## Usage

```bash
# Preview the migration plan
npx schema-flow plan --dir examples/todo-app/schema

# Apply to a database
npx schema-flow run --dir examples/todo-app/schema --db postgresql://localhost/todo
```
