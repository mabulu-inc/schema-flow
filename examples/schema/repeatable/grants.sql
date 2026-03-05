-- Repeatable: grants
-- Column-level access control for the orders table.
-- Re-applied whenever this file changes.

-- Users: full column access (RLS restricts rows)
GRANT SELECT, INSERT, UPDATE ON orders TO app_user;

-- Managers: full column access
GRANT SELECT, UPDATE ON orders TO app_manager;

-- Auditors: read-only, payment details hidden
GRANT SELECT (id, tenant_id, user_id, amount, status, created_at) ON orders TO app_auditor;

-- Auditors: masked payment details via view
GRANT SELECT ON orders_audit TO app_auditor;

-- Service: full access
GRANT ALL ON orders TO app_service;
