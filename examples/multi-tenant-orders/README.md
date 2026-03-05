# Multi-Tenant Order Processing

A multi-tenant order system demonstrating column-level security, modular roles,
membership-based access control, and security functions for API integration.

## The pattern: generated column + column-level grants

The `orders` table stores a raw `payment_method` column and a generated
`payment_masked` column that always contains the masked form (`****4242`).
Column-level grants then control which roles see which columns:

```yaml
# orders.yaml (simplified)
columns:
  - name: payment_method
    type: text
    nullable: true

  - name: payment_masked
    type: text
    generated: "CASE WHEN payment_method IS NOT NULL THEN '****' || right(payment_method, 4) ELSE NULL END"

grants:
  # Full access for operational roles
  - to: [app_user, app_manager, app_service]
    privileges: [SELECT, INSERT, UPDATE]

  # Auditors: every column EXCEPT payment_method
  - to: app_auditor
    privileges: [SELECT]
    columns: [id, tenant_id, user_id, amount, currency, status, payment_masked, notes, created_at, updated_at]
```

### Why this beats a masked view

| Concern             | View approach                        | Generated column approach            |
| ------------------- | ------------------------------------ | ------------------------------------ |
| Objects to manage   | Table + view + separate grants       | Single table, everything inline      |
| RLS                 | May bypass depending on SECURITY     | Always applies — it's the same table |
| Indexes             | View can't use table indexes         | All indexes serve all roles          |
| Consistency         | View query can drift from table      | Generated column is always in sync   |
| Declarative         | Requires a separate view + grants    | Column-level grants are inline       |

## Schema at a glance

```
users (global)
  id, email, display_name
  + timestamps
  RLS: see self + co-members of current tenant

tenants
  id (uuid PK), name, slug, is_active
  + timestamps

memberships
  id, user_id FK(users), status, invited_by FK(users), invited_email
  + tenant_isolation, timestamps

membership_roles
  id, membership_id FK, role (enum), granted_by FK(users)
  + tenant_isolation, timestamps

currencies (global)
  code (PK), name, symbol, decimal_digits

orders
  id, user_id FK, amount, currency FK(currencies), status (enum),
  payment_method (restricted), payment_masked (generated),
  notes
  + tenant_isolation, timestamps

order_items
  id, order_id FK, product_name, quantity, unit_price,
  line_total (generated: quantity * unit_price)
  + tenant_isolation
```

## Modular roles

A user can hold multiple roles within a single tenant. Roles are additive — a
user with `[user, manager]` gets the combined access of both.

```
Sam @ Acme Corp  → [user, manager, admin]  — data + security management
Sam @ Globex Inc → [user, auditor]         — own orders + audit (masked payment)
Jordan @ Acme    → [user]                  — can only place and view own orders
Pat @ Acme       → [user, admin]           — own data + can manage members/roles
```

Roles are stored in `membership_roles` and managed through security functions.
The API never writes to these tables directly.

### Role types

Roles fall into two categories:

**Data access roles** — control what rows and columns you can see:

| Membership role | PG role        | Purpose                              |
| --------------- | -------------- | ------------------------------------ |
| `user`          | `app_user`     | Own data only                        |
| `manager`       | `app_manager`  | All tenant data                      |
| `auditor`       | `app_auditor`  | Read-only, masked payment fields     |

**Capability roles** — control what actions you can perform:

| Membership role | Purpose                                              |
| --------------- | ---------------------------------------------------- |
| `admin`         | Manage members and roles within the tenant           |

**System roles** (not assignable via membership):

| PG role        | Purpose                              |
| -------------- | ------------------------------------ |
| `app_service`  | Backend/API operations               |
| `app_admin`    | Cross-tenant platform access         |

The `admin` membership role is a **capability**, not a data access tier. A user
with `[user, admin]` gets `app_user` data access but can call `grant_role()`,
`revoke_role()`, and `create_membership()`. A user with `[manager, admin]` gets
full data access AND security management.

When a user has multiple data-access roles, `begin_session()` returns the
highest-privilege PG role (manager > auditor > user). The `app.roles` session
variable contains all roles for application-level checks.

## Security functions

All security functions use `SECURITY DEFINER` so they run as the function owner,
bypassing RLS. The API calls these instead of manipulating tables directly.

### Registration flow

```sql
-- 1. User signs up (API running as app_service)
SELECT register_account('sam@example.com', 'Sam Mayfield');
-- Returns: 42 (new user ID)

-- 2. Create membership with initial roles
SELECT create_membership(42, 'tenant-uuid', ARRAY['user']);
-- Returns: 1 (membership ID)

-- 3. Start a session
SELECT * FROM begin_session(42, 'tenant-uuid');
-- Returns: roles = {user}, pg_role = 'app_user'

-- 4. API sets the PG role for this transaction
SET LOCAL ROLE app_user;

-- All subsequent queries use RLS for app_user + session variables
```

### Managing roles (tenant admin)

```sql
-- Tenant admin grants auditor role to an existing member
-- (granted_by is set automatically from app.user_id)
SELECT grant_role(42, 'tenant-uuid', 'auditor');

-- Tenant admin revokes a role (cannot remove the last role)
SELECT revoke_role(42, 'tenant-uuid', 'auditor');

-- Tenant admin invites a new user with [user, admin] roles
SELECT create_membership(99, 'tenant-uuid', ARRAY['user', 'admin']);
```

### Authorization model

The security functions enforce authorization internally:

| Caller context | Allowed? | How it works |
|---|---|---|
| No active session (system) | Yes | `app_service` calls during registration |
| Session with `admin` role | Yes | Tenant admin, same-tenant only |
| Session without `admin` role | **Denied** | Raises exception |
| `app_admin` (platform) | Yes | No session needed |

Tenant admins are scoped — they can only manage members in their own tenant.
Cross-tenant operations require `app_admin` (platform level).

### Function reference

| Function | Args | Returns | Purpose |
|---|---|---|---|
| `register_account` | email, display_name | user ID | Create a new user account |
| `create_membership` | user_id, tenant_id, roles[] | membership ID | Link user to tenant with roles |
| `begin_session` | user_id, tenant_id | roles[], pg_role | Validate + set session variables |
| `grant_role` | user_id, tenant_id, role | void | Add a role (idempotent) |
| `revoke_role` | user_id, tenant_id, role | void | Remove a role (prevents last-role removal) |

## Cross-tenant admin access

`app_admin` can query across all tenants without any session variable. This uses
a standard PostgreSQL pattern: scope the RESTRICTIVE policy by role.

### How it works

1. **The `tenant_isolation` mixin applies a RESTRICTIVE policy with a `to:` clause**
   that lists only the tenant-bound roles:

   ```yaml
   # tenant_isolation.yaml
   policies:
     - name: "{table}_tenant_isolation"
       for: ALL
       permissive: false
       to: [app_user, app_manager, app_auditor, app_service]
       using: "tenant_id = current_setting('app.tenant_id')::uuid"
       check: "tenant_id = current_setting('app.tenant_id')::uuid"
   ```

   Because `app_admin` is **not** in the `to:` list, the RESTRICTIVE policy
   never applies to it. PostgreSQL only evaluates a policy against a role if
   that role is listed in `to:` (or if `to:` is `PUBLIC` / omitted).

2. **Each table adds a PERMISSIVE policy granting `app_admin` full access:**

   ```yaml
   # In orders.yaml, users.yaml, order_items.yaml, etc.
   policies:
     - name: admin_full_access
       for: ALL
       to: [app_admin]
       using: "true"
       check: "true"
   ```

3. **Result:** `app_admin` sees all rows in all tenants. No special session
   variables, no bypass flags — just explicit role scoping that is fully
   auditable in YAML.

### Why this pattern

- **No magic:** the access rules are visible in the YAML. `app_admin` is absent
  from the restrictive policy and present in a permissive one — that's the whole
  mechanism.
- **Additive:** adding a new tenant-bound role means adding it to the mixin's
  `to:` list. Admin access doesn't change.
- **Safe default:** if you forget to add a role to the mixin's `to:` list, that
  role gets zero rows (no permissive policy matches) rather than unrestricted
  access.

## Features demonstrated

| Feature                  | Where                                          |
| ------------------------ | ---------------------------------------------- |
| Global user identities   | `users` table — not tenant-scoped              |
| Membership-based access  | `memberships` + `membership_roles` tables      |
| Modular roles            | Multiple roles per user per tenant             |
| Security functions       | 5 SECURITY DEFINER functions for API use       |
| Tenant admin role        | Tenant-scoped security management              |
| Tenants table            | Source of truth for tenant IDs, FK enforced    |
| Tenant isolation mixin   | RESTRICTIVE policy + FK on every tenant table  |
| Cross-tenant admin       | Role-scoped RESTRICTIVE policy                 |
| Generated columns        | `orders.payment_masked`, `order_items.line_total` |
| Column-level grants      | Auditors see masked payment, not raw           |
| Auto sequence grants     | INSERT roles auto-get USAGE on serial sequences |
| Enum types               | `order_status`, `membership_status`, `membership_role` |
| Check constraints        | Positive amounts, valid currency, invite rules |
| Partial indexes          | Active orders, pending invitations             |
| Foreign keys             | CASCADE deletes                                |
| Force RLS                | Even table owner is subject to tenant policy   |

## Usage

```bash
npx schema-flow plan --dir examples/multi-tenant-orders/schema
npx schema-flow run  --dir examples/multi-tenant-orders/schema --db postgresql://localhost/orders
```
