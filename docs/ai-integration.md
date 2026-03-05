# AI Integration

schema-flow is designed to be discoverable by AI coding assistants. Here's how to set up your project so tools like Claude, Copilot, and Cursor understand your schema-flow setup.

## CLAUDE.md Template

Copy this into your project root as `CLAUDE.md`:

```markdown
# CLAUDE.md — schema-flow project instructions

## Database Schema Management

This project uses **schema-flow** for declarative PostgreSQL schema management.

- Schema definitions: `schema/tables/*.yaml`, `schema/enums/*.yaml`,
  `schema/functions/*.yaml`, `schema/views/*.yaml`, `schema/roles/*.yaml`
- Pre-migration scripts: `schema/pre/*.sql`
- Post-migration scripts: `schema/post/*.sql`
- Reusable mixins: `schema/mixins/*.yaml`
- Extensions: `schema/extensions.yaml`

## Key Commands

npx @mabulu-inc/schema-flow plan          # preview what SQL would run
npx @mabulu-inc/schema-flow run           # apply pre → migrate → post
npx @mabulu-inc/schema-flow drift         # compare YAML vs live DB
npx @mabulu-inc/schema-flow validate      # dry-run in a rolled-back transaction
npx @mabulu-inc/schema-flow generate      # generate YAML from existing DB

## Rules for Schema Changes

- **All structural changes go through YAML files.** Do not write raw
  ALTER TABLE statements.
- **Use pre/post scripts for data migrations.** Scaffold with
  `npx @mabulu-inc/schema-flow new pre <name>`.
- **Columns are NOT NULL by default.** Set `nullable: true` explicitly.
- **Destructive operations require `--allow-destructive`.**
- **Use mixins for shared patterns** (timestamps, soft_delete, tenant_id).
- **Run `plan` before `run`.** Always preview changes before applying.
```

## llms.txt

The repository includes an [`llms.txt`](https://github.com/mabulu-inc/schema-flow/blob/main/llms.txt) file following the [llmstxt.org](https://llmstxt.org/) standard. This provides AI tools with a structured summary of the project — commands, YAML format, directory structure, and links to detailed docs.

## Tips for AI-Assisted Schema Development

When working with an AI assistant on schema-flow projects:

1. **Point it at the YAML reference.** Link to or paste from [yaml-reference.md](./yaml-reference) so the AI knows every available key.

2. **Let it read existing YAML files.** The AI can understand your conventions (naming, mixins, RLS patterns) from existing files and generate consistent new ones.

3. **Use `plan` output as feedback.** If the AI generates YAML that produces unexpected SQL, share the `plan` output and it can adjust.

4. **Ask it to generate YAML, not SQL.** The AI should define the desired state in YAML. schema-flow handles the ALTER statements.
