# M4–M5 Acceptance Checklist

This checklist separates source changes from runtime evidence.

## M4 — Databricks Asset Bundles

- [ ] `databricks bundle validate -t dev` passes with the target workspace.
- [ ] Bundle plan reviewed for unexpected resources.
- [ ] Dev deployment succeeds; capture job URL and deployment commit SHA.
- [ ] Pipeline run succeeds against the intended Unity Catalog catalog.
- [ ] Validation job uses source and target paths that exist in the deployed runtime.
- [ ] Production target uses an explicitly configured service principal and separate secrets.

## M5 — Snowflake

- [ ] Connector version is compatible with the selected Databricks Runtime.
- [ ] SQL scripts applied in dependency order and reviewed for account-specific grants.
- [ ] Gold tables loaded through a staging-and-MERGE strategy for retry safety.
- [ ] Re-run of the same batch does not duplicate business keys.
- [ ] Delta and Snowflake row counts, distinct keys, monetary aggregates, null profiles,
      and deterministic sampled records reconcile.
- [ ] Negative test modifies a target value and validation exits non-zero.
- [ ] Analyst-facing secure view excludes sensitive fields; role grants verified with
      an analyst-role session.
- [ ] Task is not resumed until its warehouse, owner role, privileges, and CDC semantics
      have been reviewed.

Attach run URLs, timestamps, commit SHA, report artifacts, and measured results here.
Never fill these boxes based on configuration files alone.
