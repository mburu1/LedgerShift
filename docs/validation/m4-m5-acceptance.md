# M4–M5 Acceptance Checklist

This checklist separates code changes from account-backed operational evidence.

## M4 — Databricks Asset Bundles
- [ ] `databricks bundle validate -t dev` passes against the intended workspace.
- [ ] Bundle plan reviewed for unexpected resource changes.
- [ ] Dev deployment succeeds; job URL and commit SHA captured.
- [ ] Pipeline succeeds in the workspace and writes to intended catalog/volume.
- [ ] Validation paths exist and represent the same batch.
- [ ] Production target uses a distinct workspace, service principal and secrets.

## M5 — Snowflake
- [ ] Connector version is compatible with selected Databricks Runtime.
- [ ] SQL scripts applied in dependency order; account-specific grants reviewed.
- [ ] Staging/MERGE replay tested with stable batch ID and business key.
- [ ] Re-running a batch does not duplicate business keys.
- [ ] Delta/Snowflake counts, keys, aggregates, checksums and sampled rows reconcile.
- [ ] Deliberately altered target produces FAIL and non-zero exit.
- [ ] Analyst role verifies secure view excludes sensitive columns.
- [ ] Task remains suspended until warehouse, owner, privileges and CDC semantics are checked.

Attach run URLs, timestamps, commit SHA, report artifacts and measurements. Do not check boxes
based solely on configuration files.
