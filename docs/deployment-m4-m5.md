# M4–M5: Databricks Asset Bundles and Snowflake

## Scope and evidence

This milestone adds Asset Bundle resources, a configurable Spark-to-Snowflake load API,
Snowflake serving contracts and analytics, and a fail-closed Delta/Snowflake reconciliation
path. Source artifacts are not proof of successful cloud deployment.

## M4 — Asset Bundles

1. Install the Databricks CLI and authenticate:
   `databricks auth login --host https://<workspace-host> --profile dev`.
2. Validate: `databricks bundle validate -t dev`.
3. Review the plan: `databricks bundle plan -t dev`.
4. Deploy only after reviewing the plan: `databricks bundle deploy -t dev`.
5. Capture the pipeline run URL, commit SHA, runtime, target catalog and output paths.
6. The current local pipeline reads `LEDGERSHIFT_DATA_DIR/source/snapshot/orders.jsonl`
   and writes local Delta paths. A developer machine's files are not automatically present
   on a remote workspace. Upload data to a governed Volume and configure source/output paths
   before claiming a cloud run.
7. Configure a separate production workspace and non-empty service principal; do not reuse
   development credentials in production.

## M5 — Snowflake serving

Configure environment variables or a secret scope; never commit or log values:
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`,
`SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, optional
`SNOWFLAKE_ROLE`.

Install a Spark Snowflake connector compatible with the deployed Databricks Runtime.
It is not bundled with the standard Python dependencies.

Apply scripts in order after selecting the correct database:
1. `01_schemas.sql`
2. `02_tables.sql`
3. `04_analytics_window.sql`
4. `05_secure_views.sql`

The optional `03_streams_tasks.sql` uses illustrative warehouse `LEDGERSHIFT_WH`; replace
it before execution. Keep the task suspended until role, grants, warehouse and CDC semantics
have been reviewed.

## Load semantics

`load_dataframe` supports direct append/overwrite for controlled loads. Append is not
idempotent. `load_dataframe_via_stage` writes a batch to staging and merges by specified
business keys. Serialize writers per target and test the connector version. Source deletes
need an explicit tombstone contract. Configure target columns to match the staging contract.

## Delta-to-Snowflake reconciliation

After gold and Snowflake serving tables exist:

```bash
python -m ledgershift.validation.runner \
  --source data/gold/fact_orders \
  --target-format snowflake \
  --target-table MART.FACT_ORDERS \
  --key order_id
```

The runner compares row counts, recognized distinct keys, supported monetary/date aggregates,
shared-column null/distinct/checksum profiles, and one deterministic hash bucket of sampled
records. It writes JSON and Markdown reports and exits non-zero on any failed check. A single
hash bucket is not full row-level proof; run all buckets or implement a full-key diff for
high-assurance sign-off. Use matching names and compatible types.

## Acceptance evidence

- [ ] Bundle validation and plan review against a real workspace.
- [ ] Dev deployment and pipeline run succeed; metadata captured.
- [ ] Snowflake DDL is applied in a test account and grants are reviewed.
- [ ] Replay test confirms no duplicate business keys.
- [ ] Delta/Snowflake report PASS; tampering produces FAIL/non-zero exit.
- [ ] Analyst role cannot access excluded sensitive columns.
- [ ] No secrets appear in source, logs or artifacts.

Keep M4/M5 pending cloud verification until evidence is attached.
