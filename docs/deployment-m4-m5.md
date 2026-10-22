# M4–M5: Databricks Asset Bundles and Snowflake

## Scope and evidence

This milestone adds Asset Bundle resources, a validated Spark-to-Snowflake load API,
Snowflake serving contracts and analytics, and a fail-closed Delta/Snowflake reconciliation
path. Source artifacts are not proof of successful cloud deployment. Record cloud evidence
only after running against the actual workspace and account.

## M4 — Asset Bundles

1. Install a current Databricks CLI and authenticate:
   `databricks auth login --host https://<workspace-host> --profile dev`.
2. Validate: `databricks bundle validate -t dev`.
3. Review plan: `databricks bundle plan -t dev`.
4. Deploy only after reviewing the plan: `databricks bundle deploy -t dev`.
5. Record pipeline run URL, commit SHA, runtime, target catalog, and output paths.
6. The current pipeline reads `LEDGERSHIFT_DATA_DIR/source/snapshot/orders.jsonl` and writes
   local Delta paths. A developer machine's local files are not automatically present in a
   remote workspace. Upload source data to a governed Volume and configure the job's source
   and output paths before claiming a workspace run.
7. Configure a separate production workspace and non-empty service principal; never reuse
   development credentials for production.

## M5 — Snowflake serving

Configure through environment variables or a secret scope; never commit or log values:
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`,
`SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, optional
`SNOWFLAKE_ROLE`.

Install a Spark Snowflake connector compatible with the deployed Databricks Runtime.
The connector is not included in the standard Python dependencies.

Apply scripts in this order after selecting the intended database:
1. `01_schemas.sql`
2. `02_tables.sql`
3. `04_analytics_window.sql`
4. `05_secure_views.sql`

The optional `03_streams_tasks.sql` uses an illustrative warehouse identifier,
`LEDGERSHIFT_WH`; replace it before execution. Keep the task suspended until the role,
grants, warehouse and CDC semantics have been reviewed.

## Load semantics

`load_dataframe` supports direct append/overwrite for controlled loads. Append is not
idempotent. `load_dataframe_via_stage` writes a batch to a staging table, then merges by
caller-specified business keys. Serialize writers for the same target, agree on a stable
batch ID and target column contract, and test connector compatibility. Source deletes require
an explicit tombstone contract.

## Delta-to-Snowflake reconciliation

After gold tables and the Snowflake serving table exist:

```bash
python -m ledgershift.validation.runner \
  --source data/gold/fact_orders \
  --target-format snowflake \
  --target-table MART.FACT_ORDERS \
  --key order_id
```

The runner compares row counts, recognized key counts, monetary/date aggregates, shared-column
null/distinct/checksum profiles, and one deterministic hash bucket of sampled records. It
writes JSON and Markdown reports and exits non-zero on any failed check. For high-assurance
sign-off, run all hash buckets or add a full ordered-key diff; a single sample bucket is not
a complete proof of row-level equality. Use matching column names and compatible data types.

## Acceptance evidence

- [ ] Bundle validates and deployment plan is reviewed against a real workspace.
- [ ] Dev deployment and job run succeed; metadata is captured.
- [ ] Snowflake DDL is applied in a test account, with grants reviewed.
- [ ] A replay test confirms no duplicate business keys.
- [ ] Delta/Snowflake reconciliation report is PASS; tampering produces FAIL/non-zero exit.
- [ ] Analyst role cannot access excluded sensitive columns.
- [ ] Secrets are absent from source, logs and artifacts.

Keep M4/M5 marked pending cloud verification until this evidence is attached.
