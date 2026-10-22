# M4–M5: Databricks Asset Bundles and Snowflake

## Scope and evidence

This change adds a Databricks Asset Bundle definition, job resource declarations,
a configurable Spark-to-Snowflake writer, Snowflake serving contracts, and
window-function analytics. These are repository artifacts, not evidence of a
successful cloud deployment. Record actual workspace and Snowflake runs only
after they have been executed.

## M4 — Asset Bundles

1. Install a current Databricks CLI and authenticate with a named profile:
   `databricks auth login --host https://<workspace-host> --profile dev`.
2. Validate the bundle: `databricks bundle validate -t dev`.
3. Review planned resources: `databricks bundle plan -t dev`.
4. Deploy only after validation and plan review: `databricks bundle deploy -t dev`.
5. Run the job and record its run URL, commit SHA, runtime, target catalog, and
   validation output in the deployment evidence log.
6. Configure a separate production workspace and approved service principal
   before using the prod target. Do not reuse development credentials.

The bundle is an initial deployable scaffold. Confirm task entrypoints and
runtime dependencies against the current pipeline CLI before treating a job
run as successful. Unity Catalog, workspace permissions, and compute policies
may require separate Terraform-managed provisioning.

## M5 — Snowflake serving

Required environment variables:

- `SNOWFLAKE_ACCOUNT` (account identifier or connector URL expected by your
  installed connector)
- `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`
- Optional: `SNOWFLAKE_ROLE`

Never commit credentials or print connector options in logs. Install the
Snowflake Spark connector compatible with the deployed Databricks Runtime;
the connector is not bundled into the application automatically.

Apply SQL in order: `01_schemas.sql`, `02_tables.sql`,
`04_analytics_window.sql`, then `05_secure_views.sql`. The streams/tasks
script is intentionally not included until a concrete CDC source and task
ownership model are defined.

## Loading and idempotency

`load_dataframe` writes a caller-supplied Spark DataFrame to a target table.
Append is not idempotent. For repeatable loads, write each batch to a staging
table with a deterministic batch ID, validate uniqueness and row counts, then
MERGE into the serving table on its business key in a controlled Snowflake SQL
transaction. Preserve deletes explicitly if the source CDC contract requires
them.

## Validation acceptance criteria

- Bundle validates for dev and prod using real workspace configuration.
- Dev job completes and run metadata is captured.
- Snowflake table schemas match gold data types and business keys.
- Load is rerunnable without duplicate business records.
- Reconciliation checks row counts, distinct business keys, aggregate amounts,
  null profiles, and sampled key-level differences between Delta gold and
  Snowflake.
- A deliberately altered target is rejected by validation.
- Credentials are stored in a secret scope or CI secret store and are absent
  from source, logs, and committed artifacts.

Do not mark M4 or M5 complete until these checks have run against the target
services. Local validation of configuration and SQL text is not cloud evidence.
