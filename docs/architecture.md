# LedgerShift architecture

The initial local slice consists of reproducible synthetic source data, explicit fault manifests, and deterministic validation primitives. The intended medallion flow is source simulator -> append-only bronze -> validated/deduplicated silver plus quarantine -> gold marts -> Snowflake serving. Databricks administration is managed through Terraform; Airflow orchestrates jobs; CI/CD validates and deploys bundles.

## Engineering boundaries
- generator: synthetic source entities and deterministic fault injection.
- pipelines: Spark transformations; business rules do not belong in orchestration.
- validation: source-to-target checks and auditable reports.
- infra: workspace and governance as code.
- Credentials come from environment variables or secret scopes, never committed files.

Cloud deployment and migration claims must remain marked incomplete until evidenced by real runs.
