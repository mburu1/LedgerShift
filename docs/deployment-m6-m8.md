# M6–M8: Governance, orchestration and delivery

## M6 — Terraform governance

Terraform manages the Unity Catalog catalog, bronze/silver/gold/ops schemas and separate data-engineering, analyst and operations groups. Analysts receive read-only access to gold; write privileges are limited to data engineers. Terraform state and credentials must never be committed.

Validate locally:

```bash
cd infra/terraform
terraform fmt -recursive
terraform init -backend=false
terraform validate
terraform plan -var-file=envs/dev.tfvars
```

Copy an example tfvars file to a local ignored file and supply credentials through environment variables. A plan is not an applied deployment. Select a remote state backend with locking and a cloud identity strategy before team or production use.

## M7 — Airflow orchestration

Install the provider in orchestration/airflow/requirements.txt and configure an Airflow connection named databricks_default. Set Airflow Variable ledgershift_databricks_job_id to the deployed bundle job ID. The DAG should serialize runs, retry transient failures and pass a deterministic idempotency token to Databricks Jobs API.

The Databricks job must fail when reconciliation fails. Configure alert delivery before claiming operational alerting is complete. A DAG import test and successful plus deliberately failed run are required evidence.

## M8 — CI/CD

- CI runs Python lint/tests, Terraform formatting/validation and Databricks bundle validation.
- Pushes to main deploy the dev bundle.
- Production deployment is manually dispatched through the GitHub production environment, configured with required reviewers.
- Configure DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_PROD_HOST and DATABRICKS_PROD_TOKEN as GitHub secrets. Prefer OIDC/workload identity where supported.
- Never put tokens in Terraform variables files, DAG code or workflow literals.

## Acceptance evidence (not implied by these files)

- [ ] Terraform fmt, validate and reviewed plan pass.
- [ ] Workspace grants are inspected and least privilege confirmed.
- [ ] Airflow DAG imports; successful and fault-injected runs demonstrate retry behavior.
- [ ] GitHub Actions CI passes.
- [ ] Dev bundle deploy succeeds against a real workspace.
- [ ] Production approval and deployment are verified.
- [ ] Deployment claims include run URLs, timestamps and commit SHA.
