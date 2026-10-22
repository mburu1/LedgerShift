"""Schedule the bundled LedgerShift pipeline and reconciliation job."""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

with DAG(
    dag_id="ledgershift_daily",
    description="Run Bronze-to-Gold, Snowflake loading and reconciliation",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "ledgershift",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=20),
    },
    tags=["ledgershift", "data-platform"],
) as dag:
    run_pipeline = DatabricksRunNowOperator(
        task_id="run_pipeline_and_reconcile",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.ledgershift_databricks_job_id }}",
        idempotency_token="{{ dag.dag_id }}-{{ run_id | replace(':', '-') }}",
        notebook_params={
            "batch_id": "{{ ds_nodash }}-{{ run_id | replace(':', '-') }}",
            "run_date": "{{ ds }}",
        },
        do_xcom_push=True,
    )
