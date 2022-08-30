from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
  'owner': 'airflow'
}

notebook_params = {
    "CopyActivityExecutionGroupName": "AdventureWorksRawCopyProcess",
    "PipelineScheduleName": "Daily"
}

with DAG('Ingestion',
  start_date = days_ago(2),
  dagrun_timeout=timedelta(minutes=60),
  schedule_interval = None,
  default_args = default_args
  ) as dag:

  ingest_run_now = DatabricksRunNowOperator(
    task_id = 'run_now',
    databricks_conn_id = 'databricks_default',
    notebook_params=notebook_params,
    job_id = 632840580891638
  )