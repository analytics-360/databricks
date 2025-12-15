from __future__ import annotations
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.bash import BashOperator
import pendulum

DATABRICKS_CONN_ID = 'Databricks'

default_args = {
  'owner': 'airflow'
}

with DAG(
  dag_id='databricks_etl_workflow',
  start_date = pendulum.now().subtract(days=2),
  schedule = None,
  default_args = default_args,
  catchup=False,
  tags=['databricks', 'etl'],
  ) as dag:

  extract_data = BashOperator(
        task_id='extract_data',
        bash_command='echo "Starting data extraction..."',
    )
  
  transform_data = DatabricksRunNowOperator(
    task_id = 'run_databricks_job',
    databricks_conn_id = DATABRICKS_CONN_ID,
    job_id = 848746857260033
  )

  load_data = BashOperator(
        task_id='load_data',
        bash_command='echo "Loading final data to warehouse..."',
    )
  
  extract_data >> transform_data >> load_data
