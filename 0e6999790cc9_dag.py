from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta 

# Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/veronica.cano.rodriguez@gmail.com/s3_bucket_to_databricks',
}

default_args = {
    'owner': 'Vero',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    '0e6999790cc9_dag',
    start_date=datetime(2024, 2, 20),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )

opr_submit_run

