from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(days=1),
}

dag = DAG(
    "GE_checkpoint_run",
    default_args=default_args,
    description="running GE checkpoint",
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=5),
)

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id="checkpoint_run",
    bash_command="(cd /home/airflow/gcsfuse/great_expectations/ ; great_expectations --v3-api checkpoint run gcs_checkpoint ) ",
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31 - 1,
)
