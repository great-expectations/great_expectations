# <snippet name="version-0.18.8 docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/ge_checkpoint_bigquery.py full">
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
    "GX_checkpoint_run",
    default_args=default_args,
    description="running GX checkpoint",
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=5),
)

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id="checkpoint_run",
    bash_command="(cd /home/airflow/gcsfuse/great_expectations/ ; great_expectations checkpoint run bigquery_checkpoint ) ",
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31 - 1,
)
# </snippet>
