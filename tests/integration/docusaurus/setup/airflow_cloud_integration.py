import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import great_expectations as gx

# Replace <YOUR_ACCESS_TOKEN> and <YOUR_CLOUD_ORGANIZATION_ID> with your credentials.
# To get your user access tokesn and organization ID, see:
# (https://docs.greatexpectations.io/docs/cloud/set_up_gx_cloud#get-your-user-access-token-and-organization-id).

GX_CLOUD_ACCESS_TOKEN = os.environ.get("GX_CLOUD_ACCESS_TOKEN")
GX_CLOUD_ORGANIZATION_ID = os.environ.get("GX_CLOUD_ORGANIZATION_ID")

CHECKPOINT_NAME = "haebichan_checkpoint"


# <snippet name="tests/integration/docusaurus/setup/airflow_cloud_integration.py run_gx_airflow">
def run_gx_airflow():
    context = gx.get_context(cloud_mode=True)
    checkpoint = context.get_checkpoint(name=CHECKPOINT_NAME)
    checkpoint.run()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 9),
}

gx_dag = DAG(
    "gx_dag",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # This is set to run daily at midnight. Adjust as needed.
    catchup=False,
)

run_gx_task = PythonOperator(
    task_id="gx_airflow",
    python_callable=run_gx_airflow,
    dag=gx_dag,
)
# </snippet>
run_gx_task

