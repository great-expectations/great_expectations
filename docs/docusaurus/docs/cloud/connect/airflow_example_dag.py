# <snippet name="cloud/connect/airflow_example_dag.py full example code">
import os

import pendulum
from airflow.decorators import dag, task

import great_expectations as gx


@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 8, 9),
    catchup=False,
)
def gx_dag_with_deco():
    os.environ["NO_PROXY"] = "*"  # https://github.com/apache/airflow/discussions/24463
    print("Great Expectations DAG Started")

    @task
    def run_checkpoint():
        print("Running Checkpoint")
        # Replace <YOUR_ACCESS_TOKEN>, <YOUR_CLOUD_ORGANIZATION_ID>, and <CHECKPOINT_NAME> with your credentials
        # You can also set GX_CLOUD_ACCESS_TOKEN and GX_CLOUD_ORGANIZATION_ID as environment variables
        GX_CLOUD_ACCESS_TOKEN = "<YOUR_ACCESS_TOKEN>"
        GX_CLOUD_ORGANIZATION_ID = "<YOUR_CLOUD_ORGANIZATION_ID>"
        # alternatively set CHECKPOINT_NAME to be a runtime parameter
        CHECKPOINT_NAME = "<CHECKPOINT_NAME>"
        context = gx.get_context(
            cloud_access_token=GX_CLOUD_ACCESS_TOKEN,
            cloud_organization_id=GX_CLOUD_ORGANIZATION_ID,
        )
        checkpoint = context.get_checkpoint(name=CHECKPOINT_NAME)
        checkpoint.run()
        return f"Checkpoint ran: {CHECKPOINT_NAME}"

    run_checkpoint()


run_this = gx_dag_with_deco()
