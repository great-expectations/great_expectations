---
sidebar_label: 'Connect GX Cloud and Airflow'
title: 'Connect GX Cloud and Airflow'
id: connect_airflow
description: Connect GX Cloud to an Airflow Orchestrator.
---

In this quickstart, you'll learn how to use GX Cloud with Apache Airflow. You'll create a simple DAG that runs a Checkpoint that you have already set up in GX Cloud, and then trigger it through a local installation of an Airflow server.

Apache Airflow is an orchestration tool that allows you to schedule and monitor your data pipelines. For more information about Apache Airflow, see the [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html).

## Prerequisites

- You have a [GX Cloud account](https://greatexpectations.io/cloud).

- You have installed Apache Airflow and initialized the database (__airflow db init__).

- You have [connected GX Cloud to a Data Asset on a Data Source](/cloud/data_assets/manage_data_assets.md#create-a-data-asset).

- You have [created an Expectation Suite](/cloud/expectation_suites/manage_expectation_suites.md) and [added Expectations](/cloud/expectations/manage_expectations.md#create-an-expectation).

- You have [added a Checkpoint to your Expectation](/cloud/checkpoints/manage_checkpoints.md#add-a-checkpoint).


## Run Airflow Standalone to create a freshh local Airflow environment

1. The `airflow standalone` command initializes the database, creates a user, and starts all components.

    ``` title="Terminal input"
    airflow standalone
    ```

    This command will eventually output a username a password for the Airflow UI like this:

    ``` title="Terminal input"
    standalone | Airflow is ready
    standalone | Login with username: admin  password: Bpu6RgmPMMaDeeq5
    standalone | Airflow Standalone is for development purposes only. Do not use this in production!
    ```

2. Access Airflow UI:

    Once the web server is running, open a web browser and go to http://localhost:8080 (by default) to access the Airflow UI using the username and password from the last step


## Create a DAG file for your GX Cloud Checkpoint

1. Open a terminal, browse to the `airflow` folder in your home directory, and then run the following code to create a new DAG named `gx_dag.py`:

    ```bash title="Terminal input"
    cd ~/airflow
    mkdir dags
    cd dags
    touch gx_dag.py
    ```

2. Open the `gx_dag.py` DAG file and add the following code:

    ```python title="Python"
    import os
    import great_expectations as gx
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime

    # Replace <YOUR_ACCESS_TOKEN> and <YOUR_CLOUD_ORGANIZATION_ID> with your credentials.

    GX_CLOUD_ACCESS_TOKEN = "<YOUR_ACCESS_TOKEN>"
    GX_CLOUD_ORGANIZATION_ID = "<YOUR_CLOUD_ORGANIZATION_ID>"

    CHECKPOINT_NAME = "<YOUR_CHECKPOINT>"

    def run_gx_airflow():

        context = gx.get_context(
            cloud_access_token=GX_CLOUD_ACCESS_TOKEN,
            cloud_organization_id=GX_CLOUD_ORGANIZATION_ID,
        )
        checkpoint = context.get_legacy_checkpoint(name=CHECKPOINT_NAME)
        checkpoint.run()

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 8, 9),
    }

    gx_dag = DAG(
        'gx_dag',
        default_args=default_args,
        schedule= '0 0 * * *', # This is set to run daily at midnight. Adjust as needed.
        catchup=False
    )

    run_gx_task = PythonOperator(
        task_id='gx_airflow',
        python_callable=run_gx_airflow,
        dag=gx_dag,
    )

    run_gx_task
    ```

3. Save your changes and close the `gx_dag.py` DAG file.

## Run the DAG (Manually)

1. Restart the airflow server to pick up the new DAG file.

2. Sign in to Airflow using the username and password from the first standalone run

3. In the **Actions** column, click **Trigger DAG** for `gx_dag` and confirm your DAG runs as expected.

## Clean up local Airflow environment

1. Delete the local files and sqllite database

    ```bash title="Terminal input"
    rm -rf ~/airflow
    ```
