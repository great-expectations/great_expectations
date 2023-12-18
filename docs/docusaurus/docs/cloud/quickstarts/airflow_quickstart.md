---
sidebar_label: 'Quickstart for GX Cloud and Airflow'
title: 'Quickstart for GX Cloud and Airflow'
id: airflow_quickstart
description: Connect GX Cloud to an Airflow Orchestrator.
---

In this quickstart, you'll learn how to use GX Cloud with Apache Airflow. You'll create a simple DAG that runs a Checkpoint that you have already set up in GX Cloud, and then trigger it through a local installation of an Airflow server.

Apache Airflow is an orchestration tool that allows you to schedule and monitor your data pipelines. For more information about Apache Airflow, see the [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html).

## Prerequisites

- You have a [GX Cloud Beta account](https://greatexpectations.io/cloud).

- You have installed Apache Airflow and initialized the database (__airflow db init__).

- You have [connected GX Cloud to a Data Asset on a Data Source](/docs/cloud/data_assets/manage_data_assets#create-a-data-asset).

- You have [created an Expectation Suite](/docs/cloud/expectation_suites/manage_expectation_suites.md) and [added Expectations](/docs/cloud/expectations/manage_expectations#create-an-expectation).

- You have [added a Checkpoint to your Expectation](/docs/cloud/checkpoints/manage_checkpoints#add-a-checkpoint).


## Create a local Airflow project and set dependencies

1. Open a terminal, navigate to the directory where you want to create your Airflow project, and then run the following code:

    ```bash title="Terminal input"
    mkdir gx-cloud-airflow && cd gx-cloud-airflow
    ```
    After running the CLI, a new directory is created and you're taken to that directory.

2. Start the Airflow Scheduler and Web Server

    ```
    airflow scheduler
    airflow webserver
    ```

    The scheduler manages task scheduling and the web server starts the UI for Airflow.

3. Access Airflow UI:

    Once the web server is running, open a web browser and go to http://localhost:8080 (by default) to access the Airflow UI.


## Create a DAG file for your GX Cloud Checkpoint

1. Open a terminal, browse to the `dags` folder of your Airflow project, and then run the following code to create a new DAG named `gx_dag.py`:

    ```bash title="Terminal input"
    touch gx_dag.py
    ```

2. Open the `gx_dag.py` DAG file and add the following code:

    ```python
    import os
    import great_expectations as gx
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime

    # Replace <YOUR_ACCESS_TOKEN> and <YOUR_CLOUD_ORGANIZATION_ID> with your credentials.
    # To get your user access token and organization ID, see:
    # (https://docs.greatexpectations.io/docs/cloud/set_up_gx_cloud#get-your-user-access-token-and-organization-id).

    GX_CLOUD_ACCESS_TOKEN = "<YOUR_ACCESS_TOKEN>"
    GX_CLOUD_ORGANIZATION_ID = "<YOUR_CLOUD_ORGANIZATION_ID>"

    CHECKPOINT_NAME = "<YOUR_CHECKPOINT>"

    def run_gx_airflow():

        context = gx.get_context()
        checkpoint = context.get_checkpoint(name = CHECKPOINT_NAME)
        checkpoint.run()

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 8, 9),
    }

    gx_dag = DAG(
        'gx_dag',
        default_args=default_args,
        schedule_interval= '0 0 * * *', # This is set to run daily at midnight. Adjust as needed.
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

1. Sign in to Airflow. The default username and password are `admin`.

2. In the **Actions** column, click **Trigger DAG** for `gx_airflow` and confirm your DAG runs as expected.
