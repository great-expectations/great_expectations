---
sidebar_label: 'Quickstart for GX Cloud and Airflow'
title: 'Quickstart for GX Cloud and Airflow'
id: airflow_quickstart
description: Connect GX Cloud to an Airflow Orchestrator.
---

In this quickstart, you'll learn how to use GX Cloud with Apache Airflow. Apache Airflow is an orchestration tool that allows you to schedule and monitor your data pipelines. For more information about Apache Airflow, see the [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html).

## Prerequisites

- You have a [GX Cloud Beta account](https://greatexpectations.io/cloud).

- You have the [Astro CLI](https://docs.astronomer.io/astro/cli/overview) installed.

- You have [connected GX Cloud to a Data Asset on a Data Source](/docs/data_assets/manage_data_assets#create-a-data-asset).

- You have [created an Expectation Suite](/docs/expectation_suites/manage_expectation_suites.md) and [added Expectations](/docs/expectations/manage_expectations#create-an-expectation).

- You have [added a Checkpoint to your Expectation](/docs/checkpoints/manage_checkpoints#add-a-checkpoint).


## Create a local Airflow project and set dependencies

1. Open a terminal, navigate to the directory where you want to create your Airflow project, and then run the following code:

    ```bash title="Terminal input"
    mkdir gx-cloud-airflow && cd gx-cloud-airflow
    astro dev init
    ```
    After running the code, a new directory is created, you're taken to that directory, and a new Airflow project is initialized.

2. Browse to the directory where you created your Airflow project, open the `requirements.txt` file, and then add the following text as a new line: 

    ```
    airflow-provider-great-expectations==0.2.7
    ```

    This text adds the GX Airflow Provider to the Airflow project.
    
3. Save your changes and close the `requirements.txt` file.

4. Open the `packages.txt` file and add the following text as a new line:

    ```
    libgeos-c1v5
    ```
    This text adds the `libgeos-c1v5` library to the Airflow project.

5. Save your changes and close the `packages.txt` file.

## Create a DAG file for your GX Cloud Checkpoint

1. Open a terminal, browse to the `dags` folder of your Airflow project, and then run the following code to create a new DAG named `gx_dag`:

    ```bash title="Terminal input"
    touch gx_dag.py
    ```

2. Open the `gx_dag` DAG file and add the following code:

    ```python 
    import os
    import great_expectations as gx
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime

    def run_gx_airflow():
        os.environ["GX_CLOUD_ACCESS_TOKEN"] = "<YOUR_ACCESS_TOKEN>"
        os.environ["GX_CLOUD_ORGANIZATION_ID"] = "<YOUR_CLOUD_ORGANIZATION_ID>"

    # Replace YOUR_ACCESS_TOKEN and YOUR_CLOUD_ORGANIZATION_ID with your values. See [Get your user access token and organization ID](/docs/cloud/set_up_gx_cloud#get-your-user-access-token-and-organization-id).

    context = gx.get_context()
    checkpoint_name = '<YOUR_CHECKPOINT_NAME>' 
    checkpoint = context.get_checkpoint(name = checkpoint_name)
    checkpoint.run()

     default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 8, 9),  # Adjust the start date as needed
    }

    gx_dag = DAG(
        'gx_dag',  
        default_args=default_args,
        schedule_interval= '59 23 * * 0',    
        catchup=False
    )

    run_data_wrangling_task = PythonOperator(
        task_id='gx_airflow',
        python_callable=run_gx_airflow,
        dag=gx_dag,
    )

    run_data_wrangling_task
    ```

3. Save your changes and close the `gx_dag` DAG file.

## Run the DAG

1. Run the following command in the root directory of your Airflow project to start the server:

    ```bash title="Terminal input"
    astro dev start
    ```

2. Sign in to Airflow. The default username and password are `admin`.

3. In the **Actions** column, click **Play** for `gx_airflow` and confirm your DAG runs as expected.