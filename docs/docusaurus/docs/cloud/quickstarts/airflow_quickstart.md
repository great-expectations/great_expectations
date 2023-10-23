---
sidebar_label: 'Quickstart for GX Cloud and Airflow'
title: 'Quickstart for GX Cloud and Airflow'
id: airflow_quickstart
description: Connect Great Expectations to an Airflow Orchestrator.
---

In this quickstart, you'll learn how to use GX Cloud with an Airfow instance.

## Prerequisites

- You have a [GX Cloud Beta account](https://greatexpectations.io/cloud).

- You have the [Astro CLI](https://docs.astronomer.io/astro/cli/overview) installed.

- You have created at least one [Data Asset](https://google.com), [Expectation Suite](https://google.com), [Expectation](https://google.com) and [Checkpoint](https://google.com) within GX Cloud.

:::note Great Expectations Cloud

If you are new to GX Cloud and have not yet set up any Checkpoints, refer to this [Quickstart Guide](https://google.com)

:::

## Create a local Airflow project and set dependencies

In your terminal, navigate to the directory where you would like to create your Airflow project. Run the following commands to create a new directory, navigate into that directory, and then initialize a new Airflow project:

```bash title="Terminal input"
mkdir gx-cloud-airflow && cd gx-cloud-airflow
astro dev init
```

Once the project has been created, open the `requirements.txt` file and add the GX Airflow Provider as a new line and save the file:

```
airflow-provider-great-expectations==0.2.7
```

Open the `packages.txt` file in the root directory of your project and add the `libgeos-c1v5` library as a new line and save the file:

```
libgeos-c1v5
```

## Create a DAG file for your GX checkpoint

Inside of the `dags` folder in the root folder of your Airflow project, create a new file:

```bash title="Terminal input"
touch gx_dag.py
```

Open the file and start by importing the dependencies we'll need to run our DAG:

```python
import os
import great_expectations as gx

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
```

Define the main method with which we will run our GX checkpoint and set our environment variables to use your GX User Access Token and Cloud Organization ID:

```python
def run_gx_airflow():
    os.environ["GX_CLOUD_ACCESS_TOKEN"] = "<YOUR_ACCESS_TOKEN>"
    os.environ["GX_CLOUD_ORGANIZATION_ID"] = "<YOUR_CLOUD_ORGANIZATION_ID>"
```

:::note How to find your access credentials

Refer to our documentation on how to [Get your user access token and organization ID](/cloud/set_up_gx_cloud#get-your-user-access-token-and-organization-id)

:::

Complete the method by accessing a GX context and running a checkpoint based on the name that you have previously given it:

```python
    context = gx.get_context()

    checkpoint_name = '<YOUR_CHECKPOINT_NAME>' 

    checkpoint = context.get_checkpoint(name = checkpoint_name)

    checkpoint.run()
```

Define the default arugments for the DAG:

```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 9),  # Adjust start date
}
```

Create the DAG instance:

```python
gx_dag = DAG(
    'gx_dag',  
    default_args=default_args,
    schedule_interval= '59 23 * * 0',    
    catchup=False
)
```

Finally, use a `PythonOperator` to create a task:

```python
run_data_wrangling_task = PythonOperator(
    task_id='gx_airflow',
    python_callable=run_gx_airflow,
    dag=gx_dag,
)

run_data_wrangling_task
```

## Open the Airflow Dashboard and run the DAG

Run the following command in the root directory of your Airflow project to start the server:

```bash title="Terminal input"
astro dev start
```

By default, a new browser window will open that will allow you to login. By default, the username and password are `admin/admin`.

Once you are logged in,, you will see the new DAG that we have defined earlier called `gx_airflow`. Under the Actions column, click the Play button to verify that your DAG is set up and running correctly.