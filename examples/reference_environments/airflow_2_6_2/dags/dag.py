#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
### DAG Tutorial Documentation
This DAG is demonstrating an Extract -> Transform -> Load pipeline with Great Expectations validations.
It is based on the Airflow tutorial DAG: https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html
"""
from __future__ import annotations

# [START tutorial]
# [START import_module]
import json
from textwrap import dedent

import great_expectations as gx
import pandas as pd
import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

# [END import_module]

# [START instantiate_dag]
with DAG(
    "tutorial_dag_with_gx",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 2},
    # [END default_args]
    description="DAG tutorial with Great Expectations validations",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START extract_function]
    def extract(**kwargs):
        ti = kwargs["ti"]
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        ti.xcom_push("order_data", data_string)

    # [END extract_function]

    def validate_order_data_extract(**kwargs):
        ti = kwargs["ti"]
        extract_data_string = ti.xcom_pull(task_ids="extract", key="order_data")
        order_data = json.loads(extract_data_string)

        df = pd.DataFrame(order_data.items(), columns=["order_id", "order_value"])
        df["order_value"] = df["order_value"].astype(float)

        # Note: the following is a simple example of how to use Great Expectations in a DAG.
        # In a production environment, you would likely want to use a different data source
        # and/or a stored expectation suite rather than building it on each run.
        print("gx.__version__:", gx.__version__)
        print("Setting up context...")
        context = gx.get_context()

        # Explicitly create data docs site to use filesystem store with known file location.
        # This is done to simplify hosting data docs so that they can easily be served in a separate container since
        # the default for an ephemeral context is to write to a temp directory.
        # NOTE: Using the worker filesystem to store data docs is not recommended for production,
        # you may want to use a different store_backend e.g. S3, GCS or ABS. To set that up you can replace
        # the configuration below in the `add_data_docs_site` call:
        context.add_data_docs_site(
            site_config={
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "/gx/gx_stores/data_docs",
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            },
            site_name="local_site_for_hosting",
        )
        print("Connecting to data...")
        datasource = context.sources.add_pandas(name="my_pandas_datasource")
        data_asset = datasource.add_dataframe_asset(name="my_df", dataframe=df)
        my_batch_request = data_asset.build_batch_request()
        print("Validating data...")
        context.add_or_update_expectation_suite("my_expectation_suite")
        validator = context.get_validator(
            batch_request=my_batch_request,
            expectation_suite_name="my_expectation_suite",
        )
        validator.expect_column_values_to_be_between(
            column="order_value", min_value=0, max_value=1000
        )
        validator.save_expectation_suite(discard_failed_expectations=False)
        checkpoint = context.add_or_update_checkpoint(
            name="my_quickstart_checkpoint",
            validator=validator,
        )
        checkpoint_result = checkpoint.run()
        if not checkpoint_result.success:
            raise Exception("Validation failed!")
        print("Validation succeeded!")

    # [START transform_function]
    def transform(**kwargs):
        ti = kwargs["ti"]
        extract_data_string = ti.xcom_pull(task_ids="extract", key="order_data")
        order_data = json.loads(extract_data_string)

        total_order_value = 0
        for value in order_data.values():
            total_order_value += value

        total_value = {"total_order_value": total_order_value}
        total_value_json_string = json.dumps(total_value)
        ti.xcom_push("total_order_value", total_value_json_string)

    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        ti = kwargs["ti"]
        total_value_string = ti.xcom_pull(task_ids="transform", key="total_order_value")
        total_order_value = json.loads(total_value_string)

        print(total_order_value)

    # [END load_function]

    # [START main_flow]
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a hardcoded JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
    """
    )

    validate_order_data_extract_task = PythonOperator(
        task_id="validate_order_data_extract",
        python_callable=validate_order_data_extract,
    )
    validate_order_data_extract_task.doc_md = dedent(
        """\
    #### Validate_order_data_extract task
    A simple task to validate the extracted data before running the rest of the data pipeline.
    """
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )

    extract_task >> validate_order_data_extract_task >> transform_task >> load_task

# [END main_flow]

# [END tutorial]
