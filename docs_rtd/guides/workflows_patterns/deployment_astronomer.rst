.. _deployment_astronomer:

Deploying Great Expectations with Astronomer
=============================================

This guide will help you deploy Great Expectations within an Airflow pipeline that runs in `Astronomer <https://www.astronomer.io/>`_.  You can use Great Expectations to automate validation of data integrity and navigate your DAG based on the output of validations.

Note that you can also define your Data Context in code by following this documentation: :ref:`How to instantiate a Data Context without a yml file <how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file>`

.. admonition:: Prerequisites: This workflow pattern assumes you have already:

    - Followed the instructions for :ref:`creating a Great Expectations validation task in an Airflow DAG<deployment_airflow>`
    - Set up an `Astronomer deploy <https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart/>`_

Using the Great Expectations Airflow Operator in an Astronomer Deployment
-------------------------------------------------------------------------
=========================================================================

There are only few additional requirements to deploy a DAG using the Great Expectations operator with Astronomer. Most importantly, you will need to set relevant environment variables.

Step 1: Set the DataContext root directory
-------------------------------------------

Great Expectations needs to know where to find the :ref:`Data Context<reference__core_concepts__data_context>` by passing the Data Context root directory, which you can then access in the DAG. We recommend adding this variable to your Dockerfile, but you can use any of the methods described in the `Astronomer documentation <https://www.astronomer.io/docs/cloud/stable/deploy/environment-variables/>`_.

For example, if you decide to keep ``great_expectations`` directory in the ``include`` directory in your Astronomer project, the environment variable will look like this:

.. code-block:: bash

    ENV GE_DATA_CONTEXT_ROOT_DIR=/usr/local/airflow/include/great_expectations

You can then pass the root directory as an argument into the task:

.. code-block:: python

    import os
    ge_root_dir = os.environ['GE_DATA_CONTEXT_ROOT_DIR']

    my_ge_task = GreatExpectationsOperator(
        task_id='my_task,
        expectation_suite_name='my_suite',
        batch_kwargs={
            'table': 'my_table',
            'datasource': 'my_datasource'
        },
        data_context_root_dir=ge_root_dir,
        dag=dag
    )


Step 2: Set the environment variables for credentials
-----------------------------------------------------

You will need to configure environment variables for any credentials required for external data connections, see :ref:`how_to_guides__configuring_data_contexts__how_to_use_a_yaml_file_or_environment_variables_to_populate_credentials` for an explanation of how to use environment variables in your ``great_expectations.yml``.

Then add the environment variables to your local ``.env file`` in your Astronomer deploy and as secret environment variables in the Astronomer Cloud settings, following the instructions in the `Astronomer documentation <https://www.astronomer.io/docs/cloud/stable/deploy/environment-variables/>`_.
