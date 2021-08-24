.. _deployment_airflow:

Deploying Great Expectations with Airflow
=========================================

This guide will help you deploy Great Expectations within an Airflow pipeline. You can use Great Expectations to automate validation of data integrity and navigate your DAG based on the output of validations.

.. admonition:: Prerequisites: This workflow pattern assumes you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectations Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Optional: Configured a :ref:`Checkpoint <tutorials__getting_started__validate_your_data>`.
    - Configured an Airflow pipeline (DAG).

There are three supported methods for running :ref:`validation<reference__core_concepts__validation>` in an Airflow DAG:

#. Recommended: Using the ``GreatExpectationsOperator`` in the Great Expectations Airflow Provider package
#. Using an Airflow ``PythonOperator`` to run validations using Python code
#. Invoking the Great Expectations CLI to run a Checkpoint using an Airflow ``BashOperator``

Check out this link for an example of Airflow pipelines with Great Expectations validations ensuring that downstream tasks are protected from upstream issues:

- `Great Expectations Pipeline Tutorial <https://github.com/superconductive/ge_tutorials>`_

In the first link and the diagram below, you can see a common pattern of using validation tasks to ensure that the data flowing from one task to the next is correct, and alert the team if it is not. Another common pattern is to branch and change your DAG based on a validation (e.g. send data for more cleaning before moving to the next task, store it for a postmortem, etc.).

.. image:: ge_tutorials_pipeline.png
    :width: 800
    :alt: Airflow pipeline from Great Expectations tutorials repository.


We will now explain the supported methods for using Great Expectations within an Airflow pipeline.


Running validation using the ``GreatExpectationsOperator``
-----------------------------------------------------------

The ``GreatExpectationsOperator`` in the `Great Expectations Airflow Provider package <https://github.com/great-expectations/airflow-provider-great-expectations>`_ is a convenient way to invoke validation with Great Expectations in an Airflow DAG. See the `example DAG in the examples folder <https://github.com/great-expectations/airflow-provider-great-expectations/blob/main/great_expectations_provider/example_dags/example_great_expectations_dag.py>`_ for several methods to use the operator.

1. Ensure that the ``great_expectations`` directory that defines your Data Context is accessible by your DAG. Typically, it will be located in the same project as your DAG, but you can point the operator at any location.

2. Install Great Expectations and the Great Expectations provider in your environment

.. code-block:: bash

    pip install great_expectations airflow-provider-great-expectations

3. Import the operator in your DAG file

.. code-block:: python

    from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

4. Create a task using the ``GreatExpectationsOperator``

The ``GreatExpectationsOperator`` supports multiple ways of invoking validation with Great Expectations: a) using an expectation suite name and batch_kwargs, b) using a list of expectation suite names and batch_kwargs (using the ``assets_to_validate`` parameter), c) using a checkpoint. This means that the parameters depend on how you would like to invoke Great Expectations validation. As a simple example, assuming you have a single Expectation Suite “my_suite” and a simple batch of data, such as a database table called “my_table”, you can use the following parameters:

.. code-block:: python

    my_ge_task = GreatExpectationsOperator(
        task_id='my_task,
        expectation_suite_name='my_suite',
        batch_kwargs={
            'table': 'my_table',
            'datasource': 'my_datasource'
        },
        dag=dag
    )

**Note**: If your ``great_expectations`` directory is not located in the same place as your DAG file, you will need to provide the ``data_context_root_dir`` parameter.

By default, a ``GreatExpectationsOperator`` task will run validation and raise an ``AirflowException`` if any of the tests fails. To override this behavior and continue running even if tests fail, set the ``fail_task_on_validation_failure`` flag to ``False``.

For more information about possible parameters and examples, see the `README in the repository <https://github.com/great-expectations/airflow-provider-great-expectations>`_, and the `example DAG in the provider package <https://github.com/great-expectations/airflow-provider-great-expectations/tree/main/great_expectations_provider/example_dags>`_


Running validation using a ``PythonOperator``
-----------------------------------------------

If the current version of the ``GreatExpectationsOperator`` does not support your use case, you can also fall back to running validation using a standard ``PythonOperator`` as described in this section.

1. **Create validation Methods**

    Create the methods to validate data that will be called in your DAG. In this example our data is contained in a file.

.. code-block:: python

    from airflow import AirflowException
    from airflow.operators.python_operator import PythonOperator
    import great_expectations as ge

    ...

    def validate_data(ds, **kwargs):

        # Retrieve your data context
        context = ge.data_context.DataContext(<insert path to your great_expectations.yml>)

        # Create your batch_kwargs
        batch_kwargs_file = {
            "path": <insert path to your data file>,
            "datasource": "my_pandas_datasource"}

        # Create your batch (batch_kwargs + expectation suite)
        batch_file = context.get_batch(batch_kwargs_file, <insert name of your expectation suite>)

        # Run the validation
        results = context.run_validation_operator(
            "action_list_operator",
            assets_to_validate=[batch_file],
            # This run_id can be whatever you choose
            run_id=f"airflow: {kwargs['dag_run'].run_id}:{kwargs['dag_run'].start_date}")

        # Handle result of validation
        if not results["success"]:
            raise AirflowException("Validation of the data is not successful ")


2. **Add validation Methods to DAG**

    Validation steps can be added after data retrieval, transformation or loading steps to ensure that the steps were completed successfully.

.. code-block:: python

    # Create validation task
    task_validate_data = PythonOperator(
        task_id='task_validate_data',
        python_callable=validate_data,
        provide_context=True,
        dag=dag)

    # Add to DAG
    task_retrieve_data.set_downstream(task_validate_data)
    task_validate_data.set_downstream(task_load_data)
    task_load_data.set_downstream(task_transform_data)
    task_transform_data.set_downstream(task_validate_transformed_data)


Running a validation using a Checkpoint & ``BashOperator``
----------------------------------------------------------

Please see this how-to guide for :ref:`How to run a Checkpoint in Airflow <how_to_guides__validation__how_to_run_a_checkpoint_in_airflow>`.

Additional resources
--------------------

- `Great Expectations Pipeline Tutorial <https://github.com/superconductive/ge_tutorials>`_ showing Great Expectations implemented in an airflow pipeline.

Comments
--------

.. discourse::
    :topic_identifier: 34
