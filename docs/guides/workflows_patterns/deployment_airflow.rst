.. _deloyment_airflow:

Deploying Great Expectations with Airflow
=========================================

This guide will help you deploy Great Expectations within an Airflow pipeline. You can use Great Expectations to automate validation of data integrity and navigate your DAG based on the output of validations.

.. admonition:: Prerequisites: This workflow pattern assumes you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectations Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Optional: Configured a :ref:`Checkpoint <tutorials__getting_started__set_up_your_first_checkpoint>`.
    - Configured an Airflow pipeline (DAG).

There are two supported methods: using an Airflow ``PythonOperator`` to run Validations using python code or invoking the Great Expectations CLI to run a Checkpoint using an Airflow ``BashOperator``.

.. note::
    There is not currently a Great Expectations custom Airflow operator, however we have heard from users who have implemented their own. We love community contributions so please share your operators!

Check out this link for an example of Airflow pipelines with Great Expectations Validations ensuring that downstream tasks are protected from upstream issues:

- `Great Expectations Pipeline Tutorial <https://github.com/superconductive/ge_tutorials>`_

In the first link and the diagram below, you can see a common pattern of using Validation tasks to ensure that the data flowing from one task to the next is correct, and alert the team if it is not. Another common pattern is to branch and change your DAG based on a Validation (e.g. send data for more cleaning before moving to the next task, store it for a postmortem, etc.).

.. image:: ge_tutorials_pipeline.png
    :width: 800
    :alt: Airflow pipeline from Great Expectations tutorials repository.


We will now explain the two supported methods for using Great Expectations within an Airflow pipeline.

Running a Validation using a ``PythonOperator``
-----------------------------------------------

1. **Create Validation Methods**

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


2. **Add Validation Methods to DAG**

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


Running a Validation using a Checkpoint & ``BashOperator``
----------------------------------------------------------

Please see this how-to guide for :ref:`How to run a Checkpoint in Airflow <how_to_guides__validation__how_to_run_a_checkpoint_in_airflow>`.

Additional resources
--------------------

- `Great Expectations Pipeline Tutorial <https://github.com/superconductive/ge_tutorials>`_ showing Great Expectations implemented in an airflow pipeline.

Comments
--------

.. discourse::
    :topic_identifier: 34
