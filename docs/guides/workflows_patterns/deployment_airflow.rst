.. _deloyment_airflow:

#########################################
Deploying Great Expectations with Airflow
#########################################

This guide will help you deploy Great Expectations within an Airflow pipeline. You can use Great Expectations to automate validation of data integrity and navigate your DAG based on the output of validations.

.. admonition:: Prerequisites: This workflow pattern assumes you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectations Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Optional: Configured a :ref:`Checkpoint <tutorials__getting_started__set_up_your_first_checkpoint>`.
    - Configured an airflow pipeline (DAG) with access to your data sources.

There are two supported methods: using an Airflow ``PythonOperator`` to run validations using python code or invoking the Great Expectations CLI to run a Checkpoint using an Airflow ``BashOperator``.

Steps
-----

Great Expectations via ``PythonOperator``

#. **Create validation methods**

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
            "datasource": "input_files"}

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


#. Add validation methods to DAG

    Validation steps can be added after pipeline steps to ensure that the steps were completed successfully.

    .. code-block:: python

    # Create validation task
    task_validate_data = PythonOperator(
        task_id='task_validate_data',
        python_callable=validate_data,
        provide_context=True,
        dag=dag)

    # Add to DAG
    task_retrieve_data >> task_validate_data >> task_load_data >> task_transform_data >> task_validate_transformed_data


Great Expectations via Checkpoints & ``BashOperator``

Please see this how-to guide for :ref:`How to run a Checkpoint in Airflow <how_to_guides__validation__how_to_run_a_checkpoint_in_airflow>`.


If it would be useful to you, please comment with a +1 and feel free to add any suggestions or questions below.  Also, please reach out to us on `Slack <greatexpectations.io/slack>`_ if you would like to learn more, or have any questions.

.. discourse::
    :topic_identifier: 34