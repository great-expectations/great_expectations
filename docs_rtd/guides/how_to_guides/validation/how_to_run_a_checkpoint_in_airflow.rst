.. _how_to_guides__validation__how_to_run_a_checkpoint_in_airflow:

How to run a Checkpoint in Airflow
==================================

This guide will help you run a Great Expectations checkpoint in Apache Airflow, which allows you to trigger validation of a data asset using an Expectation Suite directly within an Airflow DAG.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
    - :ref:`Created an Expectation Suite <how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_the_cli>`
    - :ref:`Created a checkpoint for that Expectation Suite and a data asset <how_to_guides__validation__how_to_create_a_new_checkpoint>`
    - Created an Airflow DAG file

Using checkpoints is the most straightforward way to trigger a validation run from within Airflow. The following sections describe two alternative approaches to accomplishing this.


Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        Running a checkpoint with a BashOperator
        ----------------------------------------
        You can use a simple `BashOperator` in Airflow to trigger the checkpoint run. The following snippet shows an Airflow task for an Airflow DAG named `dag` that triggers the run of a checkpoint we named `my_checkpoint`:

            .. code-block:: python

                validation_task = BashOperator(
                    task_id='validation_task',
                    bash_command='great_expectations checkpoint run my_checkpoint',
                    dag=dag
                )

        Running the `checkpoint script` output with a PythonOperator
        ------------------------------------------------------------

        Another option is to use the output of the `checkpoint script` command and paste it into a method that is called from a PythonOperator in the DAG. This gives you more fine-grained control over how to respond to validation results:

        1. Run `checkpoint script`

            .. code-block:: bash

                great_expectations checkpoint script my_checkpoint

                ...

                A python script was created that runs the checkpoint named: `my_checkpoint`
                - The script is located in `great_expectations/uncommitted/my_checkpoint.py`
                - The script can be run with `python great_expectations/uncommitted/my_checkpoint.py`

        2. Navigate to the generated Python script and copy the content
        3. Create a method in your Airflow DAG file and call it from a PythonOperator:

            .. code-block:: python

                def run_checkpoint():
                    # paste content from the checkpoint script here

                task_run_checkpoint = PythonOperator(
                    task_id='run_checkpoint',
                    python_callable=run_checkpoint,
                    dag=dag,
                )

        Additional Resources
        --------------------

        - :ref:`Check out the detailed tutorial on Checkpoints <tutorials__getting_started__validate_your_data>`


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        Running a checkpoint with a BashOperator
        ----------------------------------------
        You can use a simple `BashOperator` in Airflow to trigger the checkpoint run. The following snippet shows an Airflow task for an Airflow DAG named `dag` that triggers the run of a checkpoint we named `my_checkpoint`:

            .. code-block:: python

                validation_task = BashOperator(
                    task_id='validation_task',
                    bash_command='great_expectations --v3-api checkpoint run my_checkpoint',
                    dag=dag
                )

        Running the `checkpoint script` output with a PythonOperator
        ------------------------------------------------------------

        Another option is to use the output of the `great_expectations --v3-api checkpoint script` command and paste it into a method that is called from a PythonOperator in the DAG. This gives you more fine-grained control over how to respond to validation results:

        1. Run `great_expectations --v3-api checkpoint script`

            .. code-block:: bash

                great_expectations --v3-api checkpoint script my_checkpoint

                ...

                A python script was created that runs the checkpoint named: `my_checkpoint`
                - The script is located in `great_expectations/uncommitted/my_checkpoint.py`
                - The script can be run with `python great_expectations/uncommitted/my_checkpoint.py`

        2. Navigate to the generated Python script and copy the content
        3. Create a method in your Airflow DAG file and call it from a PythonOperator:

            .. code-block:: python

                def run_checkpoint():
                    # paste content from the checkpoint script here

                task_run_checkpoint = PythonOperator(
                    task_id='run_checkpoint',
                    python_callable=run_checkpoint,
                    dag=dag,
                )

        Additional Resources
        --------------------

        Please see :ref:`How to configure a New Checkpoint using "test_yaml_config" <how_to_guides_how_to_configure_a_new_checkpoint_using_test_yaml_config>` for additional Checkpoint configuration and `DataContext.run_checkpoint()` examples.


.. discourse::
    :topic_identifier: 224
