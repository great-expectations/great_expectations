.. _how_to_guides__validation__how_to_run_a_checkpoint_in_airflow:

How to run a Checkpoint in Airflow
==================================

This guide will help you run a Great Expectations checkpoint in Apache Airflow, which allows you to trigger validation of a data asset using an Expectation Suite directly within an Airflow DAG.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

- :ref:`Set up a working deployment of Great Expectations <getting_started>`
- :ref:`Created an Expectation Suite <how_to_guides__creating_and_editing_expectations__how_to_create_a_new_expectation_suite_using_the_cli>`
- :ref:`Created a checkpoint for that Expectation Suite and a data asset <how_to_guides__validation__how_to_create_a_new_checkpoint>`
- Created an Airflow DAG file

Using checkpoints is the most straightforward way to trigger a validation run from within Airflow. There are multiple ways to achieve this, but we suggest using a simple `BashOperator` in Airflow to trigger the checkpoint run. The following snippet shows an Airflow task for an Airflow DAG named `dag` that triggers the run of a checkpoint we named `my_checkpoint`:

    .. code-block:: python
    validation_task = BashOperator(
        task_id='validation_task',
        bash_command='great_expectations checkpoint run my_checkpoint',
        dag=dag
    )

Additional Resources
--------------

- :ref:`Check out the detailed tutorial on Checkpoints <tutorials__getting_started__set_up_your_first_checkpoint>`


.. discourse::
    :topic_identifier: 224
