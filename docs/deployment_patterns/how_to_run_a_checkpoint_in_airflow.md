---
title: How to run a Checkpoint in Airflow
---
import Prerequisites from '../guides/connecting_to_your_data/components/prerequisites.jsx'

This guide will help you run a Great Expectations checkpoint in Apache Airflow, which allows you to trigger validation of a data asset using an Expectation Suite directly within an Airflow DAG.

<Prerequisites>

- [Set up a working deployment of Great Expectations](../tutorials/getting_started/intro.md)
- [Created an Expectation Suite](../tutorials/getting_started/create_your_first_expectations.md)
- [Created a checkpoint for that Expectation Suite and a data asset](../guides/validation/checkpoints/how_to_create_a_new_checkpoint.md)
- Created an Airflow DAG file

</Prerequisites>

Using checkpoints is the most straightforward way to trigger a validation run from within Airflow. 

The following sections describe two alternative approaches to accomplishing this:
 1. [Running a checkpoint with a BashOperator](#option-1-running-a-checkpoint-with-a-bashoperator)
 2. [Running the `checkpoint script` output with a PythonOperator](#option-2-running-the-checkpoint-script-output-with-a-pythonoperator)

Steps
-----

### Option 1: Running a checkpoint with a BashOperator

You can use a simple `BashOperator` in Airflow to trigger the checkpoint run. The following snippet shows an Airflow task for an Airflow DAG named `dag` that triggers the run of a checkpoint we named `my_checkpoint`:

```python
validation_task = BashOperator(
    task_id='validation_task',
    bash_command='great_expectations --v3-api checkpoint run my_checkpoint',
    dag=dag
)
```

### Option 2: Running the `checkpoint script` output with a PythonOperator

Another option is to use the output of the `great_expectations --v3-api checkpoint script` command and paste it into a method that is called from a PythonOperator in the DAG. This gives you more fine-grained control over how to respond to Validation Results:

1. Run `great_expectations --v3-api checkpoint script`

    ```bash
    great_expectations --v3-api checkpoint script my_checkpoint

    ...

    A Python script was created that runs the checkpoint named: `my_checkpoint`
    - The script is located in `great_expectations/uncommitted/my_checkpoint.py`
    - The script can be run with `python great_expectations/uncommitted/my_checkpoint.py`
    ```

2. Navigate to the generated Python script and copy the content
3. Create a method in your Airflow DAG file and call it from a PythonOperator:

    ```python
    def run_checkpoint():
        # paste content from the checkpoint script here

    task_run_checkpoint = PythonOperator(
        task_id='run_checkpoint',
        python_callable=run_checkpoint,
        dag=dag,
    )
    ```

Additional Resources
--------------------

Please see [How to configure a New Checkpoint using "test_yaml_config"](../guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config.md) for additional Checkpoint configuration and `DataContext.run_checkpoint()` examples.

