---
sidebar_label: 'Manage Checkpoints'
title: 'Manage Checkpoints'
description: Create and manage Checkpoints in GX Cloud.
---

A Checkpoint validates Expectation Suite data. After you create a Checkpoint to validate data, you can save and reuse the Checkpoint. 

To learn more about Checkpoints, see [Checkpoint](../../terms/checkpoint.md).

## Prerequisites

- You have [set up your environment](../set_up_gx_cloud.md) and the GX Agent is running. 

- You have a [Data Asset](/docs/cloud/data_assets/manage_data_assets#create-a-data-asset).

- You have created an [Expectation](/docs/cloud/expectations/manage_expectations#create-an-expectation).

## Add a Checkpoint

1. In Jupyter Notebook, run the following code to import the `great_expectations` module and the existing Data Context:

    ```python title="Jupyter Notebook"
    import great_expectations as gx
    context = gx.get_context()
    ```
2. Run the following code to retrieve the Expectation Suite:

    ```python title="Jupyter Notebook"
    expectation_suite = context.get_expectation_suite(expectation_suite_name=<expectation_name>)
    ```

3. Run the following code to assign a name to the Checkpoint:

    ```python title="Jupyter Notebook"
    checkpoint_name = <checkpoint_name> 
    ```

4. Run the following code to define the Checkpoint configuration. Replace `data_source_name` and `data_asset_name` with the names of an existing Data Source and Data Asset. If you haven't connected to a Data Source and created a Data Asset, see [Manage Data Assets](/docs/cloud/data_assets/manage_data_assets).

    ```python title="Jupyter Notebook"
    checkpoint_config = {
        "name": checkpoint_name,
        "validations": [{
            "expectation_suite_name": expectation_suite.expectation_suite_name,
             "expectation_suite_ge_cloud_id": expectation_suite.ge_cloud_id,
             "batch_request": {
                "datasource_name": "<data_source_name>",
                "data_asset_name": "<data_asset_name>",
             },
        }],
    } 
    ```

5. Run the following code to add the Checkpoint:

    ```python title="Jupyter Notebook"
    checkpoint = context.add_or_update_checkpoint(**checkpoint_config) 
    ```

6. Optional. Run the following code to confirm the Checkpoint name:

    ```python title="Jupyter Notebook"
    print(checkpoint) 
    ```

7. Optional. Run the following code to run the Checkpoint:
    
    ```python title="Jupyter Notebook"
    result = checkpoint.run() 
    ```

## Run a Checkpoint

1. In GX Cloud, click **Checkpoints**.

2. Optional. To run a Checkpoint on a failing Checkpoint, click **Failures Only**.

3. Optional. To run a specific Checkpoint, select it in the **Checkpoints** pane.

4. Click **Run Checkpoint** for the Checkpoint you want to run.


## Edit a Checkpoint configuration

1. In Jupyter Notebook, run the following code to import the `great_expectations` module and the existing Data Context:

    ```python title="Jupyter Notebook"
    import great_expectations as gx
    context = gx.get_context()
    ```
2. Run the following Python code to retrieve the Checkpoint:

    ```python title="Jupyter Notebook"
    retrieved_checkpoint = context.get_checkpoint(name="<checkpoint_name>") 
    ```
3. Edit the Checkpoint configuration. 

4. Run the following code to update the Checkpoint configuration:

    ```python title="Jupyter Notebook"
    checkpoint = context.add_or_update_checkpoint(**checkpoint_config) 
    ```

## Delete a Checkpoint

1. In GX Cloud, click **Checkpoints**.

2. Click **Delete Checkpoint** for the Checkpoint you want to delete.

3. Click **Delete**.
