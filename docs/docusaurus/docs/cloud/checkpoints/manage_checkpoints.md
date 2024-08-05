---
sidebar_label: 'Manage Checkpoints'
title: 'Manage Checkpoints'
description: Create and manage Checkpoints in GX Cloud.
---

A Checkpoint validates Expectation Suite data. After you create a Checkpoint to validate data, you can save and reuse the Checkpoint. 

<!-- [//]: # (TODO: To learn more about Checkpoints, see Checkpoint.) -->

## Prerequisites

- You have a [Data Asset](/cloud/data_assets/manage_data_assets.md#create-a-data-asset).

- You have created an [Expectation](/cloud/expectations/manage_expectations.md#create-an-expectation).

## Add a Checkpoint

1. Run the following code to import the `great_expectations` module and the existing Data Context:

    ```python title="Python"
    import great_expectations as gx
    context = gx.get_context()
    ```
2. Run the following code to retrieve the Expectation Suite:

    ```python title="Python"
    expectation_suite = context.suites.get(name=<expectation_name>)
    ```

3. Run the following code to assign a name to the Checkpoint:

    ```python title="Python"
    checkpoint_name = <checkpoint_name> 
    ```

4. Run the following code to define the Checkpoint configuration.

    ```python title="Python"
    checkpoint_config = {
        "name": checkpoint_name,
        "validations": [{
            "expectation_suite_name": expectation_suite.name,
            "expectation_suite_id": expectation_suite.id,
            "batch_request": {
                "datasource_name": "<data_source_name>",
                "data_asset_name": "<data_asset_name>",
             },
        }],
    } 
    ```
    Replace `data_source_name` and `data_asset_name` with the names of an existing Data Source and Data Asset. If you haven't connected to a Data Source and created a Data Asset, see [Manage Data Assets](/cloud/data_assets/manage_data_assets.md).

5. Run the following code to add the Checkpoint:

    ```python title="Python"
    checkpoint = context.add_or_update_checkpoint(**checkpoint_config) 
    ```

6. Optional. Run the following code to confirm the Checkpoint name:

    ```python title="Python"
    print(checkpoint) 
    ```

7. Optional. Run the following code to run the Checkpoint:
    
    ```python title="Python"
    result = checkpoint.run() 
    ```

## Run a Checkpoint

1. In GX Cloud, click **Checkpoints**.

2. Optional. To run a Checkpoint on a failing Checkpoint, click **Failures Only**.

3. Optional. To run a specific Checkpoint, select it in the **Checkpoints** pane.

4. Click **Run Checkpoint** for the Checkpoint you want to run.


## Add a Validation to a Checkpoint

Add Validation data to a Checkpoint to aggregate individual Expectation Suite or Data Source Validations.

1. In GX Cloud, click **Checkpoints**.

2. Click **Edit Checkpoint** in the **Checkpoints** list for the Checkpoint you want to add the Validation.

3. Copy the code snippet and then close the **Edit Checkpoint** dialog.

4. Paste the code snippet into a Python interpreter and then add the following code block:

    ```python title="Python"
        validations = [
        {
            "batch_request": {
                "datasource_name": "your_datasource_name",
                "data_asset_name": "your_data_asset_name",
            },
            "expectation_suite_name": "your.expectation.suite.name",
        },
    ]
    ```
    Replace `your_datasource_name`, `your_data_asset_name`, and `your.expectation.suite.name` with your own values.

5. Optional. Repeat step 4 to add additional Validations.

6. Run the following code to update the Checkpoint configuration:

    ```python title="Python"
    checkpoint = context.add_or_update_checkpoint(**checkpoint_config) 
    ```

## Edit a Checkpoint name

1. In GX Cloud, click **Checkpoints**.

2. Click **Edit Checkpoint** in the **Checkpoints** list for the Checkpoint you want to edit.

3. Enter a new name for the Checkpoint and then click **Save**.

4. Update the Checkpoint name in all code that included the previous Checkpoint name.

## Edit a Checkpoint configuration

1. Run the following code to import the `great_expectations` module and the existing Data Context:

    ```python title="Python"
    import great_expectations as gx
    context = gx.get_context()
    ```
2. In GX Cloud, click **Checkpoints**.

3. Click **Edit Checkpoint** in the **Checkpoints** list for the Checkpoint you want to edit.

4. Copy the code snippet and then close the **Edit checkpoint** dialog.

5. Paste the the code snippet into a Python interpreter and then edit the Checkpoint configuration.

6. Run the following code to update the Checkpoint configuration:

    ```python title="Python"
    checkpoint = context.add_or_update_checkpoint(**checkpoint_config) 
    ```

## Configure the Checkpoint result format parameter 

You can use the `result_format` parameter to define the level of detail you want returned with your Validation Results. For example, you can return a success or failure message, a summary of observed values, a list of failing values, or you can add a query or a filter function that returns all failing rows. 

<!-- [//]: # (TODO: For more information, see Result format.) -->

Run the following code to apply `result_format` to every Expectation in a Suite:

```python name="docs/docusaurus/docs/snippets/result_format.py result_format_checkpoint_example"
```

Replace `my_checkpoint` and `test_suite` with your own values. You define your Checkpoint configuration below the `runtime_configuration` key. The results are stored in the Validation Result after you run the Checkpoint.

## Delete a Checkpoint

1. In GX Cloud, click **Checkpoints**.

2. Click **Delete Checkpoint** for the Checkpoint you want to delete.

3. Click **Delete**.
