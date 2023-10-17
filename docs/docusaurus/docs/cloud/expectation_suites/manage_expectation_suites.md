---
sidebar_label: 'Manage Expectation Suites'
title: 'Manage Expectation Suites'
description: Create and manage Expectation Suites in GX Cloud.
---

Expectation Suites contain multiple Expectations for a single Data Asset. Like Expectations, they can help you better understand your data and help you improve data quality. A default Expectation Suite is created when you create a Data Asset. 

To learn more about Expectation Suites, see [Expectation Suites](../../terms/expectation_suite.md).

## Prerequisites

- You have [set up your environment](../set_up_gx_cloud.md) and the GX Agent is running. 

- You have a [Data Asset](/docs/cloud/data_assets/manage_data_assets#create-a-data-asset).

## Automatically create an Expectation Suite that tests for missing data

Automatically create an Expectation Suite that you can use to determine if your Data Asset contains missing data (null values). Creating Expectation Suites automatically saves you writing and then running the same code for each column in your Data Asset.

1. In GX Cloud, click **Data Assets** and select a Data Asset in the **Data Assets** list.

2. Click the **Expectations** tab.

3. Click **Create New Suite** in the **Expectation Suites** pane.

4. Click **Missingness** and then enter a name for the Expectation Suite in the **Suite name** field.

5. Click **Generate Expectations**. 

    It can take several minutes to create the Expectation Suite. When the process is complete, a new Expectation Suite appears in the **Expectation Suites** pane.

6. Optional. Run a Validation on the Expectation Suite. See [Run a Validation](/docs/cloud/validations/manage_validations#run-a-validation).

## Manually create an empty Expectation Suite 

1. In Jupyter Notebook, run the following code to import the `great_expectations` module and the existing Data Context:

    ```python title="Jupyter Notebook"
    import great_expectations as gx
    context = gx.get_context()
    ```

2. Run the following code to create an empty Expectation Suite:

    ```python title="Jupyter Notebook"
    expectation_suite = context.add_expectation_suite(
    expectation_suite_name="<expectation_suite_name>"
    )
    ```
    Replace `<expectation_suite_name>` with a meaningful name for the Expectation Suite.

3. Add Expectations to the Expectation Suite. See [Create an Expectation](/docs/cloud/expectations/manage_expectations#create-an-expectation).

4. Optional. Run a Validation on the Expectation Suite. See [Run a Validation](/docs/cloud/validations/manage_validations#run-a-validation).

## Delete an Expectation Suite

1. In GX Cloud, delete all Checkpoints associated with the Expectation Suite. See [Delete a Checkpoint](/docs/cloud/checkpoints/manage_checkpoints#delete-a-checkpoint). 

2. Click **Expectation Suites**.

3. Click **Delete** for the Expectation Suite you want to delete.

4. Click **Delete**.

## Related documentation

- [Manage Expectations](../expectations/manage_expectations.md)

- [Manage Validations](../validations/manage_validations.md)