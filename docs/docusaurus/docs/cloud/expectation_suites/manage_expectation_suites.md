---
sidebar_label: 'Manage Expectation Suites'
title: 'Manage Expectation Suites'
description: Create and manage Expectation Suites in GX Cloud.
---

Expectation Suites contain multiple Expectations for a single Data Asset. Like Expectations, they can help you better understand your data and help you improve data quality. A default Expectation Suite is created when you create a Data Asset.

To learn more about Expectation Suites, see [Expectation Suites](../../terms/expectation_suite.md).

## Prerequisites

- You have [set up your environment](../set_up_gx_cloud.md). 

- You have a [Data Asset](/docs/cloud/data_assets/manage_data_assets#create-a-data-asset).

## Create an Expectation Suite with the Missingness Data Assistant

Use the Missingness Data Assistant to quickly create an Expectation Suite that you can use to determine if your Data Asset contains missing data (null values). Creating Expectation Suites with the Missingness Data Assistant saves you writing and then running the same code for each column in your Data Asset.

1. In GX Cloud, click **Data Assets** and select a Data Asset in the **Data Assets** list.

2. Click the **Expectations** tab.

3. Click **Create New Suite** in the **Expectation Suites** pane.

4. Click **Missingness** and then enter a name for the Expectation Suite in the **Suite name** field.

5. Click **Generate Expectations**. 

    A new Missingness Expectation Suite appears in the Expectation Suites pane.

6. Optional. Run a Validation on the Expectation Suite. See [Run a Validation](/docs/cloud/validations/manage_validations#run-a-validation).

## Delete an Expectation Suite

1. In GX Cloud, delete all Checkpoints associated with the Expectation Suite. See [Delete a Checkpoint](/docs/cloud/checkpoints/manage_checkpoints#delete-a-checkpoint). 

2. Click **Expectation Suites**.

3. Click **Delete** for the Expectation Suite you want to delete.

4. Click **Delete**.

## Related documentation

- [Manage Expectations](../expectations/manage_expectations.md)

- [Manage Validations](../validations/manage_validations.md)
