---
sidebar_label: 'Manage Validations'
title: 'Manage Validations'
description: Create and manage Validations in GX Cloud.
---

When you run a validation on an Expectation, a Checkpoint is added. The Checkpoint saves the Validation Results, runs the Actions you specify, and displays the Validation Results.

<!-- [//]: # (TODO: To learn more about Validations, see Validator.) -->

## Prerequisites

- You have created an [Expectation](/cloud/expectations/manage_expectations.md#create-an-expectation).

## Run a Validation

1. In GX Cloud, click **Data Assets**.

2. Click a Data Asset in the **Data Assets** list.

3. Click the **Expectations** tab and then select an Expectation Suite in the **Expectation Suites** list.

4. Click **Validate**.

5. When the confirmation message appears, click **See results**, or click the **Validations** tab and select the Validation in the **Batches & run history** pane.

6. Optional. Click **Share** to copy the URL for the Validation Results and share them with another GX Cloud user.

## Run a Validation on a Data Asset containing partitions

When you connect to a Data Asset, you can add a partition to create Expectations and run validations on subsets of Data Asset records. If you've added a partition, you can run a Validation on the latest Batch of data, or you can select a specific year, year and month, or year, month, and day period for the Validation. 

To make the identification of Data Assets containing partitions easier, partition information appears in the Data Asset **Overview** page and in the **Batches & run history** pane on the **Validations** page.

1. In GX Cloud, click **Data Assets**.

2. Click a Data Asset in the **Data Assets** list.

3. Click the **Expectations** tab and then select an Expectation Suite in the **Expectation Suites** list.

4. Click **Validate**.

5. Select one of the following options:

    - **Latest Batch** - Run the Validation on the latest Batch of data.

    - **Custom Batch** - Enter the **Year**, **Month/Year**, or the **Year/Month/Day** value to run the Validation on a Batch of data for a specific period.

6. Click **Validate**.

7. When the confirmation message appears, click **See results**, or click the **Validations** tab and select the Validation in the **Batches & run history** pane.

8. Optional. Click **Share** to copy the URL for the Validation Results and share them with another GX Cloud user.

## View Validation run history

1. In GX Cloud, click **Data Assets**.

2. Click a Data Asset in the **Data Assets** list.

3. Click the **Validations** tab.

4. Select an Expectation Suite in the **Expectation Suites** list.

5. On the **Validations** page, select one of the following options:

    - To view only run validation failures, click **Failures Only**.

    - To view the run history for specific Validation, select a Validation in the **Run History** pane.
    
    - To view the run history of all Validations, select **All Runs** to view a graph showing the Validation run history for all columns.

   :::tip

   Items in run history with a calendar icon are run on a GX-managed schedule.

   :::

6. Optional. Hover over a circle in the Validation timeline to view details about a specific Validation run, including the observed values.

    ![Validation timeline detail](/img/view_validation_timeline_detail.png)

7. Optional. To hide the Validation timeline, click the **Validation timeline** (![Validation timeline icon](/img/validation_timeline.png)) icon.

