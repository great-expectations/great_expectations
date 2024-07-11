---
sidebar_label: 'Manage Expectation Suites'
title: 'Manage Expectation Suites'
description: Create and manage Expectation Suites in GX Cloud.
---

Expectation Suites contain multiple Expectations for a single Data Asset. Like Expectations, they can help you better understand your data and help you improve data quality. A default Expectation Suite is created when you create a Data Asset. 

<!-- [//]: # (TODO: To learn more about Expectation Suites, see Expectation Suites.) -->

## Prerequisites

- You have deployed the GX Agent. See [Deploy the GX Agent](../deploy_gx_agent.md).

- You have a [Data Asset](/cloud/data_assets/manage_data_assets.md#create-a-data-asset).

## Create an Expectation Suite

If you have specific business requirements, or you want to examine specific data, you can create an empty Expectation Suite and then add Expectations individually.

1. In GX Cloud, click **Data Assets** and select a Data Asset in the **Data Assets** list.

2. Click the **Expectations** tab.

3. Click **New Suite** in the **Expectation Suites** pane.

4. Enter a name for the Expectation Suite in the **Expectation Suite name** field.

5. Select a validation schedule frequency and start time for your Expectation Suite.

6. Click **Create Suite**. 

7. Add Expectations to the Expectation Suite. See [Create an Expectation](/cloud/expectations/manage_expectations.md#create-an-expectation).

8. Optional. Run a Validation on the Expectation Suite. See [Run a Validation](/cloud/validations/manage_validations.md#run-a-validation).

## Edit an Expectation Suite

1. In GX Cloud, click **Data Assets**.

2. Click the Expectations tab.

3. Select an Expectation Suite in the Expectation Suites list.

4. Click **Edit** on the schedule component for the Expectation Suite you want to edit.

5. Edit the Expectation Suite name or validation schedule and then click **Save**.

5. Optional. If the Expectation Suite name was changed, update the name in all code that included the previous Expectation Suite name.

## Delete an Expectation Suite

1. In GX Cloud, delete all Checkpoints associated with the Expectation Suite. See [Delete a Checkpoint](/cloud/checkpoints/manage_checkpoints.md#delete-a-checkpoint). 

2. Click **Expectation Suites**.

3. Click **Delete** for the Expectation Suite you want to delete.

4. Click **Delete**.

## Related documentation

- [Manage Expectations](../expectations/manage_expectations.md)

- [Manage Validations](../validations/manage_validations.md)