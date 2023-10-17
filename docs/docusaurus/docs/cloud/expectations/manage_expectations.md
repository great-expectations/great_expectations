---
sidebar_label: 'Manage Expectations'
title: 'Manage Expectations'
description: Create and manage Expectations in GX Cloud.
---

An Expectation is a verifiable assertion about your data. They make implicit assumptions about your data explicit, and they provide a flexible, declarative language for describing expected behavior. They can help you better understand your data and help you improve data quality.

To learn more about Expectations, see [Expectation](../../terms/expectation.md).

## Prerequisites

- You have [set up your environment](../set_up_gx_cloud.md) and the GX Agent is running. 

- You have a [Data Asset](/docs/cloud/data_assets/manage_data_assets#create-a-data-asset).

## Create an Expectation

1. In GX Cloud, click **Data Assets**.

2. In the **Data Assets** list, click the Data Asset name.

3. Click the **Expectations** tab.

4. Click **New Expectation**.

5. Select an Expectation type, enter the column name, and then complete the optional fields.

    If you prefer to work in a code editor, or you want to configure an Expectation from the [Expectations Gallery](https://greatexpectations.io/expectations/), click the **JSON Editor** tab and define your Expectation parameters in the code pane.

6. Click **Save**.

7. Optional. Repeat steps 1 to 4 to add additional Expectations.

8. Optional. Run a Validation. See [Run a Validation](/docs/cloud/validations/manage_validations#run-a-validation).

## Edit an Expectation

1. In GX Cloud, click **Data Assets**.

2. In the **Data Assets** list, click the Data Asset name.

3. Click the **Expectations** tab.

4. Click **Edit Expectations** for the Expectation that you want to edit.

5. Edit the Expectation configuration.

    If you prefer to work in a code editor, or you configured an Expectation from the [Expectations Gallery](https://greatexpectations.io/expectations/), click the **JSON Editor** tab and edit the Expectation parameters in the code pane.

6. Click **Save**.

## Delete an Expectation

1. In GX Cloud, click **Data Assets**.

2. In the **Data Assets** list, click the Data Asset name.

3. Click the **Expectations** tab.

4. Click **Delete Expectation** for the Expectation you want to delete. 

5. Click **Yes, delete Expectation**. 

## Related documentation

- [Manage Expectation Suites](../expectation_suites/manage_expectation_suites.md)