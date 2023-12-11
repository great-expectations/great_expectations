---
sidebar_label: 'Manage Expectations'
title: 'Manage Expectations'
description: Create and manage Expectations in GX Cloud.
---

An Expectation is a verifiable assertion about your data. They make implicit assumptions about your data explicit, and they provide a flexible, declarative language for describing expected behavior. They can help you better understand your data and help you improve data quality. An Expectation Suite contains multiple Expectations.

To learn more about Expectations, see [Expectation](/docs/oss/terms/expectation).

## Prerequisites

- You have [set up your environment](../set_up_gx_cloud.md) and the GX Agent is running. 

- You have a [Data Asset](/docs/cloud/data_assets/manage_data_assets#create-a-data-asset).

## Available Expectations

The following table lists the available GX Cloud Expectations.

| Expectation                              | Description                                                                    | 
|------------------------------------------|--------------------------------------------------------------------------------|
| `expect_column_max_to_be_between`        | Expect the column maximum to be between a minimum and a maximum value.         | 
| `expect_column_mean_to_be_between`       | Expect the column mean to be between a minimum and a maximum value (inclusive).| 
| `expect_column_median_to_be_between`     | Expect the column median to be between a minimum and a maximum value.          | 
| `expect_column_min_to_be_between`        | Expect the column minimum to be between a minimum value and a maximum value.   | 
| `expect_column_values_to_be_in_set`      | Expect each column value to be in a given set.                                 | 
| `expect_column_values_to_be_in_type_list`| Expect a column to contain values from a specified type list.                  |
| `expect_column_values_to_be_null`        | Expect the column values to be null.                                           |
| `expect_column_values_to_be_of_type`     | Expect a column to contain values of a specified data type.                    |
| `expect_column_values_to_be_unique`      | Expect each column value to be unique.                                         |
| `expect_column_values_to_not_be_null`    | Expect the column values to not be null.                                       |
| `expect_table_columns_to_match_ordered_list` | Expect the columns to exactly match a specified list.                      |
| `expect_table_row_count_to_be_between`   | Expect the number of rows to be between two values.                            |
| `expect_table_row_count_to_equal`        | Expect the number of rows to equal a value.                                    |                                          

## Add an Expectation

1. In GX Cloud, click **Data Assets**.

2. In the **Data Assets** list, click the Data Asset name.

3. Click the **Expectations** tab.

4. Click **New Expectation**.

5. Select an Expectation type. See [Available Expectation types](#available-expectation-types).

    If you prefer to work in a code editor, or you want to configure an Expectation from the [Expectations Gallery](https://greatexpectations.io/expectations/) that isn't listed, click the **JSON Editor** tab and define your Expectation parameters in the code pane.

6. Complete the fields in the **Create Expectation** pane.

7. Click **Save**.

8. Optional. Repeat steps 1 to 4 to add additional Expectations.

9. Optional. Run a Validation. See [Run a Validation](/docs/cloud/validations/manage_validations#run-a-validation).

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