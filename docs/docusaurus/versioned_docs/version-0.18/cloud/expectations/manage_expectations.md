---
sidebar_label: "Manage Expectations"
title: "Manage Expectations"
description: Create and manage Expectations in GX Cloud.
---

An Expectation is a verifiable assertion about your data. They make implicit assumptions about your data explicit, and they provide a flexible, declarative language for describing expected behavior. They can help you better understand your data and help you improve data quality. An Expectation Suite contains multiple Expectations.

:::info Custom SQL Query Expectations

To create custom SQL query Expectations, you'll need to use the GX API. See [Create a Custom Query Expectation](/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations.md).

:::

To learn more about Expectations, see [Expectation](/reference/learn/terms/expectation.md).

## Prerequisites

- You have a [Data Asset](/cloud/data_assets/manage_data_assets.md#create-a-data-asset).

## Available Expectations

The following table lists the available GX Cloud Expectations.

| Data Quality Issue | Expectation                                               | Description                                                                                                                            |
| ------------------ | --------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Cardinality        | `expect_column_values_to_be_unique`                       | Expect each column value to be unique.                                                                                                 |
| Cardinality        | `expect_compound_columns_to_be_unique`                    | Expect the compound columns to be unique.                                                                                              |
| Cardinality        | `expect_select_column_values_to_be_unique_within_record`  | Expect the values for each record to be unique across the columns listed. Note that records can be duplicated.                         |
| Cardinality        | `expect_column_proportion_of_unique_values_to_be_between` | Expect the proportion of unique values to be between a minimum value and a maximum value.                                              |
| Cardinality        | `expect_column_unique_value_count_to_be_between`          | Expect the number of unique values to be between a minimum value and a maximum value.                                                  |
| Data Integrity     | `expect_column_pair_values_to_be_equal`                   | Expect the values in column A to be the same as column B.                                                                              |
| Data Integrity     | `expect_multicolumn_sum_to_equal`                         | Expect that the sum of row values in a specified column list is the same for each row, and equal to a specified sum total.             |
| Distribution       | `expect_column_pair_values_A_to_be_greater_than_B`        | Expect the values in column A to be greater than column B.                                                                             |
| Distribution       | `expect_column_values_to_be_between`                      | Expect the column entries to be between a minimum value and a maximum value (inclusive).                                               |
| Distribution       | `expect_column_z_scores_to_be_less_than`                  | Expect the Z-scores of a column's values to be less than a given threshold.                                                            |
| Distribution       | `expect_column_stdev_to_be_between`                       | Expect the column standard deviation to be between a minimum value and a maximum value.                                                |
| Distribution       | `expect_column_sum_to_be_between`                         | Expect the column sum to be between a minimum value and a maximum value.                                                               |
| Missingness        | `expect_column_values_to_be_null`                         | Expect the column values to be null.                                                                                                   |
| Missingness        | `expect_column_values_to_not_be_null`                     | Expect the column values to not be null.                                                                                               |
| Numerical Data     | `expect_column_max_to_be_between`                         | Expect the column maximum to be between a minimum and a maximum value.                                                                 |
| Numerical Data     | `expect_column_mean_to_be_between`                        | Expect the column mean to be between a minimum and a maximum value (inclusive).                                                        |
| Numerical Data     | `expect_column_median_to_be_between`                      | Expect the column median to be between a minimum and a maximum value.                                                                  |
| Numerical Data     | `expect_column_min_to_be_between`                         | Expect the column minimum to be between a minimum value and a maximum value.                                                           |
| Pattern matching   | `expect_column_value_length_to_equal`                     | Expect the column entries to be strings with length between a minimum value and a maximum value (inclusive).                           |
| Pattern matching   | `expect_column_value_length_to_be_between`                | Expect the column entries to be strings with length between a minimum value and a maximum value (inclusive).                           |
| Pattern matching   | `expect_column_values_to_match_like_pattern`              | Expect the column entries to be strings that match a given like pattern expression.                                                    |
| Pattern matching   | `expect_column_values_to_match_like_pattern_list`         | Expect the column entries to be strings that match any of a provided list of like pattern expressions.                                 |
| Pattern matching   | `expect_column_values_to_match_regex`                     | Expect the column entries to be strings that match a given regular expression.                                                         |
| Pattern matching   | `expect_column_values_to_match_regex_list`                | Expect the column entries to be strings that can be matched to either any of or all of a list of regular expressions.                  |
| Pattern matching   | `expect_column_values_to_not_match_like_pattern`          | Expect the column entries to be strings that do NOT match a given like pattern expression.                                             |
| Pattern matching   | `expect_column_values_to_not_match_like_pattern_list`     | Expect the column entries to be strings that do NOT match any of a provided list of like pattern expressions.                          |
| Pattern matching   | `expect_column_values_to_not_match_regex`                 | Expect the column entries to be strings that do NOT match a given regular expression.                                                  |
| Pattern matching   | `expect_column_values_to_not_match_regex_list`            | Expect the column entries to be strings that do not match any of a list of regular expressions. Matches can be anywhere in the string. |
| Schema             | `expect_column_to_exist`                                  | Checks for the existence of a specified column within a table.                                                                         |
| Schema             | `expect_column_values_to_be_in_type_list`                 | Expect a column to contain values from a specified type list.                                                                          |
| Schema             | `expect_column_values_to_be_of_type`                      | Expect a column to contain values of a specified data type.                                                                            |
| Schema             | `expect_table_column_count_to_be_between`                 | Expect the number of columns in a table to be between two values.                                                                      |
| Schema             | `expect_table_column_count_to_equal`                      | Expect the number of columns in a table to equal a value.                                                                              |
| Schema             | `expect_table_columns_to_match_ordered_list`              | Expect the columns in a table to exactly match a specified list.                                                                       |
| Schema             | `expect_table_columns_to_match_set`                       | Expect the columns in a table to match an unordered set.                                                                               |
| Sets               | `expect_column_values_to_be_in_set`                       | Expect each column value to be in a given set.                                                                                         |
| Sets               | `expect_column_values_to_not_be_in_set`                   | Expect column entries to not be in the set.                                                                                            |
| Sets               | `expect_column_distinct_values_to_be_in_set`              | Expect the set of distinct column values to be contained by a given set.                                                               |
| Sets               | `expect_column_distinct_values_to_contain_set`            | Expect the set of distinct column values to contain a given set.                                                                       |
| Sets               | `expect_column_distinct_values_to_equal_set`              | Expect the set of distinct column values to equal a given set.                                                                         |
| Sets               | `expect_column_most_common_value_to_be_in_set`            | Expect the most common value to be within the designated value set.                                                                    |
| Volume             | `expect_table_row_count_to_be_between`                    | Expect the number of rows to be between two values.                                                                                    |
| Volume             | `expect_table_row_count_to_equal`                         | Expect the number of rows to equal a value.                                                                                            |
| Volume             | `expect_table_row_count_to_equal_other_table`             | Expect the number of rows to equal the number in another table within the same database.                                               |

## Add an Expectation

1. In GX Cloud, click **Data Assets**.

2. In the **Data Assets** list, click the Data Asset name.

3. Click the **Expectations** tab.

4. Click **New Expectation**.

5. Select an Expectation type. See [Available Expectations](#available-expectations).

6. If you are adding your first expectation on this data asset, you may be able to select a time-based Batch interval for that asset.

- A batch is a feature of the data asset, and allows you to validate your data incrementally. A batch interval can only be defined once per data asset.

- In order to be able to select a batch interval, the data asset must have at least one DATE or DATETIME column.

- Select the **Entire table** tab to provide all Data Asset records to your Expectations and validations, or select the **Yearly**/**Monthly**/**Daily** tab to use subsets of Data Asset records for your Expectations and validations.

- Select **Yearly** to partition Data Asset records by year, select **Monthly** to partition Data Asset records by year and month, or select **Daily** to partition Data Asset records by year, month, and day.

- **Batch column** - Select a name column from a prefilled list of DATE and DATETIME columns containing the date and time data.

7. Complete the mandatory and optional fields for the Expectation. A recurring validation schedule will be applied automatically to your Expectation, based on the settings of your Expectation Suite.

8. Click **Save** or click **Save & Add More** and then repeat steps 4 and 5 to add additional Expectations.

9. Optional. Run a Validation. See [Run a Validation](/cloud/validations/manage_validations.md#run-a-validation).

## Edit an Expectation

1. In GX Cloud, click **Data Assets**.

2. In the **Data Assets** list, click the Data Asset name.

3. Click the **Expectations** tab.

4. Click **Edit Expectations** for the Expectation that you want to edit.

5. Edit the Expectation configuration.

6. Click **Save**.

## View Expectation history

View the Expectation history to determine when an Expectation was changed and who made the change.

1. In GX Cloud, click **Expectation Suites**.

2. In the **Expectation Suites** list, click the Expectation Suite name.

3. Click the **Change Log** tab.

4. Optional. Select an Expectation in the **Columns** pane to view the change history for a specific Expectation.

   The date, time, and email address of the users who created, edited, or deleted the Expectation appears below the Expectation name. Strikethrough text indicates an Expectation was deleted.

## Delete an Expectation

1. In GX Cloud, click **Data Assets**.

2. In the **Data Assets** list, click the Data Asset name.

3. Click the **Expectations** tab.

4. Click **Delete Expectation** for the Expectation you want to delete.

5. Click **Yes, delete Expectation**.

## Related documentation

- [Manage Expectation Suites](../expectation_suites/manage_expectation_suites.md)
