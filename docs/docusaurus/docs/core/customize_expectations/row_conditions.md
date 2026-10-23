---
title: Restrict an Expectation to specific rows
description: Filter your data so that only a subset of rows are validated for a given Expectation.
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqPreconfiguredDataSourceAndAsset from '../_core_components/prerequisites/_data_source_and_asset_connected_to_data.md';

By default, Expectations apply to the entire dataset retrieved in a Batch. However, there are instances when an Expectation may not be relevant for every row. Validating every row could lead to false positives or false negatives in the Validation Results.

For example, you might define an Expectation that a column indicating the country of origin for a product should not be null. If this Expectation is only applicable when the product is an import, applying it to every row in the Batch could result in many false negatives when the country of origin column is null for products produced locally.

To address this issue, GX Core allows you to restrict Expectations to apply to only a subset of the data retrieved in a Batch.

## Create an Expectation with row conditions

To restrict an Expectation to a subset of the data retrieved in a Batch, use the `row_condition` argument. The `row_condition` argument takes a boolean expression built with Python objects. Rows will be validated for the Expectation when the `row_condition` expression evaluates to `True`. Conversely, if the `row_condition` evaluates to `False`, the corresponding row will not be validated for the Expectation. 

### Prerequisites

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqPreconfiguredDataContext/>.
- Recommended. <PrereqPreconfiguredDataSourceAndAsset/> for [testing your customized Expectation](/core/define_expectations/test_an_expectation.md).

### Procedure

   <Tabs queryString="verbosity" groupId="verbosity" defaultValue='instructions' values={[{label: 'Instructions', value:'instructions'}, {label: 'Sample code', value:'sample_code'}]}>

   <TabItem value="instructions" label="Instructions">

   1. Determine the `row_condition` expression.

      To support complex business use cases, logical clauses can be combined with AND / OR relationships within the `row_condition` argument.

      ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/row_conditions.py - determine expression"
      ```

      Here are some examples of how to create common patterns in row conditions:

      - **A and B**.
         ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/row_conditions.py - a and b"
         ```

      - **A or B**.
         ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/row_conditions.py - a or b"
         ```

      - **(A and B) or (C and D)**.
         ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/row_conditions.py - a and b or c and d"
         ```

      - **A and (B or C)**. This pattern is not supported verbatim, but you can achieve the same result with **(A and B) or (A and C)**.
         ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/row_conditions.py - a and b or c"
         ```

      The following comparison operators are supported: `==`, `!=`, `>`, `<`, `>=`, `<=`, `is_in`, `is_not_in`, `is_null`, `is_not_null`. Here are some examples of using different kinds of operators:

      ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/row_conditions.py - operators"
      ```

      A null value is neither equal nor unequal to another value, so `!=` and `is_not_in` don't select rows where the column is null. To include those rows, combine the comparison with `is_null`, for example `(Column("department") != "Sales") | Column("department").is_null()`.

   2. Configure the Expectation.

      ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/row_conditions.py - add Expectation"
      ```

   3. Optional. Configure additional variations of the Expectation.

      Expectations that have different row conditions are treated as unique, even if they are of the same type, apply to the same column, and belong to the same Expectation Suite. This allows you to validate your data through multiple lenses.

      For instance, the following code establishes an Expectation that the value in the `cycle_type` column is either `unicycle`, `bicycle`, or `tricycle`.

      ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/row_conditions.py - Expectation without row conditions"
      ```

      While, for example, the following code creates an Expectation that the value of the `cycle_type` column is `unicycle` if the item has one wheel.

      ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/row_conditions.py - Expectation with row conditions"
      ```

   Now you can [add your Expectations to an Expectation Suite](/docs/core/define_expectations/organize_expectation_suites/).

   </TabItem>

   <TabItem value="sample_code" label="Sample code">

   ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/row_conditions.py - full code example"
   ```

   </TabItem>

   </Tabs>


## Row conditions in Data Docs

If an Expectation has row conditions, this will be indicated in the Data Docs. Each Expectation with row conditions is prefaced with **if `row_condition`, then values must ...** as illustrated in the following example:

![if PClass=="1st", then values must belong to this set: 1.](/docs/oss/images/conditional_data_docs_screenshot.png)

If the `row_condition` is a complex expression, it will be divided into several components to enhance readability.

## Scope and limitations

Keep the following in mind when working with row conditions:
- An Expectation can have up to 100 condition statements grouped in any number of condition blocks. 
- The following Expectations do not accept the `row_condition` argument:
   - `expect_column_to_exist`
   - `expect_query_results_to_match_comparison`
   - `expect_table_columns_to_match_ordered_list`
   - `expect_table_columns_to_match_set`
   - `expect_table_column_count_to_be_between`
   - `expect_table_column_count_to_equal`
   - `unexpected_rows_expectation` 
