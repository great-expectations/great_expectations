---
title: Apply Conditional Expectations to specific rows within a Batch
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqPreconfiguredDataSourceAndAsset from '../_core_components/prerequisites/_data_source_and_asset_connected_to_data.md';

By default Expectations apply to the entire dataset retrieved in a Batch.  However, sometimes an Expectation is not relevant for every row and validating every row could cause false positives or false negatives in the Validation Results.

For example, you may define an Expectation that a column specifying the country of origin of a product should not be null.  If that Expectation is only relevant when the product is a foreign import then applying the Expectation to every row in the Batch could result in a large number of false negatives when the country of origin column is null for products produced by local industry.

To solve this issue GX allows you to define Conditional Expectations that only apply to a subset of the data retrieved in a Batch.

## Create a Conditional Expectation

Great Expectations lets you express Conditional Expectations with a `row_condition` argument that can be passed to all Expectations that evaluate rows within a Dataset.  The `row_condition` argument should be a boolean expression string.  The Conditional Expectation will validate rows that result in the `row_condition` string being `True`.  When the `row_condition` string evaluates as `False`, the row in question will not be validated by the Expectation.

### Prerequisites

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqPreconfiguredDataContext/>.
- Recommended. <PrereqPreconfiguredDataSourceAndAsset/> for [testing your customized Expectation](/core/define_expectations/test_an_expectation.md).

### Procedure

<Tabs 
   queryString="procedure"
   defaultValue="instructions"
   values={[
      {value: 'instructions', label: 'Instructions'},
      {value: 'sample_code', label: 'Sample code'}
   ]}
>

<TabItem value="instructions" label="Instructions">

In this procedure, your Data Context is assumed to be stored in the variable `context` and your Expectation Suite is assumed to be stored in the variable `suite`.  `suite` can be a newly created and empty Expectation Suite, or an existing Expectation Suite retrieved from the Data Context.

The data used in the examples for this procedure is passenger data for the Titanic, including what class of ticket the passenger held and whether or not they survived the journey.

1. Determine the `condition_parser` for your `row_condition`.

   The `condition_parser` defines the syntax of `row_condition` strings.  When implementing conditional Expectations with pandas, this argument must be set to `"pandas"`. When implementing conditional Expectations with Spark or SQLAlchemy, this argument must be set to `"great_expectations"`.

   Conditional Expectations will fail if the Batch they are validating comes from a different type of Data Source than is indicated by the `condition_parser` argument.

2. Determine the `row_condition` expression.

   The `row_condition` argument should be a boolean expression string which will be evaluated for each row in the Batch the Expectation validates.  When the `row_condition` evaluates as `True` the row will be included in the Expectation's validations.  When the `row_condition` evaluates as `False`, the Expectation will be skipped for that row.

   The syntax of the `row_condition` argument is based on the `condition_parser` that was previously specified.  

   <Tabs queryString="condition_parser" groupId="condition_parser" defaultValue='pandas' values={[{label: 'pandas', value:'pandas'}, {label: 'Spark/SQL', value:'spark_sql'}]}>
   
   <TabItem value="pandas" label="pandas">
   
      In pandas the `row_condition` value is passed to `pandas.DataFrame.query()` before Expectation Validation and the returned rows from the evaluated Batch will be validated by the Conditional Expectation. 

   </TabItem>

   <TabItem value="spark_sql" label="Spark/SQL">

      In Spark and SQLAlchemy, the `row_condition` value uses SQL syntax and is parsed as a data filter or a query before Expectation Validation.

   </TabItem>

   </Tabs>


3. Create a Conditional Expectation.

   A Conditional Expectation is created exactly like a regular Expectation, except that the `row_condition` and `condition_parser` parameters are provided in addition to the Expectation's other arguments.

   <Tabs queryString="condition_parser" groupId="condition_parser" defaultValue='pandas' values={[{label: 'pandas', value:'pandas'}, {label: 'Spark/SQL', value:'spark_sql'}]}>
   
   <TabItem value="pandas" label="pandas">

      ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/expectation_row_conditions.py - pandas example row_condition"
      ```

   </TabItem>

   <TabItem value="spark_sql" label="Spark/SQL">

      ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/expectation_row_conditions.py - spark example row_condition"
      ```

   </TabItem>

   </Tabs>

   Do not use single quotes, newlines, or `\n` inside the specified `row_condition` as shown in the following examples:

   ```python title="Python" 
   row_condition = "PClass=='1st'"  # Don't do this. Single quotes aren't valid!
   
   row_condition="""
   PClass=="1st"
   """  # Don't do this.  Newlines and \n aren't valid!
   
   row_condition = 'PClass=="1st"'  # Do this instead.
   ```

   <Tabs className="hidden" queryString="condition_parser" groupId="condition_parser" defaultValue='pandas' values={[{label: 'pandas', value:'pandas'}, {label: 'Spark/SQL', value:'spark_sql'}]}>
   
   <TabItem value="pandas" label="pandas">

      With pandas you can indicate variables from the environment by prefacing them with `@`.  You can also indicate columns with a space in their name by wrapping the name with backticks: `` ` ``.

      Some examples of valid `row_condition` values for pandas include:

      ```python title="Python"
      row_condition = '`foo foo`=="bar bar"'  # The value of the column "foo foo" is "bar bar"
   
      row_condition = 'foo==@bar'  # the value of the foo field is equal to the value of the bar environment variable
      ```

      For more information on the syntax accepted by pandas `row_condition` values see [pandas.DataFrame.query](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html).

   </TabItem>

   <TabItem value="spark_sql" label="Spark/SQL">

      For Spark and SQL, you will also want to specify your columns using the `col()` function. 

      Some examples of valid `row_condition` values for Spark and SQL include: 
    
      ```python title="Python"
      row_condition='col("foo") == "Two  Two"'  # foo is 'Two Two'
    
      row_condition='col("foo").notNull()'  # foo is not null
    
      row_condition='col("foo") > 5'  # foo is greater than 5
    
      row_condition='col("foo") <= 3.14'  # foo is less than 3.14
    
      row_condition='col("foo") <= date("2023-03-13")'  # foo is earlier than 2023-03-13
    
      ```

   </TabItem>

   </Tabs>

4. Optional. Create additional Conditional Expectations.

   Expectations with different conditions are treated as unique even if they are of the same type and apply to the same column within an Expectation Suite.  This allows you to create one unconditional Expectation and an arbitrary number of Conditional Expectations (each with a different condition).  

   For example, the following code creates a unconditional Expectation that the value of the `"Survived"` column is either 0 or 1:

   ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/expectation_row_conditions.py - example unconditional Expectation"
   ```

   And this code creates a Conditional version of the same Expectation that specifies the value of the `"Survived"` column is `1` if the individual was a first class passenger:

   ```python title="Python" name="docs/docusaurus/docs/core/customize_expectations/_examples/expectation_row_conditions.py - example conditional Expectation"
   ```


</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python"
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()

suite = context.suites.add(ExpectationSuite(name="my_expectation_suite"))

expectation = suite.add_expectation(
   gxe.ExpectColumnValuesToBeInSet(
      column="Survived",
      value_set=[0, 1]
   )
)

conditional_expectation = suite.add_expectation(
   gxe.ExpectColumnValuesToBeInSet(
      column='Survived',
      value_set=[1],
      condition_parser='pandas',
      row_condition='PClass=="1st"'
   )
)
```

</TabItem>

</Tabs>

## Data Docs and Conditional Expectations

Conditional Expectations are displayed differently from standard Expectations in the Data Docs. Each Conditional Expectation is qualified with *if 'row_condition_string', then values must be...* as shown in the following image:

![Image](/docs/oss/images/conditional_data_docs_screenshot.png)

If *'row_condition_string'* is a complex expression, it is split into several components to improve readability.

## Scope and limitations

While conditions can be attached to most Expectations, the following Expectations cannot be conditioned and do not take the `row_condition` argument:

* `expect_column_to_exist`
* `expect_table_columns_to_match_ordered_list`
* `expect_table_column_count_to_be_between`
* `expect_table_column_count_to_equal`


