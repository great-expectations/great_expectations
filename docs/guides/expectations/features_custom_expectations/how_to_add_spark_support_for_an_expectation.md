---
title: How to add Spark support for Custom Expectations
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you implement native Spark support for your [Custom Expectation](../creating_custom_expectations/overview.md). 

<Prerequisites>

 - Created a [Custom Expectation](../creating_custom_expectations/overview.md)
    
</Prerequisites>

Great Expectations supports a number of [Execution Engines](../../../reference/execution_engine.md), including a Spark Execution Engine. 
These Execution Engines provide the computing resources used to calculate the [Metrics](../../../reference/metrics.md) defined in the Metric class of your Custom Expectation.

If you decide to contribute your Expectation, its entry in the [Expectations Gallery](https://greatexpectations.io/expectations/) will reflect the Execution Engines that it supports.

We will add Spark support for the Custom Expectations implemented in our guides on [how to create Custom Column Aggregate Expectations](../creating_custom_expectations/how_to_create_custom_column_aggregate_expectations.md) 
and [how to create Custom Column Map Expectations](../creating_custom_expectations/how_to_create_custom_column_map_expectations.md).

## Steps

### 1. Specify your backends

To avoid surprises and help clearly define your Custom Expectation, it can be helpful to determine beforehand what backends you plan to support, and test them along the way.

Within the `examples` defined inside your Expectation class, the `test_backends` key specifies which backends and SQLAlchemy dialects to run tests for. Add entries corresponding to the functionality you want to add: 
    
```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L86-L132
```

:::note
You may have noticed that specifying `test_backends` isn't required for successfully testing your Custom Expectation.

If not specified, Great Expectations will attempt to determine the implemented backends automatically, but wll only run SQLAlchemy tests against sqlite.
:::

### 2. Implement the Spark logic for your Custom Expectation

Great Expectations provides a variety of ways to implement an Expectation in Spark. Two of the most common include: 
1.  Defining a partial function that takes a Spark DataFrame column as input
2.  Directly executing queries on Spark DataFrames to determine the value of your Expectation's metric directly 

<Tabs
  groupId="-type"
  defaultValue='partialfunction'
  values={[
  {label: 'Partial Function', value:'partialfunction'},
  {label: 'Query Execution', value:'queryexecution'},
  ]}>

<TabItem value="partialfunction">

Great Expectations allows for much of the PySpark DataFrame logic to be abstracted away by specifying metric behavior as a partial function. 
To do this, we use one of the `@column_*_partial` decorators:
- `@column_aggregate_partial` for Column Aggregate Expectations
- `@column_condition_partial` for Column Map Expectations
- `@column_pair_condition_partial` for Column Pair Map Expectations
- `@multicolumn_condition_partial` for Multicolumn Map Expectations

These decorators expect an appropriate `engine` argument. In this case, we'll pass our `SparkDFExecutionEngine`.
The decorated method takes in a Spark `Column` object and will either return a `pyspark.sql.functions.function` or a `pyspark.sql.Column.function` that Great Expectations will use to generate the appropriate SQL queries.

For our Custom Column Aggregate Expectation `ExpectColumnMaxToBeBetweenCustom`, we're going to leverage PySpark's `max` SQL Function and the `@column_aggregate_partial` decorator.

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L76-L79
```

If we need a builtin function from `pyspark.sql.functions`, usually aliased to `F`, the import logic in 
`from great_expectations.expectations.metrics.import_manager import F`
allows us to access these functions even when PySpark is not installed.

<details>
<summary>Applying Python Functions</summary>
<code>F.udf</code> allows us to use a Python function as a Spark User Defined Function for Column Map Expectations, 
giving us the ability to define custom functions and apply them to our data.
<br/><br/>
Here is an example of <code>F.udf</code> applied to <code>ExpectColumnValuesToEqualThree</code>:

```python
@column_condition_partial(engine=SparkDFExecutionEngine)
def _spark(cls, column, strftime_format, **kwargs):
    def is_equal_to_three(val):
        return (val == 3)

    success_udf = F.udf(is_equal_to_three, sparktypes.BooleanType())
    return success_udf(column)
```

For more on <code>F.udf</code> and the functionality it provides, see the <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.udf.html">Apache Spark UDF documentation</a>.
</details>
</TabItem> 
    
<TabItem value="queryexecution">

The most direct way of implementing a metric is by computing its value by constructing or directly executing querys using objects provided by the `@metric_*` decorators:
- `@metric_value` for Column Aggregate Expectations
  - Expects an appropriate `engine`, `metric_fn_type`, and `domain_type`
- `@metric_partial` for all Map Expectations
  - Expects an appropriate `engine`, `partial_fn_type`, and `domain_type`

Our `engine` will reflect the backend we're implementing (`SparkDFExecutionEngine`), while our `fn_type` and `domain_type` are unique to the type of Expectation we're implementing.

These decorators enable a higher-complexity workflow, allowing you to explicitly structure your queries and make intermediate queries to your database. 
While this approach can result in extra roundtrips to your database, it can also unlock advanced functionality for your Custom Expectations.

For our Custom Column Map Expectation `ExpectColumnValuesToEqualThree`, we're going to implement the `@metric_partial` decorator, 
specifying the type of value we're computing (`MAP_CONDITION_FN`) and the domain over which we're computing (`COLUMN`):

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L53-L65
```

The decorated method takes in a valid [Execution Engine](../../../reference/execution_engine.md) and relevant `kwargs`,
and will return a tuple of:
- A `pyspark.sql.column.Column` defining the query to be executed
- `compute_domain_kwargs`
- `accessor_domain_kwargs`

These will be used to execute our query and compute the results of our metric.

To do this, we need to access our Compute Domain directly:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L66-L75
```

This allows us to build and return a query to be executed, providing the result of our metric:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L77-L79
```

:::note
Because in Spark we are implementing the window function directly, we have to return the *unexpected* condition: `False` when `column == 3`, otherwise `True`.
:::

</TabItem>
</Tabs>

### 3. Verifying your implementation

If you now run your file, `print_diagnostic_checklist()` will attempt to execute your example cases using this new backend.

If your implementation is correctly defined, and the rest of the core logic in your Custom Expectation is already complete,
you will see the following in your Diagnostic Checklist:

```console
✔ Has at least one positive and negative example case, and all test cases pass
```

If you've already implemented the Pandas backend covered in our How-To guides for creating [Custom Expectations](../creating_custom_expectations/overview.md) 
and the SQLAlchemy backend covered in our guide on [how to add SQLAlchemy support for Custom Expectations](./how_to_add_sqlalchemy_support_for_an_expectation.md), 
you should see the following in your Diagnostic Checklist:

```console
✔ Has core logic that passes tests for all applicable Execution Engines
```

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've successfully implemented Spark support for a Custom Expectation! &#127881;
</b></p>
</div>

### 4. Contribution (Optional)

This guide will leave you with core functionality sufficient for [contribution](../contributing/how_to_contribute_a_custom_expectation_to_great_expectations.md) back to Great Expectations at an Experimental level.

If you're interested in having your contribution accepted at a Beta level, your Custom Expectation will need to support SQLAlchemy, Spark, and Pandas.

For full acceptance into the Great Expectations codebase at a Production level, we require that your Custom Expectation meets our code standards, including linting, test coverage, and style. 
If you believe your Custom Expectation is otherwise ready for contribution at a Production level, please submit a [Pull Request](https://github.com/great-expectations/great_expectations/pulls), and we will work with you to ensure your Custom Expectation meets these standards.

:::note
For more information on our code standards and contribution, see our guide on [Levels of Maturity](../../../contributing/contributing_maturity.md#contributing-expectations) for Expectations.

To view the full scripts used in this page, see them on GitHub:
- [expect_column_max_to_be_between_custom.py](https://github.com/great-expectations/great_expectations/blob/hackathon-docs/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py)
- [expect_column_values_to_equal_three.py](https://github.com/great-expectations/great_expectations/blob/hackathon-docs/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py)
:::