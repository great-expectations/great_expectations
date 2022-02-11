---
title: How to add SQLAlchemy support for Custom Expectations
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you implement native SQLAlchemy support for your [Custom Expectation](../creating_custom_expectations/overview.md). 

<Prerequisites>

 - Created a [Custom Expectation](../creating_custom_expectations/overview.md)

</Prerequisites>

Great Expectations supports a number of [Execution Engines](../../../reference/execution_engine.md), including a SQLAlchemy Execution Engine. 
These Execution Engines provide the computing resources used to calculate the [Metrics](../../../reference/metrics.md) defined in the Metric class of your Custom Expectation.

If you decide to contribute your Expectation, its entry in the [Expectations Gallery](https://greatexpectations.io/expectations/) will reflect the Execution Engines that it supports.

We will add SQLAlchemy support for the Custom Expectations implemented in [How to create Custom Column Aggregate Expectations](../creating_custom_expectations/how_to_create_custom_column_aggregate_expectations.md) 
and [How to create Custom Column Map Expectations](../creating_custom_expectations/how_to_create_custom_column_map_expectations.md).

## Steps

### 1. Specify your backends and dialects

While SQLAlchemy is able to provide a common interface to a variety of SQL dialects, some functions may not work in a particular dialect, or in some cases they may return different values. 
To avoid surprises, it can be helpful to determine beforehand what backends and dialects you plan to support, and test them along the way. 

Within the `examples` defined inside your Expectation class, the `test_backends` key specifies which backends and SQLAlchemy dialects to run tests for. Add entries corresponding to the functionality you want to add: 
    
```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L106-L119
```

:::note
You may have noticed that specifying `test_backends` isn't required for successfully testing your Custom Expectation.

If not specified, Great Expectations will attempt to determine the implemented backends automatically, but wll only run SQLAlchemy tests against sqlite.
:::

### 2. Implement the SQLAlchemy logic for your Custom Expectation

Great Expectations provides a variety of ways to implement an Expectation in SQLAlchemy. Two of the most common include: 
1.  Defining a partial function that takes a SQLAlchemy column as input
2.  Directly executing queries using SQLAlchemy objects to determine the value of your Expectation's metric directly 

<Tabs
  groupId="metric-type"
  defaultValue='partialfunction'
  values={[
  {label: 'Partial Function', value:'partialfunction'},
  {label: 'Query Execution', value:'queryexecution'},
  ]}>

<TabItem value="partialfunction">

Great Expectations allows for much of the SQLAlchemy logic for executing queries be abstracted away by specifying metric behavior as a partial function. 
To do this, we use one of the `@column_*_partial` decorators:
- `@column_aggregate_partial` for Column Aggregate Expectations
- `@column_condition_partial` for Column Map Expectations
- `@column_pair_condition_partial` for Column Pair Map Expectations
- `@multicolumn_condition_partial` for Multicolumn Map Expectations

These decorators expect an appropriate `engine=` argument. In this case, we'll pass our `SqlAlchemyExecutionEngine`. 
The decorated method takes in an SQLAlchemy `Column` object and will either return a `sqlalchemy.sql.functions.Function` or a `sqlalchemy.sql.expression.ColumnOperator` that Great Expectations will use to generate the appropriate SQL queries. 
  
For our Custom Column Map Expectation `ExpectColumnValuesToEqualThree`, we're going to leverage SQLAlchemy's `in_` ColumnOperator and the `@column_condition_partial` decorator.

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L71-L73
```

<details>
<summary>Getting <code>func</code>-y?</summary>
We can also take advantage of SQLAlchemy's <code>func</code> special object instance. 
<br/>
<code>func</code> allows us to pass common generic functions which SQLAlchemy will compile appropriately for the targeted dialect, 
giving us the flexibility to not have write that targeted code ourselves! 
<br/><br/>
Here's an example from <a href="https://greatexpectations.io/expectations/expect_column_sum_to_be_between">ExpectColumnSumToBeBetween</a>:

```python file=../../../../great_expectations/expectations/metrics/column_aggregate_metrics/column_sum.py#L21-L23
```
</details>


</TabItem>
<TabItem value="queryexecution">

The most direct way of implementing a metric is by computing its value by constructing or directly executing querys using objects provided by the `@metric_*` decorators:
- `@metric_value` for Column Aggregate Expectations
  - Expects an appropriate `engine`, `metric_fn_type`, and `domain_type`
- `@metric_partial` for all Map Expectations
  - Expects an appropriate `engine`, `partial_fn_type`, and `domain_type`

Our `engine` will reflect the backend we're implementing (`SqlAlchemyExecutionEngine`), while our `fn_type` and `domain_type` are unique to the type of Expectation we're implementing.

These decorators enable a higher-complexity workflow, allowing you to explicitly structure your queries and make intermediate queries to your database. 
While this approach can result in extra roundtrips to your database, it can also unlock advanced functionality for your Custom Expectations.

For our Custom Column Aggregate Expectation `ExpectColumnMaxToBeBetweenCustom`, we're going to implement the `@metric_value` decorator, 
specifying the type of value we're computing (`AGGREGATE_VALUE`) and the domain over which we're computing (`COLUMN`):

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L35-L39
```

The decorated method takes in a valid [Execution Engine](../../../reference/execution_engine.md) and relevant `kwargs`,
and will return a computed value.

To do this, we need to access our Compute Domain directly:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L40-L58
```

This allows us to build a query and use our Execution Engine to execute that query against our data to return the actual value we're looking for, instead of returning a query to find that value:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L60-L63
```

<details>
<summary>Getting <code>func</code>-y?</summary>
While this approach allows for highly complex queries, here we're taking advantage of SQLAlchemy's <code>func</code> special object instance. 
<br/>
<code>func</code> allows us to pass common generic functions which SQLAlchemy will compile appropriately for the targeted dialect, 
giving us the flexibility to not have write that targeted code ourselves!
</details>
</TabItem>
</Tabs>

### 3. Verifying our implementation

If you now run your file, `print_diagnostic_checklist` will attempt to execute your example cases using this new backend.

If your implementation is correctly defined, and the rest of the core logic in your Custom Expectation is already complete,
you will see the following in your Diagnostic Checklist:

```console
✔ Has at least one positive and negative example case, and all test cases pass
```

If you've already implemented the Pandas backend covered in our How-To guides for creating [Custom Expectations](../creating_custom_expectations/overview.md) 
and the Spark backend covered in [How to add Spark support for Custom Expectations](./how_to_add_spark_support_for_an_expectation.md), 
you should see the following in your Diagnostic Checklist:

```console
✔ Has core logic that passes tests for all applicable Execution Engines
```

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've successfully implemented SQLAlchemy support for a Custom Expectation! &#127881;
</b></p>
</div>

### 4. Contribution (Optional)

This guide will leave you with core functionality sufficient for [contribution](../../../contributing/how_to_contribute_a_custom_expectation_to_great_expectations.md) back to Great Expectations at an Experimental level.

If you're interested in having your contribution accepted at a Beta level, your Custom Expectation will need to support SQLAlchemy, Spark, and Pandas.

For full acceptance into the Great Expectations codebase at a Production level, we require that your Custom Expectation meets our code standards, including linting, test coverage, and style. 
If you believe your Custom Expectation is otherwise ready for contribution at a Production level, please submit a [Pull Request](https://github.com/great-expectations/great_expectations/pull-requests), and we will work with you to ensure your Custom Expectation meets these standards.

:::note
For more information on our code standards and contribution, see our guide on [Levels of Maturity](../../../contributing/contributing_maturity.md#contributing-expectations) for Expectations.

To view the full scripts used in this page, see them on GitHub:
- [expect_column_max_to_be_between_custom.py](https://github.com/great-expectations/great_expectations/blob/hackathon-docs/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py)
- [expect_column_values_to_equal_three.py](https://github.com/great-expectations/great_expectations/blob/hackathon-docs/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py)
:::
