---
title: How to create a custom Metric
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

This guide will help you implement a custom Metric in Pandas using the appropriate MetricProvider class. 

<Prerequisites>

  * [Set up your dev environment](/docs/contributing/contributing_setup) to contribute
  * [Signed the Contributor License Agreement](/docs/contributing/contributing_checklist) (CLA)

</Prerequisites>

### Steps

The details of this process differ based on the type of Expectations you are implementing. 

<Tabs
  groupId="expectation-type"
  defaultValue='columnmap'
  values={[
  {label: 'ColumnMapExpectation', value:'columnmap'},
  {label: 'ColumnExpectation', value:'column'},
  {label: 'ColumnPairMapExpectation', value:'columnpairmap'},
  {label: 'TableExpectation', value:'table'},
  ]}>

<TabItem value="columnmap">

Expectations that extend ColumnMapExpectation class work as follows:

* First they ask a yes/no question from every row in that column (that’s the “map”).
* Then they ask what percentage of rows gave a positive answer to the first question. If the answer to the second question is above a specified threshold (controlled by the mostly argument), the Expectation considers the data valid.
 
`ColumnMapExpectation class` (the parent of your Expectation’s class) does all the work of the second step. It leaves you to define the yes/no question that the Expectation asks about every row in the column. “Questions” are modeled as Metrics in Great Expectations. A Metric is any observable property of data (e.g., numeric stats like mean/median/mode of a column, but also richer properties of data, such as histogram). You can read more about the relationship between Expectations and Metrics in our [Core Concepts: Expectations and Metrics](/docs/reference/metrics).

* `ExpectColumnValuesToEqualThree` class that the template implements declares that the metric that maps each row in the column to the answer to its yes/no question is called `column_values.equal_three`:

````python
map_metric = "column_values.equal_three"
````

The parent class expects the variable map_metric to be set. Change the value of map_metric to something that fits your Metric. Follow these two naming conventions:

* the name should start with “column_values.”, because it is a “column map” Metric
* the second part of the name (after the “.”) should be in snake_case format
* While many metrics are already implemented within Great Expectations (e.g., column_values.match_regex, column_values.json_parseable, etc.), column_values.equal_three is not. You will define and implement this new Metric.

The convention is to implement a new Metric Provider (a class that can compute a metric) that your Expectation depends on in the same file as the Expectation itself.

Search for `class ColumnValuesEqualThree` and rename it to ColumnValues<CamelCase version of the second part of the metric name that you declared in the previous step\>.

The Metric Provider class declares the condition metric that it can compute. “Condition metric” is a metric that answers a yes/no question:

````python
condition_metric_name = "column_values.equal_three"
````

The parent class expects the variable `condition_metric_name` to be set. Change the value of `condition_metric_name` to the same name that you used for `map_metric` in your Expectation class.

The Expectation declares that it needs a yes/no Metric “X” and the Metric Provider declares that it can compute this Metric. A match made in heaven.

* Implement the computation of the Metric in your new Metric Provider class for at least one Execution Engines that Great Expectations supports, such as pandas, sqlalchemy, or Spark. Most contributors find that starting with Pandas is the easiest and fastest way to build.

The parent class of your Metric Provider class is `ColumnMapMetricProvider`. It uses Python Decorators to hide most of the complexity from you and give you a clear and simple API to implement one method per backend that computes the metric.

:::note

If you have never used Python Decorators and don’t know what they are and how they work, no worries - this should not stop you from successfully implementing your Expectation. Decorators allow the parent class to “wrap” your methods, which means to execute some code before and after your method runs. All you need to know is the name of the Decorator to add (with “@”) above your method definition.
:::

Find the following code snippet in your Metric Provider class:

````python
@column_condition_partial(engine=PandasExecutionPandasExecutionEngineEngine)
def _pandas(cls, column, **kwargs):
    return column == 3
````

This means that the method `_pandas` is a metric function that is decorated as a `column_condition_partial`. It will be called with the engine-specific column type (e.g., a Series in pandas case). It must return a boolean value for each row of the column. The `engine` argument of `column_condition_partial` is set to `PandasExecutionEngine` to signal to the method in the parent that the method computes the Metric for pandas backend. There is nothing special about the name of the method `_pandas` - it can be called anything else, but why make things more complicated than they must be?

Implement this method to compute your Metric.

:::note

How to support additional arguments your Expectation needs.

The Expectation in the template (expect_column_values_to_equal_three) did not need to accept any additional arguments to evaluate the data.

Here is how you could modify expect_column_values_to_equal_three to expect_column_values_to_equal_integer, where users would have to specify the value of the integer as an argument:

Find the snippet success_keys = ("mostly",) in the class that implements your Expectation. Add your arguments to success_keys

````python
success_keys = ("integer", "mostly")
````
Success keys are arguments that determine the values of the Expectation’s metrics and when the Expectation will succeed.

In the class that implements Metric Provider set the variable condition_value_keys to a tuple of your arguments:
````python
condition_value_keys = ("integer",)
````

Metric Provider parent class expects the value of this variable to contain all the additional arguments required to compute the Metric.

`value_keys` work for Metrics like `success_keys` do for Expectations, but they are used to determine the value of the metric (hence the name!). If your metric needs additional user-supplied parameters, you add them to the value_keys.

For a map Metric producing a yes/no question answer, you use condition_value_keys (because it’s the condition part of the metric).

* Add named arguments to the methods that compute the Metric for each backend in your Metric Provider class:

````python
@column_condition_partial(engine=PandasExecutionEngine)
def _pandas(cls, column, integer=None, **kwargs):
    return column == integer
````

* Add the new arguments to the test cases in the examples.
:::

:::note

Some Column Map Metrics that map every row of a column to yes/no need a numeric value pre-computed for each row in order to produce the answer.

This requires defining a new Metric. The parent class of your Metric Provider class (`ColumnMapMetricProvider`) provides support for this case.

A good example of this pattern is `expect_column_value_z_scores_to_be_less_than` - one of the core Expectations.

The Expectation declares “column_values.z_score.under_threshold” as its `condition_metric_name` (the Metric that answers the yes/no question for every row).

The `ColumnValuesZScore` Metric Provider class that computes this Metric declares an additional metric:

````python
function_metric_name = "column_values.z_score"
````

The class implements methods decorated with `@column_function_partial` to compute the Z score for every row for each backend.

Consult the following files for the details of this pattern:

* [great-expectations/great_expectations/expectations/core/expect_column_value_z_scores_to_be_less_than.py](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/expectations/core/expect_column_value_z_scores_to_be_less_than.py)
* [great-expectations/great_expectations/expectations/metrics/column_map_metrics/column_values_z_score.py](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/expectations/metrics/column_map_metrics/column_values_z_score.py)
:::

</TabItem>
<TabItem value="column">

Expectations that extend ColumnExpectation class are evaluated for a single column, but produce an aggregate metric, such as a mean, standard deviation, number of unique values, type, etc.

Define success_keys of your Expectation

````python
success_keys = ("min_value", "strict_min", "max_value", "strict_max")
````

Expectations rely on Metrics to produce their result. A Metric is any observable property of data (e.g., numeric stats like mean/median/mode of a column, but also richer properties of data, such as histogram). You can read more about the relationship between Expectations and Metrics in our [Core Concepts: Expectations and Metrics](/docs/reference/metrics).

* `ExpectColumnCustomMedianToBeBetween` class that the template implements declares the list of Metrics it needs computes for producing its result:

````python
metric_dependencies = ("column.custom.median",)
````

The parent class expects the variable `metric_dependencies` to be set. Change the value of `metric_dependencies` to something that fits your Metric. Follow these two naming conventions:

* the name should start with “column.”, because it is a column Metric
* the second part of the name (after the “.”) should be in snake_case format

* While many column metrics are already implemented within Great Expectations (e.g., `column.max`, `column.mean`, `column.value_counts`, etc.), `column.custom.median` is not. You will define and implement this new Metric.

The convention is to implement a new Metric Provider (a class that can compute a metric) that your Expectation depends on in the same file as the Expectation itself.

Search for `class ColumnCustomMedian` and rename it to Column<CamelCase version of the second part of the metric name that you declared in the previous step\>.

The Metric Provider class declares the metric that it can compute.

````python
metric_name = "column.custom.median"
````

The parent class expects the variable metric_name to be set. Change the value of metric_name to the same name that you used for metric_dependencies in your Expectation class.

The Expectation declares that it needs a Metric “X” and the Metric Provider declares that it can compute this Metric.

* Implement the computation of the Metric in your new Metric Provider class for at least one of the three backends (Execution Engines) that Great Expectations supports: pandas, sqlalchemy, Spark. Most contributors find starting with Pandas is the easiest and fastest way to build.

The parent class of your Metric Provider class is `ColumnAggregateMetricProvider`. It uses Python Decorators to hide most of the complexity from you and give you a clear and simple API to implement one method per backend that computes the metric.

:::note

If you have never used Python Decorators and don’t know what they are and how they work, no worries - this should not stop you from successfully implementing your Expectation. Decorators allow the parent class to “wrap” your methods, which means to execute some code before and after your method runs. All you need to know is the name of the Decorator to add (with “@”) above your method definition.
:::

Find the following code snippet in your Metric Provider class:

````python
@column_aggregate_value(engine=PandasExecutionEngine)
def _pandas(cls, column, **kwargs):
    """Pandas Median Implementation"""
    return column.median()
````

This means that the method `_pandas` is a metric function that is decorated as a `column_aggregate_value`. It will be called with the engine-specific column type (e.g., a Series in pandas case). It must return a value that is computed over this column. The engine argument of `column_condition_partial` is set to `PandasExecutionEngine` to signal to the method in the parent that the method computes the Metric for pandas backend. There is nothing special about the name of the method `_pandas` - it can be called anything else, but why make things more complicated than they must be?

Implement this method to compute your Metric.

</TabItem>
<TabItem value="columnpairmap">

:::caution Under Construction
:::

</TabItem>
<TabItem value="table">

:::caution Under Construction
:::

</TabItem>
</Tabs>