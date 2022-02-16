---
title: How to contribute a Custom Expectation to Great Expectations
---
import Prerequisites from '../guides/expectations/creating_custom_expectations/components/prerequisites.jsx'
import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

This guide will help you contribute your Custom Expectations to Great Expectations’ shared library. Your Custom Expectation will be featured in the Expectations Gallery, 
along with many others developed by data practitioners from around the world as part of this collaborative community effort.

<Prerequisites>

  * [Created a Custom Expectation](../guides/expectations/creating_custom_expectations/overview.md)

</Prerequisites>

## Steps

### 1. Verify that your Custom Expectation is ready for contribution

We accept contributions into the Great Expectations codebase at several levels: Experimental, Beta, and Production. The requirements to meet these benchmarks are available in our 
document on [levels of maturity for Expectations](./contributing_maturity.md).

If you call the `print_diagnostic_checklist()` method on your Custom Expectation, you should see a checklist similar to this one:

:::warning
checklist
:::

If you've satisified at least the first four checks, you're ready to make a contribution!

:::warning
Not quite there? See our guides on creating Custom Expectations for help!
:::

### 2. Double-check your Library Metadata

We want to verify that your Custom Expectation is properly credited and accurately described. 

Ensure that your Custom Expectation's `library_metadata` has correct information for the following:

- `contributors`: You and anyone else who helped you create this Custom Expectation.
- `tags`: These are simple descriptors of your Custom Expectation's functionality and domain (`statistics`, `flexible comparisons`, `geography`, etc.).
- `requirements`: If your Custom Expectation relies on any third-party packages, verify that those dependencies are listed here.
- `package` (optional): **PLACEHOLDER** If you're contributing to a specific package, be sure to list it here!

<details>
<summary>Packages?</summary>
If you're interested in learning more about Custom Expectation Packages, see our guide on packaging your Custom Expectations.

Not contributing to a specifc package? Your Custom Expectation will be automatically published in the [PyPI package `great-expectations-experimental`](https://pypi.org/project/great-expectations-experimental/). 
This package contains all of our Experimental community-contributed Custom Expectations, and is separate from the core `great-expectations` package.
</details>

### 3. Open a Pull Request

You're ready to open a [Pull Request](https://github.com/great-expectations/great_expectations/pulls)! 

As a part of this process, we ask you to:

- Sign our [Contributor License Agreement (CLA)](./contributing_misc.md#contributor-license-agreement-cla)
- Provide some information for our reviewers to expedite your contribution process, including:
  - A [CONTRIB] tag in your title
  - Titleing your Pull Request with the name of your Custom Expectation
  - A brief summary of the functionality and use-cases for your Custom Expectation
  - A description of any previous discussion or coordination related to this Pull Request
- Update your branch with the most recent code from the Great Expectations main repository
- Resolve any failing tests and merge conflicts


### 4. Run diagnostics on your Expectation.

Expectations contain a self diagnostic tool that will help you during development. The simplest way to run it is to execute the file as a standalone script. Note: if you prefer, you can also run it within a notebook or IDE.

````console
python expect_column_values_to_equal_three.py
````

Running the script will execute the `run_diagnostics` method for your new class. Initially, it will just return:

````python
{
  "description": {
    "camel_name": "ExpectColumnValuesToEqualThree",
    "snake_name": "expect_column_values_to_equal_three",
    "short_description": "",
    "docstring": ""
  },
  "library_metadata": {
    "maturity": "experimental",
    "package": "experimental_expectations",
    "tags": [],
    "contributors": []
  },
  "renderers": {},
  "examples": [],
  "metrics": [],
  "execution_engines": {}
  "test_report": [],
  "diagnostics_report": []
}
````

This output is a report on the completeness of your Expectation.

You will repeat this step many times during developing your Expectation. `run_diagnostics` creates an easy and fast “dev loop” for you - make a small change in the code, run `run_diagnostics`, examine its output for failures and next steps.

From this point on, we will start filling in the pieces of your Expectation. You don’t have to fill in all the pieces to submit your Expectation. For example, you may choose to provide only Pandas implementation. Another contributor may add a Spark implementation in a separate PR later. Expectation development can be done in bite-size pieces.

:::note
* If you prefer to do your development in Jupyter Notebook and copy your Expectation into the file after you are done, you will run run_diagnostics directly in the notebook (instead of executing the file):
````python
diagnostics_report = ExpectColumnValuesToEqualThree().run_diagnostics()
print(json.dumps(diagnostics_report, indent=2))
````
:::

#### 5. Add an example test.

Search for `examples = [` in your file.

These examples serve a dual purpose:

* help the users of the Expectation understand its logic by providing examples of input data that the Expectation will evaluate as valid and as invalid. When your Expectation is released, its entry in the Expectations Gallery site will render these examples.

* provide test cases that the Great Expectations testing framework can execute automatically.
We will explain the structure of these tests using the example provided in one of the templates that implements `expect_column_values_to_equal_three`:

````python
examples = [{
    "data": {
        "mostly_threes": [3, 3, 3, 3, 3, 3, 2, -1, None, None],
    },
    "tests": [
        {
            "title": "positive_test_with_mostly",
            "include_in_gallery": True,
            "exact_match_out": False,
            "in": {"column": "mostly_threes", "mostly": 0.6},
            "out": {
                "success": True,
                "unexpected_index_list": [6, 7],
                "unexpected_list": [2, -1],
            },
        }
    ],
}]
````

The value of `examples` is a list of examples.

Each example is a dictionary with two keys:

* `data`: defines the input data of the example as a table/data frame. In this example the table has one column named “mostly_threes” with 10 rows. If you define multiple columns, make sure that they have the same number of rows. If possible, include test data and tests that includes null values (None in the Python test definition).

* `tests`: a list of test cases that use the data defined above as input to validate
	* `title` should be a descriptive name for the test case. Make sure to have no spaces.
	* 'include_in_gallery': set it to True if you want this test case to be visible in the gallery as an example (true for most test cases).
	* `in` contains exactly the parameters that you want to pass in to the Expectation. `"in": {"column": "mostly_threes", "mostly": 0.6}` in the example above is equivalent to `expect_column_values_to_equal_three(column="mostly_threes, mostly=0.6)`
	* `out` is based on the Validation Result returned when executing the Expectation.
	* `exact_match_out`: if you set `exact_match_out=False`, then you don’t need to include all the elements of the result object - only the ones that are important to test.

Uncomment that code snippet and replace with your examples.

Run `run_diagnostics` again. The newly added examples will appear in the output. They are not executed as tests yet, because most of the code in the Expectation is still commented out.

:::note

When you define data in your examples, we will mostly guess the type of the columns. Sometimes you need to specify the precise type of the columns for each backend. Then you use schema attribute (on the same level as data and tests in the dictionary):

````python
"schemas": {
  "spark": {
    "mostly_threes": "IntegerType",
  },
  "sqlite": {
    "mostly_threes": "INTEGER",
  },
````
:::

#### 6. Implement the logic.

The details of this step differ based on the type of Expectations you are implementing. 

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

#### 7. Fill in the `library_metadata` dictionary.

Find this code snippet in your file and edit tags and contributors:

````python
library_metadata = {
    "maturity": "experimental",  # "experimental", "beta", or "production"
    "tags": [  # Tags for this Expectation in the gallery
        #         "experimental"
    ],
    "contributors": [  # GitHub handles for all contributors to this Expectation.
        #         "@your_name_here", # Don't forget to add your GitHub handle here!
    ],
    "package": "experimental_expectations",
}
````

#### 8. Implement (some) renderers.

Renderers are methods in the class that implements your Expectation that can display your Expectation and its Validation Result as HTML or other human-friendly format.

The template file that you used to start your development has some renderer implementations commented out. You can use them as a starting point.

For more comprehensive documentation consult this [how-to guide](/docs/guides/expectations/advanced/how_to_create_renderers_for_custom_expectations).

#### 9. Submit your contribution

Follow [Contribution Checklist](/docs/contributing/contributing_checklist) to submit your contribution.





There are four Expectation subclasses that make the development of particular types of Expectations significantly easier by handling boilerplate code and letting you focus on the business logic of your Expectation. Consider choosing one that suites your Expectation:

* `ColumnMapExpectation` - Expectations of this type validate a single column of tabular data. First they ask a yes/no question from every row in that column. Then they ask what percentage of rows gave a positive answer to the first question. If the answer to the second question is above a specified threshold, the Expectation considers the data valid.
* `ColumnExpectation` s are also evaluated for a single column, but produce an aggregate metric, such as a mean, standard deviation, number of unique values, type, etc.
* `ColumnPairMapExpectation`s are similar to `ColumnMapExpectations`, except that they are based on two columns, instead of one.
* `TableExpectation` s are a generic catchall for other types of Expectations applied to tabular data.

Choose the template that fits your Expectation. Starting your development with these templates is significantly easier than developing from scratch:

* `ColumnMapExpectation`: `examples/expectations/column_map_expectation_template.py`
* `ColumnExpectation`: `examples/expectations/column_aggregate_expectation_template.py`
* `ColumnPairMapExpectation`: coming soon…
* `TableExpectation`: coming soon…