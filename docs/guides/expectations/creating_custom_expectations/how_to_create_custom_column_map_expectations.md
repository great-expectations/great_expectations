---
title: How to create a Custom Column Map Expectation
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'

**ColumnMapExpectations** are evaluated for a single column. They ask a yes/no question for every row in that column, then ask what percentage of rows gave a positive answer to that question. If that percentage is high enough, the Expectation considers that data valid.

This guide will walk you through the process of creating your own ColumnMapExpectations.

We will be following this **PLACEHOLDER**.

<Prerequisites>

- Read the [overview for creating Custom Expectations](overview).

</Prerequisites>

### Steps

#### 1. Choose a name for your Expectation

First, decide on a name for your own Expectation. By convention, `ColumnMapExpectations` always start with `expect_column_values_`. You can see other naming conventions in the [Expectations section](/docs/contributing/style_guides/code_style#expectations)  of the code Style Guide.

Your Expectation will have two versions of the same name: a `CamelCaseName` and a `snake_case_name`. For example, this tutorial will use:

- `ExpectColumnValuesToEqualThree`
- `expect_column_values_to_equal_three`

#### 2. Copy and rename the template file

By convention, each Expectation is kept in its own python file, named with the snake_case version of the Expectation's name.

You can find the template file for a custom ColumnMapExpectation [here](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/column_map_expectation_template.py). Download the file, place it in the appropriate directory, and rename it to the appropriate name.

```bash
mv column_map_expectation_template.py /SOME_DIRECTORY/expect_column_values_to_equal_three.py
```

<details>
  <summary>Where should I put my Expectation file?</summary>
  <div>
    <p>
        During development, you don't actually need to put the file anywhere in particular. It's self-contained, and can be executed anywhere as long as <code>great_expectations</code> is installed.
    </p>
    <p>
        But to use your new Expectation alongside the other components of Great Expectations, you'll need to make sure the file is in the right place. The right place depends on what you intend to use it for.
    </p>
    <p>
        <ul>
            <li>If you're building a Custom Expectation for personal use, you'll need to put it in the <code>great_expectations/plugins/expectations</code> folder of your Great Expectations deployment. When you instantiate the corresponding <code>DataContext</code>, it will automatically make all plugins in the directory available for use.</li>
            <li>If you're building a Custom Expectation to contribute to the open source project, you'll need to put it in the repo for the Great Expectations library itself. Most likely, this will be within a package within <code>contrib/</code>: <code>great_expectations/contrib/SOME_PACKAGE/SOME_PACKAGE/expectations/</code>. To use these Expectations, you'll need to install the package.</li>
        </ul>
    </p>
  </div>
</details>

#### 3. Run checklist diagnostics on your file

Once you've copied and renamed the template file, you can execute it as follows.

```bash
python expect_column_values_to_equal_three.py
```

The template file is set up so that this will run the Expectation's `generate_diagnostic_checklist` method. This will run a diagnostic script on your new Expectation, and return a checklist of steps to get it to full production readiness.

```
Completeness checklist for ExpectColumnValuesToMatchSomeCriteria:
  ✔ library_metadata object exists
    Has a docstring, including a one-line short description
    Has at least one positive and negative example case
    Core logic exists and passes tests on at least one Execution Engine
    Has all four statement Renderers: question, descriptive, prescriptive, diagnostic
    Has default ParameterBuilders and Domain hooks to support Profiling
    Core logic exists and passes tests for all applicable Execution Engines and backends
    All Renderers exist and produce typed output
    Linting for type hints and other code standards passes
    Input validation exists
```

When in doubt, the next step to implement is the first one that doesn't have a ✔ next to it. This guide covers the first four steps on the checklist.

#### 4. Change the Expectation class name and add a docstring

Let's start by updating your Expectations's name and docstring.

Replace the Expectation class name
```python
# This class defines the Expectation itself
class ExpectColumnValuesToMatchSomeCriteria(ColumnMapExpectation):
    """TODO: Add a docstring here"""
```

with your real Expectation class name, in upper camel case:
```python
class ExpectColumnValuesToEqualThree(ColumnMapExpectation):
```

You can also go ahead and write a new one-line docstring, replacing
```python
    """TODO: add a docstring here"""
```

with something like:
```python
    """Expect values in this column to equal 3."""
```

You'll also need to change the class name at the bottom of the file, by replacing this line:

```python
checklist = ExpectColumnValuesToMatchSomeCriteria().generate_diagnostic_checklist()
```

with this one:
```python
checklist = ExpectColumnValuesToEqualThree().generate_diagnostic_checklist()
```

Later, you can go back and write a more thorough docstring.

At this point you can re-run your diagnostic checklist. You should see something like this:
```
$ python expect_column_values_to_equal_three.py

Completeness checklist for ExpectColumnValuesToEqualThree:
  ✔ library_metadata object exists
  ✔ Has a docstring, including a one-line short description
    Has at least one positive and negative example case
    Core logic exists and passes tests on at least one Execution Engine
...
```

Congratulations! You're one step closer to implementing a Custom Expectation.

#### 5. Add test cases

Next, we're going to search for `examples = []` in your file, and replace it at least two test examples.

These examples serve a dual purpose:

* First, they provide test cases that the Great Expectations testing framework can execute automatically.

* Second, they help the users of the Expectation understand its logic by providing examples of input data that the Expectation will evaluate as valid and as invalid. When your Expectation is released, its entry in the Gallery will render these examples.

[pic]

Your examples will look something like this:

```python
examples = [
    {
        "data": {
            "all_threes": [3, 3, 3, 3, 3],
            "some_zeroes": [3, 3, 3, 0, None],
        },
        "tests": [
            {
                "title": "positive_test",
                "exact_match_out": False,
                "include_in_gallery": True,
                "in": {
                    "column": "all_threes"
                },
                "out": {
                    "success": True,
                },
            }
            {
                "title": "negative_test",
                "exact_match_out": False,
                "include_in_gallery": True,
                "in": {
                    "column": "all_zeroes"
                },
                "out": {
                    "success": False,
                    "unexpected_index_list": [3, 4],
                    "unexpected_list": [0, None],
                },
            }
        ],
    }
]
```

Here's a quick overview of how to create test cases to populate `examples`. The overall structure is a list of dictionaries. Each dictionary has two keys:

* `data`: defines the input data of the example as a table/data frame. In this example the table has one column named `all_threes` and a second column named `some_zeroes`. Both columns have 5 rows. (Note: if you define multiple columns, make sure that they have the same number of rows.)

* `tests`: a list of test cases that use the data defined above as input to validate
	* `title` should be a descriptive name for the test case. Make sure to have no spaces.
	* 'include_in_gallery': set it to True if you want this test case to be visible in the gallery as an example (true for most test cases).
	* `in` contains exactly the parameters that you want to pass in to the Expectation. `"in": {"column": "mostly_threes", "mostly": 0.6}` in the example above is equivalent to `expect_column_values_to_equal_three(column="mostly_threes, mostly=0.6)`
	* `out` is based on the Validation Result returned when executing the Expectation.
	* `exact_match_out`: if you set `exact_match_out=False`, then you don’t need to include all the elements of the result object - only the ones that are important to test.


Run `run_diagnostics` again. The newly added examples will appear in the output. They are not executed as tests yet, because the Expectation itself hasn't been implemented yet, but they'll check the box for example cases.

```
$ python expect_column_values_to_equal_three.py

Completeness checklist for ExpectColumnValuesToEqualThree:
  ✔ library_metadata object exists
  ✔ Has a docstring, including a one-line short description
  ✔ Has at least one positive and negative example case
    Core logic exists and passes tests on at least one Execution Engine
...
```

#### 6. Implement your Metric and connect it to your Expectation

This is the stage where you implement the actual business logic for your Expectation.


Metric class name : ColumnValuesEqualThree
condition_metric_name = "column_values.equal_three"

map_metric = "column_values.equal_three"


<details>
  <summary>Other Expectation parameters: <code>success_keys</code> and <code>default_kwarg_values</code></summary>
  <div>
    <p>
* **Success Keys** - A tuple consisting of values that must / could be provided by the user and defines how the Expectation evaluates success.
    </p>
    <p>
* **Default Kwarg Values** (Optional) - Default values for success keys and the defined domain, among other values.
An example of Expectation Parameters is shown below (notice that we are now in a new Expectation class):
    </p>
  </div>
</details>


#### 7. Update `library_metadata`

```python
library_metadata = {
    "maturity": "experimental",  # "experimental", "beta", or "production"
    "tags": [  # Tags for this Expectation in the gallery
        #         "experimental"
    ],
    "contributors": [  # Github handles for all contributors to this Expectation.
        #         "@your_name_here", # Don't forget to add your github handle here!
    ],
    # "package": "experimental_expectations", # This should be auto-populated.
}
```

Expectations rely on Metrics to produce their result. A Metric is any observable property of data (e.g., numeric stats like mean/median/mode of a column, but also richer properties of data, such as histogram). You can read more about the relationship between Expectations and Metrics in our [Core Concepts: Metrics](../../../reference/metrics.md).

If your Metric does not yet exist within the framework, you will need to implement it yourself in a new class - a task that is quick and simple within the new modular framework. The convention is to implement a new Metric Provider (a class that can compute a metric) that your Expectation depends on in the same file as the Expectation itself.

The parent class expects the variable `condition_metric_name` to be set. Change the value of `condition_metric_name` to something that fits your Metric. Follow these two naming conventions:

* The name should start with `column_values.`, because it is a column map Metric
* The second part of the name (after the `.`) should be in snake_case format

The parent class of your Metric Provider class is `ColumnMapMetricProvider`. It uses Python Decorators to hide most of the complexity from you, and give you a clear and simple API to implement one method per backend that computes the metric.
Implement the computation of the metric in your new Metric Provider class for at least one of the three backends ([**Execution Engines**](../../../reference/execution_engine.md)) that Great Expectations supports: Pandas, SQLAlchemy, and Spark.

Here is the implementation of our example metric for Pandas:

:::caution Under Construction
:::

This means that the method `_pandas` is a metric function that is decorated as a `column_condition_partial`. It will be called with the engine-specific column type. It must return a boolean value for each row of the column. 
The `engine` argument of `column_condition_partial` is set to `PandasExecutionEngine` to signal to the method in the parent class that this method computes the Metric for the Pandas backend.

:::note
If you have never used Python Decorators and don’t know what they are and how they work, no worries - this should not stop you from successfully implementing your Expectation. Decorators allow the parent class to “wrap” your methods, which means to execute some code before and after your method runs. All you need to know is the name of the Decorator to add (with “@”) above your method definition.
:::


## Next Steps
