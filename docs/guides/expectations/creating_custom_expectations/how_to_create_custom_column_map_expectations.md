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

The template file is set up so that this will run the expectations `generate_diagnostic_checklist` method. This will run a diagnostic script on your new Expectation, return a checklist of steps to get it to full production readiness, and recommend a next step.

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

Not all Expectation diagnostics can be automated, but automating most of the core steps helps make it fast and simple to author, submit, and review new Expectations that satisfy the code quality standards for Great Expectations.


#### 4. Change the Expectation class name and add a docstring

Replace the Expectation class name
```
class ExpectColumnValuesToMatchSomeCriteria(ColumnMapExpectation):
```

with your real Expectation class name, in upper camel case:
```
class ExpectColumnValuesToEqualThree(ColumnMapExpectation):
```

You'll also need to change the class name at the bottom of the file, by replacing this:

```
diagnostics_report = ExpectColumnValuesToMatchSomeCriteria().run_diagnostics()
```

with this:
```
diagnostics_report = ExpectColumnValuesToEqualThree().run_diagnostics()
```


You can also go ahead and write a new one-line docstring, replacing
```
"""TODO: add a docstring here"""
```

with something like:
```
"""Expect values in this column to equal 3.

"""
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
    Has all four statement Renderers: question, descriptive, prescriptive, diagnostic
    Has default ParameterBuilders and Domain hooks to support Profiling
    Core logic exists and passes tests for all applicable Execution Engines and backends
    All Renderers exist and produce typed output
    Linting for type hints and other code standards passes
    Input validation exists
```


#### 5. Add test cases

```python
examples = [
    {
        "data": {
            "all_threes": [3, 3, 3, 3, 3, 3, 3, 3, 3, 3],
            "mostly_threes": [3, 3, 3, 3, 3, 3, 2, -1, None, None],
        },
        "tests": [
            {
                "title": "positive_test_with_mostly",
                "exact_match_out": False,
                "include_in_gallery": True,
                "in": {"column": "mostly_threes", "mostly": 0.6},
                "out": {
                    "success": True,
                    "unexpected_index_list": [6, 7],
                    "unexpected_list": [2, -1],
                },
            }
        ],
    }
]
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

```
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
