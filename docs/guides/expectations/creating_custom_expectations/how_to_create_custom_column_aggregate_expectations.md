---
title: How to create a Custom Column Aggregate Expectation
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'


**ColumnExpectations** are one of the most common types of [**Expectation**](../../../reference/expectations/expectations.md). 
They are evaluated for a single column, and produce an aggregate metric, such as a mean, standard deviation, number of unique values, column type, etc. If that metric meets the conditions you set, the Expectation considers that data valid.

This guide will walk you through the process of creating your own custom ColumnExpectation.

<Prerequisites>

- Read the [overview for creating Custom Expectations](overview).

</Prerequisites>

### Steps

#### 1. Choose a name for your Expectation

First, decide on a name for your own Expectation. By convention, `ColumnExpectations` always start with `expect_column_`. 
For more on Expectation naming conventions, see the [Expectations section](/docs/contributing/style_guides/code_style#expectations) of the Code Style Guide.

Your Expectation will have two versions of the same name: a `CamelCaseName` and a `snake_case_name`. For example, this tutorial will use:

- `ExpectColumnMaxToBeBetweenCustom`
- `expect_column_max_to_be_between_custom`

#### 2. Copy and rename the template file

By convention, each Expectation is kept in its own python file, named with the snake_case version of the Expectation's name.

You can find the template file for a custom ColumnExpectation [here](/examples/expectations/column_aggregate_expectation_template.py).
Download the file, place it in the appropriate directory, and rename it to the appropriate name.

```bash 
mv column_aggregate_expectation_template.py /SOME_DIRECTORY/expect_column_max_to_be_between_custom.py
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

#### 3. Generate a diagnostic checklist for your Expectation

Once you've copied and renamed the template file, you can execute it as follows.

```bash 
python expect_column_max_to_be_between_custom.py
```

The template file is set up so that this will run the Expectation's `generate_diagnostic_checklist` method. This will run a diagnostic script on your new Expectation, and return a checklist of steps to get it to full production readiness.

```
Completeness checklist for ExpectColumnAggregateToMatchSomeCriteria:
  ✔ Has a library_metadata object
    Has a docstring, including a one-line short description
    Has at least one positive and negative example case
    Has core logic and passes tests on at least one Execution Engine
    Has basic input validation and type checking
    Has all four statement Renderers: question, descriptive, prescriptive, diagnostic
    Has core logic that passes tests for all applicable Execution Engines and SQL dialects
  ✔ Passes all linting checks
    Has a full suite of tests, as determined by project code standards
    Has passed a manual review by a code owner for code standards and style guides
```

When in doubt, the next step to implement is the first one that doesn't have a ✔ next to it. This guide covers the first four steps on the checklist.

#### 4. Change the Expectation class name and add a docstring

Let's start by updating your Expectation's name and docstring.

Replace the Expectation class name
```python file=../../../../examples/expectations/column_aggregate_expectation_template.py#L47-L49
```

with your real Expectation class name, in upper camel case:
```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L40
```

You can also go ahead and write a new one-line docstring, replacing
```python file=../../../../examples/expectations/column_aggregate_expectation_template.py#L49
```

with something like:
```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L41
```

You'll also need to change the class name at the bottom of the file, by replacing this line:
```python file=../../../../examples/expectations/column_aggregate_expectation_template.py#L85
```

with this one:
```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L97
```

:::warning Under Construction:::

Expectations rely on Metrics to produce their result. A Metric is any observable property of data (e.g., numeric stats like mean/median/mode of a column, but also richer properties of data, such as histogram). You can read more about the relationship between Expectations and Metrics in our [Core Concepts: Metrics](../../../reference/metrics.md).

If your Metric does not yet exist within the framework, you will need to implement it yourself in a new class - a task that is quick and simple within the new modular framework. The convention is to implement a new Metric Provider (a class that can compute a metric) that your Expectation depends on in the same file as the Expectation itself.

The parent class expects the variable `metric_name` to be set. Change the value of `metric_name` to something that fits your Metric. Follow these two naming conventions:

* The name should start with `column.`, because it is a column Metric
* The second part of the name (after the `.`) should be in snake_case format

The parent class of your Metric Provider class is `ColumnMetricProvider`. It uses Python Decorators to hide most of the complexity from you, and give you a clear and simple API to implement one method per backend that computes the metric.
Implement the computation of the metric in your new Metric Provider class for at least one of the three backends ([**Execution Engines**](../../../reference/execution_engine.md)) that Great Expectations supports: Pandas, SQLAlchemy, and Spark.

Here is the implementation of our example metric for Pandas:


This means that the method `_pandas` is a metric function that is decorated as a `column_aggregate_value`. It will be called with the engine-specific column type. It must return a value that is computed over this column. 
The `engine` argument of `column_aggregate_value` is set to `PandasExecutionEngine` to signal to the method in the parent class that this method computes the Metric for the Pandas backend.

:::note
If you have never used Python Decorators and don’t know what they are and how they work, no worries - this should not stop you from successfully implementing your Expectation. Decorators allow the parent class to “wrap” your methods, which means to execute some code before and after your method runs. All you need to know is the name of the Decorator to add (with “@”) above your method definition.
:::

Below lies the full implementation of an aggregate metric class, with implementations for Pandas, SQLAlchemy, and Apache Spark Dialects.



#### 3. Define Parameters

The structure of a Modular Expectation now exists within its own specialized class. This structure has 3 fundamental components: Metric Dependencies, Configuration Validation, and Expectation Validation. In this step, we will address setting up our parameters.

In this guide, we're focusing on a `ColumnExpectation`, which can define metric dependencies simply using the metric_dependencies property.

Add the following attributes to your Expectation class:

* **Metric Dependencies** - A tuple consisting of the names of all metrics necessary to evaluate the Expectation. Using this shortcut tuple will provide the dependent metric with the same domain kwargs and value kwargs as the Expectation.

* **Success Keys** - A tuple consisting of values that must / could be provided by the user and defines how the Expectation evaluates success.

* **Default Kwarg Values** (Optional) - Default values for success keys and the defined domain, among other values.

An example of Expectation Parameters is shown below (notice that we are now in a new Expectation class):



#### 4. Validate Configuration

We have almost reached the end of our journey in implementing an Expectation! 
Now, if we have requested certain parameters from the user, we would like to validate that the user has entered them correctly via a `validate_configuration` method, and raise an error if the Expectation has been incorrectly configured.



In this method, the user provides a configuration, and we check that certain conditions are satisfied by the configuration. We need to verify that the basic configuration parameters are set:



And validate optional configuration parameters. For example, if the user has given us a minimum and maximum threshold, it is important to verify that our minimum threshold does not exceed our maximum threshold:


#### 5. Validate

In this step, we simply need to validate that the results of our Metrics meet our Expectation.

The validate method is implemented as `_validate`. 
This method takes a dictionary named `metrics`, which contains all Metrics requested by your Metric dependencies, 
and performs a simple validation against your success keys (i.e. important thresholds) in order to return a dictionary indicating whether the Expectation has evaluated successfully or not:


You have now implemented your own Custom Expectation! For more information about Expectations and Metrics, please reference the [Core Concepts](../../../reference/core_concepts.md) documentation.

:::note
To use a custom Expectation, you need to ensure it has been placed into your `plugins/` directory and imported into the running Python interpreter.
:::

## Next Steps

When developing an Expectation, we highly encourage the writing of tests and implementation of renderers to verify that the Expectation works as intended and is providing the best possible results in your Data Docs.
If you plan on contributing your Custom Expectation into the `contrib` library of Great Expectations, there are certain baseline requirements that must be met with regard to backend implementation, renderer implementation, and testing.

Great Expectations provides templates to get you started on developing Custom Expectations for contribution, including renderers & test cases. The ColumnExpectation template can be found [here](../../../../examples/expectations/column_aggregate_expectation_template.py).

:::caution Under Construction
Please see the following documentation for more on:
* Maturity Levels
* Creating Example Cases & Tests For Custom Expectations
* Implementing Renderers For Custom Expectations
* Contributing Expectations To Great Expectations
:::


In the new Modular Expectation design, Expectations rely on [**Metrics**](../../../reference/metrics.md) defined by separate MetricProvider Classes, which are then referenced within the Expectation and used for computation. For more on Metric naming conventions, see our guide [here](../../../reference/metrics.md#metrics-naming-conventions).

Once you’ve decided on an Expectation to implement, think of the different aggregations, mappings, or metadata you’ll need to validate your data within the Expectation - each of these will be a separate Metric that must be implemented prior to validating your Expectation.

Fortunately, many Metrics have already been implemented for pre-existing Expectations, so it is possible you will find that the Metric you’d like to implement already exists within the Great Expectations framework and can be readily deployed. If so, you can skip to [Step 3](#3-define-parameters)!
