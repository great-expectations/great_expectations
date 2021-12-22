---
title: How to create a Custom Column Aggregate Expectation
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'

Beginning in version 0.13, we have introduced a new API focused on enabling Modular [**Expectations**](../../../reference/expectations/expectations.md). They utilize a class structure that is significantly easier to build than ever before!

**ColumnExpectations** are evaluated for a single column, and produce an aggregate [**Metric**](../../../reference/metrics.md), such as a mean, standard deviation, number of unique values, column type, etc.

This guide will walk you through the process of creating your own Modular ColumnExpectations in a few simple steps!

We will be following this [complete example](https://github.com/superconductive/ge_tutorials/tree/main/getting_started_tutorial_final_v3_api/great_expectations/plugins/column_custom_max_expectation.py).

<Prerequisites>

</Prerequisites>

### Steps

#### 1. Plan Metric Dependencies

In the new Modular Expectation design, Expectations rely on Metrics defined by separate MetricProvider Classes, which are then referenced within the Expectation and used for computation. For more on Metric naming conventions, see our guide [here](../../../reference/metrics.md#metrics-naming-conventions).

Once you’ve decided on an Expectation to implement, think of the different aggregations, mappings, or metadata you’ll need to validate your data within the Expectation - each of these will be a separate Metric that must be implemented prior to validating your Expectation.

Fortunately, many Metrics have already been implemented for pre-existing Expectations, so it is possible you will find that the Metric you’d like to implement already exists within the Great Expectations framework and can be readily deployed. If so, you can skip to [Step 3](#3-define-parameters)!

#### 2. Implement your Metric

Expectations rely on Metrics to produce their result. A Metric is any observable property of data (e.g., numeric stats like mean/median/mode of a column, but also richer properties of data, such as histogram). You can read more about the relationship between Expectations and Metrics in our [Core Concepts: Expectations and Metrics](../../../reference/metrics).

If your Metric does not yet exist within the framework, you will need to implement it yourself in a new class - a task that is quick and simple within the new modular framework. The convention is to implement a new Metric Provider (a class that can compute a metric) that your Expectation depends on in the same file as the Expectation itself.

The parent class expects the variable `metric_name` to be set. Change the value of `metric_name` to something that fits your Metric. Follow these two naming conventions:

* the name should start with “column.”, because it is a column Metric
* the second part of the name (after the “.”) should be in snake_case format

The parent class of your Metric Provider class is `ColumnMetricProvider`. It uses Python Decorators to hide most of the complexity from you, and give you a clear and simple API to implement one method per backend that computes the metric.
Implement the computation of the metric in your new Metric Provider class for at least one of the three backends ([**Execution Engines**](../../../reference/execution_engine.md)) that Great Expectations supports: Pandas, SQLAlchemy, and Spark.

Here is the implementation of our example metric for Pandas:

```python file=../../../../examples/expectations/column_custom_max_expectation.py#L40-L43
```
This means that the method `_pandas` is a metric function that is decorated as a `column_aggregate_value`. It will be called with the engine-specific column type. It must return a value that is computed over this column. 
The `engine` argument of `column_aggregate_value` is set to `PandasExecutionEngine` to signal to the method in the parent class that this method computes the Metric for the Pandas backend.

:::note
If you have never used Python Decorators and don’t know what they are and how they work, no worries - this should not stop you from successfully implementing your Expectation. Decorators allow the parent class to “wrap” your methods, which means to execute some code before and after your method runs. All you need to know is the name of the Decorator to add (with “@”) above your method definition.
:::

Below lies the full implementation of an aggregate metric class, with implementations for Pandas, SQLAlchemy, and Apache Spark Dialects.

```python file=../../../../examples/expectations/column_custom_max_expectation.py#L35-L54
```

#### 3. Define Parameters

The structure of a Modular Expectation now exists within its own specialized class. This structure has 3 fundamental components: Metric Dependencies, Configuration Validation, and Expectation Validation. In this step, we will address setting up our parameters.

In this guide, we're focusing on a `ColumnExpectation`, which can define metric dependencies simply using the metric_dependencies property.

Add the following attributes to your Expectation class:

* **Metric Dependencies** - A tuple consisting of the names of all metrics necessary to evaluate the Expectation. Using this shortcut tuple will provide the dependent metric with the same domain kwargs and value kwargs as the Expectation.

* **Success Keys** - A tuple consisting of values that must / could be provided by the user and defines how the Expectation evaluates success.

* **Default Kwarg Values** (Optional) - Default values for success keys and the defined domain, among other values.

An example of Expectation Parameters is shown below (notice that we are now in a new Expectation class):

```python file=../../../../examples/expectations/column_custom_max_expectation.py#L57-L71
```

#### 4. Validate Configuration

We have almost reached the end of our journey in implementing an Expectation! 
Now, if we have requested certain parameters from the user, we would like to validate that the user has entered them correctly via a `validate_configuration` method, and raise an error if the Expectation has been incorrectly configured.

```python file=../../../../examples/expectations/column_custom_max_expectation.py#L110-L127
```

In this method, the user provides a configuration, and we check that certain conditions are satisfied by the configuration. We need to verify that the basic configuration parameters are set:

```python file=../../../../examples/expectations/column_custom_max_expectation.py#L129-L135
```

And validate optional configuration parameters. For example, if the user has given us a minimum and maximum threshold, it is important to verify that our minimum threshold does not exceed our maximum threshold:

```python file=../../../../examples/expectations/column_custom_max_expectation.py#L137-L160
```

#### 5. Validate

In this step, we simply need to validate that the results of our Metrics meet our Expectation.

The validate method is implemented as `_validate`. This method takes a dictionary named `metrics`, which contains all Metrics requested by your Metric dependencies, and performs a simple validation against your success keys (i.e. important thresholds) in order to return a dictionary indicating whether the Expectation has evaluated successfully or not:

```python file=../../../../examples/expectations/column_custom_max_expectation.py#L73-L108
```

:::note
To use a custom Expectation, you need to ensure it has been placed into your `plugins/` directory and imported into the running Python interpreter.
:::

You have now implemented your own Custom Expectation! For more information about Expectations and Metrics, please reference the Core Concepts documentation.

## Next Steps

When developing an Expectation, we highly encourage the writing of tests and implementation of renderers to verify that the Expectation works as intended and is providing the best possible results in your Data Docs.
If you plan on contributing your Custom Expectation into the `contrib` library of Great Expectations, there are certain baseline requirements that must be met with regard to backend implementation, renderer implementation, and testing.

Great Expectations provides templates to get you started on developing Custom Expectations for contribution, including renderers & test cases. The ColumnExpectation template can be found [here](../../../../examples/expectations/column_aggregate_expectation_template.py).

:::caution Under Construction
Please see the following documentation for more on:
* Maturity Levels
* Creating Example Cases & Tests For Custom Expectations
* Implementing Renderers For Custom Expectations
:::