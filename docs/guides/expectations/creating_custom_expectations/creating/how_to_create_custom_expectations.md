---
title: How to create custom Expectations
---

This document provides examples that walk through several methods for building and deploying custom expectations.

Beginning in version 0.13, we have introduced a new API focused on enabling Modular Expectations that are defined in individual classes.

This guide will walk you through the process of creating your own Modular Expectations in 6 simple steps!

See also this [complete example](https://github.com/superconductive/ge_tutorials/tree/main/getting_started_tutorial_final_v3_api/great_expectations/plugins/column_custom_max_expectation.py).


:::note Prerequisites
This how-to guide assumes you have already:

* [Set up a working deployment of Great Expectations](/docs/tutorials/getting_started/intro)
:::

Modular Expectations are new in version 0.13. They utilize a class structure that is significantly easier to build than ever before and are explained below!

#### 1. Plan Metric Dependencies

In the new Modular Expectation design, Expectations rely on Metrics defined by separate MetricProvider Classes, which are then referenced within the Expectation and used for computation. For more on Metric Naming Conventions, see our guide on [metric naming conventions](/docs/reference/metrics).

Once you’ve decided on an Expectation to implement, think of the different aggregations, mappings, or metadata you’ll need to validate your data within the Expectation - each of these will be a separate metric that must be implemented prior to validating your Expectation.

Fortunately, many Metrics have already been implemented for pre-existing Expectations, so it is possible you will find that the Metric you’d like to implement already exists within the GE framework and can be readily deployed.

If your Expectation requires creating a custom Metric, it will be defined in a separate class in the next section [how to create a custom Metric](how_to_create_a_custom_metric).

#### 2. Define Parameters

We have already reached the point where we can start building our Expectation!

The structure of a Modular Expectation now exists within its own specialized class - indicating it will usually exist in a separate file from the Metric. This structure has 3 fundamental components: Metric Dependencies, Configuration Validation, and Expectation Validation. In this step, we will address setting up our parameters.

In this guide, we focus on a `ColumnExpectation` which can define metric dependencies simply using the metric_dependencies property.

Add the following attributes to your Expectation class:

* **Metric Dependencies** - A tuple consisting of the names of all metrics necessary to evaluate the Expectation. Using this shortcut tuple will provide the dependent metric with the same domain kwargs and value kwargs as the Expectation.

* **Success Keys** - A tuple consisting of values that must / could be provided by the user and defines how the Expectation evaluates success.

* **Default Kwarg Values** (Optional) - Default values for success keys and the defined domain, among other values.

An example of Expectation Parameters is shown below (notice that we are now in a new Expectation class and building our Expectation in a separate file from our Metric):

````python
class ExpectColumnMaxToBeBetweenCustom(ColumnExpectation):
   # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
   metric_dependencies = ("column.aggregate.custom.max",)
   success_keys = ("min_value", "strict_min", "max_value", "strict_max")

   # Default values
   default_kwarg_values = {
       "row_condition": None,
       "condition_parser": None,
       "min_value": None,
       "max_value": None,
       "strict_min": None,
       "strict_max": None,
       "mostly": 1
   }
````

#### 3. Validate

In this step, we simply need to validate that the results of our metrics meet our Expectation.

The validate method is implemented as _validate. This method takes a dictionary named Metrics, which contains all metrics requested by your metric dependencies, and performs a simple validation against your success keys (i.e. important thresholds) in order to return a dictionary indicating whether the Expectation has evaluated successfully or not:

````python
def _validate(
   self,
   configuration: ExpectationConfiguration,
   metrics: Dict,
   runtime_configuration: dict = None,
   execution_engine: ExecutionEngine = None,
):
   """Validates the given data against the set minimum and maximum value thresholds for the column max"""
   column_max = metrics["column.aggregate.custom.max"]

   # Obtaining components needed for validation
   min_value = self.get_success_kwargs(configuration).get("min_value")
   strict_min = self.get_success_kwargs(configuration).get("strict_min")
   max_value = self.get_success_kwargs(configuration).get("max_value")
   strict_max = self.get_success_kwargs(configuration).get("strict_max")

   # Checking if mean lies between thresholds
   if min_value is not None:
       if strict_min:
           above_min = column_max > min_value
       else:
           above_min = column_max >= min_value
   else:
       above_min = True

   if max_value is not None:
       if strict_max:
           below_max = column_max < max_value
       else:
           below_max = column_max <= max_value
   else:
       below_max = True

   success = above_min and below_max

   return {"success": success, "result": {"observed_value": column_max}}
````
