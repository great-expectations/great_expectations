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

#### 2. Implement your Metric

If your metric does not yet exist within the framework, you will need to implement it yourself within a new file - a task that is quick and simple within the new modular framework.

Below lies the full implementation of an aggregate metric class, with implementations for Pandas, SQLAlchemy, and Apache Spark dialects. (Other implementations can be found in the dictionary of metrics).

````python
from great_expectations.execution_engine import (
   PandasExecutionEngine,
   SparkDFExecutionEngine,
   SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics import (
   ColumnAggregateMetricProvider,
   column_aggregate_value, column_aggregate_partial,
)
from great_expectations.expectations.metrics.import_manager import F, sa

class ColumnCustomMax(ColumnAggregateMetricProvider):
    """MetricProvider Class for Custom Aggregate Max MetricProvider"""

    metric_name = "column.aggregate.custom.max"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Max Implementation"""
        return column.max()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        """SqlAlchemy Max Implementation"""
        return sa.func.max(column)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, _table, _column_name, **kwargs):
        """Spark Max Implementation"""
        types = dict(_table.dtypes)
        return F.maxcolumn()
````

#### 3. Define Parameters

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

#### 4. Validate Configuration

We have almost reached the end of our journey in implementing an Expectation! Now, if we have requested certain parameters from the user, we would like to validate that the user has entered them correctly via a validate_configuration method.

In this method, the user provides a configuration and we check that certain conditions are satisfied by the configuration. For example, if the user has given us a minimum and maximum threshold, it is important to verify that our minimum threshold does not exceed our maximum threshold:

````python
def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
   """
   Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
   necessary configuration arguments have been provided for the validation of the expectation.

   Args:
       configuration (OPTIONAL[ExpectationConfiguration]): \
           An optional Expectation Configuration entry that will be used to configure the expectation
   Returns:
       True if the configuration has been validated successfully. Otherwise, raises an exception
   """
   min_val = None
   max_val = None

   # Setting up a configuration
   super().validate_configuration(configuration)
   if configuration is None:
       configuration = self.configuration

   # Ensuring basic configuration parameters are properly set
   try:
       assert (
           "column" in configuration.kwargs
       ), "'column' parameter is required for column map expectations"
   except AssertionError as e:
       raise InvalidExpectationConfigurationError(str(e))

 # Validating that Minimum and Maximum values are of the proper format and type
 if "min_value" in configuration.kwargs:
     min_val = configuration.kwargs["min_value"]

 if "max_value" in configuration.kwargs:
     max_val = configuration.kwargs["max_value"]

 try:
     # Ensuring Proper interval has been provided
     assert (
         min_val is not None or max_val is not None
     ), "min_value and max_value cannot both be none"
     assert min_val is None or isinstance(
         min_val, (float, int)
     ), "Provided min threshold must be a number"
     assert max_val is None or isinstance(
         max_val, (float, int)
     ), "Provided max threshold must be a number"
````

#### 5. Validate

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

#### 6. Test

When developing an Expectation, there are several different points at which you should test what you have written:

During development, you should import and run your Expectation, writing additional tests for get_evaluation parameters if it is complicated

It is often helpful to generate examples showing the functionality of your Expectation, which helps verify the Expectation works as intended.

If you plan on contributing your Expectation back to the library of main Expectations, you should build a JSON test for it in the `tests/test_definitions/name_of_your_expectation directory`.

#### 7. Import: To use a custom Expectation, you need to ensure it has been imported into the running Python interpreter. While including the module in your plugins/ directory will make it *available* to import, you must still import the Expectation:

````python
# get a validator
# Note: attempting to run our expectation now would fail, because even though
# our Expectation is in our DataContext plugins/ directory it has not been imported.

from custom_module import ExpectColumnMaxToBeBetweenCustom

# now we can run our expectation
validator.expect_column_max_to_be_between_custom('col', min_value=0, max_value=5)
````

#### 8. Optional: Implement [Custom Data Docs Renderers](/docs/guides/expectations/advanced/how_to_create_renderers_for_custom_expectations)

We have now implemented our own Custom Expectations! For more information about Expectations and Metrics, please reference the core concepts documentation.

1. Arguments for Custom Expectations currently **must be provided as keyword arguments**; positional arguments should be avoided.
