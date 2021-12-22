---
title: How to add configuration validation for an Expectation. 
---

This document describes how to add configuration validation for an Expectation. 

:::note Prerequisites
This how-to guide assumes you have already:

* [Set up a working deployment of Great Expectations](/docs/tutorials/getting_started/intro)
:::

Our Expectation will typically be configured using input parameters: for example, we might expect the fraction of null values to be most `mostly=.05`. We can define the `validate_configuration` method of our Expectation to ensure that the input parameters constitute a valid configuration, so that it raises an exception if `mostly=-2` or `mostly=Joe`. 

In the following example, given input parameters of a minimum and maximum threshold, we verify that our minimum threshold does not exceed our maximum threshold:

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
