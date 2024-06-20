---
title: Create an Expectation
description: Create and modify an Expectation in Python.
hide_table_of_contents: true
---
import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';

import StandardArgumentsTable from './_expectations/_standard_arguments_table.md';

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

An Expectation is a verifiable assertion about your data. Expectations make implicit assumptions about your data explicit, and they provide a flexible, declarative language for describing expected behavior. They can help you better understand your data and help you improve data quality. 

## Prerequisites

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Choose an Expectation to create.

   GX comes with many built in Expectations to cover your data quality needs.  You can find a catalog of these Expectations in the [Expectation Gallery](https://greatexpectations.io/expectations/).  When browsing the Expectation Gallery you can filter the available Expectations by the data quality issue they address and by the Data Sources they support.  There is also a search bar that will let you filter Expectations by matching text in their name or docstring.

   In your code, you will find the classes for Expectations in the `expectations` module:

   ```python title="Python"
   from great_expectations import expectations as gxe
   ```

2. Determine the Expectation's evaluation parameters

   To determine the parameters your Expectation uses to evaluate data, reference the Expectation's entry in the [Expectation Gallery](https://greatexpectations.io/expectations/).  Under the **Args** section you will find a list of parameters that are necessary for the Expectation to be evaluated, along with the a description of the value that should be provided.

   Parameters with a default value do not need to be set.  Parameters that are set to `None` can be provided at runtime.  Values for all other parameters must be provided when the Expectation is created.

   For example, take the [**Args** section for `ExpectColumnMaxToBeBetween` in the Expectation Gallery](https://greatexpectations.io/expectations/expect_column_max_to_be_between#args).  In this section, you will see that for `ExpectColumnMaxToBeBetween`'s parameters:

      - `column` must be a string.  Since it cannot be set to `None` its value must be provided when the Expectation is created.
      - `min_value` and `max_value` can be either set when the Expectation is created, or be set to `None` when the Expectation is created and then have appropriate values passed in at runtime, instead.
      - `strict_min` and `strict_max` both have default values that can be overriden when the Expectation is created, or left as is.  Because they cannot be set to `None` when the Expectation is created these parameters also cannot be passed in at runtime.

3. Optional. Determine the Expectation's other parameters

   In addition to the parameters that are required for an Expectation to evaluate data all Expectations also support some standard parameters that determine how strictly Expectations are evaluated and permit the addition of metadata.  In the Expectations Gallery these are found under each Expectation's **Other Parameters** section.

   These parameters are:

   <StandardArgumentsTable/>

4. Create the Expectation.

   Using the Expectation class you picked and the parameters you determined when referencing the Expectation Gallery, you can create your Expectation.

   In this example the `ExpectColumnMaxToBeBetween` Expectation is created and all of its parameters are defined in advance while leaving `strict_min` and `strict_max` as their default values:

   ```python title="Python"
   preset_expectation = gxe.ExpectColumnMaxToBeBetween(
       column="passenger_count",
       min_value=4,
       max_value=6
   )
   ```
   
   While in this example the same `ExpectColumnMaxToBeBetween` Expectation is created, but the values for `min_value` and `max_value` will be provided at runtime rather than set when the Expectation is created:

   ```python title="Python"
   runtime_expectation = gxe.ExpectColumnMaxToBeBetween(
       column="passenger_count",
       min_value=None,
       max_value=None
   )
   ```
   
   The runtime `expectation_parameters` dictionary for the above example would look like:

   ```python title="Python"
   runtime_expectation_parameters = {
      "min_value": 4,
      "max_value": 6
   }
   ```

   In some cases, you may have Expectations with the same parameters all of which are intended to be passed in at runtime.  When you create these Expectations you can provide instructions for GX to look up a specific parameter under a different name in the runtime `expectations_parameter` dictionary.  This is done by setting the value for the parameter as a dictionary containing the key `$PARAMETER` paired with the key to look for in the `expectations_parameter` dictionary.

   In this example, `ExpectColumnMaxToBeBetween` is set for both the `passenger_count` and the `fare` fields, and the values for `min_value` and `max_value` in each Expectation will be passed in at runtime.  To differentiate between the parameters for each Expectation a more specific key is set for finding the parameter in the runtime `expectation_parameters` dictionary:

   ```python title="Python"
   passenger_expectation = gxe.ExpectColumnMaxToBeBetween(
      column="passenger_count",
      min_value={"$PARAMETER": "expect_passenger_max_to_be_above"},
      max_value={"$PARAMETER": "expect_passenger_max_to_be_below"}
   )
   
   fare_expectation = gxe.ExpectColumnMaxToBeBetween(
      column="fare",
      min_value={"$PARAMETER": expect_fare_max_to_be_above"},
      max_value={"$PARAMETER": expect_fare_max_to_be_below"}
   )
   ```
   
   The runtime `expectation_parameters` dictionary for the above example would look like:

   ```python title="Python"
   runtime_expectation_parameters = {
      "expect_passenger_max_to_be_above": 4,
      "expect_passenger_max_to_be_below": 6,
      "expect_fare_max_to_be_above": 10.00,
      "expect_fare_max_to_be_below": 500.00
   }
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

   ```python title="Python"
   # All Expectations are found in the `expectations` module:
   from great_expectations import expectations as gxe
   
   # This Expectation has all values set in advance:
   preset_expectation = gxe.ExpectColumnMaxToBeBetween(
       column="passenger_count",
       min_value=1,
       max_value=6
   )
   
   # This Expectation requires `min_value` and `max_value`
   #   to be passed in at runtime:
   runtime_expectation = gxe.ExpectColumnMaxToBeBetween(
       column="passenger_count",
       min_value=None,
       max_value=None
   )
   
   # This is an example of a dictionary that could be passed in at runtime
   #  above Expectation.  A different dictionary with different values
   #  (but the same keys) could be passed in every time the Expectation
   #  is run.
   runtime_expectation_parameters = {
      "min_value": 4,
      "max_value": 6
   }
   
   # In this case, two Expectations are created that will be passed
   #  parameters at runtime, so a way to differentiate between the two
   #  in the runtime dictionary needs to be specified.

   passenger_expectation = gxe.ExpectColumnMaxToBeBetween(
      column="passenger_count",
      min_value={"$PARAMETER": "expect_passenger_max_to_be_above"},
      max_value={"$PARAMETER": "expect_passenger_max_to_be_below"}
   )
   fare_expectation = gxe.ExpectColumnMaxToBeBetween(
      column="fare",
      min_value={"$PARAMETER": expect_fare_max_to_be_above"},
      max_value={"$PARAMETER": expect_fare_max_to_be_below"}
   )
   
   # A dictionary containing the parameters for both of the above
   #   Expectations would look like:
   runtime_expectation_parameters = {
      "expect_passenger_max_to_be_above": 4,
      "expect_passenger_max_to_be_below": 6,
      "expect_fare_max_to_be_above": 10.00,
      "expect_fare_max_to_be_below": 500.00
   }
   ```

</TabItem>

</Tabs>

   

