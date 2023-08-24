---
title: Edit an existing Expectation Suite 
tag: [how-to, getting started]
description: Modify specific Expectations in an Expectation Suite.
keywords: [Expectations, ExpectationsSuite]
---

import Prerequisites from '/docs/components/_prerequisites.jsx'

Use the information provided here to learn how to edit an Expectation Suite. Editing Expectations does not edit or alter the Batch data.

All the code used in the examples is available in GitHub at this location: [how_to_edit_an_expectation_suite.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite.py).


## Prerequisites

<Prerequisites>

- A working installation of Great Expectations
- A Filesystem Data Context for your Expectations
- A Data Source from which to request a Batch of data for introspection
- An Expectation Suite

</Prerequisites> 

## Import the Great Expectations module and instantiate a Data Context

Run the following code to create a new Data Context with the `get_context()` method:

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite get_context"
```

## Create a Validator from Data 

Run the following code to connect to `.csv` data stored in the `great_expectations` GitHub repository:

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite create_validator"
```

## Retrieve an existing Expectation Suite 

Run the following code to retrieve an Expectation Suite:

```python
context.get_expectation_suite("expectation_suite_name")
```
Replace `expectation_suite_name` with the name of your Expectation Suite.

## View the Expectations in the Expectation Suite

Run the following code to print the Suite to console or Jupyter Notebook the `show_expectations_by_expectation_type()` method:

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite show_suite"
```

The output appears similar to the following example: 

```bash 
    [ { 'expect_column_values_to_be_between': { 'auto': True,
                                                'column': 'passenger_count',
                                                'domain': 'column',
                                                'max_value': 6,
                                                'min_value': 1,
                                                'mostly': 1.0,
                                                'strict_max': False,
                                                'strict_min': False}},
      { 'expect_column_values_to_not_be_null': { 'column': 'pickup_datetime',
                                                'domain': 'column'}}]
```

## Instantiate ExpectationConfiguration 

From the Expectation Suite, you can create an ExpectationConfiguration object using the output from `show_expectations_by_expectation_type(). The following is the example output from the first Expectation in the Expectation Suite. 

It runs the `expect_column_values_to_be_between` Expectation on the `passenger_count` column and expects the min and max values to be `1` and `6` respectively. 

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite example_dict_1"
```

The following is the same configuration with an `ExpectationConfiguration` object:  

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite import_expectation_configuration"
```
```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite example_configuration_1"
```

## Update the Configuration and Expectation Suite

In the following example, the `max_value` of the Expectation is adjusted from `4` to `6` with a new `ExpectationConfiguration`: 

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite updated_configuration"
```

To update the Expectation Suite you use the `add_expectation()` function. For example:

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite add_configuration"
```
The `add_expectation()` function performs an 'upsert' into the `ExpectationSuite` and updates the existing Expectation, or adds a new one if it doesn't.

To check that the Expectation Suite has been updated, you can run the `show_expectations_by_expectation_type()` function again, or run `find_expectation()` and then confirm that the expected Expectation exists in the suite. For example:

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite find_configuration"
```

You'll need to perform the search with a new `ExpectationConfiguration`, but you don't need to include all the `kwarg` values.

## Remove the ExpectationConfiguration (Optional)

To remove an `ExpectationConfiguration`, you can use the `remove_configuration()` function. Similar to `find_expectation()`, you call the `remove_configuration()` function with `ExpectationConfiguration`. For example:

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite remove_configuration"
```

The output of `show_expectations_by_expectation_type()` should appear similar to this example: 

```bash 
[ 
  { 'expect_column_values_to_not_be_null': { 'column': 'pickup_datetime',
                                             'domain': 'column'}}]
```

## Save Expectation Suite changes

After editing an Expectation Suite, you can use the `save_suite()` function to save it to your Data Context. For example:

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite save_suite"
```
To make sure your Expectation Suite changes are reflected in the Validator, use `context.get_validator()` to overwrite the `validator`, or create a new one from the updated Data Context.

## Related documentation

- To learn more about the functions available with Expectation Suites, see the [`ExpectationSuite` API Documentation](https://docs.greatexpectations.io/docs/reference/api/core/ExpectationSuite_class). 

