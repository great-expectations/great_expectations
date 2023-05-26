---
title: How to Edit an Expectation Suite 
tag: [how-to, getting started]
description: Create an Expectation Suite using a Validator. Then, examine and modify specific Expectations in the Suite.
keywords: [Expectations, ExpectationsSuite]
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import IfYouStillNeedToSetupGX from '/docs/components/prerequisites/_if_you_still_need_to_setup_gx.md'

In this guide, you'll learn how to create Expectations and interactively edit the resulting Expectation Suite.

:::info Does this process edit my data?
No.  The interactive method used to create and edit Expectations does not edit or alter the Batch data.
:::

## Prerequisites

<Prerequisites>

- Great Expectations installed in a Python environment
- A Filesystem Data Context for your Expectations
- Created a Datasource from which to request a Batch of data for introspection

</Prerequisites> 

<details>
<summary>

### If you haven't set up Great Expectations

</summary>

<IfYouStillNeedToSetupGX />

</details>


## Steps

### 1. Import the Great Expectations module and instantiate a Data Context

The simplest way to create a new Data Context is by using the `get_context()` method.

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite get_context"
```

### 2. Create a Validator from Data 

Run the following command to connect to `.csv` data stored in the `great_expectations` GitHub repository:

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite create_validator"
```

### 3. Create Expectations with Validator 

Run the following commands to create two Expectations. The first Expectation uses domain knowledge (the `pickup_datetime` shouldn't be null), and the second Expectation uses `auto=True` to detect a range of values in the `passenger_count` column.


```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite add_2_expectations"
```

Under the hood, the Validator will be creating and updating an Expectation Suite, which we can view next. 

### 4. View the Expectations in the Expectation Suite

There are a number of different ways that this can be done, with one way being using the `show_expectations_by_expectation_type()` function, which will use `prettyprint` to print the Suite to the console in a way that can be easily visualized. 

First load the `ExpectationSuite` from the `Validator`: 

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite get_suite"
```

Now use the `show_expectations_by_expectation_type()` to print the Suite to console or Jupyter Notebook.

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite show_suite"
```

Your output will look something similar to this: 

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

### 5. Instantiate ExpectationConfiguration 

From the Expectation Suite, you will be able to create an ExpectationConfiguration object using the output from `show_expectations_by_expectation_type()` Here is the example output of the first Expectation in our suite.

It runs the `expect_column_values_to_be_between` Expectation on the `passenger_count` column and expects the min and max values to be `1` and `6` respectively. 

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite example_dict_1"
```

Here is the same configuration, but this time as a `ExpectationConfiguration` object.  

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite import_expectation_suite"
```
```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite example_configuration_1"
```

### 6. Update Configuration and ExpectationSuite

Let's say that you are interested in adjusting the `max_value` of the Expectation to be `4` instead of `6`. Then you could create a new `ExpectationConfiguration` with the new value: 

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite updated_configuration"
```

And update the ExpectationSuite by calling `add_expectation()`. The `add_expectation()` function will perform an 'upsert' into the `ExpectationSuite`, meaning it will update an existing Expectation if it already exists, or add a new one if it doesn't. 

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite add_configuration"
```

You can check that the ExpectationSuite has been correctly updated by either running the `show_expectations_by_expectation_type()` function again, or by running `find_expectation()` and confirming that the expected Expectation exists in the suite.  The search will need to be performed with a new `ExpectationConfiguration`, but will not need to inclued all of the  `kwarg` values.

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite find_configuration"
```

### 7. (Optional) Remove Configuration 

If you would like to remove an ExpectationConfiguration, you can use the `remove_configuration()` function. 

Similar to `find_expectation()`, the `remove_configuration()` function needs to be called with an `ExpectationConfiguration`.

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite remove_configuration"
```

The output of `show_expectations_by_expectation_type()` should now look like this: 

```bash 
[ 
  { 'expect_column_values_to_not_be_null': { 'column': 'pickup_datetime',
                                             'domain': 'column'}}]
```

### 8. Save ExpectationSuite 

Finally, when you are done editing the `ExpectationSuite`, you can save it to your Data Context by using the `save_suite()` function. 

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite save_suite"
```

## Related Documentation

* If you would like to learn more about the functions available at the Expectation Suite-level, please refer to our [API Documentation for `ExpectationSuite`](https://docs.greatexpectations.io/docs/reference/api/core/ExpectationSuite_class). 

* To view the full script used for example code on this page, see it on GitHub:
[how_to_edit_an_expectation_suite.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite.py)
