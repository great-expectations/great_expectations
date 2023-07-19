---
title: Use auto-initializing Expectations
---

import Prerequisites from '../../guides/connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Use the information provided here to learn how you can use auto-initializing <TechnicalTag tag="expectation" text="Expectations" /> to automate parameter estimation when you create Expectations interactively using a <TechnicalTag tag="batch" text="Batch" /> or Batches that have been loaded into a <TechnicalTag tag="validator" text="Validator" />.

This guide assumes that you are creating and editing expectations in a Jupyter Notebook.  For more information about this process, see [How to create and edit expectations with instant feedback from a sample batch of data](./how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md).  

Additionally, this guide assumes that you are using a multi-batch <TechnicalTag tag="batch_request" text="Batch Request" /> to provide your sample data. Auto-initializing Expectations will work when run on a single Batch, but they really shine when run on multiple Batches that would have otherwise needed to be individually processed if a manual approach were taken.

The following scripts are used in this topic and are available in GitHub:

- [is_expectation_auto_initializing.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/expectations/auto_initializing_expectations/is_expectation_auto_initializing.py)
- [auto_initializing_expect_column_mean_to_be_between.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/expectations/auto_initializing_expectations/auto_initializing_expect_column_mean_to_be_between.py)

## Prerequisites

- Completion of the [Quickstart guide](tutorials/quickstart/quickstart.md)
- [A configured Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context)
- [A configured Datasource](/docs/guides/connecting_to_your_data/connect_to_data_lp)
- [An understanding of how to configure a BatchRequest](/docs/0.15.50/guides/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource)
- [An understanding of how to create and edit Expectations with instant feedback from a sample batch of data](./how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md)


## Determine if your Expectation is auto-initializing

Not all Expectations are auto-initializing.  In order to be an auto-initializing Expectation, an Expectation must have parameters that can be estimated.  As an example: `ExpectColumnToExist` only takes in a `Domain` (which is the column name) and checks whether the column name is in the list of names in the table's metadata.  This would be an example of an Expectation that would not work under the auto-initializing framework.

An example of Expectations that would work under the auto-initializing framework would be the ones that have numeric ranges, like `ExpectColumnMeanToBeBetween`, `ExpectColumnMaxToBeBetween`, and `ExpectColumnSumToBeBetween`.

To check whether the Expectation you are interested in works under the auto-initializing framework, run the `is_expectation_auto_initializing()` method of the `Expectation` class.

For example:

```python name="tests/integration/docusaurus/expectations/auto_initializing_expectations/is_expectation_auto_initializing.py is_expectation_self_initializing False"
```

will return `False` and print the message:

```markdown title="Console output"
The Expectation expect_column_to_exist is not able to be auto-initialized.
```

However, the command:

```python name="tests/integration/docusaurus/expectations/auto_initializing_expectations/is_expectation_auto_initializing.py is_expectation_self_initializing True"
```

will return `True` and print the message:

```markdown title="Console output"
The Expectation expect_column_mean_to_be_between is able to be auto-initialized. Please run by using the auto=True parameter.
```

For the purposes of this guide, we will be using `expect_column_mean_to_be_between` as our example Expectation.

## Run the expectation with `auto=True`

Say you are interested in constructing an Expectation that captures the average distance of taxi trips across all of 2018.  You have a <TechnicalTag tag="datasource" text="Datasource" /> that provides 12 Batches (one for each month of the year) and you know that `expect_colum_mean_to_be_between` is the Expectation you want to implement.

### Run the expectation manually

The Expectation `expect_column_mean_to_be_between()` has the following parameters:

- column (str): The column name.
- min_value (float or None): The minimum value for the column mean.
- max_value (float or None): The maximum value for the column mean.
- strict_min (boolean): If True, the column mean must be strictly larger than min_value, default=False
- strict_max (boolean): If True, the column mean must be strictly smaller than max_value, default=False

Without the auto-initialization framework you would have to get the values for `min_value` and `max_value` for your series of 12 Batches by calculating the mean value for each Batch and using calculated `mean` values to determine the `min_value` and `max_value` parameters to pass your Expectation.  This, although not _difficult_, would be a monotonous and time-consuming task.

### Using `auto=True`

Auto-initializing Expectations automate this sort of calculation across batches.  To perform the same calculation described above (the mean ranges across the 12 Batches in the 2018 taxi data) the only thing you need to do is run the Expectation with `auto=True`

```python name="tests/integration/docusaurus/expectations/auto_initializing_expectations/auto_initializing_expect_column_mean_to_be_between.py run expectation"
```

Now the Expectation will calculate the `min_value` (2.83) and `max_value` (3.06) using all the Batches that are loaded into the Validator.  In our case, that means all 12 Batches associated with the 2018 taxi data.

## Save your Expectation with the calculated values

Now that the Expectation's upper and lower bounds have come from the Batches, you can save your <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> and move on.

```python name="tests/integration/docusaurus/expectations/auto_initializing_expectations/auto_initializing_expect_column_mean_to_be_between.py save suite"
```
