---
title: How to create and edit Expectations based on domain knowledge, without inspecting data directly
---

import Prerequisites from '../../guides/connecting_to_your_data/components/prerequisites.jsx'

This guide shows how to create an Expectation Suite without a sample Batch.

Here are some of the reasons why you may wish to do this:

1. You don't have a sample.
2. You don't currently have access to the data to make a sample.
3. You know exactly how you want your Expectations to be configured.
4. You want to create Expectations parametrically (you can also do this in interactive mode).
5. You don't want to spend the time to validate against a sample.

If you have a use case we have not considered, please [contact us on Slack](https://greatexpectations.io/slack).

<Prerequisites>

- Configured a [Data Context](../../tutorials/getting_started/initialize_a_data_context.md).
- Have your Data Context configured to save Expectations to your filesystem (please see [How to configure an Expectation store to use a filesystem](../../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_on_a_filesystem.md)) or another Expectation Store if you are in a hosted environment.

</Prerequisites>

## Steps

### 1. Use the CLI to generate a helper notebook

From the command line, run:

```bash
great_expectations suite new
```

### 2. Create Expectation Configurations in the helper notebook

You are adding Expectation configurations to the suite. Since there is no sample Batch of data, no Validation happens during this process. To illustrate how to do this, consider a hypothetical example. Suppose that you have a table with the columns ``account_id``, ``user_id``, ``transaction_id``, ``transaction_type``, and ``transaction_amt_usd``. Then the following code snipped adds an Expectation that the columns of the actual table will appear in the order specified above:

```python
# Create an Expectation
expectation_configuration = ExpectationConfiguration(
   # Name of expectation type being added
   expectation_type="expect_table_columns_to_match_ordered_list",
   # These are the arguments of the expectation
   # The keys allowed in the dictionary are Parameters and
   # Keyword Arguments of this Expectation Type
   kwargs={
      "column_list": [
         "account_id", "user_id", "transaction_id", "transaction_type", "transaction_amt_usd"
      ]
   },
   # This is how you can optionally add a comment about this expectation.
   # It will be rendered in Data Docs.
   # See this guide for details:
   # `How to add comments to Expectations and display them in Data Docs`.
   meta={
      "notes": {
         "format": "markdown",
         "content": "Some clever comment about this expectation. **Markdown** `Supported`"
      }
   }
)
# Add the Expectation to the suite
suite.add_expectation(expectation_configuration=expectation_configuration)
```

Here are a few more example expectations for this dataset:

```python
expectation_configuration = ExpectationConfiguration(
   expectation_type="expect_column_values_to_be_in_set",
   kwargs={
      "column": "transaction_type",
      "value_set": ["purchase", "refund", "upgrade"]
   },
   # Note optional comments omitted
)
suite.add_expectation(expectation_configuration=expectation_configuration)
```

```python
expectation_configuration = ExpectationConfiguration(
   expectation_type="expect_column_values_to_not_be_null",
   kwargs={
      "column": "account_id",
      "mostly": 1.0,
   },
   meta={
      "notes": {
         "format": "markdown",
         "content": "Some clever comment about this expectation. **Markdown** `Supported`"
      }
   }
)
suite.add_expectation(expectation_configuration=expectation_configuration)
```

```python
expectation_configuration = ExpectationConfiguration(
   expectation_type="expect_column_values_to_not_be_null",
   kwargs={
      "column": "user_id",
      "mostly": 0.75,
   },
   meta={
      "notes": {
         "format": "markdown",
         "content": "Some clever comment about this expectation. **Markdown** `Supported`"
      }
   }
)
suite.add_expectation(expectation_configuration=expectation_configuration)
```

You can see all the available Expectations in the [Expectation Gallery](https://greatexpectations.io/expectations).

### 3. Save your Expectation Suite

Run the final cell in the helper notebook to save your Expectation Suite.

This will create a JSON file with your Expectation Suite in the Store you have configured, which you can then load and use for [Validation](../../reference/validation.md).
