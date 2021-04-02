## Create & Edit Expectations

{% if batch_request %}
Add expectations by calling specific expectation methods on the `validator` object. They all begin with `.expect_` which makes autocompleting easy using tab.
{% else %}
You are adding Expectation configurations to the suite.  Since there is no sample batch of data, no validation happens during this process. 
To illustrate how to do this, consider a hypothetical example.  Suppose that you have a table with the columns "account_id", "user_id", "transaction_id", "transaction_type", and "transaction_amt_usd".
Then the following code snipped adds an expectation that the columns of the actual table will appear in the order specified above:
```
# create an Expectation
expectation_configuration: ExpectationConfiguration  = ExpectationConfiguration(
    expectation_type="expect_table_columns_to_match_ordered_list", # name of expectation type being added
    # These are the arguments of the expectation
    # The keys allowed in the dictionary are Parameters and Keyword Arguments of this Expectation Type
    kwargs={
        "column_list": ["account_id", "user_id", "transaction_id", "transaction_type", "transaction_amt_usd"]
    }, 
    # This is how you can optionally add a comment about this expectation. 
    # It will be rendered in Data Docs
    # (see this guide for details: https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_docs/how_to_add_comments_to_expectations_and_display_them_in_data_docs.html).
    meta={ 
        "notes": {
            "format": "markdown",
            "content": "Some clever comment about this expectation. **Markdown** `Supported`"
        }
    }
)
# add the Expectation to the suite
suite.add_expectation(expectation_configuration=expectation_configuration)
```
&nbsp;

Here are a few more example expectations for this dataset:
```
# create another Expectation
expectation_configuration = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set", 
    kwargs={
        "column": "transaction_type",
        "value_set": ["purchase", "refund", "upgrade"]
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
&nbsp;

```
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
&nbsp;

```
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
{% endif %}
&nbsp;

You can see all the available expectations in the **[expectation glossary](https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html?utm_source=notebook&utm_medium=create_expectations)**.
