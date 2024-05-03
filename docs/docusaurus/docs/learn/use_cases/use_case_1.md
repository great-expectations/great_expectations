---
sidebar_label: 'Data observability: Schema'
title: 'Data observability: Schema'
description: Use GX to validate data schema.
---

Data **schema** refers to the organization and format of your data.

## The importance of schema

Managing and validating data schema is fundamental for successful collaboration between data producers and data consumers.

As a data consumer within an organization, your data producers could be internal or external to your organization. While it might be typical to see more focus on testing and validation of externally-sourced data, a common source of frustration within organizations are unexpected schema changes that go unnoticed until something mysteriously breaks.

Schema changes to upstream data sources can have unintended downstream consequences. Without explicit agreements and contracts between data providers and consumers, organizations expose themselves to the risk of bad data or schema changes negatively impacting their operations and products.

## Using GX to validate schema throughout your data supply chain
Proactive data testing with a tool like Great Expectations provides a means to communicate and codify schema requirements between data producers and consumers within an organization. GX provides a collection of Expectations that cover table level and column level schema characteristics.

The GX Python API enables you to integrate GX at any point in your data pipeline to verify schema requirements. For example:
* GX can be used to validate CSV files landed in a file store before the data is ingested into a database
* GX can be used in your medallion architecture pipeline to verify that bronze, silver, and gold table data retains the expected schema as it moves through the hierarchy.

## A GX recipe to validate schema
Below is a simple recipe that demonstrates schema testing for a pandas dataframe.

```
import great_expectations as gx
import pandas as pd

# Filler code lines for draft data observability use case.

data = [
    # ...
    # ... example for use case
    # ...
]

df = pd.DataFrame(data)

context = gx.get_context()

data_source = context.sources.add_pandas(name="pandas-data-source")
data_asset = data_source.add_dataframe_asset(name="df")
batch_request = data_asset.build_batch_request(dataframe=df)

context.add_or_update_expectation_suite(
    expectation_suite_name="schema-expectations"
)

validator = context.get_validator(
    batch_request=customers_batch_request,
    expectation_suite_name="schema-expectations",
)

validator.expect_table_columns_to_match_ordered_list(...)
validator.expect_column_values_to_be_of_type(...)
validator.expect_column_values_to_not_be_null(...)
validator.expect_column_values_to_match_regex(...)
validator.expect_column_values_to_be_in_set(...)
```