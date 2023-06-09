---
title: Request data from a Data Asset
tag: [how-to, connect to data]
description: A technical guide demonstrating how to request data from a Data Asset.
keywords: [Great Expectations, Data Asset, Batch Request, fluent configuration method]
---

<!-- ## Prerequisites -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'


<!-- ### Retrieve existing DataAsset from existing Datsource -->
import GetExistingDataAssetFromExistingDatasource from '/docs/components/setup/datasource/data_asset/_get_existing_data_asset_from_existing_datasource.md'

This guide demonstrates how you can request data from a Datasource that has been defined with the `context.sources.add_*` method.

## Prerequisites

<Prerequisites> 

- An installation of GX
- A Datasource with a configured Data Asset

</Prerequisites> 

## Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

## Retrieve your Data Asset

<GetExistingDataAssetFromExistingDatasource />

## Build an `options` dictionary for your Batch Request (Optional)

An `options` dictionary can be used to limit the Batches returned by a Batch Request.  Omitting the `options` dictionary will result in all available Batches being returned.

The structure of the `options` dictionary will depend on the type of Data Asset being used.  The valid keys for the `options` dictionary can be found by checking the Data Asset's `batch_request_options` property.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_batch_request_options"
```

The `batch_request_options` property is a tuple that contains all the valid keys that can be used to limit the Batches returned in a Batch Request.

You can create a dictionary of keys pulled from the `batch_request_options` tuple and values that you want to use to specify the Batch or Batches your Batch Request should return, then pass this dictionary in as the `options` parameter when you build your Batch Request.

## Build your Batch Request

We will use the `build_batch_request(...)` method of our Data Asset to generate a Batch Request.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_batch_request"
```

For `dataframe` Data Assets, the `dataframe` is always specified as the argument of exactly one API method:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py build_batch_request_with_dataframe"
```

## Verify that the correct Batches were returned

The `get_batch_list_from_batch_request(...)` method will return a list of the Batches a given Batch Request refers to.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_batch_list"
```

Because Batch definitions are quite verbose, it is easiest to determine what data the Batch Request will return by printing just the `batch_spec` of each Batch.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py print_batch_spec"
```

## Next steps

Now that you have a retrieved data from a Data Asset, you may be interested in creating Expectations about your data:
- [How to create Expectations while interactively evaluating a set of data](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)
- [How to use the Onboarding Data Assistant to evaluate data](/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant)


