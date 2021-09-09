---
title: How to get a Batch of data from a configured Datasource
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'

This guide will help you load a `Batch` for introspection and validation using an active `Data Connector`. For guides on loading batches of data from specific `Datasources` using a `Data Connector` see the `Datasource` specific guides in the "Connecting to your data" section.

What used to be called a “Batch” in the old API was replaced with [Validator](../../reference/validation.md). A `Validator` knows how to validate a particular `Batch` of data on a particular [Execution Engine](../../reference/execution_engine.md) against a particular [Expectation Suite](../../reference/expectations/expectations.md). In interactive mode, the `Validator` can store and update an `Expectation Suite` while conducting Data Discovery or Exploratory Data Analysis.

You can read more about the core classes that make Great Expectations run in our [Core Concepts reference guide](../../reference/core_concepts.md).

<Prerequisites>

- [Configured and loaded a Data Context](../../tutorials/getting_started/initialize_a_data_context.md)
- [Configured a Datasource and Data Connector](../../reference/datasources.md)
  
</Prerequisites>

To load a `Batch`, the steps you will take are the same regardless of the type of `Datasource` or `Data Connector` you have set up. To learn more about `Datasources`, `Data Connectors` and `Batch(es)` see our [Datasources Core Concepts Guide](../../reference/datasources.md) in the [Core Concepts reference guide](../../reference/core_concepts.md). 

1. **Construct a BatchRequest**

    ```python
    batch_request = BatchRequest(
        datasource_name="insert_your_datasource_name_here",
        data_connector_name="insert_your_data_connector_name_here",
        data_asset_name="insert_your_data_asset_name_here",
    )
    ```
   
    Optionally, you can provide additional parameters to filter the retrieved `Batch(es)`. See [Datasources Core Concepts Guide](../../reference/datasources.md) for more info on filtering besides `batch_filter_parameters` and `limit` including custom filter functions and sampling. The example BatchRequest below shows several non-exhaustive possibilities. 

    ```python
    # Here is an example `data_connector_query` filtering based on parameters from `group_names` 
    # previously defined in a regex pattern in your Data Connector:
    data_connector_query = {
        "batch_filter_parameters": {
            "param_1_from_your_data_connector_eg_year": "2021",
            "param_2_from_your_data_connector_eg_month": "01",
        }
    }
   
    # Here is an example `data_connector_query` filtering based on an `index` which can be 
    # any valid python slice. The example here is retrieving the latest batch using `-1`:
    data_connector_query = {
        "index": -1,
    }
      
    limit = 1000 # Number of rows to return per batch
   
    batch_request = BatchRequest(
        datasource_name="insert_your_datasource_name_here",
        data_connector_name="insert_your_data_connector_name_here",
        data_asset_name="insert_your_data_asset_name_here",
        data_connector_query=data_connector_query,   
        limit=limit,
    )
    ```
   
    You may also wish to list available batches to verify that your `BatchRequest` is retrieving the correct `Batch(es)`, or to see which are available. You can use `context.get_batch_list()` for this purpose, which can take a variety of flexible input types. Some examples are shown below:

    ```python
    context.get_batch_list(
        datasource_name="insert_your_datasource_name_here",
        data_connector_name="insert_your_data_connector_name_here",
        data_asset_name="insert_your_data_asset_name_here",
    )
    ```
   
    ```python
    context.get_batch_list(
        batch_request=batch_request
    )
    ```
   
    ```python
    context.get_batch_list(
        datasource_name="insert_your_datasource_name_here",
        data_connector_name="insert_your_data_connector_name_here",
        data_asset_name="insert_your_data_asset_name_here",
        data_connector_query=data_connector_query,   
        limit=limit,
    )
    ```


2. **Get access to your Batch via a Validator**

    ```python
    # First create an expectation suite to use with our validator
    context.create_expectation_suite(
        expectation_suite_name="test_suite", overwrite_existing=True,
    )
    # Now create our validator
    my_validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite",
    )
    ``` 

3. **Check your data**

    You can check that the first few lines of the `Batch` you loaded into your `Validator` are what you expect by running:

    ```python
    my_validator.head()
    ```

    Now that you have a `Validator`, you can use it to create `Expectations` or validate the data.



