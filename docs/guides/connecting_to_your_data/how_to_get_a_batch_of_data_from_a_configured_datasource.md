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

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L39-L44
    ```
   
    Since a `BatchRequest` can return multiple `Batch(es)`, you can optionally provide additional parameters to filter the retrieved `Batch(es)`. See [Datasources Core Concepts Guide](../../reference/datasources.md) for more info on filtering besides `batch_filter_parameters` and `limit` including custom filter functions and sampling. The example `BatchRequest`s below shows several non-exhaustive possibilities. 

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L61-L71
    ```

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L75-L89
    ```
   
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L94-L104
    ```
   
    You may also wish to list available batches to verify that your `BatchRequest` is retrieving the correct `Batch(es)`, or to see which are available. You can use `context.get_batch_list()` for this purpose, which can take a variety of flexible input types similar to a `BatchRequest`. Some examples are shown below:

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L109-L114
    ```
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L117-L118
    ```
   
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L121-L127
    ```
   
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L136-L142
    ```

2. **Get access to your Batch via a Validator**

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L147-L154
    ```

3. **Check your data**

    You can check that the first few lines of the `Batch` you loaded into your `Validator` are what you expect by running:

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L156
    ```

    Now that you have a `Validator`, you can use it to create `Expectations` or validate the data.



