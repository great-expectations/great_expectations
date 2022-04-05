---
title: How to get a Batch of data from a configured Datasource
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you load a <TechnicalTag tag="batch" text="Batch" /> for introspection and validation using an active <TechnicalTag tag="data_connector" text="Data Connector" />. For guides on loading batches of data from specific <TechnicalTag tag="datasource" text="Datasources" /> using a Data Connector see the [Datasource specific guides in the "Connecting to your data" section](./index.md).

What used to be called a “Batch” in the old API was replaced with <TechnicalTag tag="validator" text="Validator" />. A Validator knows how to <TechnicalTag tag="validation" text="Validate" /> a particular Batch of data on a particular <TechnicalTag tag="execution_engine" text="Execution Engine" /> against a particular <TechnicalTag tag="expectation_suite" text="Expectation Suite" />. In interactive mode, the Validator can store and update an Expectation Suite while conducting Data Discovery or Exploratory Data Analysis.

You can read more about the core classes that make Great Expectations run in our [Core Concepts reference guide](../../reference/core_concepts.md).

<Prerequisites>

- [Configured and loaded a Data Context](../../tutorials/getting_started/initialize_a_data_context.md)
- [Configured a Datasource and Data Connector](../../reference/datasources.md)
  
</Prerequisites>

## Steps: Loading a Batch of data

To load a `Batch`, the steps you will take are the same regardless of the type of `Datasource` or `Data Connector` you have set up. To learn more about `Datasources`, `Data Connectors` and `Batch(es)` see our [Datasources Core Concepts Guide](../../reference/datasources.md) in the [Core Concepts reference guide](../../reference/core_concepts.md). 

### 1. Construct a BatchRequest

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

### 2. Get access to your Batch via a Validator

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L147-L154
```

### 3. Check your data

You can check that the first few lines of the `Batch` you loaded into your `Validator` are what you expect by running:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.py#L156
```

Now that you have a `Validator`, you can use it to create `Expectations` or validate the data.

## Additional Batch querying and loading examples 

We will use the "Yellow Taxi" dataset example from
[How to configure a DataConnector to introspect and partition a file system or blob store](./how_to_configure_a_dataconnector_to_introspect_and_partition_a_file_system_or_blob_store.md)
to demonstrate the `Batch` querying possibilities enabled by the particular data partitioning strategy specified as part
of the `Data Connector` configuration.

### 1. Partition only by file name and type

In this example, the <TechnicalTag tag="data_asset" text="Data Asset" /> representing a relatively general naming structure of files in a directory, with
each file name having a certain prefix (e.g., `yellow_tripdata_sample_`) and whose contents are of the desired type
(e.g., CSV) is `taxi_data_flat` in the `Data Connector` `configured_data_connector_name`:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py#L27-L40
```

To query for `Batch` objects, set `data_asset_name` to `taxi_data_flat` in the following `BatchRequest`
specification. (Customize for your own data set, as appropriate.)

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py#L92-L96
```

Then perform the relevant checks: verify that the expected number of `Batch` objects was retrieved and confirm the
size of a `Batch`.  For example (be sure to adjust this code to match the specifics of your data and configuration):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py#L102-L104
```

### 2. Partition by year and month

Next, use the more detailed partitioning strategy represented by the `Data Asset` `taxi_data_year_month` in the
`Data Connector` `configured_data_connector_name`:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py#L27-L47
```

The `Data Asset` `taxi_data_year_month` in the above example configuration identifies three parts of a file path:
`name` (as in "company name"), `year`, and `month`.  This partitioning affords a rich set of filtering capabilities
ranging from specifying the exact values of the file name structure's components to allowing Python functions for
implementing custom criteria.

To perform experiments supported by this configuration, set `data_asset_name` to `taxi_data_year_month` in the
following `BatchRequest` specification (customize for your own data set, as appropriate):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py#L108-L113
```

To obtain the data for the nine months of February through October, apply the following custom filter:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py#L118-L121
```

Now, perform the relevant checks: verify that the expected number of `Batch` objects was retrieved and confirm the
size of a `Batch`:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py#L123-L125
```

You can then identify a particular `Batch` (e.g., corresponding to the year and month of interest) and retrieve it
for data analysis as follows:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py#L129-L140
```

Note that in the present example, there can be up to three `BATCH_FILTER_PARAMETER` key-value pairs, because the
regular expression for the data asset `taxi_data_year_month` defines three groups: `name`, `year`, and `month`.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py#L145-L148
```

(Be sure to adjust the above code snippets to match the specifics of your data and configuration.)

Now, perform the relevant checks: verify that the expected number of `Batch` objects was retrieved and confirm the
size of a `Batch`:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py#L150-L152
```

:::note
Omitting the `batch_filter_parameters` key from the `data_connector_query` will be interpreted in the least restrictive
(most broad) query, resulting in the largest number of `Batch` objects to be returned.
:::


To view the full script used in this page, see it on GitHub:

- [yaml_example_complete.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_complete.py)
