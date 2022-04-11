---
title: How to configure a DataConnector to introspect and partition a file system or blob store
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you introspect and partition any file type data store (e.g., filesystem, cloud blob storage) using
an `Active Data Connector`.  For background on connecting to different backends, please see the
`Datasource` specific guides in the "Connecting to your data" section.

File-based introspection and partitioning are useful for:
- Exploring the types, subdirectory location, and filepath naming structures of the files in your dataset, and
- Organizing the discovered files into <TechnicalTag tag="data_asset" text="Data Assets" /> according to the identified structures.

`Partitioning` enables you to select the desired subsets of your dataset for [Validation](../../reference/validation.md).

<Prerequisites>

- [Configured and loaded a Data Context](../../tutorials/getting_started/initialize_a_data_context.md)
- [Configured a Datasource and Data Connector](../../reference/datasources.md)
  
</Prerequisites>

We will use the "Yellow Taxi" dataset to walk you through the configuration of `Data Connectors`.  Starting with the
bare-bones version of either an `Inferred Asset Data Connector` or a `Configured Asset Data Connector`, we gradually
build out the configuration to achieve the introspection of your files with the semantics consistent with your goals.

To learn more about <TechnicalTag tag="datasource" text="Datasources" />, <TechnicalTag tag="datasource" text="Data Connectors" />, and <TechnicalTag tag="batch" text="Batch(es)" />, please see our [Datasources Core Concepts Guide](../../reference/datasources.md) in the [Core Concepts reference guide](../../reference/core_concepts.md).

## Preliminary Steps

### 1. Instantiate your project's DataContext

Import Great Expectations.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L5
```

### 2. Obtain DataContext

Load your DataContext into memory using the `get_context()` method.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L7
```

## Configuring Inferred Asset Data Connector and Configured Asset Data Connector

<Tabs
  groupId="inferred-or-configured"
  defaultValue='inferred'
  values={[
  {label: 'Inferred Asset Data Connector', value:'inferred'},
  {label: 'Configured Asset Data Connector', value:'configured'},
  ]}>

<TabItem value="inferred">

### 1. Configure your Datasource

Start with an elementary `Datasource` configuration, containing just one general `Inferred Asset Data Connector`
component:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L9-L25
```

Using the above example configuration, add in the path to a directory that contains your data.  Then run this code to
test your configuration:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L33
```

Given that the `glob_directive` in the example configuration is `*.csv`, if you specified a directory containing CSV
files, then you will see them listed as `Available data_asset_names` in the output of `test_yaml_config()`.

Feel free to adjust your configuration and re-run `test_yaml_config()` to experiment as pertinent to your case.

An integral part of the recommended approach, illustrated as part of this exercise, will be the use of the internal
Great Expectations utility

```python
context.test_yaml_config(
    yaml_string, pretty_print: bool = True,
    return_mode: str = "instantiated_class",
    shorten_tracebacks: bool = False,
)
```

to ensure the correctness of the proposed `YAML` configuration prior to incorporating it and trying to use it.

For instance, try the following erroneous `DataConnector` configuration as part of your `Datasource` (you can
paste it directly underneath -- or instead of -- the `default_inferred_data_connector_name` configuration section):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L35-L44
```

Then add in the path to a directory that contains your data, and again run this code to test your configuration:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L33
```

Notice that the output reports only one `data_asset_name`, called `DEFAULT_ASSET_NAME`, signaling a misconfiguration.

Now try another erroneous `DataConnector` configuration as part of your `Datasource` (you can paste it directly
underneath -- or instead of -- your existing `DataConnector` configuration sections):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L79-L88
```

where you would add in the path to a directory that does not exist; then run this code again to test your configuration:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L33
```

You will see that the list of `Data Assets` is empty.  Feel free to experiment with the arguments to

```python
context.test_yaml_config(
    yaml_string, pretty_print: bool = True,
    return_mode: str = "instantiated_class",
    shorten_tracebacks: bool = False,
)
```

For instance, running

```python
context.test_yaml_config(yaml_string, return_mode="report_object")
```

will return the information appearing in standard output converted to the `Python` dictionary format.

Any structural errors (e.g., indentation, typos in class and configuration key names, etc.) will result in an exception
raised and sent to standard error.  This can be converted to an exception trace by running

```python
context.test_yaml_config(yaml_string, shorten_tracebacks=True)
```

showing the line numbers, where the exception occurred, most likely caused by the failure of the required class (in this
case `InferredAssetFilesystemDataConnector`) from being successfully instantiated.

### 2. Save the Datasource configuration to your DataContext

Once the basic `Datasource` configuration is error-free and satisfies your requirements, save it into your `DataContext`
by using the `add_datasource()` function.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L125
```

### 3. Get names of available Data Assets
 
Getting names of available data assets using an `Inferred Asset Data Connector` affords you the visibility into types
and naming structures of files in your filesystem or blob storage:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L126-L133
```

</TabItem>
<TabItem value="configured">

### 1. Add Configured Asset Data Connector to your Datasource

Set up the bare-bones `Configured Asset Data Connector` to gradually apply structure to the discovered assets and
partition them according to this structure.  To begin, add the following `configured_data_connector_name` section to
your `Datasource` configuration (please feel free to change the name as you deem appropriate for your use case):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L147-L164
```

Now run this code to test your configuration:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L33
```

The message `Available data_asset_names (0 of 0)`, corresponding to the `configured_data_connector_name` `Data
Connector`, should appear in standard output, correctly reflecting the fact that the `assets` section of the
configuration is empty.

### 2. Add a Data Asset for Configured Asset Data Connector to partition only by file name and type

You can employ a data asset that reflects a relatively general file structure (e.g., `taxi_data_flat` in the example
configuration) to represent files in a directory, which contain a certain prefix (e.g., `yellow_tripdata_sample_`) and
whose contents are of the desired type (e.g., CSV).

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L170-L185
```

Now run `test_yaml_config()` as part of evolving and testing components of Great Expectations `YAML` configuration:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L33
```

Verify that exactly one `Data Asset` is reported for the `configured_data_connector_name` `Data Connector` and that the
structure of the file names corresponding to the `Data Asset` identified, `taxi_data_flat`, is consistent with the
regular expressions pattern specified in the configuration for this `Data Asset`.

### 3. Add a Data Asset for Configured Asset Data Connector to partition by year and month

In recognition of a finer observed file path structure, you can refine the partitioning strategy.  For instance, the
`taxi_data_year_month` in the following example configuration identifies three parts of a file path: `name` (as in
"company name"), `year`, and `month`:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L224-L246
```

and run

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L33
```

Verify that now two `Data Assets` (`taxi_data_flat` and `taxi_data_year_month`) are reported for the
`configured_data_connector_name` `Data Connector` and that the structures of the file names corresponding to the two
`Data Assets` identified are consistent with the regular expressions patterns specified in the configuration for these
`Data Assets`.

This partitioning affords a rich set of filtering capabilities ranging from specifying the
exact values of the file name structure's components to allowing Python functions for implementing custom criteria.

Finally, once your `Data Connector` configuration satisfies your requirements, save the enclosing `Datasource` into your
`DataContext` using

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py#L125
```

Consult the
[How to get a Batch of data from a configured Datasource](./how_to_get_a_batch_of_data_from_a_configured_datasource.md)
guide for examples of considerable flexibility in querying `Batch` objects along the different dimensions materialized
as a result of partitioning the dataset as specified by the `taxi_data_flat` and `taxi_data_year_month` `Data Assets`.

</TabItem>

</Tabs>

To view the full scripts used in this page, see them on GitHub:

- [yaml_example_gradual.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/files/yaml_example_gradual.py)
