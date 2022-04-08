---
title: How to configure a DataConnector to introspect and partition tables in SQL
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you introspect and partition tables in an SQL database using `SimpleSqlalchemyDatasource`, which
operates as a proxy to `InferredAssetSqlDataConnector` and `ConfiguredAssetSqlDataConnector`.  For background, please
see the [Datasource specific guides](../connecting_to_your_data/index.md) in the [Connecting to your data](../connecting_to_your_data/connect_to_data_overview.md) section of our documentation.

The SQL database introspection and partitioning are useful for:
- Exploring the schema and column metadata of the tables in your SQL database, and
- Organizing the tables into <TechnicalTag tag="data_asset" text="Data Assets" /> according to the partitioning considerations informed by this exploration.

Partitioning enables you to select the desired subsets of your dataset for [Validation](../../reference/validation.md).

<Prerequisites>

- [Configured and loaded a Data Context](../../tutorials/getting_started/initialize_a_data_context.md)
- [Configured a Datasource and Data Connector](../../reference/datasources.md)

</Prerequisites>

We will use the "Yellow Taxi" dataset to walk you through the configuration of `SimpleSqlalchemyDatasource`, where
the `introspection` section characterizes `InferredAssetSqlDataConnector` objects and the `tables` section characterizes
`ConfiguredAssetSqlDataConnector` objects.  Starting with the bare-bones version of either the `introspection` section
or the `tables` section of the `SimpleSqlalchemyDatasource` configuration, we gradually build out the configuration to
achieve the introspection of your SQL database with the semantics consistent with your goals.

:::info
Only `introspection` and `tables` are the legal top-level keys in the `SimpleSqlalchemyDatasource` configuration.
:::

To learn more about <TechnicalTag tag="datasource" text="Datasources" />, <TechnicalTag tag="data_connector" text="Data Connectors" />, and <TechnicalTag tag="batch" text="Batch(es)" />, please see our [Datasources Core Concepts Guide](../../reference/datasources.md) in the [Core Concepts reference guide](../../reference/core_concepts.md).

## Preliminary Steps

### 1. Instantiate your project's DataContext

Import Great Expectations.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L4
```

### 2. Obtain DataContext

Load your DataContext into memory using the `get_context()` method.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L9
```

## Configuring Introspection and Tables

<Tabs
  groupId="introspection-or-tables"
  defaultValue='introspection'
  values={[
  {label: 'Introspection (InferredAssetSqlDataConnector)', value:'introspection'},
  {label: 'Tables (ConfiguredAssetSqlDataConnector)', value:'tables'},
  ]}>

<TabItem value="introspection">

### 1. Configure your SimpleSqlalchemyDatasource for introspection

Start with an elementary `SimpleSqlalchemyDatasource` configuration, containing just a basic `introspection` component:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L13-L20
```

Using the above example configuration, specify the connection string for your database.  Then run this code to test your
configuration:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L31
```

Notice that the output reports the presence of exactly one `InferredAssetSqlDataConnector` (called `whole_table`, as per
the configuration).

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

For instance, try the following erroneous `SimpleSqlalchemyDatasource` configuration (it contains an illegal top-level
key):

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L35-L42
```

Then specify the connection string for your database, and again run this code to test your configuration:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L48
```

Notice that the output reports an empty Data Connectors list, signaling a misconfiguration.

Feel free to experiment with the arguments to

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
case `InferredAssetSqlDataConnector`) from being successfully instantiated.

### 2. Customize the introspection configuration to fit your needs

`SimpleSqlalchemyDatasource` supports a number of configuration options to assist you with the `introspection` of your
SQL database:

- the database views will included in the list of identified `Data References` (by setting the `include_views` flag to
`true`)
- if any exceptions occur during the `introspection` operation, then the process will continue (by setting the
`skip_inapplicable_tables` flag to `true`)
- specifying `excluded_tables` will have the effect of excluding only the tables on this list, while including the rest
- specifying `included_tables` will have the effect of including only the tables on this list, while excluding the rest

The following `YAML` configurqation example utilizes several of these configuration directives:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L52-L65
```

### 3. Save the Datasource configuration to your DataContext

Once the `SimpleSqlalchemyDatasource` configuration is error-free and satisfies your requirements, save it into your
`DataContext` by using the `add_datasource()` function.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L73
```

### 4. Get names of available Data Assets

Getting names of available data assets using an `InferredAssetSqlDataConnector` affords you the visibility into types
and naming structures of tables in your SQL database:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L77-L79
```

</TabItem>

<TabItem value="tables">

### 1. Configure your SimpleSqlalchemyDatasource to characterize tables

Start with an elementary `SimpleSqlalchemyDatasource` configuration, containing just a basic `tables` component:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L85-L94
```

Using the above example configuration, specify the connection string for your database.  Then run this code to test your
configuration:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L100
```

Notice that the output reports the presence of exactly one `ConfiguredAssetSqlDataConnector` (called `whole_table`, as
per the configuration) and that `Available data_asset_names (1 of 1)`, the name of the single `Data Asset` being
`yellow_tripdata_sample_2019_01`.

### 2. Enhance your SimpleSqlalchemyDatasource with ability to attribute metadata to tables

Add `Data Asset Name` identification attributes (`data_asset_name_prefix` and `data_asset_name_suffix`) and set the
`include_schema_name` flag in your `ConfiguredAssetSqlDataConnector` (named `whole_table`) configuration section.  These
directives will result in the reported properties of your table to contain annotations, customized for your purposes:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L104-L117
```

Using the above example configuration, specify the connection string for your database.  Then run this code to test your
configuration:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L123
```

Notice that the output reports the presence of exactly one `ConfiguredAssetSqlDataConnector` (called `whole_table`, as
per the configuration) and that `Available data_asset_names (1 of 1)`, the name of the single `Data Asset` this time
being `taxi__yellow_tripdata_sample_2019_01__asset`, correctly reflecting the enhanced configuration directives.

Finally, once your `Data Connector` configuration satisfies your requirements, save the enclosing `Datasource` into your
`DataContext` using

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py#L127
```

</TabItem>

</Tabs>


To view the full script used in this page, see it on GitHub:

- [yaml_example_gradual.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_introspect_and_partition_your_data/sql_database/yaml_example_gradual.py)
