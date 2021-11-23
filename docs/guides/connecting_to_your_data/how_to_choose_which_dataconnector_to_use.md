---
title: How to choose which DataConnector to use
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide demonstrates how to choose which `DataConnector`s to configure within your `Datasource`s.

<Prerequisites>

- [Understand the basics of Datasources in the V3 (Batch Request) API](../../reference/datasources.md)
- Learned how to configure a [Data Context using test_yaml_config](../setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config.md)

</Prerequisites>

Great Expectations provides three types of `DataConnector` classes. Two classes are for connecting to Data Assets stored as file-system-like data (this includes files on disk, but also S3 object stores, etc) as well as relational database data:

- An InferredAssetDataConnector infers `data_asset_name` by using a regex that takes advantage of patterns that exist in the filename or folder structure.
- A ConfiguredAssetDataConnector allows users to have the most fine-tuning, and requires an explicit listing of each Data Asset you want to connect to.

| InferredAssetDataConnectors | ConfiguredAssetDataConnectors |
| --- | --- |
| InferredAssetFilesystemDataConnector | ConfiguredAssetFilesystemDataConnector |
| InferredAssetFilePathDataConnector | ConfiguredAssetFilePathDataConnector |
| InferredAssetAzureDataConnector | ConfiguredAssetAzureDataConnector |
| InferredAssetGCSDataConnector | ConfiguredAssetGCSDataConnector |
| InferredAssetS3DataConnector | ConfiguredAssetS3DataConnector |
| InferredAssetSqlDataConnector | ConfiguredAssetSqlDataConnector |
| InferredAssetDBFSDataConnector | ConfiguredAssetDBFSDataConnector |

InferredAssetDataConnectors and ConfiguredAssetDataConnectors are used to define Data Assets and their associated data_references. A Data Asset is an abstraction that can consist of one or more data_references to CSVs or relational database tables. For instance, you might have a `yellow_tripdata` Data Asset containing information about taxi rides, which consists of twelve data_references to twelve CSVs, each consisting of one month of data.

The third type of `DataConnector` class is for providing a batch's data directly at runtime:

- A `RuntimeDataConnector` enables you to use a `RuntimeBatchRequest` to wrap either an in-memory dataframe, filepath, or SQL query, and must include batch identifiers that uniquely identify the data (e.g. a `run_id` from an AirFlow DAG run).

If you know for example, that your Pipeline Runner will already have your batch data in memory at runtime, you can choose to configure a `RuntimeDataConnector` with unique batch identifiers. Reference the documents on [How to configure a RuntimeDataConnector](guides/connecting_to_your_data/how_to_configure_a_runtimedataconnector.md) and [How to create a Batch of data from an in-memory Spark or Pandas dataframe](guides/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_or_pandas_dataframe.md) to get started with `RuntimeDataConnectors`.

If you aren't sure which type of the remaining `DataConnector`s to use, the following examples will use `DataConnector` classes designed to connect to files on disk, namely `InferredAssetFilesystemDataConnector` and `ConfiguredAssetFilesystemDataConnector` to demonstrate the difference between these types of `DataConnectors`.

### When to use an InferredAssetDataConnector

If you have the following `<MY DIRECTORY>/` directory in your filesystem, and you want to treat the `yellow_tripdata_*.csv` files as batches within the `yellow_tripdata` Data Asset, and also do the same for files in the `green_tripdata` directory:

```
<MY DIRECTORY>/yellow_tripdata/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata/yellow_tripdata_2019-03.csv
<MY DIRECTORY>/green_tripdata/2019-01.csv
<MY DIRECTORY>/green_tripdata/2019-02.csv
<MY DIRECTORY>/green_tripdata/2019-03.csv
```

This configuration:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py#L8-L26
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py#L37-L60
```

</TabItem>
</Tabs>

will make available the following Data Assets and data_references:

```bash
Available data_asset_names (2 of 2):
    green_tripdata (3 of 3): ['green_tripdata/*2019-01.csv', 'green_tripdata/*2019-02.csv', 'green_tripdata/*2019-03.csv']
    yellow_tripdata (3 of 3): ['yellow_tripdata/*2019-01.csv', 'yellow_tripdata/*2019-02.csv', 'yellow_tripdata/*2019-03.csv']

Unmatched data_references (0 of 0):[]
```

Note that the `InferredAssetFileSystemDataConnector` **infers** `data_asset_names` **from the regex you provide.** This is the key difference between InferredAssetDataConnector and ConfiguredAssetDataConnector, and also requires that one of the `group_names` in the `default_regex` configuration be `data_asset_name`.

The `glob_directive` is provided to give the `DataConnector` information about the directory structure to expect for each Data Asset. The default `glob_directive` for the `InferredAssetFileSystemDataConnector` is `"*"` and therefore must be overridden when your data_references exist in subdirectories.

### When to use a ConfiguredAssetDataConnector

On the other hand, `ConfiguredAssetFilesSystemDataConnector` requires an explicit listing of each Data Asset you want to connect to. This tends to be helpful when the naming conventions for your Data Assets are less standardized, but the user has a strong understanding of the semantics governing the segmentation of data (files, database tables).

If you have the same `<MY DIRECTORY>/` directory in your filesystem,

```
<MY DIRECTORY>/yellow_tripdata/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata/yellow_tripdata_2019-03.csv
<MY DIRECTORY>/green_tripdata/2019-01.csv
<MY DIRECTORY>/green_tripdata/2019-02.csv
<MY DIRECTORY>/green_tripdata/2019-03.csv
```

Then this configuration:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py#L90-L114
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py#L125-L151
```

</TabItem>
</Tabs>

will make available the following Data Assets and data_references:

```bash
Available data_asset_names (2 of 2):
    green_tripdata (3 of 3): ['2019-01.csv', '2019-02.csv', '2019-03.csv']
    yellow_tripdata (3 of 3): ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'yellow_tripdata_2019-03.csv']

Unmatched data_references (0 of 0):[]
```

### Additional Notes

- Additional examples and configurations for `ConfiguredAssetFilesystemDataConnector`s can be found here: [How to configure a ConfiguredAssetDataConnector](./how_to_configure_a_configuredassetdataconnector.md)
- Additional examples and configurations for `InferredAssetFilesystemDataConnector`s can be found here: [How to configure an InferredAssetDataConnector](./how_to_configure_an_inferredassetdataconnector.md)
- Additional examples and configurations for `RuntimeDataConnector`s can be found here: [How to configure a RuntimeDataConnector](./how_to_configure_a_runtimedataconnector.md)

To view the full script used in this page, see it on GitHub:
- [how_to_choose_which_dataconnector_to_use.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_choose_which_dataconnector_to_use.py)
