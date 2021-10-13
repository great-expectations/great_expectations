---
title: How to choose which DataConnector to use
---

Great Expectations provides three types of `DataConnector` classes. Two classes are for connecting to file-system-like data (this includes files on disk, but also S3 object stores, etc) as well as relational database data:

- A ConfiguredAssetDataConnector allows users to have the most fine-tuning, and requires an explicit listing of each DataAsset you want to connect to. Examples of this type of `DataConnector` include `ConfiguredAssetFilesystemDataConnector` and `ConfiguredAssetS3DataConnector`.
- An InferredAssetDataConnector infers `data_asset_name` by using a regex that takes advantage of patterns that exist in the filename or folder structure. Examples of this type of `DataConnector` include `InferredAssetFilesystemDataConnector` and `InferredAssetS3DataConnector`.

The third type of `DataConnector` class is for providing a batch's data directly at runtime:

- A `RuntimeDataConnector` enables you to use a `RuntimeBatchRequest` to wrap either an in-memory dataframe, filepath, or SQL query, and must include batch identifiers that uniquely identify the data (e.g. a `run_id` from an AirFlow DAG run).

If you know for example, that your Pipeline Runner will already have your batch data in memory at runtime, you can choose to configure a `RuntimeDataConnector` with unique batch identifiers. Reference the documents on [How to configure a RuntimeDataConnector](guides/connecting_to_your_data/how_to_configure_a_runtimedataconnector.md) and [How to create a Batch of data from an in-memory Spark or Pandas dataframe](guides/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_or_pandas_dataframe.md) to get started with `RuntimeDataConnectors`.

If you aren't sure which type of the remaining `DataConnector`s to use, the following examples will use `DataConnector` classes designed to connect to files on disk, namely `InferredAssetFilesystemDataConnector` and `ConfiguredAssetFilesystemDataConnector` to demonstrate the difference between these types of `DataConnectors`.

------------------------------------------
When to use an InferredAssetDataConnector
------------------------------------------

If you have the following `my_data/` directory in your filesystem, and you want to treat the `A-*.csv` files as batches within the `A` DataAsset, and do the same for `B` and `C`:

```
my_data/A/A-1.csv
my_data/A/A-2.csv
my_data/A/A-3.csv
my_data/B/B-1.csv
my_data/B/B-2.csv
my_data/B/B-3.csv
my_data/C/C-1.csv
my_data/C/C-2.csv
my_data/C/C-3.csv
```

This config...

```yaml
class_name: Datasource
data_connectors:
  my_data_connector:
    class_name: InferredAssetFilesystemDataConnector
    base_directory: my_data/
    default_regex:
      pattern: (.*)/.*-(\d+)\.csv
      group_names:
        - data_asset_name
        - id
```

...will make available the following DataAssets and data_references:

```bash
Available data_asset_names (3 of 3):
   A (3 of 3): [
      'A/A-1.csv',
      'A/A-2.csv',
      'A/A-3.csv'
   ]
   B (3 of 3): [
      'B/B-1.csv',
      'B/B-2.csv',
      'B/B-3.csv'
   ]
   C (3 of 3): [
      'C/C-1.csv',
      'C/C-2.csv',
      'C/C-3.csv'
   ]

Unmatched data_references (0 of 0): []
```

Note that the `InferredAssetFileSystemDataConnector` **infers** `data_asset_names` **from the regex you provide.** This is the key difference between InferredAssetDataConnector and ConfiguredAssetDataConnector, and also requires that one of the `group_names` in the `default_regex` configuration be `data_asset_name`.

------------------------------------------
When to use a ConfiguredAssetDataConnector
------------------------------------------

On the other hand, `ConfiguredAssetFilesSystemDataConnector` requires an explicit listing of each DataAsset you want to connect to. This tends to be helpful when the naming conventions for your DataAssets are less standardized.

If you have the following `my_messier_data/` directory in your filesystem,

```
 my_messier_data/1/A-1.csv
 my_messier_data/1/B-1.txt

 my_messier_data/2/A-2.csv
 my_messier_data/2/B-2.txt

 my_messier_data/2017/C-1.csv
 my_messier_data/2018/C-2.csv
 my_messier_data/2019/C-3.csv

 my_messier_data/aaa/D-1.csv
 my_messier_data/bbb/D-2.csv
 my_messier_data/ccc/D-3.csv
```

Then this config...

```yaml
class_name: Datasource
execution_engine:
  class_name: PandasExecutionEngine
data_connectors:
  my_data_connector:
    class_name: ConfiguredAssetFilesystemDataConnector
    glob_directive: "*/*"
    base_directory: my_messier_data/
    assets:
      A:
         pattern: (.+A)-(\d+)\.csv
         group_names:
           - name
           - id
       B:
         pattern: (.+B)-(\d+)\.txt
         group_names:
           - name
           - val
       C:
         pattern: (.+C)-(\d+)\.csv
         group_names:
           - name
           - id
       D:
         pattern: (.+D)-(\d+)\.csv
         group_names:
           - name
           - id
```

...will make available the following DataAssets and data_references:

```bash
Available data_asset_names (4 of 4):
   A (2 of 2): [
      '1/A-1.csv',
      '2/A-2.csv'
   ]
   B (2 of 2): [
      '1/B-1.txt',
      '2/B-2.txt'
   ]
   C (3 of 3): [
      '2017/C-1.csv',
      '2018/C-2.csv',
      '2019/C-3.csv'
   ]
   D (3 of 3): [
      'aaa/D-1.csv',
      'bbb/D-2.csv',
      'ccc/D-3.csv'
   ]
```

----------------
Additional Notes
----------------

- Additional examples and configurations for `ConfiguredAssetFilesystemDataConnector`s can be found here: [How to configure a ConfiguredAssetDataConnector](./how_to_configure_a_configuredassetdataconnector.md)
- Additional examples and configurations for `InferredAssetFilesystemDataConnector`s can be found here: [How to configure an InferredAssetDataConnector](./how_to_configure_an_inferredassetdataconnector.md)
- Additional examples and configurations for `RuntimeDataConnector`s can be found here: [How to configure a RuntimeDataConnector](./how_to_configure_a_runtimedataconnector.md)
