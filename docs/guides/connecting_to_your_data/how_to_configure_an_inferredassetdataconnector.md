---
title: How to configure an InferredAssetDataConnector
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide demonstrates how to configure an InferredAssetDataConnector, and provides several examples you
can use for configuration.

<Prerequisites>

- [Understand the basics of Datasources in 0.13 or later](../../reference/datasources.md)
- Learned how to configure a [Data Context using test_yaml_config](../setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config.md)

</Prerequisites>

Great Expectations provides two types of `DataConnector` classes for connecting to `DataAsset`s stored as file-system-like data. This includes files on disk,
but also S3 object stores, etc:

- A ConfiguredAssetDataConnector requires an explicit listing of each `DataAsset` you want to connect to. This allows more fine-tuning, but also requires more setup.
- An InferredAssetDataConnector infers `data_asset_name` by using a regex that takes advantage of patterns that exist in the filename or folder structure.

InferredAssetDataConnector has fewer options, so it's simpler to set up. It’s a good choice if you want to connect to a single `DataAsset`, or several `DataAssets` that all share the same naming convention.

If you're not sure which one to use, please check out [How to choose which DataConnector to use](./how_to_choose_which_dataconnector_to_use.md).

## Steps

### 1. Instantiate your project's DataContext

Import these necessary packages and modules:

<Tabs
  groupId="yaml-or-python"
  defaultValue='python'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L3-L4
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L1-L4
```

</TabItem>
</Tabs>

### 2. Set up a Datasource

All the examples below assume you’re testing configurations using something like:

<Tabs
  groupId="yaml-or-python"
  defaultValue='python'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python
datasource_yaml = """
name: taxi_datasource
class_name: Datasource
execution_engine:
  class_name: PandasExecutionEngine
data_connectors:
  default_inferred_data_connector_name:
    <DATACONNECTOR CONFIGURATION GOES HERE>
"""
context.test_yaml_config(yaml_config=datasource_config)
```

</TabItem>
<TabItem value="python">

```python
datasource_config = {
    "name": "taxi_datasource",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_inferred_data_connector_name": {
          <DATACONNECTOR CONFIGURATION GOES HERE>
        },
    },
}
context.test_yaml_config(yaml.dump(datasource_config))
```

</TabItem>
</Tabs>

If you’re not familiar with the `test_yaml_config` method, please check out: [How to configure Data Context components using test_yaml_config](../setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config.md)

### 3. Choose a DataConnector

InferredAssetDataConnectors like `InferredAssetFilesystemDataConnector` and `InferredAssetS3DataConnector`
require a `default_regex` parameter, with a configured regex `pattern` and capture `group_names`.

Imagine you have the following files in `my_directory/`:

```
<MY DIRECTORY>/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata_2019-03.csv
```

We can imagine two approaches to loading the data into GE.

The simplest approach would be to consider each file to be its own DataAsset. In that case, the configuration would look like the following:

<Tabs
  groupId="yaml-or-python"
  defaultValue='python'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L9-L24
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L33-L51
```

</TabItem>
</Tabs>

Notice that the `default_regex` is configured to have one capture group (`(.*)`) which captures the entire filename. That capture group is assigned to `data_asset_name` under `group_names`.
Running `test_yaml_config()` would result in 3 DataAssets : `yellow_tripdata_2019-01`, `yellow_tripdata_2019-02` and `yellow_tripdata_2019-03`.

However, a closer look at the filenames reveals a pattern that is common to the 3 files. Each have `yellow_tripdata_` in the name, and have date information afterwards. These are the types of patterns that InferredAssetDataConnectors allow you to take advantage of.

We could treat `yellow_tripdata_*.csv` files as batches within the `yellow_tripdata` `DataAsset` with a more specific regex `pattern` and adding `group_names` for `year` and `month`.

**Note: ** We have chosen to be more specific in the capture groups for the `year` and `month` by specifying the integer value (using `\d`) and the number of digits, but a simpler capture group like `(.*)` would also work.

<Tabs
  groupId="yaml-or-python"
  defaultValue='python'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L77-L94
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L105-L123
```

</TabItem>
</Tabs>

Running `test_yaml_config()` would result in 1 DataAsset `yellow_tripdata` with 3 associated data_references: `yellow_tripdata_2019-01.csv`, `yellow_tripdata_2019-02.csv` and `yellow_tripdata_2019-03.csv`, seen also in Example 1 below.

A corresponding configuration for `InferredAssetS3DataConnector` would look similar but would require `bucket` and `prefix` values instead of `base_directory`.

<Tabs
  groupId="yaml-or-python"
  defaultValue='python'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L147-L165
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L178-L197
```

</TabItem>
</Tabs>

The following examples will show scenarios that InferredAssetDataConnectors can help you analyze, using `InferredAssetFilesystemDataConnector`.


### Example 1: Basic configuration for a single DataAsset

Continuing the example above, imagine you have the following files in the directory `<MY DIRECTORY>`:

```
<MY DIRECTORY>/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata_2019-03.csv
```

Then this configuration...

<Tabs
  groupId="yaml-or-python"
  defaultValue='python'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L225-L242
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L253-L271
```

</TabItem>
</Tabs>

...will make available `yelow_tripdata` as a single DataAsset with the following data_references:

```bash
Available data_asset_names (1 of 1):
    yellow_tripdata (3 of 3): ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'yellow_tripdata_2019-03.csv']

Unmatched data_references (0 of 0):[]
```

Once configured, you can get `Validators` from the `Data Context` as follows:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L294-L303
```

### Example 2: Basic configuration with more than one DataAsset

Here’s a similar example, but this time two Data Assets are mixed together in one folder.

**Note**: For an equivalent configuration using `ConfiguredAssetFilesSystemDataconnector`, please see Example 2
in [How to configure a ConfiguredAssetDataConnector](./how_to_configure_a_configuredassetdataconnector).

```
<MY DIRECTORY>/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/green_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/green_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata_2019-03.csv
<MY DIRECTORY>/green_tripdata_2019-03.csv
```

The same configuration as Example 1...

<Tabs
  groupId="yaml-or-python"
  defaultValue='python'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L225-L242
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L253-L271
```

</TabItem>
</Tabs>

...will now make `yellow_tripdata` and `green_tripdata` both available as Data Assets, with the following data_references:

```bash
Available data_asset_names (2 of 2):
    green_tripdata (3 of 3): ['green_tripdata_2019-01.csv', 'green_tripdata_2019-02.csv', 'green_tripdata_2019-03.csv']
    yellow_tripdata (3 of 3): ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'yellow_tripdata_2019-03.csv']

Unmatched data_references (0 of 0): []
```


### Example 3: Nested directory structure with the data_asset_name on the inside

Here’s a similar example, with a nested directory structure...

```
2018/10/yellow_tripdata.csv
2018/10/green_tripdata.csv
2018/11/yellow_tripdata.csv
2018/11/green_tripdata.csv
2018/12/yellow_tripdata.csv
2018/12/green_tripdata.csv
2019/01/yellow_tripdata.csv
2019/01/green_tripdata.csv
2019/02/yellow_tripdata.csv
2019/02/green_tripdata.csv
2019/03/yellow_tripdata.csv
2019/03/green_tripdata.csv
```

Then this configuration...

<Tabs
  groupId="yaml-or-python"
  defaultValue='python'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L306-L323
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py#L334-L352
```

</TabItem>
</Tabs>

...will now make `yellow_tripdata` and `green_tripdata` both available as Data Assets, with the following data_references:

```bash
Available data_asset_names (2 of 2):
    green_tripdata (3 of 6): ['2018/10/green_tripdata.csv', '2018/11/green_tripdata.csv', '2018/12/green_tripdata.csv']
    yellow_tripdata (3 of 6): ['2018/10/yellow_tripdata.csv', '2018/11/yellow_tripdata.csv', '2018/12/yellow_tripdata.csv']

Unmatched data_references (0 of 0):[]
```

Example 4: Nested directory structure with the data_asset_name on the outside
-----------------------------------------------------------------------------

In the following example, files are placed in a folder structure with the `data_asset_name` defined by the folder name (A, B, C, or D)

```
A/A-1.csv
A/A-2.csv
A/A-3.csv
B/B-1.csv
B/B-2.csv
B/B-3.csv
C/C-1.csv
C/C-2.csv
C/C-3.csv
D/D-1.csv
D/D-2.csv
D/D-3.csv
```

Then this configuration...

```yaml
class_name: InferredAssetFilesystemDataConnector
base_directory: /

default_regex:
  group_names:
    - data_asset_name
    - letter
    - number
  pattern: (\w{1})/(\w{1})-(\d{1})\.csv
```

...will now make `A` and `B` and `C` into data_assets, with each containing 3 data_references

```bash
Available data_asset_names (3 of 4):
    A (3 of 3): ['test_dir_charlie/A/A-1.csv',
                'test_dir_charlie/A/A-2.csv',
                'test_dir_charlie/A/A-3.csv']
    B (3 of 3): ['test_dir_charlie/B/B-1.csv',
                'test_dir_charlie/B/B-2.csv',
                'test_dir_charlie/B/B-3.csv']
    C (3 of 3): ['test_dir_charlie/C/C-1.csv',
                'test_dir_charlie/C/C-2.csv',
                'test_dir_charlie/C/C-3.csv']

Unmatched data_references (0 of 0): []
```

Example 5: Redundant information in the naming convention (S3 Bucket)
----------------------------------------------------------------------

Here’s another example of a nested directory structure with data_asset_name defined in the bucket_name.

```
my_bucket/2021/01/01/log_file-20210101.txt.gz,
my_bucket/2021/01/02/log_file-20210102.txt.gz,
my_bucket/2021/01/03/log_file-20210103.txt.gz,
my_bucket/2021/01/04/log_file-20210104.txt.gz,
my_bucket/2021/01/05/log_file-20210105.txt.gz,
my_bucket/2021/01/06/log_file-20210106.txt.gz,
my_bucket/2021/01/07/log_file-20210107.txt.gz,
```


Here’s a configuration that will allow all the log files in the bucket to be associated with a single data_asset, `my_bucket`

```yaml
class_name: InferredAssetFilesystemDataConnector
base_directory: /

default_regex:
  group_names:
    - year
    - month
    - day
    - data_asset_name
  pattern: (\w{11})/(\d{4})/(\d{2})/(\d{2})/log_file-.*\.csv
```

All the log files will be mapped to a single data_asset named `my_bucket`.

```bash
Available data_asset_names (1 of 1):
    my_bucket (3 of 7): [
        'my_bucket/2021/01/03/log_file-*.csv',
        'my_bucket/2021/01/04/log_file-*.csv',
        'my_bucket/2021/01/05/log_file-*.csv'
    ]

Unmatched data_references (0 of 0): []
```


Example 6: Random information in the naming convention
-------------------------------------------------------------------------------

In the following example, files are placed in folders according to the date of creation, and given a random hash value in their name.

```
2021/01/01/log_file-2f1e94b40f310274b485e72050daf591.txt.gz
2021/01/02/log_file-7f5d35d4f90bce5bf1fad680daac48a2.txt.gz
2021/01/03/log_file-99d5ed1123f877c714bbe9a2cfdffc4b.txt.gz
2021/01/04/log_file-885d40a5661bbbea053b2405face042f.txt.gz
2021/01/05/log_file-d8e478f817b608729cfc8fb750ebfc84.txt.gz
2021/01/06/log_file-b1ca8d1079c00fd4e210f7ef31549162.txt.gz
2021/01/07/log_file-d34b4818c52e74b7827504920af19a5c.txt.gz
```

Here’s a configuration that will allow all the log files to be associated with a single data_asset, `log_file`

```yaml
class_name: InferredAssetFilesystemDataConnector
base_directory: /

default_regex:
  group_names:
    - year
    - month
    - day
    - data_asset_name
  pattern: (\d{4})/(\d{2})/(\d{2})/(log_file)-.*\.txt\.gz
```

... will give you the following output

```bash
Available data_asset_names (1 of 1):
    log_file (3 of 7): [
        '2021/01/03/log_file-*.txt.gz',
        '2021/01/04/log_file-*.txt.gz',
        '2021/01/05/log_file-*.txt.gz'
    ]

Unmatched data_references (0 of 0): []
```

Example 7: Redundant information in the naming convention (timestamp of file creation)
--------------------------------------------------------------------------------------

In the following example, files are placed in a single folder, and the name includes a timestamp of when the files were created

```
log_file-2021-01-01-035419.163324.txt.gz
log_file-2021-01-02-035513.905752.txt.gz
log_file-2021-01-03-035455.848839.txt.gz
log_file-2021-01-04-035251.47582.txt.gz
log_file-2021-01-05-033034.289789.txt.gz
log_file-2021-01-06-034958.505688.txt.gz
log_file-2021-01-07-033545.600898.txt.gz
```

Here’s a configuration that will allow all the log files to be associated with a single data_asset named `log_file`.

```yaml
class_name: InferredAssetFilesystemDataConnector
base_directory: /

default_regex:
  group_names:
    - data_asset_name
    - year
    - month
    - day
  pattern: (log_file)-(\d{4})-(\d{2})-(\d{2})-.*\.*\.txt\.gz
```

All the log files will be mapped to the data_asset `log_file`.

```bash
Available data_asset_names (1 of 1):
    some_bucket (3 of 7): [
        'some_bucket/2021/01/03/log_file-*.txt.gz',
        'some_bucket/2021/01/04/log_file-*.txt.gz',
        'some_bucket/2021/01/05/log_file-*.txt.gz'
]

Unmatched data_references (0 of 0): []
```
