---
title: How to configure a ConfiguredAssetDataConnector
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide demonstrates how to configure a ConfiguredAssetDataConnector, and provides several examples you can use for configuration.

<Prerequisites>

- [Understand the basics of Datasources in 0.13 or later](../../reference/datasources.md)
- Learned how to configure a [Data Context using test_yaml_config](../setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config.md)

</Prerequisites>

Great Expectations provides two `DataConnector` classes for connecting to `DataAsset`s stored as file-system-like data. This includes files on disk,
but also S3 object stores, etc:

- A ConfiguredAssetDataConnector allows you to specify that you have multiple `DataAsset`s in a `Datasource`, but also requires an explicit listing of each `DataAsset` you want to connect to. This allows more fine-tuning, but also requires more setup.
- An InferredAssetDataConnector infers `data_asset_name` by using a regex that takes advantage of patterns that exist in the filename or folder structure.

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

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L3-L4
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L1-L4
```

</TabItem>
</Tabs>

### 2. Set up a Datasource

All of the examples below assume you’re testing configuration using something like:

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
  default_configured_data_connector_name:
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
        "default_configured_data_connector_name": {
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

ConfiguredAssetDataConnectors like `ConfiguredAssetFilesystemDataConnector` and `ConfiguredAssetS3DataConnector` require `DataAsset`s to be
explicitly named. Each `DataAsset` can have their own regex `pattern` and `group_names`, and if configured, will override any
`pattern` or `group_names` under `default_regex`.

Imagine you have the following files in `my_directory/`:

```
<MY DIRECTORY>/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata_2019-03.csv
```

We could create a DataAsset `yellow_tripdata` that contains 3 data_references (`yellow_tripdata_2019-01.csv`, `yellow_tripdata_2019-02.csv`, and `yellow_tripdata_2019-03.csv`).
In that case, the configuration would look like the following:

<Tabs
  groupId="yaml-or-python"
  defaultValue='python'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L9-L25
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L34-L54
```

</TabItem>
</Tabs>

Notice that we have specified a pattern that captures the year-month combination after `yellow_tripdata_` in the filename and assigns it to the `group_name` `month`.

The configuration would also work with a regex capturing the entire filename (ie `pattern: (.*)\\.csv`).  However, capturing the month on its own allows for `batch_identifiers` to be used to retrieve a specific Batch of the `DataAsset`.

Later on we could retrieve the data in `yellow_tripdata_2019-02.csv` of `yellow_tripdata` as its own batch using `context.get_validator()` by specifying `{"month": "2019-02"}` as the `batch_identifier`.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L72-L87
```

This ability to access specific Batches using `batch_identifiers` is very useful when validating `DataAsset`s that span multiple files.
For more information on `batches` and `batch_identifiers`, please refer to the [Core Concepts document](../../reference/dividing_data_assets_into_batches.md).

A corresponding configuration for `ConfiguredAssetS3DataConnector` would look similar but would require `bucket` and `prefix` values instead of `base_directory`.

<Tabs
  groupId="yaml-or-python"
  defaultValue='python'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L99-L115
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L128-L147
```

</TabItem>
</Tabs>

The following examples will show scenarios that ConfiguredAssetDataConnectors can help you analyze, using `ConfiguredAssetFilesystemDataConnector`.

### Example 1: Basic Configuration for a single DataAsset

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

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L175-L191
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L202-L222
```

</TabItem>
</Tabs>

...will make available `yelow_tripdata` as a single DataAsset with the following data_references:

```bash
Available data_asset_names (1 of 1):
    yellow_tripdata (3 of 3): ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'yellow_tripdata_2019-03.csv']

Unmatched data_references (0 of 0):[]
```

Once configured, you can get a `Validator` from the `Data Context` as follows:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L238-L248
```

But what if the regex does not match any files in the directory?

Then this configuration...

<Tabs
  groupId="yaml-or-python"
  defaultValue='python'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L260-L276
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L287-L307
```

</TabItem>
</Tabs>

...will give you this output

```bash
Available data_asset_names (1 of 1):
    yellow_tripdata (0 of 0): []

Unmatched data_references (3 of 3):['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'yellow_tripdata_2019-03.csv']
```

Notice that `yellow_tripdata` has 0 data_references, and there are 3 `Unmatched data_references` listed.
This would indicate that some part of the configuration is incorrect and would need to be reviewed.
In our case, changing `pattern` to : `yellow_tripdata_(.*)\\.csv` will fix our problem and give the same output to above.


### Example 2: Basic configuration with more than one DataAsset

Here’s a similar example, but this time two Data Assets are mixed together in one folder.

**Note**: For an equivalent configuration using `InferredAssetFileSystemDataConnector`, please see Example 2 in  [How to configure an InferredAssetDataConnector](./how_to_configure_an_inferredassetdataconnector).

```
<MY DIRECTORY>/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/green_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/green_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata_2019-03.csv
<MY DIRECTORY>/green_tripdata_2019-03.csv
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

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L329-L351
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L362-L386
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

### Example 3: Example with Nested Folders

In the following example, files are placed folders that match the `data_asset_names` we want: `A`, `B`, `C`, and `D`.

```
test_dir/A/A-1.csv
test_dir/A/A-2.csv
test_dir/A/A-3.csv
test_dir/B/B-1.txt
test_dir/B/B-2.txt
test_dir/B/B-3.txt
test_dir/C/C-2017.csv
test_dir/C/C-2018.csv
test_dir/C/C-2019.csv
test_dir/D/D-aaa.csv
test_dir/D/D-bbb.csv
test_dir/D/D-ccc.csv
test_dir/D/D-ddd.csv
test_dir/D/D-eee.csv
```

```yaml
module_name: great_expectations.datasource.data_connector
class_name: ConfiguredAssetFilesystemDataConnector
base_directory: test_dir/
assets:
  A:
    base_directory: A/
  B:
    base_directory: B/
    pattern: (.*)-(.*)\.txt
    group_names:
      - part_1
      - part_2
  C:
    glob_directive: "*"
    base_directory: C/
  D:
    glob_directive: "*"
    base_directory: D/
default_regex:
  pattern: (.*)-(.*)\.csv
  group_names:
    - part_1
    - part_2
```

...will now make `A`, `B`, `C` and `D`  available a DataAssets, with the following data_references:

```bash
Available data_asset_names (4 of 4):
   A (3 of 3): [
      'A-1.csv',
      'A-2.csv',
      'A-3.csv',
   ]
   B (3 of 3):  [
      'B-1',
      'B-2',
      'B-3',
   ]
   C (3 of 3): [
      'C-2017',
      'C-2018',
      'C-2019',
   ]
   D (5 of 5): [
      'D-aaa.csv',
      'D-bbb.csv',
      'D-ccc.csv',
      'D-ddd.csv',
      'D-eee.csv',
   ]
```

Example 4: Example with Explicit data_asset_names and more complex nesting
--------------------------------------------------------------------------

In this example, the assets `alpha`, `beta` and `gamma` are being explicitly defined in the configuration, and have a more complex nesting pattern.

```
my_base_directory/alpha/files/go/here/alpha-202001.csv
my_base_directory/alpha/files/go/here/alpha-202002.csv
my_base_directory/alpha/files/go/here/alpha-202003.csv
my_base_directory/beta_here/beta-202001.txt
my_base_directory/beta_here/beta-202002.txt
my_base_directory/beta_here/beta-202003.txt
my_base_directory/beta_here/beta-202004.txt
my_base_directory/gamma-202001.csv
my_base_directory/gamma-202002.csv
my_base_directory/gamma-202003.csv
my_base_directory/gamma-202004.csv
my_base_directory/gamma-202005.csv
```

The following configuration...

```yaml
class_name: ConfiguredAssetFilesystemDataConnector
base_directory: my_base_directory/
default_regex:
  pattern: ^(.+)-(\d{4})(\d{2})\.(csv|txt)$
  group_names:
    - data_asset_name
    - year_dir
    - month_dir
assets:
  alpha:
    base_directory: my_base_directory/alpha/files/go/here/
    glob_directive: "*.csv"
  beta:
    base_directory: my_base_directory/beta_here/
    glob_directive: "*.txt"
  gamma:
    glob_directive: "*.csv"
```

...will make `alpha`, `beta` and `gamma`  available a DataAssets, with the following data_references:

```bash
Available data_asset_names (3 of 3):
   alpha (3 of 3): [
      'alpha-202001.csv',
      'alpha-202002.csv',
      'alpha-202003.csv'
   ]
   beta (4 of 4):  [
      'beta-202001.txt',
      'beta-202002.txt',
      'beta-202003.txt',
      'beta-202004.txt'
   ]
   gamma (5 of 5): [
      'gamma-202001.csv',
      'gamma-202002.csv',
      'gamma-202003.csv',
      'gamma-202004.csv',
      'gamma-202005.csv',
   ]
```
