---
title: How to configure a ConfiguredAssetDataConnector
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide demonstrates how to configure a ConfiguredAssetDataConnector, and provides several examples you can use for configuration.

<Prerequisites>

- [Understand the basics of Datasources in 0.13 or later](../../reference/datasources.md)
- Learned how to configure a [Data Context using test_yaml_config](../setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config.md)

</Prerequisites>

Great Expectations provides two `DataConnector` classes for connecting to <TechnicalTag tag="data_asset" text="Data Assets" /> stored as file-system-like data (this includes files on disk, but also S3 object stores, etc) as well as relational database data:

- A ConfiguredAssetDataConnector allows you to specify that you have multiple Data Assets in a `Datasource`, but also requires an explicit listing of each Data Asset you want to connect to. This allows more fine-tuning, but also requires more setup.
- An InferredAssetDataConnector infers `data_asset_name` by using a regex that takes advantage of patterns that exist in the filename or folder structure.

If you're not sure which one to use, please check out [How to choose which DataConnector to use](./how_to_choose_which_dataconnector_to_use.md).

## Steps

### 1. Instantiate your project's DataContext

Import these necessary packages and modules:

<Tabs 
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L1-L4
```

</TabItem>

<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L3-L4
```

</TabItem>

</Tabs>

### 2. Set up a Datasource

All of the examples below assume you’re testing configuration using something like:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
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
  <DATACONNECTOR NAME GOES HERE>:
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
        "<DATA CONNECTOR NAME GOES HERE>": {
          "<DATACONNECTOR CONFIGURATION GOES HERE>"
        },
    },
}
context.test_yaml_config(yaml.dump(datasource_config))
```

</TabItem>

</Tabs>

If you’re not familiar with the `test_yaml_config` method, please check out: [How to configure Data Context components using test_yaml_config](../setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config.md)

### 3. Add a ConfiguredAssetDataConnector to a Datasource configuration

ConfiguredAssetDataConnectors like `ConfiguredAssetFilesystemDataConnector` and `ConfiguredAssetS3DataConnector` require Data Assets to be explicitly named. A Data Asset is an abstraction that can consist of one or more data_references to CSVs or relational database tables. For instance, you might have a `yellow_tripdata` Data Asset containing information about taxi rides, which consists of twelve data_references to twelve CSVs, each consisting of one month of data. Each Data Asset can have their own regex `pattern` and `group_names`, and if configured, will override any `pattern` or `group_names` under `default_regex`.

Imagine you have the following files in `<MY DIRECTORY>/`:

```
<MY DIRECTORY>/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata_2019-03.csv
```

We could create a Data Asset `yellow_tripdata` that contains 3 data_references (`yellow_tripdata_2019-01.csv`, `yellow_tripdata_2019-02.csv`, and `yellow_tripdata_2019-03.csv`).
In that case, the configuration would look like the following:

<Tabs groupId="yaml-or-python" defaultValue='yaml' values={[{label: 'YAML', value:'yaml'}, {label: 'Python', value:'python'}]}>

<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L9-L25
```

</TabItem>

<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L36-L56
```

</TabItem>

</Tabs>

Notice that we have specified a pattern that captures the year-month combination after `yellow_tripdata_` in the filename and assigns it to the `group_name` `month`.

The configuration would also work with a regex capturing the entire filename (e.g. `pattern: (.*)\.csv`).  However, capturing the month on its own allows for `batch_identifiers` to be used to retrieve a specific <TechnicalTag tag="batch" text="Batch" /> of the Data Asset. For more information about capture groups, refer to the Python documentation on [regular expressions](https://docs.python.org/3/library/re.html#re.Match.group).

Later on we could retrieve the data in `yellow_tripdata_2019-02.csv` of `yellow_tripdata` as its own batch using `context.get_validator()` by specifying `{"month": "2019-02"}` as the `batch_identifier`.

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L72-L87
```

This ability to access specific Batches using `batch_identifiers` is very useful when validating Data Assets that span multiple files.
For more information on `batches` and `batch_identifiers`, please refer to the [Core Concepts document](../../reference/dividing_data_assets_into_batches.md).

A corresponding configuration for `ConfiguredAssetS3DataConnector` would look similar but would require `bucket` and `prefix` values instead of `base_directory`.

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L99-L116
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L128-L149
```

</TabItem>

</Tabs>

The following examples will show scenarios that ConfiguredAssetDataConnectors can help you analyze, using `ConfiguredAssetFilesystemDataConnector`.

### Example 1: Basic Configuration for a single Data Asset

Continuing the example above, imagine you have the following files in the directory `<MY DIRECTORY>`:

```
<MY DIRECTORY>/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata_2019-03.csv
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

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L176-L192
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L203-L223
```

</TabItem>

</Tabs>

will make available `yelow_tripdata` as a single Data Asset with the following data_references:

```bash
Available data_asset_names (1 of 1):
    yellow_tripdata (3 of 3): ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'yellow_tripdata_2019-03.csv']

Unmatched data_references (0 of 0):[]
```

Once configured, you can get a <TechnicalTag tag="validator" text="Validator" /> from the <TechnicalTag tag="data_context" text="Data Context" /> as follows:

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L239-L249
```

But what if the regex does not match any files in the directory?

Then this configuration:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L261-L277
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L288-L308
```

</TabItem>

</Tabs>

will give you this output

```bash
Available data_asset_names (1 of 1):
    yellow_tripdata (0 of 0): []

Unmatched data_references (3 of 3):['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'yellow_tripdata_2019-03.csv']
```

Notice that `yellow_tripdata` has 0 `data_references`, and there are 3 `Unmatched data_references` listed.
This would indicate that some part of the configuration is incorrect and would need to be reviewed.
In our case, changing `pattern` to `yellow_tripdata_(.*)\.csv` will fix our problem and give the same output to above.


### Example 2: Basic configuration with more than one Data Asset

Here’s a similar example, but this time two Data Assets are mixed together in one folder.

**Note**: For an equivalent configuration using `InferredAssetFileSystemDataConnector`, please see Example 2 in  [How to configure an InferredAssetDataConnector](./how_to_configure_an_inferredassetdataconnector.md).

```
<MY DIRECTORY>/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/green_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/green_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata_2019-03.csv
<MY DIRECTORY>/green_tripdata_2019-03.csv
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

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L330-L352
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L363-L387
```

</TabItem>

</Tabs>

will now make `yellow_tripdata` and `green_tripdata` both available as Data Assets, with the following data_references:

```bash
Available data_asset_names (2 of 2):
    green_tripdata (3 of 3): ['green_tripdata_2019-01.csv', 'green_tripdata_2019-02.csv', 'green_tripdata_2019-03.csv']
    yellow_tripdata (3 of 3): ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'yellow_tripdata_2019-03.csv']

Unmatched data_references (0 of 0): []
```

### Example 3: Example with Nested Folders

In the following example, files are placed folders that match the `data_asset_names` we want (`yellow_tripdata` and `green_tripdata`), but the filenames follow different formats.

```
<MY DIRECTORY>/yellow_tripdata/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata/yellow_tripdata_2019-03.csv
<MY DIRECTORY>/green_tripdata/2019-01.csv
<MY DIRECTORY>/green_tripdata/2019-02.csv
<MY DIRECTORY>/green_tripdata/2019-03.csv
```

The following configuration:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L415-L439
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L450-L476
```

</TabItem>

</Tabs>

will now make `yellow_tripdata` and `green_tripdata` available a Data Assets, with the following data_references:

```bash
Available data_asset_names (2 of 2):
    green_tripdata (3 of 3): ['2019-01.csv', '2019-02.csv', '2019-03.csv']
    yellow_tripdata (3 of 3): ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'yellow_tripdata_2019-03.csv']

Unmatched data_references (0 of 0):[]
```

### Example 4: Example with Explicit data_asset_names and more complex nesting

In this example, the assets `yellow_tripdata` and `green_tripdata` are being explicitly defined in the configuration, and have a more complex nesting pattern.

```
<MY DIRECTORY>/yellow/tripdata/yellow_tripdata_2019-01.txt
<MY DIRECTORY>/yellow/tripdata/yellow_tripdata_2019-02.txt
<MY DIRECTORY>/yellow/tripdata/yellow_tripdata_2019-03.txt
<MY DIRECTORY>/green_tripdata/green_tripdata_2019-01.csv
<MY DIRECTORY>/green_tripdata/green_tripdata_2019-02.csv
<MY DIRECTORY>/green_tripdata/green_tripdata_2019-03.csv
```

The following configuration:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L503-L527
```

</TabItem>
<TabItem value="python">

```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py#L538-L566
```

</TabItem>

</Tabs>

will make `yellow_tripdata` and `green_tripdata` available as Data Assets, with the following data_references:

```bash
Available data_asset_names (2 of 2):
    green_tripdata (3 of 3): ['green_tripdata_2019-01.', 'green_tripdata_2019-02.', 'green_tripdata_2019-03.']
    yellow_tripdata (3 of 3): ['yellow_tripdata_2019-01.', 'yellow_tripdata_2019-02.', 'yellow_tripdata_2019-03.']

Unmatched data_references (0 of 0):[]
```

### Additional Notes
To view the full script used in this page, see it on GitHub:
- [how_to_configure_a_configuredassetdataconnector.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_configure_a_configuredassetdataconnector.py)
