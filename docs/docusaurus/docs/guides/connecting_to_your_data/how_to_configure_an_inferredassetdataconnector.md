---
title: How to configure an InferredAssetDataConnector
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide demonstrates how to configure an InferredAssetDataConnector, and provides several examples you
can use for configuration.

## Prerequisites

<Prerequisites>

- [An understanding of Datasource version 0.13 or later basics](../../terms/datasource.md)

</Prerequisites>

Great Expectations provides two types of `DataConnector` classes for connecting to <TechnicalTag tag="data_asset" text="Data Assets" /> stored as file-system-like data (this includes files on disk, but also S3 object stores, etc) as well as relational database data:

- A ConfiguredAssetDataConnector allows you to specify that you have multiple Data Assets in a `Datasource`, but also requires an explicit listing of each Data Asset you want to connect to. This allows more fine-tuning, but also requires more setup.
- An InferredAssetDataConnector infers `data_asset_name` by using a regex that takes advantage of patterns that exist in the filename or folder structure.

InferredAssetDataConnector has fewer options, so it's simpler to set up. It’s a good choice if you want to connect to a single Data Asset, or several Data Assets that all share the same naming convention.

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

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py imports yaml"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py imports python"
```

</TabItem>

</Tabs>

### 2. Set up a Datasource

All the examples below assume you’re testing configurations using something like:

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
  <DATA CONNECTOR NAME GOES HERE>:
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
        "<DATACONNECTOR NAME GOES HERE>": {
          "<DATACONNECTOR CONFIGURATION GOES HERE>"
        },
    },
}
context.test_yaml_config(yaml.dump(datasource_config))
```

</TabItem>

</Tabs>


### 3. Add an InferredAssetDataConnector to a Datasource configuration

InferredAssetDataConnectors like `InferredAssetFilesystemDataConnector` and `InferredAssetS3DataConnector`
require a `default_regex` parameter, with a configured regex `pattern` and capture `group_names`.

Imagine you have the following files in `my_directory/`:

```
<MY DIRECTORY>/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata_2019-03.csv
```

We can imagine two approaches to loading the data into GX.

The simplest approach would be to consider each file to be its own Data Asset. In that case, the configuration would look like the following:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_yaml each file own data asset"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_config each file own data asset"
```

</TabItem>

</Tabs>

Notice that the `default_regex` is configured to have one capture group (`(.*)`) which captures the entire filename. That capture group is assigned to `data_asset_name` under `group_names`. For InferredAssetDataConnectors `data_asset_name` is a required `group_name`, and it's associated capture group is the way each `data_asset_name` is inferred.
Running `test_yaml_config()` would result in 3 Data Assets : `yellow_tripdata_2019-01`, `yellow_tripdata_2019-02` and `yellow_tripdata_2019-03`.

However, a closer look at the filenames reveals a pattern that is common to the 3 files. Each have `yellow_tripdata_` in the name, and have date information afterwards. These are the types of patterns that InferredAssetDataConnectors allow you to take advantage of.

We could treat `yellow_tripdata_*.csv` files as <TechnicalTag tag="batch" text="Batches" /> within the `yellow_tripdata` Data Asset with a more specific regex `pattern` and adding `group_names` for `year` and `month`.

**Note: ** We have chosen to be more specific in the capture groups for the `year` and `month` by specifying the integer value (using `\d`) and the number of digits, but a simpler capture group like `(.*)` would also work. For more information about capture groups, refer to the Python documentation on [regular expressions](https://docs.python.org/3/library/re.html#re.Match.group).

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_yaml each file own data asset"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_config each file own data asset"
```

</TabItem>

</Tabs>

Running `test_yaml_config()` would result in 1 Data Asset `yellow_tripdata` with 3 associated data_references: `yellow_tripdata_2019-01.csv`, `yellow_tripdata_2019-02.csv` and `yellow_tripdata_2019-03.csv`, seen also in Example 1 below.

A corresponding configuration for `InferredAssetS3DataConnector` would look similar but would require `bucket` and `prefix` values instead of `base_directory`.

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_yaml add an InferredAssetDataConnector to a Datasource configuration"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_config add an InferredAssetDataConnector to a Datasource configuration"
```

</TabItem>

</Tabs>

The following examples will show scenarios that InferredAssetDataConnectors can help you analyze, using `InferredAssetFilesystemDataConnector`.


### Example 1: Basic configuration for a single Data Asset

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

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_yaml basic configuration with more than one Data Asset"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_config basic configuration with more than one Data Asset"
```

</TabItem>

</Tabs>

will make available `yelow_tripdata` as a single Data Asset with the following data_references:

```bash
Available data_asset_names (1 of 1):
    yellow_tripdata (3 of 3): ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv', 'yellow_tripdata_2019-03.csv']

Unmatched data_references (0 of 0):[]
```

Once configured, you can get <TechnicalTag tag="validator" text="Validators" /> from the <TechnicalTag tag="data_context" text="Data Context" /> as follows:

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py get_validator"
```

Since this `BatchRequest` does not specify which data_reference to load, the `ActiveBatch` for the validator will be the last data_reference that was loaded. In this case, `yellow_tripdata_2019-03.csv` is what is being used by `validator`. We can verfiy this with:

```python
print(validator.active_batch_definition)
```

which prints:
```bash
{
  "datasource_name": "taxi_datasource",
  "data_connector_name": "default_inferred_data_connector_name",
  "data_asset_name": "yellow_tripdata",
  "batch_identifiers": {
    "year": "2019",
    "month": "03"
  }
}
```

Notice that the `batch_identifiers` for this `batch_definition` specify `"year": "2019", "month": "03"`. The parameter `batch_identifiers` can be used in our `BatchRequest` to return the data_reference CSV of our choosing using the `group_names` defined in our `DataConnector`:

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py batch_request 2019-02"
```

```python
print(validator.active_batch_definition)
```

which prints:
```bash
{
  "datasource_name": "taxi_datasource",
  "data_connector_name": "default_inferred_data_connector_name",
  "data_asset_name": "yellow_tripdata",
  "batch_identifiers": {
    "year": "2019",
    "month": "02"
  }
}
```

This ability to access specific Batches using `batch_identifiers` is very useful when validating Data Assets that span multiple files.
For more information on `batches` and `batch_identifiers`, please refer to our [Batches documentation](../../terms/batch.md).

### Example 2: Basic configuration with more than one Data Asset

Here’s a similar example, but this time two Data Assets are mixed together in one folder.

**Note**: For an equivalent configuration using `ConfiguredAssetFilesSystemDataconnector`, please see Example 2
in [How to configure a ConfiguredAssetDataConnector](./how_to_configure_a_configuredassetdataconnector.md).

```
<MY DIRECTORY>/yellow_tripdata_2019-01.csv
<MY DIRECTORY>/green_tripdata_2019-01.csv
<MY DIRECTORY>/yellow_tripdata_2019-02.csv
<MY DIRECTORY>/green_tripdata_2019-02.csv
<MY DIRECTORY>/yellow_tripdata_2019-03.csv
<MY DIRECTORY>/green_tripdata_2019-03.csv
```

The same configuration as Example 1:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_yaml basic configuration with more than one Data Asset"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_config basic configuration with more than one Data Asset"
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


### Example 3: Nested directory structure with the data_asset_name on the inside

Here’s a similar example, with a nested directory structure:

```
<MY DIRECTORY>/2018/10/yellow_tripdata.csv
<MY DIRECTORY>/2018/10/green_tripdata.csv
<MY DIRECTORY>/2018/11/yellow_tripdata.csv
<MY DIRECTORY>/2018/11/green_tripdata.csv
<MY DIRECTORY>/2018/12/yellow_tripdata.csv
<MY DIRECTORY>/2018/12/green_tripdata.csv
<MY DIRECTORY>/2019/01/yellow_tripdata.csv
<MY DIRECTORY>/2019/01/green_tripdata.csv
<MY DIRECTORY>/2019/02/yellow_tripdata.csv
<MY DIRECTORY>/2019/02/green_tripdata.csv
<MY DIRECTORY>/2019/03/yellow_tripdata.csv
<MY DIRECTORY>/2019/03/green_tripdata.csv
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

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_yaml nested directory structure with the data_asset_name on the inside"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_config nested directory structure with the data_asset_name on the inside"
```

</TabItem>

</Tabs>

will now make `yellow_tripdata` and `green_tripdata` both available as Data Assets, with the following data_references:

```bash
Available data_asset_names (2 of 2):
    green_tripdata (3 of 6): ['2018/10/green_tripdata.csv', '2018/11/green_tripdata.csv', '2018/12/green_tripdata.csv']
    yellow_tripdata (3 of 6): ['2018/10/yellow_tripdata.csv', '2018/11/yellow_tripdata.csv', '2018/12/yellow_tripdata.csv']

Unmatched data_references (0 of 0):[]
```

The `glob_directive` is provided to give the `DataConnector` information about the directory structure to expect for each Data Asset. The default `glob_directive` for the `InferredAssetFileSystemDataConnector` is `"*"` and therefore must be overridden when your data_references exist in subdirectories.

### Example 4: Nested directory structure with the data_asset_name on the outside

In the following example, files are placed in a folder structure with the `data_asset_name` defined by the folder name (`yellow_tripdata` or `green_tripdata`)

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

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_yaml nested directory structure with the data_asset_name on the outside"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_config nested directory structure with the data_asset_name on the outside"
```

</TabItem>

</Tabs>

will now make `yellow_tripdata` and `green_tripdata` into Data Assets, with each containing 3 data_references

```bash
Available data_asset_names (2 of 2):
    green_tripdata (3 of 3): ['green_tripdata/2019-01.csv', 'green_tripdata/2019-02.csv', 'green_tripdata/2019-03.csv']
    yellow_tripdata (3 of 3): ['yellow_tripdata/yellow_tripdata_2019-01.csv', 'yellow_tripdata/yellow_tripdata_2019-02.csv', 'yellow_tripdata/yellow_tripdata_2019-03.csv']

Unmatched data_references (0 of 0):[]
```

### Example 5: Redundant information in the naming convention

In the following example, files are placed in a folder structure with the `data_asset_name` defined by the folder name (`yellow_tripdata` or `green_tripdata`), but then the term `yellow_tripdata` is repeated in some filenames.

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

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py datasource_yaml redundant information in the naming convention"
```

</TabItem>

<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py python datasource_config redundant information in the naming convention"
```

</TabItem>

</Tabs>

will not display the redundant information:

```bash
Available data_asset_names (2 of 2):
    green_tripdata (3 of 3): ['green_tripdata/*2019-01.csv', 'green_tripdata/*2019-02.csv', 'green_tripdata/*2019-03.csv']
    yellow_tripdata (3 of 3): ['yellow_tripdata/*2019-01.csv', 'yellow_tripdata/*2019-02.csv', 'yellow_tripdata/*2019-03.csv']

Unmatched data_references (0 of 0):[]
```

### Additional Notes
To view the full script used in this page, see it on GitHub:
- [how_to_configure_an_inferredassetdataconnector.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_configure_an_inferredassetdataconnector.py)
