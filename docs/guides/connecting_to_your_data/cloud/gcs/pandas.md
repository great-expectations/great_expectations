---
title: How to connect to data on GCS using Pandas
---

import NextSteps from '../../components/next_steps.md'
import Congratulations from '../../components/congratulations.md'
import Prerequisites from '../../components/prerequisites.jsx'
import WhereToRunCode from '../../components/where_to_run_code.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you connect to your data stored on GCS using Pandas.
This will allow you to validate and explore your data.

<Prerequisites>

- Have access to data on a GCS bucket

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. `[🍏 CORE SKILL ICON]` Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L1-L4
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L6
```

### 3. Configure your Datasource

Great Expectations provides two types of `DataConnectors` classes for connecting to GCS.
  - An `InferredAssetGCSDataConnector` utilizes regular expressions to infer `data_asset_names` by evaluating filename patterns that exist in your bucket. This `DataConnector`, along with a `RuntimeDataConnector`, is provided as a default when utilizing our Jupyter Notebooks.
  - A `ConfiguredAssetGCSDataConnector` requires an explicit listing of each `DataAsset` you want to connect to. This allows for more granularity and control than its `Inferred` counterpart but also requires a more complex setup.

We've detailed example configurations for both options below for your reference.
Using these example configurations, add in your GCS bucket and path to a directory that contains some of your data:

<Tabs
  groupId="inferred-or-configured"
  defaultValue='inferred'
  values={[
  {label: 'Inferred (Default)', value:'inferred'},
  {label: 'Configured', value:'configured'},
  ]}>

<TabItem value="inferred">
  Inferred
  <Tabs
    groupId="yaml-or-python"
    defaultValue='yaml1'
    values={[
    {label: 'YAML', value:'yaml1'},
    {label: 'Python', value:'python1'},
    ]}>
  <TabItem value="yaml1">

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L8-L27
  ```

  Run this code to test your configuration.

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L37
  ```
  </TabItem>
  <TabItem value="python1">

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_python_example.py#L8-L27
  ```

  Run this code to test your configuration.

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_python_example.py#L38
  ```
  </TabItem>
  </Tabs>
</TabItem>
<TabItem value="configured">
  Configured
  <Tabs
    groupId="yaml-or-python"
    defaultValue='yaml2'
    values={[
    {label: 'YAML', value:'yaml2'},
    {label: 'Python', value:'python2'},
    ]}>
  <TabItem value="yaml2">

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_yaml_example.py#L8-L25
  ```

  Run this code to test your configuration.

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_yaml_example.py#L36
  ```
  </TabItem>
  <TabItem value="python2">

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_python_example.py#L8-L25
  ```

  Run this code to test your configuration.

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_python_example.py#L35
  ```
  </TabItem>
  </Tabs>
</TabItem>
</Tabs>

If you specified a GCS path containing CSV files you will see them listed as `Available data_asset_names` in the output of `test_yaml_config()`.
Please note we support the following format for GCS URL's: `gs://<BUCKET_OR_NAME>/<BLOB>`

Feel free to adjust your configuration and re-run `test_yaml_config()` as needed.

#### Authentication

### 4. Save the Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml3'},
  {label: 'Python', value:'python3'},
  ]}>
  <TabItem value="yaml3">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L39
```

</TabItem>
<TabItem value="python3">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_python_example.py#L40
```

</TabItem>
</Tabs>

### 5. Test your new Datasource

Verify your new Datasource by loading data from it into a `Validator` using a `BatchRequest`.

<Tabs
  defaultValue='runtime_batch_request'
  values={[
  {label: 'Specify a GCS path to single CSV', value:'runtime_batch_request'},
  {label: 'Specify a data_asset_name', value:'batch_request'},
  ]}>
  <TabItem value="runtime_batch_request">

Add the GCS path to your CSV in the `path` key under `runtime_parameters` in your `BatchRequest`.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L42-L48
```
Then load data into the `Validator`.
```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L56-L62
```

  </TabItem>
  <TabItem value="batch_request">

Add the name of the data asset to the `data_asset_name` in your `BatchRequest`.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L68-L72
```
Then load data into the `Validator`.
```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L56-L62
```

  </TabItem>
</Tabs>


<Congratulations />

## Additional Notes

If you are working with nonstandard CSVs, read one of these guides:

- [How to work with headerless CSVs in pandas](#TODO)
- [How to work with custom delimited CSVs in pandas](#TODO)
- [How to work with parquet files in pandas](#TODO)

To view the full scripts used in this page, see them on GitHub:

- [inferred_and_runtime_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py)
- [inferred_and_runtime_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_python_example.py)
- [configured_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_yaml_example.py)
- [configured_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_python_example.py)

## Next Steps

<NextSteps />
