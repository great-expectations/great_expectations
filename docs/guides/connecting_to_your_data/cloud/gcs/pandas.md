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

### 2. Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L4-L8
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L12
```

### 3. Configure your Datasource

Great Expectations provides two types of `DataConnectors` classes for connecting to GCS: `InferredAssetGCSDataConnector` and `ConfiguredAssetGCSDataConnector`

  - An `InferredAssetGCSDataConnector` utilizes regular expressions to infer `data_asset_names` by evaluating filename patterns that exist in your bucket. This `DataConnector`, along with a `RuntimeDataConnector`, is provided as a default when utilizing our Jupyter Notebooks.
  - A `ConfiguredAssetGCSDataConnector` requires an explicit listing of each `DataAsset` you want to connect to. This allows for more granularity and control than its `Inferred` counterpart but also requires a more complex setup.

As the `InferredAssetDataConnectors` have fewer options and are generally simpler to use, we recommend starting with them.

We've detailed example configurations for both options in the next section for your reference.

:::info Authentication

It is also important to note that GCS `DataConnectors` support various methods of authentication. You should be aware of the following options when configuring your own environment:
* `gcloud` command line tool / `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
  - This is the default option and what is used throughout this guide.
* Passing a `filename` argument to the optional `gcs_options` dictionary.
  - This argument should contain a specific filepath that leads to your credentials JSON.
  - This method utilizes `google.oauth2.service_account.Credentials.from_service_account_file` under the hood.
* Passing an `info` argument to the optional `gcs_options` dictionary.
  - This argument should contain the actual JSON data from your credentials file in the form of a string.
  - This method utilizes `google.oauth2.service_account.Credentials.from_service_account_info` under the hood.

Please note that if you use the `filename` or `info` options, you must supply these options to any GE objects that interact with GCS (i.e. `PandasExecutionEngine`).
The `gcs_options` dictionary is also responsible for storing any `**kwargs` you wish to pass to the GCS `storage.Client()` connection object (i.e. `project`)

For more details regarding storing credentials for use with Great Expectations see: [How to configure credentials](../../../setup/configuring_data_contexts/how_to_configure_credentials.md)

For more details regarding authentication, please visit the following:
* [gcloud CLI Tutorial](https://cloud.google.com/storage/docs/reference/libraries)
* [GCS Python API Docs](https://googleapis.dev/python/storage/latest/index.html)

:::

Using these example configurations, add in your GCS bucket and path to a directory that contains some of your data:
<Tabs
  groupId="inferred-or-configured"
  defaultValue='inferred'
  values={[
  {label: 'Inferred + Runtime (Default)', value:'inferred'},
  {label: 'Configured', value:'configured'},
  ]}>

<TabItem value="inferred">
  The below configuration is representative of the default setup you'll see when preparing your own environment.

  <Tabs
    groupId="yaml-or-python"
    defaultValue='yaml'
    values={[
    {label: 'YAML', value:'yaml'},
    {label: 'Python', value:'python'},
    ]}>
  <TabItem value="yaml">

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L16-L34
  ```

  Run this code to test your configuration.

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L47
  ```
  </TabItem>
  <TabItem value="python">

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_python_example.py#L11-L30
  ```

  Run this code to test your configuration.

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_python_example.py#L43
  ```
  </TabItem>
  </Tabs>
</TabItem>
<TabItem value="configured">
  The below configuration is highly tuned to the specific bucket and blobs relevant to this example. You'll have to fine-tune your own regular expressions and assets to fit your use-case.
  <Tabs
    groupId="yaml-or-python"
    defaultValue='yaml'
    values={[
    {label: 'YAML', value:'yaml'},
    {label: 'Python', value:'python'},
    ]}>
  <TabItem value="yaml">

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_yaml_example.py#L10-L27
  ```

  Run this code to test your configuration.

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_yaml_example.py#L38
  ```
  </TabItem>
  <TabItem value="python">

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_python_example.py#L10-L26
  ```

  Run this code to test your configuration.

  ```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/configured_python_example.py#L37
  ```
  </TabItem>
  </Tabs>
</TabItem>
</Tabs>

If you specified a GCS path containing CSV files you will see them listed as `Available data_asset_names` in the output of `test_yaml_config()`.

Feel free to adjust your configuration and re-run `test_yaml_config()` as needed.

### 4. Save the Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
  <TabItem value="yaml">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L47
```

</TabItem>
<TabItem value="python">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_python_example.py#L47
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

Add the GCS path to your CSV in the `path` key under `runtime_parameters` in your `RuntimeBatchRequest`.

Please note we support the following format for GCS URL's: `gs://<BUCKET_OR_NAME>/<BLOB>`.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L52-L58
```
Then load data into the `Validator`.
```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L68-L74
```

  </TabItem>
  <TabItem value="batch_request">

Add the name of the data asset to the `data_asset_name` in your `BatchRequest`.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L88-L92
```
Then load data into the `Validator`.
```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/pandas/inferred_and_runtime_yaml_example.py#L68-L74
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

To review the source code of these `DataConnectors`, also visit GitHub:
- [ConfiguredAssetGCSDataConnector](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/datasource/data_connector/configured_asset_gcs_data_connector.py)
- [InferredAssetGCSDataConnector](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/datasource/data_connector/inferred_asset_gcs_data_connector.py)
