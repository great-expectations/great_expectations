---
title: How to connect to data on Azure Blob Storage using Pandas
---

import NextSteps from '../../components/next_steps.md'
import Congratulations from '../../components/congratulations.md'
import Prerequisites from '../../components/prerequisites.jsx'
import WhereToRunCode from '../../components/where_to_run_code.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you connect to your data stored on Microsoft Azure Blob Storage (ABS) using Pandas.
This will allow you to <TechnicalTag tag="validation" text="Validate" /> and explore your data.

<Prerequisites>

- Have access to data on an ABS container

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_yaml_example.py#L5-L8
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_yaml_example.py#L15
```

### 3. Configure your Datasource

Great Expectations provides two types of <TechnicalTag tag="data_connector" text="Data Connector" /> classes for connecting to ABS: `InferredAssetAzureDataConnector` and `ConfiguredAssetAzureDataConnector`

- An `InferredAssetAzureDataConnector` utilizes regular expressions to infer `data_asset_names` by evaluating filename patterns that exist in your bucket. This `DataConnector`, along with a `RuntimeDataConnector`, is provided as a default when utilizing our Jupyter Notebooks.
- A `ConfiguredAssetAzureDataConnector` requires an explicit listing of each <TechnicalTag tag="data_asset" text="Data Asset" /> you want to connect to. This allows for more granularity and control than its `Inferred` counterpart but also requires a more complex setup.

As the `InferredAssetDataConnectors` have fewer options and are generally simpler to use, we recommend starting with them.

We've detailed example configurations for both options in the next section for your reference.

:::info Authentication

It is also important to note that the ABS `DataConnectors` for Pandas support two (mutually exclusive) methods of authentication. You should be aware of the following options when configuring your own environment:
* `account_url` key in the `azure_options` dictionary
  - This is the default option and what is used throughout this guide.
* `conn_str` key in the `azure_options` dictionary
* In all cases, the `AZURE_CREDENTIAL` environment variable is required.

The `azure_options` dictionary is also responsible for storing any `**kwargs` you wish to pass to the ABS `BlobServiceClient` connection object.

For more details regarding storing credentials for use with Great Expectations see: [How to configure credentials](../../../setup/configuring_data_contexts/how_to_configure_credentials.md)

For more details regarding authentication and access using `Python`, please visit the following:
* [Azure Authentication](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage)
* [Manage storage account access keys](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage)
* [Manage blobs with Python SDK](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python)

:::

Using these example configurations, add in your ABS container and path to a directory that contains some of your data:
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

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_yaml_example.py#L19-L43
```

Run this code to test your configuration.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_yaml_example.py#L60
```
</TabItem>

<TabItem value="python">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_python_example.py#L13-L38
```

Run this code to test your configuration.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_python_example.py#L59
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

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_yaml_example.py#L10-L27
```

Run this code to test your configuration.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_yaml_example.py#L38
```
</TabItem>
<TabItem value="python">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_python_example.py#L10-L27
```

Run this code to test your configuration.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_python_example.py#L37
```
</TabItem>
</Tabs>

</TabItem>

</Tabs>

</TabItem>

</Tabs>

If you specified an ABS path containing CSV files you will see them listed as `Available data_asset_names` in the output of `test_yaml_config()`.

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

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_yaml_example.py#L64
```

</TabItem>

<TabItem value="python">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_python_example.py#L61
```

</TabItem>

</Tabs>

### 5. Test your new Datasource

Verify your new <TechnicalTag tag="datasource" text="Datasource" /> by loading data from it into a <TechnicalTag tag="validator" text="Validator" /> using a <TechnicalTag tag="batch_request" text="Batch Request" />.

Add the name of the data asset to the `data_asset_name` in your `BatchRequest`.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_yaml_example.py#L69-L73
```

Then load data into the `Validator`.
```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_yaml_example.py#L83-L88
```

<Congratulations />

## Additional Notes

If you are working with nonstandard CSVs, read one of these guides:

- [How to work with headerless CSVs in pandas](#TODO)
- [How to work with custom delimited CSVs in pandas](#TODO)
- [How to work with parquet files in pandas](#TODO)

To view the full scripts used in this page, see them on GitHub:

- [inferred_and_runtime_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_yaml_example.py)
- [inferred_and_runtime_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/inferred_and_runtime_python_example.py)
- [configured_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_yaml_example.py)
- [configured_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/azure/pandas/configured_python_example.py)

To review the source code of these `DataConnectors`, also visit GitHub:
- [ConfiguredAssetAzureDataConnector](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/datasource/data_connector/configured_asset_azure_data_connector.py)
- [InferredAssetAzureDataConnector](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/datasource/data_connector/inferred_asset_azure_data_connector.py)
