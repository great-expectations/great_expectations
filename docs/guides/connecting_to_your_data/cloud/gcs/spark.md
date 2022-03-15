---
title: How to connect to data on GCS using Spark
---

import NextSteps from '../../components/next_steps.md'
import Congratulations from '../../components/congratulations.md'
import Prerequisites from '../../components/prerequisites.jsx'
import WhereToRunCode from '../../components/where_to_run_code.md'
import SparkDataContextNote from '../../components/spark_data_context_note.md'
import SparkAdditionalNotes from '../../components/spark_additional_notes.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you connect to your data stored on Google Cloud Storage (GCS) using Spark.
This will allow you to <TechnicalTag tag="validation" text="Validate" /> and explore your data.

<Prerequisites>

- Have access to data on a GCS bucket
- Have access to a working Spark installation

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py#L5-L11
```

<SparkDataContextNote />

Please proceed only after you have instantiated your `DataContext`.

### 3. Configure your Datasource

Using this example configuration, add in your GCS bucket and path to a directory that contains some of your data:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>

<TabItem value="yaml">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py#L23-L42
```

:::info Authentication

It is also important to note that GCS `DataConnector` for Spark supports the method of authentication that requires running the `gcloud` command line tool in order to obtain the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

For more details regarding storing credentials for use with Great Expectations see: [How to configure credentials](../../../setup/configuring_data_contexts/how_to_configure_credentials.md)

For more details regarding authentication, please visit the following:
* [gcloud CLI Tutorial](https://cloud.google.com/storage/docs/reference/libraries)
* [GCS Python API Docs](https://googleapis.dev/python/storage/latest/index.html)

:::

Run this code to test your configuration.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py#L50
```

</TabItem>

<TabItem value="python">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_python_example.py#L23-L42
```

Run this code to test your configuration.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_python_example.py#L53
```

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

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py#L52
```

</TabItem>

<TabItem value="python">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_python_example.py#L55
```

</TabItem>

</Tabs>

### 5. Test your new Datasource

Verify your new <TechnicalTag tag="datasource" text="Datasource" /> by loading data from it into a <TechnicalTag tag="validator" text="Validator" /> using a <TechnicalTag tag="batch_request" text="Batch Request" />.

<Tabs
  defaultValue='runtime_batch_request'
  values={[
  {label: 'Specify a GCS path to single CSV', value:'runtime_batch_request'},
  {label: 'Specify a data_asset_name', value:'batch_request'},
  ]}>

<TabItem value="runtime_batch_request">

Add the GCS path to your CSV in the `path` key under `runtime_parameters` in your `RuntimeBatchRequest`.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py#L55-L61
```

Then load data into the `Validator`.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py#L69-L75
```

</TabItem>

<TabItem value="batch_request">

Add the name of the <TechnicalTag tag="data_asset" text="Data Asset" /> to the `data_asset_name` in your `BatchRequest`.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py#L81-L86
```

Then load data into the `Validator`.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py#L94-L100
```

</TabItem>

</Tabs>

<Congratulations />

## Additional Notes

<SparkAdditionalNotes />

To view the full scripts used in this page, see them on GitHub:

- [spark_gcs_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_yaml_example.py)
- [spark_gcs_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/gcs/spark/inferred_and_runtime_python_example.py)

## Next Steps

<NextSteps />

