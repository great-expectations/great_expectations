---
title: How to connect to your data on a S3 using spark
---

import NextSteps from '../../components/next_steps.md'
import Congratulations from '../../components/congratulations.md'
import Prerequisites from '../../components/prerequisites.jsx'
import WhereToRunCode from '../../components/where_to_run_code.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you connect to your data stored on AWS S3 using spark.
This will allow you to validate and explore your data.

<Prerequisites>

- Have access to data on an AWS S3 bucket
- Have a access to a working spark installation

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. `[🍏 CORE SKILL ICON]` Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_yaml_example.py#L1-L4
```

:::note Load your DataContext into memory
Use one of the guides below based on your deployment:

- [How to instantiate a Data Context without a yml file](/docs/guides/setup/configuring-data-contexts/how-to-instantiate-a-data-context-without-a-yml-file)
- [How to instantiate a Data Context on an EMR Spark cluster](/docs/guides/setup/configuring-data-contexts/how-to-instantiate-a-data-context-on-an-emr-spark-cluster)
- [How to instantiate a Data Context on Databricks Spark cluster](/docs/guides/setup/configuring-data-contexts/how-to-instantiate-a-data-context-on-databricks-spark-cluster)
:::

Please proceed only after you have instantiated your `DataContext`.

### 3. Configure your Datasource

Using this example configuration, add in your S3 bucket and path to a directory that contains some of your data:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'python', value:'python'},
  ]}>
  <TabItem value="yaml">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_yaml_example.py#L20-L39
```

Run this code to test your configuration.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_yaml_example.py#L50
```

</TabItem>
<TabItem value="python">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_python_example.py#L21-L40
```

Run this code to test your configuration.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_python_example.py#L51
```

</TabItem>
</Tabs>

If you specified an S3 path containing CSV files you will see them listed as `Available data_asset_names` in the output of `test_yaml_config()`.

Feel free to adjust your configuration and re-run `test_yaml_config()` as needed.

### 4. Save the Datasource configuration to your DataContext

Save the configuration into your `DataContext` by using the `add_datasource()` function.

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'python', value:'python'},
  ]}>
  <TabItem value="yaml">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_yaml_example.py#L52
```

</TabItem>
<TabItem value="python">

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_python_example.py#L53
```

</TabItem>
</Tabs>

### 5. Test your new Datasource

Verify your new Datasource by loading data from it into a `Validator` using a `BatchRequest`.

<Tabs
  defaultValue='runtime_batch_request'
  values={[
  {label: 'Specify an S3 path to single CSV', value:'runtime_batch_request'},
  {label: 'Specify a data_asset_name', value:'batch_request'},
  ]}>
  <TabItem value="runtime_batch_request">

Add the S3 path to your CSV in the `path` key under `runtime_parameters` in your `BatchRequest`.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_yaml_example.py#L55-L61
```
Then load data into the `Validator`.
```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_yaml_example.py#L69-L75
```

  </TabItem>
  <TabItem value="batch_request">

Add the name of the data asset to the `data_asset_name` in your `BatchRequest`.

```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_yaml_example.py#L81-L85
```
Then load data into the `Validator`.
```python file=../../../../../tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_yaml_example.py#L93-L99
```

  </TabItem>
</Tabs>


<Congratulations />

## Additional Notes

If you are working with nonstandard CSVs, read one of these guides:

- [How to work with headerless CSVs in spark](#TODO)
- [How to work with custom delimited CSVs in spark](#TODO)
- [How to work with parquet files in spark](#TODO)

To view the full scripts used in this page, see them on GitHub:

- [spark_s3_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_yaml_example.py)
- [spark_s3_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/cloud/spark_s3_python_example.py)

## Next Steps

<NextSteps />
