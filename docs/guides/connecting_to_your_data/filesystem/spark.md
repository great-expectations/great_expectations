---
title: How to connect to data on a filesystem using Spark
---

import NextSteps from '../components/next_steps.md'
import Congratulations from '../components/congratulations.md'
import Prerequisites from '../components/prerequisites.jsx'
import WhereToRunCode from '../components/where_to_run_code.md'
import SparkDataContextNote from '../components/spark_data_context_note.md'
import SparkAdditionalNotes from '../components/spark_additional_notes.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you connect to your data stored on a filesystem using Spark.
This will allow you to validate and explore your data.

<Prerequisites>

- Have access to a working Spark installation
- Have access to data on a filesystem

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. 💡 Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py#L1-L4
```

<SparkDataContextNote />

Please proceed only after you have instantiated your `DataContext`.

### 3. Configure your Datasource

Using this example configuration, add in your path to a directory that contains some of your data:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
  <TabItem value="yaml">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py#L20-L38
```

Run this code to test your configuration.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py#L44
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py#L21-L39
```

Run this code to test your configuration.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py#L47
```

</TabItem>
</Tabs>

If you specified a path containing CSV files you will see them listed as `Available data_asset_names` in the output of `test_yaml_config()`.

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

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py#L46
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py#L49
```

</TabItem>
</Tabs>

### 5. Test your new Datasource

Verify your new Datasource by loading data from it into a `Validator` using a `BatchRequest`.

<Tabs
  defaultValue='runtime_batch_request'
  values={[
  {label: 'Specify a path to single CSV', value:'runtime_batch_request'},
  {label: 'Specify a data_asset_name', value:'batch_request'},
  ]}>
  <TabItem value="runtime_batch_request">

Add the path to your CSV in the `path` key under `runtime_parameters` in your `BatchRequest`.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py#L49-L55
```
Then load data into the `Validator`.
```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py#L61-L67
```

  </TabItem>
  <TabItem value="batch_request">

Add the name of the data asset to the `data_asset_name` in your `BatchRequest`.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py#L73-L77
```
Then load data into the `Validator`.
```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py#L83-L89
```

  </TabItem>
</Tabs>


<Congratulations />

## Additional Notes

<SparkAdditionalNotes />

To view the full scripts used in this page, see them on GitHub:

- [spark_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_yaml_example.py)
- [spark_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/filesystem/spark_python_example.py)

## Next Steps

<NextSteps />
