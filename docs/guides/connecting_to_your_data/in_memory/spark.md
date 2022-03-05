---
title: How to connect to in-memory data in a Spark dataframe
---

import NextSteps from '../components/next_steps.md'
import Congratulations from '../components/congratulations.md'
import Prerequisites from '../components/prerequisites.jsx'
import WhereToRunCode from '../components/where_to_run_code.md'
import SparkDataContextNote from '../components/spark_data_context_note.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you connect to your data in an in-memory dataframe using Spark.
This will allow you to validate and explore your data.

<Prerequisites>

- Have access to an in-memory Spark dataframe

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py#L1-L9
```

<SparkDataContextNote />

### 3. Configure your Datasource

Using this example configuration add in the path to a directory that contains some of your data:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'Python', value:'python'},
  ]}>
  <TabItem value="yaml">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py#L32-L42
```

Run this code to test your configuration.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py#L44
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py#L32-L42
```

Run this code to test your configuration.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py#L44
```

</TabItem>
</Tabs>

**Note**: Since the Datasource does not have data passed-in until later, the output will show that no `data_asset_names` are currently available. This is to be expected.

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

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py#L46
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py#L46
```

</TabItem>
</Tabs>

### 5. Test your new Datasource

Verify your new Datasource by loading data from it into a `Validator` using a `BatchRequest`.

Add the variable containing your dataframe (`df` in this example) to the `batch_data` key under `runtime_parameters` in your `BatchRequest`.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py#L49-L55
```

:::note Note this guide uses a toy dataframe that looks like this.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py#L15-L19
```
:::

Then load data into the `Validator`.
```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py#L57-L63
```

<Congratulations />

## Additional Notes

To view the full scripts used in this page, see them on GitHub:

- [spark_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_yaml_example.py)
- [spark_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/in_memory/spark_python_example.py)

## Next Steps

<NextSteps />
