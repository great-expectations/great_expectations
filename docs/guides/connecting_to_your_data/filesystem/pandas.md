---
title: How to connect to your data on a filesystem using pandas
---
import NextSteps from '../components/next_steps.md'
import Congratulations from '../components/congratulations.md'
import Prerequisites from '../components/prerequisites.jsx'
import WhereToRunCode from '../components/where_to_run_code.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you connect to your data stored on a filesystem using pandas.
This will allow you to validate and explore your data.

<Prerequisites>

- Have access to data on a filesystem

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. `[🍏 CORE SKILL ICON]` Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas_yaml_example.py#L1-L3
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas_yaml_example.py#L6
```

### 3. Configure your Datasource

Using this example configuration:

<Tabs
  groupId="yaml-or-python"
  defaultValue='yaml'
  values={[
  {label: 'YAML', value:'yaml'},
  {label: 'python', value:'python'},
  ]}>
  <TabItem value="yaml">

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas_yaml_example.py#L8-L20
```

</TabItem>
<TabItem value="python">

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas_python_example.py#L6-L20
```

</TabItem>
</Tabs>

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

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas_yaml_example.py#L22
```

</TabItem>
<TabItem value="python">

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas_python_example.py#L22
```

</TabItem>
</Tabs>

### 5. Test your new Datasource

Verify your new Datasource by loading data from it into a `Validator` using a `BatchRequest`.

Add the path to your CSV in the `path` key under `runtime_parameters`.

```python file=../../../../integration/code/connecting_to_your_data/filesystem/pandas_yaml_example.py#L24-L44
```

<Congratulations />

## Additional Notes

If you are working with nonstandard CSVs, read one of these guides:

- [How to work with headerless CSVs in pandas](#TODO)
- [How to work with custom delimited CSVs in pandas](#TODO)
- [How to work with parquet files in pandas](#TODO)

To view the full scripts used in this page, see them on GitHub:

- [pandas_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/knoxpod/integration/code/connecting_to_your_data/filesystem/pandas_yaml_example.py)
- [pandas_python_example.py](https://github.com/great-expectations/great_expectations/blob/knoxpod/integration/code/connecting_to_your_data/filesystem/pandas_python_example.py)

## Next Steps

<NextSteps />
