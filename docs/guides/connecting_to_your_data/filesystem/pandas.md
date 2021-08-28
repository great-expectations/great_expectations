---
title: How to connect to data on a filesystem using Pandas
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

### 2. Instantiate your project's DataContext

Import these necessary packages and modules.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py#L1-L4
```

Load your DataContext into memory using the `get_context()` method.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py#L6
```

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

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py#L8-L27
```

Run this code to test your configuration.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py#L33
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py#L8-L28
```

Run this code to test your configuration.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py#L36
```

</TabItem>
</Tabs>

If you specified a directory containing CSV files you will see them listed as `Available data_asset_names` in the output of `test_yaml_config()`.

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

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py#L35
```

</TabItem>
<TabItem value="python">

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py#L38
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

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py#L38-L44
```
Then load data into the `Validator`.
```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py#L50-L56
```

  </TabItem>
  <TabItem value="batch_request">

Add the name of the data asset to the `data_asset_name` in your `BatchRequest`.

```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py#L62-L66
```
Then load data into the `Validator`.
```python file=../../../../tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py#L72-L78
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

- [pandas_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py)
- [pandas_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py)

## Next Steps

<NextSteps />
