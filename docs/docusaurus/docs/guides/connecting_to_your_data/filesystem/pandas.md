---
title: How to connect to data on a filesystem using Pandas
---
import NextSteps from '../components/next_steps.md'
import Congratulations from '../components/congratulations.md'
import Prerequisites from '../components/prerequisites.jsx'
import WhereToRunCode from '../components/where_to_run_code.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you connect to your data stored on a filesystem using pandas. This will allow you to <TechnicalTag tag="validation" text="Validate" /> and explore your data.

## Prerequisites

<Prerequisites>

- Access to data on a filesystem

</Prerequisites>

## Steps

### 1. Choose how to run the code in this guide

<WhereToRunCode />

### 2. Instantiate your project's DataContext

Import these necessary packages and modules.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py imports"
```

Load your <TechnicalTag tag="data_context" text="Data Context" /> into memory using the `get_context()` method.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py get_context"
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

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py yaml"
```

Run this code to test your configuration.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py test_yaml_config"
```

</TabItem>
<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py yaml"
```

Run this code to test your configuration.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py test_yaml_config"
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

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py add_datasource"
```

</TabItem>
<TabItem value="python">

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py add_datasource"
```

</TabItem>

</Tabs>

### 5. Test your new Datasource

Verify your new <TechnicalTag tag="datasource" text="Datasource" /> by loading data from it into a <TechnicalTag tag="validator" text="Validator" /> using a <TechnicalTag tag="batch_request" text="Batch Request" />.

<Tabs
  defaultValue='runtime_batch_request'
  values={[
  {label: 'Specify a path to single CSV', value:'runtime_batch_request'},
  {label: 'Specify a data_asset_name', value:'batch_request'},
  ]}>
  <TabItem value="runtime_batch_request">

Add the path to your CSV in the `path` key under `runtime_parameters` in your `BatchRequest`.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py runtime_batch_request"
```

Then load data into the `Validator`.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py runtime_batch_request validator"
```

  </TabItem>
  <TabItem value="batch_request">

Add the name of the <TechnicalTag tag="data_asset" text="Data Asset" /> to the `data_asset_name` in your `BatchRequest`.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py batch_request"
```

Then load data into the `Validator`.

```python name="tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py batch_request validator"
```

</TabItem>

</Tabs>


<Congratulations />

## Additional Notes

To view the full scripts used in this page, see them on GitHub:

- [pandas_yaml_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_yaml_example.py)
- [pandas_python_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/filesystem/pandas_python_example.py)

## Next Steps

<NextSteps />
